/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.gateway.ha.handler;

import io.airlift.units.Duration;
import io.trino.gateway.ha.config.GatewayCookieConfiguration;
import io.trino.gateway.ha.config.GatewayCookieConfigurationPropertiesProvider;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayCookie;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.router.schema.RoutingSelectorResponse;
import io.trino.gateway.ha.util.QueryRequestMock;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestAuthoritativeRoutingTargetHandler
{
    @BeforeEach
    void initializeCookies()
    {
        GatewayCookieConfigurationPropertiesProvider.getInstance().initialize(new GatewayCookieConfiguration());
    }

    private final RoutingGroupSelector selector = new RoutingGroupSelector()
    {
        @Override
        public boolean isAuthoritative()
        {
            return true;
        }

        @Override
        public RoutingSelectorResponse findRoutingDestination(HttpServletRequest request)
        {
            return new RoutingSelectorResponse("cell-a");
        }
    };

    @Test
    void neverFallsBackToAnotherGroupsBackend()
            throws Exception
    {
        RoutingManager manager = mock(RoutingManager.class);
        when(manager.provideBackendConfiguration(eq("cell-a"), any())).thenReturn(backend("default-group", "http://other.invalid"));
        RoutingTargetHandler handler = new RoutingTargetHandler(manager, selector, new HaGatewayConfiguration());
        HttpServletRequest request = new QueryRequestMock().query("SELECT 1").getHttpServletRequest();
        assertThatThrownBy(() -> handler.resolveRouting(request)).isInstanceOfSatisfying(
                WebApplicationException.class,
                error -> assertThat(error.getResponse().getStatus()).isEqualTo(503));
    }

    @Test
    void ignoresForgedBackendCookiesForFreshStatements()
            throws Exception
    {
        GatewayCookieConfiguration cookieConfig = new GatewayCookieConfiguration();
        cookieConfig.setCookieSigningSecret("fixture-cookie-signing-key");
        cookieConfig.setEnabled(true);
        GatewayCookieConfigurationPropertiesProvider.getInstance().initialize(cookieConfig);
        try {
            RoutingManager manager = mock(RoutingManager.class);
            when(manager.provideBackendConfiguration(eq("cell-a"), any())).thenReturn(backend("cell-a", "http://assigned.invalid"));
            RoutingTargetHandler handler = new RoutingTargetHandler(manager, selector, new HaGatewayConfiguration());
            HttpServletRequest request = new QueryRequestMock().query("SELECT 1").getHttpServletRequest();
            when(request.getRequestURI()).thenReturn("/v1/statement");
            GatewayCookie cookie = new GatewayCookie("test", "", "http://other.invalid", List.of("/v1/statement"), List.of(), new Duration(5, MINUTES), 1);
            when(request.getCookies()).thenReturn(new Cookie[] {cookie.toCookie()});
            assertThat(handler.resolveRouting(request).routingDestination().clusterHost()).isEqualTo("http://assigned.invalid");
        }
        finally {
            cookieConfig.setEnabled(false);
            GatewayCookieConfigurationPropertiesProvider.getInstance().initialize(cookieConfig);
        }
    }

    @Test
    void unknownQueryNeverBecomesANewAdmission()
    {
        RoutingManager manager = mock(RoutingManager.class);
        RoutingTargetHandler handler = new RoutingTargetHandler(manager, selector, new HaGatewayConfiguration());
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/v1/statement/executing/20260901_120000_00001_abcde/token/1");
        assertThatThrownBy(() -> handler.resolveRouting(request)).isInstanceOfSatisfying(
                WebApplicationException.class,
                error -> assertThat(error.getResponse().getStatus()).isEqualTo(404));
    }

    @Test
    void missingHealthyBackendHasRetryableUnavailableResponse()
            throws Exception
    {
        RoutingManager manager = mock(RoutingManager.class);
        when(manager.provideBackendConfiguration(eq("cell-a"), any())).thenThrow(new IllegalStateException("No active backends"));
        RoutingTargetHandler handler = new RoutingTargetHandler(manager, selector, new HaGatewayConfiguration());
        HttpServletRequest request = new QueryRequestMock().query("SELECT 1").getHttpServletRequest();
        assertThatThrownBy(() -> handler.resolveRouting(request)).isInstanceOfSatisfying(
                WebApplicationException.class,
                error -> assertThat(error.getResponse().getStatus()).isEqualTo(503));
    }

    private static ProxyBackendConfiguration backend(String group, String url)
    {
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setRoutingGroup(group);
        backend.setProxyTo(url);
        backend.setExternalUrl(url);
        return backend;
    }
}
