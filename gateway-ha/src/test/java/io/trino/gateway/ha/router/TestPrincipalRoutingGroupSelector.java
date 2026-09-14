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
package io.trino.gateway.ha.router;

import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.PrincipalRoutingConfiguration;
import io.trino.gateway.ha.module.HaGatewayProviderModule;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import okio.Buffer;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPrincipalRoutingGroupSelector
{
    @Test
    void routesWithoutHeaderAndIgnoresSpoofedSessionAndGroup()
    {
        PrincipalRoutingGroupSelector selector = selector(new AtomicLong());
        selector.installSnapshot("{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"cell-a\"}]}");
        HttpServletRequest request = request("warehouse_a:password");
        when(request.getHeader("X-Trino-User")).thenReturn("warehouse_b");
        when(request.getHeader("X-Trino-Routing-Group")).thenReturn("cell-b");
        assertThat(selector.findRoutingDestination(request).routingGroup()).isEqualTo("cell-a");
        assertThat(selector.findRoutingDestination(request).externalHeaders()).isEmpty();
    }

    @Test
    void replacesSnapshotAndFailsClosedWhenExpired()
    {
        AtomicLong clock = new AtomicLong();
        PrincipalRoutingGroupSelector selector = selector(clock);
        assertStatus(503, () -> selector.findRoutingDestination(request("warehouse_a:password")));
        selector.installSnapshot("{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"legacy\"}]}");
        assertThat(selector.findRoutingDestination(request("warehouse_a:password")).routingGroup()).isEqualTo("legacy");
        selector.installSnapshot("{\"routes\":[]}");
        assertStatus(403, () -> selector.findRoutingDestination(request("warehouse_a:password")));
        clock.set(15_000_000_001L);
        assertStatus(503, () -> selector.findRoutingDestination(request("warehouse_a:password")));
    }

    @Test
    void invalidSnapshotsNeverRefreshLastGoodAge()
    {
        AtomicLong clock = new AtomicLong();
        PrincipalRoutingGroupSelector selector = selector(clock);
        selector.installSnapshot("{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"cell-a\"}]}");
        clock.set(14_000_000_000L);
        for (String invalid : List.of(
                "{}",
                "{\"routes\":null}",
                "{\"routes\":[{\"principal\":\"\",\"routingGroup\":\"cell-a\"}]}",
                "{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"\"}]}",
                "{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"cell-a\"},{\"principal\":\"warehouse_a\",\"routingGroup\":\"cell-b\"}]}",
                "{\"routes\":[],\"routes\":[]}",
                "{\"routes\":[]} {}")) {
            assertThatThrownBy(() -> selector.installSnapshot(invalid)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(selector.findRoutingDestination(request("warehouse_a:password")).routingGroup()).isEqualTo("cell-a");
        clock.set(15_000_000_001L);
        assertStatus(503, () -> selector.findRoutingDestination(request("warehouse_a:password")));
    }

    @Test
    void rejectsMissingMalformedAndAmbiguousCredentials()
    {
        PrincipalRoutingGroupSelector selector = selector(new AtomicLong());
        selector.installSnapshot("{\"routes\":[]}");
        for (String value : List.of("Bearer token", "Basic !!!", "Basic " + Base64.getEncoder().encodeToString(":password".getBytes(UTF_8)))) {
            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getHeaders("Authorization")).thenReturn(Collections.enumeration(List.of(value)));
            assertStatus(401, () -> selector.findRoutingDestination(request));
        }
        assertStatus(401, () -> selector.findRoutingDestination(mock(HttpServletRequest.class)));
        HttpServletRequest duplicate = mock(HttpServletRequest.class);
        when(duplicate.getHeaders("Authorization")).thenReturn(Collections.enumeration(List.of("Basic YTpi", "Basic YTpi")));
        assertStatus(400, () -> selector.findRoutingDestination(duplicate));
    }

    @Test
    void refreshSendsOnlyServiceCredentialsAndNeverFollowsRedirects()
            throws Exception
    {
        try (MockWebServer source = new MockWebServer(); MockWebServer redirect = new MockWebServer(); JettyHttpClient client = new JettyHttpClient()) {
            source.start();
            redirect.start();
            PrincipalRoutingConfiguration config = new PrincipalRoutingConfiguration();
            config.setUrl(source.url("/snapshot").toString());
            config.setToken("synthetic-service-token");
            AtomicLong clock = new AtomicLong();
            PrincipalRoutingGroupSelector selector = new PrincipalRoutingGroupSelector(config, clock::get);
            source.enqueue(new MockResponse().setBody("{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"cell-a\"}]}"));
            selector.refresh(client);
            var sent = source.takeRequest();
            assertThat(sent.getHeader("X-Duckgres-Internal-Secret")).isEqualTo("synthetic-service-token");
            assertThat(sent.getHeader("Authorization")).isNull();
            assertThat(sent.getHeader("X-Trino-User")).isNull();
            for (int i = 0; i < 1000; i++) {
                assertThat(selector.findRoutingDestination(request("warehouse_a:password")).routingGroup()).isEqualTo("cell-a");
            }
            assertThat(source.getRequestCount()).isEqualTo(1);
            source.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", redirect.url("/stolen-token")));
            clock.set(14_000_000_000L);
            selector.refresh(client);
            assertThat(redirect.getRequestCount()).isZero();
            clock.set(15_000_000_001L);
            assertStatus(503, () -> selector.findRoutingDestination(request("warehouse_a:password")));
        }
    }

    @Test
    void failedInvalidOversizedAndTimedOutResponsesDoNotRenewSnapshot()
            throws Exception
    {
        try (MockWebServer source = new MockWebServer(); JettyHttpClient client = new JettyHttpClient()) {
            source.start();
            PrincipalRoutingConfiguration config = new PrincipalRoutingConfiguration();
            config.setUrl(source.url("/snapshot").toString());
            config.setToken("synthetic-service-token");
            config.setRequestTimeoutMillis(100);
            for (MockResponse response : List.of(
                    new MockResponse().setResponseCode(503),
                    new MockResponse().setBody("{\"routes\":[],\"routes\":[]}"),
                    new MockResponse().setBody(new Buffer().write(new byte[] {(byte) 0xC3, 0x28})),
                    new MockResponse().setBody(" ".repeat(8 * 1024 * 1024 + 1)),
                    new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))) {
                AtomicLong clock = new AtomicLong();
                PrincipalRoutingGroupSelector selector = new PrincipalRoutingGroupSelector(config, clock::get);
                selector.installSnapshot("{\"routes\":[{\"principal\":\"warehouse_a\",\"routingGroup\":\"cell-a\"}]}");
                source.enqueue(response);
                clock.set(14_000_000_000L);
                selector.refresh(client);
                clock.set(15_000_000_001L);
                assertStatus(503, () -> selector.findRoutingDestination(request("warehouse_a:password")));
            }
        }
    }

    @Test
    void configuredSnapshotFailuresNeverSelectTheHeaderProvider()
    {
        HaGatewayConfiguration configuration = new HaGatewayConfiguration();
        configuration.getRouting().getPrincipalRouting().setEnabled(true);
        assertThatThrownBy(configuration::validate).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HaGatewayProviderModule.getRoutingGroupSelector(null, configuration)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOversizedAndNoncanonicalPrincipalSnapshots()
    {
        PrincipalRoutingConfiguration config = new PrincipalRoutingConfiguration();
        config.setMaxEntries(1);
        PrincipalRoutingGroupSelector selector = new PrincipalRoutingGroupSelector(config, System::nanoTime);
        for (String principal : List.of(" ", "a:b", "a\\n", "a".repeat(1025))) {
            assertThatThrownBy(() -> selector.installSnapshot("{\"routes\":[{\"principal\":\"" + principal + "\",\"routingGroup\":\"cell-a\"}]}"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> selector.installSnapshot("{\"routes\":[{\"principal\":\"a\",\"routingGroup\":\"cell-a\"},{\"principal\":\"b\",\"routingGroup\":\"cell-b\"}]}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> selector.installSnapshot(" ".repeat(8 * 1024 * 1024 + 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PrincipalRoutingGroupSelector selector(AtomicLong clock)
    {
        return new PrincipalRoutingGroupSelector(new PrincipalRoutingConfiguration(), clock::get);
    }

    private static HttpServletRequest request(String credential)
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        String authorization = "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(UTF_8));
        when(request.getHeaders("Authorization")).thenAnswer(_ -> Collections.enumeration(List.of(authorization)));
        return request;
    }

    private static void assertStatus(int status, Runnable action)
    {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(
                WebApplicationException.class,
                error -> assertThat(error.getResponse().getStatus()).isEqualTo(status));
    }
}
