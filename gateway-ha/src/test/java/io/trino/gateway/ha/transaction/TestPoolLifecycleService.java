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
package io.trino.gateway.ha.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpClient;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.PoolLifecycleConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Request handling for the pooled lifecycle protocol: feature gating, reuse of the existing
 * administration token, canonical replay hashing and endpoint binding.
 */
class TestPoolLifecycleService
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ADMIN_TOKEN = "synthetic-admin-token-for-pool-lifecycle-tests";

    private HaGatewayConfiguration configuration;
    private GatewayBackendManager backendManager;
    private HttpClient httpClient;
    private Jdbi jdbi;

    @BeforeEach
    void setup()
    {
        configuration = new HaGatewayConfiguration();
        DataStoreConfiguration dataStore = new DataStoreConfiguration();
        dataStore.setJdbcUrl("jdbc:postgresql://localhost/unused_unit_test_database");
        configuration.setDataStore(dataStore);
        configuration.getTransactionAwareness().setEnabled(true);
        configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-pool-lifecycle-tests");
        configuration.getTransactionAwareness().setAdminToken(ADMIN_TOKEN);
        backendManager = mock(GatewayBackendManager.class);
        httpClient = mock(HttpClient.class);
        jdbi = mock(Jdbi.class);
    }

    @Test
    void theProtocolIsAbsentUntilItIsExplicitlyEnabled()
    {
        PoolLifecycleService service = service(false);
        assertThat(service.isEnabled()).isFalse();
        expect(404, "POOL_DISABLED", () -> service.requireAdmin(request(Map.of("Authorization", List.of("Bearer " + ADMIN_TOKEN)))));
    }

    @Test
    void administrationReusesTheExistingTransactionAdminToken()
    {
        PoolLifecycleService service = service(true);
        assertThat(service.isEnabled()).isTrue();
        service.requireAdmin(request(Map.of("Authorization", List.of("Bearer " + ADMIN_TOKEN))));
        service.requireAdmin(request(Map.of("X-Gateway-Transaction-Admin-Token", List.of(ADMIN_TOKEN))));
        expect(403, null, () -> service.requireAdmin(request(Map.of("Authorization", List.of("Bearer wrong-token")))));
        expect(403, null, () -> service.requireAdmin(request(Map.of())));
    }

    @Test
    void theProtocolIsAlsoAbsentWhenTransactionAwarenessItselfIsDisabled()
    {
        configuration.getTransactionAwareness().setEnabled(false);
        configuration.getTransactionAwareness().setIdentityKey(null);
        configuration.getTransactionAwareness().setAdminToken(null);
        PoolLifecycleConfiguration pool = new PoolLifecycleConfiguration();
        pool.setEnabled(false);
        configuration.getTransactionAwareness().setPool(pool);
        assertThat(service(false).isEnabled()).isFalse();
    }

    @Test
    void enablingThePoolProtocolWithoutTransactionAwarenessIsRefusedAtStartup()
    {
        PoolLifecycleConfiguration pool = new PoolLifecycleConfiguration();
        pool.setEnabled(true);
        assertThatThrownBy(() -> pool.validate(false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires transaction awareness");
    }

    @Test
    void onlyASupportedVerifiedTenantIdentitySourceIsAccepted()
    {
        PoolLifecycleConfiguration pool = new PoolLifecycleConfiguration();
        pool.setEnabled(true);
        assertThat(pool.getTenantIdentitySource()).isEqualTo("NONE");
        assertThat(pool.hasVerifiedTenantIdentity()).isFalse();
        pool.setTenantIdentitySource("X-Trino-User");
        assertThatThrownBy(() -> pool.validate(true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantIdentitySource");
    }

    @Test
    void aMemberCannotBeRegisteredAgainstAnUnregisteredOrForeignBackend()
    {
        PoolLifecycleService service = service(true);
        when(backendManager.getBackendByName("backend-i-1")).thenReturn(Optional.empty());
        expect(404, "POOL_NOT_FOUND", () -> service.registerMember("pool-a", body(
                """
                {"operationId":"op-1","stepId":"register","controllerEpoch":1,"instanceId":"i-1",
                 "backendName":"backend-i-1","podUid":"pod","bootId":"boot","configRevision":"r-1"}
                """)));

        ProxyBackendConfiguration foreign = new ProxyBackendConfiguration();
        foreign.setName("backend-i-1");
        foreign.setProxyTo("http://i-1.example.test");
        foreign.setRoutingGroup("other-group");
        when(backendManager.getBackendByName("backend-i-1")).thenReturn(Optional.of(foreign));
        expect(409, "POOL_IDENTITY_CONFLICT", () -> service.registerMember("pool-a", body(
                """
                {"operationId":"op-1","stepId":"register","controllerEpoch":1,"instanceId":"i-1",
                 "backendName":"backend-i-1","podUid":"pod","bootId":"boot","configRevision":"r-1"}
                """)));
    }

    @Test
    void aSuppliedEndpointCannotDivergeFromTheRegisteredBackend()
    {
        PoolLifecycleService service = service(true);
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("backend-i-1");
        backend.setProxyTo("http://i-1.example.test");
        backend.setRoutingGroup("pool-a");
        when(backendManager.getBackendByName("backend-i-1")).thenReturn(Optional.of(backend));
        expect(409, "POOL_IDENTITY_CONFLICT", () -> service.registerMember("pool-a", body(
                """
                {"operationId":"op-1","stepId":"register","controllerEpoch":1,"instanceId":"i-1",
                 "backendName":"backend-i-1","url":"http://elsewhere.example.test",
                 "podUid":"pod","bootId":"boot","configRevision":"r-1"}
                """)));
    }

    @Test
    void anUnreachableCoordinatorCannotBeRegisteredOnAnOperatorAssertionAlone()
    {
        PoolLifecycleService service = service(true);
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("backend-i-1");
        backend.setProxyTo("http://i-1.example.test");
        backend.setRoutingGroup("pool-a");
        when(backendManager.getBackendByName("backend-i-1")).thenReturn(Optional.of(backend));
        when(httpClient.execute(any(), any())).thenThrow(new IllegalStateException("synthetic probe failure"));
        // No step was recorded, so the replay lookup finds nothing and the probe still decides.
        when(jdbi.inTransaction(any())).thenReturn(Optional.empty());
        expect(503, null, () -> service.registerMember("pool-a", body(
                """
                {"operationId":"op-1","stepId":"register","controllerEpoch":1,"instanceId":"i-1",
                 "backendName":"backend-i-1","podUid":"pod","bootId":"boot","configRevision":"r-1"}
                """)));
    }

    @Test
    void theCanonicalPayloadHashIgnoresKeyOrderAndWhitespaceButNotValues()
    {
        String first = PoolLifecycleService.canonicalHash(body(
                """
                {"operationId":"op-1","stepId":"admit","controllerEpoch":1,"receipt":{"configRevision":"r-1","readyWorkers":4}}
                """));
        String reordered = PoolLifecycleService.canonicalHash(body(
                """
                {"stepId":"admit","receipt":{"readyWorkers":4,"configRevision":"r-1"},"controllerEpoch":1,"operationId":"op-1"}
                """));
        String changed = PoolLifecycleService.canonicalHash(body(
                """
                {"operationId":"op-1","stepId":"admit","controllerEpoch":1,"receipt":{"configRevision":"r-2","readyWorkers":4}}
                """));
        assertThat(first).matches("[0-9a-f]{64}");
        assertThat(reordered).isEqualTo(first);
        assertThat(changed).isNotEqualTo(first);
    }

    @Test
    void amissingOrMalformedBodyIsRejectedBeforeAnyDatabaseWork()
    {
        PoolLifecycleService service = service(true);
        expect(400, "POOL_VALIDATION", () -> service.drainMember("pool-a", "i-1", JSON.createArrayNode()));
        expect(400, "POOL_VALIDATION", () -> service.drainMember("pool-a", "i-1", body(
                """
                {"stepId":"drain","controllerEpoch":1,"expectedGeneration":1}
                """)));
        expect(400, "POOL_VALIDATION", () -> service.drainMember("pool-a", "i-1", body(
                """
                {"operationId":"op-1","stepId":"drain","controllerEpoch":1,"expectedGeneration":-1}
                """)));
    }

    private PoolLifecycleService service(boolean poolEnabled)
    {
        configuration.getTransactionAwareness().getPool().setEnabled(poolEnabled);
        RoutingGroupSelector selector = mock(RoutingGroupSelector.class);
        TransactionAwarenessService transactions = new TransactionAwarenessService(configuration, jdbi, backendManager, httpClient, selector);
        return new PoolLifecycleService(configuration, jdbi, transactions, backendManager);
    }

    private static JsonNode body(String json)
    {
        try {
            return JSON.readTree(json);
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void expect(int status, String code, Runnable action)
    {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(WebApplicationException.class, failure -> {
            assertThat(failure.getResponse().getStatus()).isEqualTo(status);
            if (code != null) {
                assertThat(failure.getResponse().getHeaderString("X-Trino-Gateway-Error")).isEqualTo(code);
            }
        });
    }

    private static HttpServletRequest request(Map<String, List<String>> suppliedHeaders)
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.putAll(suppliedHeaders);
        when(request.getHeaders(anyString())).thenAnswer(call -> Collections.enumeration(headers.getOrDefault(call.getArgument(0), List.of())));
        when(request.getHeader(anyString())).thenAnswer(call -> headers.getOrDefault(call.getArgument(0), List.of()).stream().findFirst().orElse(null));
        return request;
    }
}
