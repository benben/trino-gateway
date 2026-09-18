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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.gateway.ha.clustermonitor.TrinoStatus;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.handler.schema.RoutingTargetResponse;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.router.schema.RoutingSelectorResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pooled eligibility and bounded pre-dispatch reselection, wired end to end: a real
 * {@link TransactionAwarenessService} resolves real requests against a real PostgreSQL lifecycle
 * state. Only the coordinator HTTP probe and the backend registry are test doubles.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
class TestPooledRouting
{
    private static final String POOL = "pool-a";
    private static final String PLAN_HASH = "c".repeat(64);
    private static final String DOMAIN = "dw.example.test";

    private static final String AUTHORIZATION = "Basic " + Base64.getEncoder().encodeToString("tenant:secret".getBytes(UTF_8));

    private final String schema = "pool_routing_test_" + UUID.randomUUID().toString().replace("-", "");
    private JdbcDatabaseContainer<?> container;
    private Jdbi admin;
    private Jdbi database;
    private PoolStore pools;
    private HaGatewayConfiguration configuration;
    private TransactionAwarenessService service;
    private GatewayBackendManager backendManager;
    private RoutingManager routingManager;
    private HttpClient httpClient;
    private RoutingGroupSelector selector;
    private boolean schemaCreated;
    private long epoch;

    @BeforeAll
    void setupDatabase()
            throws IOException
    {
        String url = System.getenv("TX_STORE_TEST_JDBC_URL");
        String username;
        String password;
        if (url == null) {
            container = createPostgreSqlContainer();
            container.start();
            url = container.getJdbcUrl();
            username = container.getUsername();
            password = container.getPassword();
        }
        else {
            username = requireNonNull(System.getenv("TX_STORE_TEST_USERNAME"), "TX_STORE_TEST_USERNAME is required");
            password = System.getenv().getOrDefault("TX_STORE_TEST_PASSWORD", "");
        }
        admin = Jdbi.create(url, username, password);
        admin.useHandle(handle -> handle.execute("CREATE SCHEMA " + schema));
        schemaCreated = true;
        database = Jdbi.create(url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema, username, password);
        for (String version : new String[] {
                "V5__transaction_awareness.sql", "V6__backend_incarnation_history.sql", "V7__query_capabilities.sql",
                "V8__drain_obligation_indexes.sql", "V9__cell_rollout_operations.sql", "V10__pool_member_lifecycle.sql",
                "V11__pool_tenant_principals.sql",
        }) {
            try (var migration = requireNonNull(getClass().getResourceAsStream("/postgresql/" + version))) {
                String sql = new String(migration.readAllBytes(), StandardCharsets.UTF_8);
                database.useHandle(handle -> handle.createScript(sql).execute());
            }
        }
        pools = new PoolStore(database);
    }

    @AfterAll
    void cleanupDatabase()
    {
        try {
            if (schemaCreated) {
                admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
            }
        }
        finally {
            if (container != null) {
                container.close();
            }
        }
    }

    @BeforeEach
    void setupService()
    {
        database.useHandle(handle -> handle.execute(
                """
                TRUNCATE pool_tenant_principal, pool_publication_receipt, pool_publication, pool_tenant_admission, pool_failure_receipt,
                  pool_member_certificate, pool_operation, pool, transaction_rollout, transaction_route,
                  transaction_admission, transaction_query_capability, transaction_query, transaction_binding, transaction_backend
                """));
        epoch = 3;
        configuration = new HaGatewayConfiguration();
        DataStoreConfiguration dataStore = new DataStoreConfiguration();
        dataStore.setJdbcUrl("jdbc:postgresql://localhost/pooled_routing_test");
        configuration.setDataStore(dataStore);
        configuration.getTransactionAwareness().setEnabled(true);
        configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-pooled-routing-tests");
        configuration.getTransactionAwareness().setAdminToken("synthetic-admin-token-for-pooled-routing-tests");
        configuration.getTransactionAwareness().getPool().setEnabled(true);
        configuration.getTransactionAwareness().getPool().setMaxPreDispatchCandidates(3);
        backendManager = mock(GatewayBackendManager.class);
        routingManager = mock(RoutingManager.class);
        httpClient = mock(HttpClient.class);
        selector = mock(RoutingGroupSelector.class);
        when(selector.findRoutingDestination(any())).thenReturn(new RoutingSelectorResponse(POOL));
        when(routingManager.getBackEndHealth(anyString())).thenReturn(Optional.of(TrinoStatus.HEALTHY));
        service = new TransactionAwarenessService(configuration, database, backendManager, httpClient, selector);
        service.setRoutingManager(routingManager);
        pools.configurePool(
                POOL,
                guard("op-configure", "configure"),
                new PoolStore.PoolSpec("POOLED", 1, 3, 1, 1, "r-1", false),
                false);
    }

    @AfterEach
    void shutdownService()
    {
        service.shutdown();
    }

    @Test
    void newWorkOnlyReachesCertifiedActiveMembersAndSpreadsAcrossThem()
    {
        member("i-1", true);
        member("i-2", true);
        member("i-3", false);
        probeSucceeds();

        Set<String> selected = new HashSet<>();
        for (int attempt = 0; attempt < 30; attempt++) {
            selected.add(resolve().routingDestination().clusterHost());
        }
        assertThat(selected).containsExactlyInAnyOrder("http://i-1.example.test", "http://i-2.example.test");
    }

    @Test
    void aPreparingMemberIsNeverSelectedEvenWhenItIsTheOnlyBackend()
    {
        member("i-1", false);
        probeSucceeds();
        assertThatThrownBy(this::resolve).isInstanceOfSatisfying(WebApplicationException.class, failure -> {
            assertThat(failure.getResponse().getStatus()).isEqualTo(503);
            assertThat(failure.getResponse().getHeaderString("X-Trino-Gateway-Error")).isEqualTo("ROUTING_STATE_NOT_ACTIVE");
        });
    }

    @Test
    void aDrainingMemberLeavesTheCandidateListWithoutAnAvoidableError()
    {
        member("i-1", true);
        member("i-2", true);
        probeSucceeds();
        drain("i-1");

        for (int attempt = 0; attempt < 10; attempt++) {
            assertThat(resolve().routingDestination().clusterHost()).isEqualTo("http://i-2.example.test");
        }
    }

    @Test
    void anInactiveLegacyRegistrationStillRoutesOnceTheMemberIsCertified()
    {
        // A pooled member's legacy registration is created inactive and is never activated through the
        // legacy route: doing so would make it routable with no certified admission. Eligibility comes
        // only from the pooled protocol, so an inactive registration must still route after admit.
        member("i-1", true);
        registerBackendsInactive();
        probeSucceeds();
        for (int attempt = 0; attempt < 5; attempt++) {
            assertThat(resolve().routingDestination().clusterHost()).isEqualTo("http://i-1.example.test");
        }
    }

    @Test
    void aMemberWithNoLegacyRegistrationAtAllIsNotSelected()
    {
        member("i-1", true);
        member("i-2", true);
        probeSucceeds();
        // i-1 has no Gateway backend registration, so the Gateway has no endpoint record for it.
        when(backendManager.getAllBackends()).thenReturn(List.of(backend("i-2")));
        when(backendManager.getActiveBackends(POOL)).thenReturn(List.of(backend("i-2")));
        for (int attempt = 0; attempt < 10; attempt++) {
            assertThat(resolve().routingDestination().clusterHost()).isEqualTo("http://i-2.example.test");
        }
    }

    @Test
    void cachedHealthOnlyOrdersCandidatesAndCannotExcludeAnEligibleMember()
    {
        member("i-1", true);
        probeSucceeds();
        when(routingManager.getBackEndHealth(anyString())).thenReturn(Optional.of(TrinoStatus.UNHEALTHY));
        assertThat(resolve().routingDestination().clusterHost()).isEqualTo("http://i-1.example.test");
        when(routingManager.getBackEndHealth(anyString())).thenReturn(Optional.empty());
        assertThat(resolve().routingDestination().clusterHost()).isEqualTo("http://i-1.example.test");
    }

    @Test
    void reselectionIsBoundedAndOnlyHappensBeforeDispatch()
    {
        member("i-1", true);
        member("i-2", true);
        member("i-3", true);
        member("i-4", true);
        probeSucceeds();
        // Every member stops admitting between the advisory snapshot and the admission attempt.
        database.useHandle(handle -> handle.createUpdate("UPDATE transaction_backend SET state = 'DRAINING' WHERE pool_id = :pool")
                .bind("pool", POOL).execute());

        assertThatThrownBy(this::resolve).isInstanceOfSatisfying(WebApplicationException.class, failure -> {
            assertThat(failure.getResponse().getStatus()).isEqualTo(503);
            assertThat(failure.getResponse().getHeaderString("X-Trino-Gateway-Error")).isEqualTo("ROUTING_STATE_NOT_ACTIVE");
        });
        // Bounded: at most maxPreDispatchCandidates attempts, and no admission row survives a rejection.
        long admissions = database.withHandle(handle -> handle.createQuery("SELECT count(*) FROM transaction_admission").mapTo(Long.class).one());
        assertThat(admissions).isZero();
    }

    @Test
    void aSuccessfulAdmissionRecordsExactlyOneDurableAdmissionForTheSelectedIncarnation()
    {
        member("i-1", true);
        probeSucceeds();
        RoutingTargetResponse response = resolve();
        assertThat(response.routingDestination().clusterHost()).isEqualTo("http://i-1.example.test");
        List<String> incarnations = database.withHandle(handle -> handle.createQuery(
                """
                SELECT b.instance_id FROM transaction_admission a JOIN transaction_backend b USING (incarnation)
                """).mapTo(String.class).list());
        assertThat(incarnations).containsExactly("i-1");
    }

    @Test
    void aRestartedCoordinatorProcessCannotServeTheOldIncarnation()
    {
        member("i-1", true);
        probeReturns("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node-i-1\",\"coordinatorId\":\"zzzzz\"}");
        assertThatThrownBy(this::resolve).isInstanceOfSatisfying(
                WebApplicationException.class,
                failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(409));
    }

    @Test
    void legacyRoutingIsUnchangedForANonPooledRoutingGroup()
    {
        // No pool row in POOLED mode for this group, so the ordinary single-route path is used.
        database.useHandle(handle -> handle.createUpdate("UPDATE pool SET api_mode = 'LEGACY' WHERE pool_id = :pool").bind("pool", POOL).execute());
        ProxyBackendConfiguration backend = backend("legacy");
        when(backendManager.getBackendByName("backend-legacy")).thenReturn(Optional.of(backend));
        when(backendManager.getAllBackends()).thenReturn(List.of(backend));
        when(backendManager.getActiveBackends(POOL)).thenReturn(List.of(backend));
        probeReturns("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node-legacy\",\"coordinatorId\":\"abcde\"}");

        RoutingTargetResponse response = service.resolve(
                request(),
                () -> {
                    throw new AssertionError("Unexpected ordinary routing");
                },
                _ -> new RoutingTargetResponse(
                        new io.trino.gateway.ha.handler.schema.RoutingDestination(
                                POOL,
                                backend.getProxyTo(),
                                java.net.URI.create(backend.getProxyTo() + "/v1/statement"),
                                backend.getExternalUrl()),
                        request()));
        assertThat(response.routingDestination().clusterHost()).isEqualTo("http://legacy.example.test");
        String legacyState = database.withHandle(handle -> handle.createQuery(
                        "SELECT state FROM transaction_backend WHERE current_name = 'backend-legacy'")
                .mapTo(String.class).one());
        assertThat(legacyState).isEqualTo("ACTIVE");
    }

    // --------------------------------------------------------------------------------------------
    // Deny-only tenant admission restriction
    // --------------------------------------------------------------------------------------------

    @Test
    void anAdmittedBareMappingCannotAdmitAnUnmappedQualifiedPrincipal()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();
        // One tenant publishes a bare login and is admitted.
        publishPrincipals("org-bare", "warehouse-bare");
        admitTenant("org-bare");

        // A request to another warehouse's host presents the same user name. The coordinator qualifies
        // before it authenticates and checks only "warehouse-b.warehouse-bare", so the admitted bare
        // mapping says nothing about this request. The unmapped qualified principal must fail closed.
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs("warehouse-bare", "warehouse-b." + DOMAIN, Map.of()));
        assertThat(admissionCount()).isZero();

        // Publishing it for a tenant whose publication has not completed still refuses.
        publishPrincipals("org-b", "warehouse-b", "warehouse-b.warehouse-bare");
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs("warehouse-bare", "warehouse-b." + DOMAIN, Map.of()));

        // Only admitting that tenant lets the qualified principal through.
        admitTenant("org-b");
        assertThat(resolveAs("warehouse-bare", "warehouse-b." + DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
    }

    @Test
    void anUnusedBareMappingCannotBlockAnAdmittedQualifiedPrincipal()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();
        // The qualified principal is admitted; a same-named bare principal belongs to a tenant that is
        // still pending. The coordinator would never authenticate the bare name for this request, so
        // the pending tenant must not block it either.
        publishPrincipals("org-a", "warehouse-a", "warehouse-a.alice");
        admitTenant("org-a");
        publishPrincipals("org-pending", "alice");

        assertThat(resolveAs("alice", "warehouse-a." + DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
    }

    @Test
    void anUnpublishedPrincipalIsRefusedAndAPublishedAdmittedOneRoutes()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();

        // Nothing published for this principal: unknown fails closed, with no durable admission left.
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs("alice", "warehouse-one." + DOMAIN, Map.of()));
        assertThat(admissionCount()).isZero();

        // Published, but its tenant's publication has not completed yet.
        publishPrincipals("org-7", "warehouse-one", "warehouse-one.alice", "warehouse-one.bob");
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs("alice", "warehouse-one." + DOMAIN, Map.of()));

        admitTenant("org-7");
        // The qualified principal is published, so the request is dispatched.
        assertThat(resolveAs("alice", "warehouse-one." + DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
        // Every published user of the same warehouse is covered by that one tenant record.
        assertThat(resolveAs("bob", "warehouse-one." + DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
        // A user of that warehouse that was never published stays refused.
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs("carol", "warehouse-one." + DOMAIN, Map.of()));
    }

    @Test
    void aBareRootLoginIsAdmittedWhenItIsPublished()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();
        // A tenant's root login is the bare warehouse name with no separator, and its tenant identifier
        // is unrelated to it. Only the published mapping can connect the two.
        publishPrincipals("org-42", "warehouse-one", "warehouse-one.alice");
        admitTenant("org-42");

        // Sent to the apex host, so the coordinator would authenticate the name as presented.
        assertThat(resolveAs("warehouse-one", DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
        // And to the tenant host, where the coordinator qualifies it; both forms are candidates.
        assertThat(resolveAs("alice", "warehouse-one." + DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
    }

    @Test
    void aForgedUserHeaderCannotAdmitAPendingPrincipal()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();
        publishPrincipals("org-admitted", "warehouse-two", "warehouse-two.alice");
        admitTenant("org-admitted");
        publishPrincipals("org-pending", "warehouse-three", "warehouse-three.mallory");

        // The credential belongs to a tenant whose publication has not completed. Naming an admitted
        // principal in a user header is not evidence of authentication and changes nothing.
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs(
                "mallory",
                "warehouse-three." + DOMAIN,
                Map.of("X-Trino-User", List.of("warehouse-two.alice"), "X-Trino-Original-User", List.of("warehouse-two.alice"))));
        assertThat(admissionCount()).isZero();
    }

    @Test
    void aClientSuppliedForwardedHostNeverReachesTheCoordinator()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();
        publishPrincipals("org-7", "warehouse-one", "warehouse-one.alice");
        admitTenant("org-7");

        RoutingTargetResponse response = resolveAs(
                "alice",
                "warehouse-one." + DOMAIN,
                Map.of("X-Forwarded-Host", List.of("warehouse-nine." + DOMAIN), "Forwarded", List.of("host=warehouse-nine." + DOMAIN)));
        assertThat(response.modifiedRequest().getHeader("X-Forwarded-Host")).isNull();
        assertThat(response.modifiedRequest().getHeader("Forwarded")).isNull();
        assertThat(Collections.list(response.modifiedRequest().getHeaderNames()))
                .noneMatch(name -> name.equalsIgnoreCase("X-Forwarded-Host") || name.equalsIgnoreCase("Forwarded"));
    }

    @Test
    void anExcludedHostLabelDoesNotQualifyAPrincipalIntoATenant()
    {
        enableTenantGate();
        member("i-1", true);
        probeSucceeds();
        // "internal" is an operational host, not a warehouse; the coordinator excludes it too, so the
        // qualified form must not become a candidate.
        publishPrincipals("org-7", "internal", "internal.alice");
        admitTenant("org-7");
        expectStatus(403, "TENANT_NOT_ADMITTED", () -> resolveAs("alice", "internal." + DOMAIN, Map.of()));
    }

    @Test
    void theRestrictionIsAbsentUntilThePoolEnablesIt()
    {
        member("i-1", true);
        probeSucceeds();
        // Default pool configuration has no tenant gate, so routing is unchanged.
        assertThat(resolveAs("alice", "tenant-a." + DOMAIN, Map.of()).routingDestination().clusterHost())
                .isEqualTo("http://i-1.example.test");
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /**
     * Resolves one request and releases its in-flight lease, as the proxy does after dispatch.
     */
    private RoutingTargetResponse resolve()
    {
        RoutingTargetResponse response = resolveWithoutRelease();
        service.completeRequest(response.modifiedRequest());
        return response;
    }

    /**
     * Rebuilds the service with the deny-only principal restriction enabled, and turns it on for the pool.
     */
    private void enableTenantGate()
    {
        service.shutdown();
        configuration.getTransactionAwareness().getPool()
                .setTenantIdentitySource(io.trino.gateway.ha.config.PoolLifecycleConfiguration.TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL);
        configuration.getTransactionAwareness().getPool().setHostQualificationDomains(List.of(DOMAIN));
        configuration.getTransactionAwareness().getPool().setExcludedHostLabels(List.of("internal"));
        configuration.getTransactionAwareness().validate(configuration.getDataStore());
        service = new TransactionAwarenessService(configuration, database, backendManager, httpClient, selector);
        service.setRoutingManager(routingManager);
        pools.configurePool(
                POOL,
                guard("op-gate", "configure"),
                new PoolStore.PoolSpec("POOLED", 1, 3, 1, 1, "r-1", true),
                true);
    }

    /**
     * Publishes a tenant's authoritative principal set, as the controller does.
     */
    private void publishPrincipals(String tenant, String warehouse, String... users)
    {
        List<String> principals = new java.util.ArrayList<>();
        principals.add(warehouse);
        principals.addAll(List.of(users));
        pools.publishTenantPrincipals(POOL, tenant, guard("op-principals-" + tenant, "principals"), "rev-1", List.copyOf(principals));
    }

    private void admitTenant(String tenant)
    {
        database.useHandle(handle -> handle.createUpdate(
                        """
                        INSERT INTO pool_tenant_admission (pool_id, tenant, state, admitted_revision, publication_id)
                        VALUES (:pool, :tenant, 'ADMITTED', 'r-1', 'pub-1')
                        ON CONFLICT (pool_id, tenant) DO UPDATE SET state = 'ADMITTED', admitted_revision = 'r-1', publication_id = 'pub-1'
                        """)
                .bind("pool", POOL).bind("tenant", tenant).execute());
    }

    private long admissionCount()
    {
        return database.withHandle(handle -> handle.createQuery("SELECT count(*) FROM transaction_admission").mapTo(Long.class).one());
    }

    private RoutingTargetResponse resolveAs(String user, String host, Map<String, List<String>> extraHeaders)
    {
        Map<String, List<String>> headers = new java.util.LinkedHashMap<>(extraHeaders);
        headers.put("Authorization", List.of("Basic " + Base64.getEncoder().encodeToString((user + ":secret").getBytes(UTF_8))));
        headers.put("Host", List.of(host));
        RoutingTargetResponse response = service.resolve(
                request(headers),
                () -> {
                    throw new AssertionError("Unexpected ordinary routing");
                },
                _ -> {
                    throw new AssertionError("Unexpected legacy backend selection");
                });
        service.completeRequest(response.modifiedRequest());
        return response;
    }

    private static void expectStatus(int status, String code, Runnable action)
    {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(WebApplicationException.class, failure -> {
            assertThat(failure.getResponse().getStatus()).isEqualTo(status);
            assertThat(failure.getResponse().getHeaderString("X-Trino-Gateway-Error")).isEqualTo(code);
        });
    }

    private RoutingTargetResponse resolveWithoutRelease()
    {
        return service.resolve(
                request(),
                () -> {
                    throw new AssertionError("Unexpected ordinary routing");
                },
                _ -> {
                    throw new AssertionError("Unexpected legacy backend selection");
                });
    }

    private PoolStore.Guard guard(String operationId, String stepId)
    {
        return new PoolStore.Guard(operationId, stepId, epoch, PLAN_HASH);
    }

    private ProxyBackendConfiguration backend(String instanceId)
    {
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("backend-" + instanceId);
        backend.setProxyTo("http://" + instanceId + ".example.test");
        backend.setExternalUrl("http://" + instanceId + ".example.test");
        backend.setRoutingGroup(POOL);
        backend.setActive(true);
        return backend;
    }

    /**
     * Registers a pooled member and optionally certifies it into service.
     */
    private void member(String instanceId, boolean active)
    {
        PoolStore.Member member = pools.registerMember(POOL, guard("op-" + instanceId, "register"),
                new PoolStore.MemberRegistration(
                        instanceId,
                        "backend-" + instanceId,
                        "http://" + instanceId + ".example.test",
                        "http://" + instanceId + ".example.test",
                        "pod-" + instanceId,
                        "boot-" + instanceId,
                        "r-1",
                        null,
                        "node-" + instanceId,
                        "abcde"));
        if (active) {
            pools.admitMember(
                    POOL,
                    instanceId,
                    guard("op-" + instanceId, "admit"),
                    member.generation(),
                    new PoolStore.ValidationReceipt(
                            "a".repeat(64),
                            "r-1",
                            "a-1",
                            "pod-" + instanceId,
                            "boot-" + instanceId,
                            "node-" + instanceId,
                            "abcde",
                            4,
                            List.of("image", "workers", "catalog-revision", "auth-revision")),
                    "node-" + instanceId,
                    "abcde",
                    300);
        }
        registerBackends();
    }

    private void drain(String instanceId)
    {
        PoolStore.Member member = pools.member(POOL, instanceId).orElseThrow();
        pools.drainMember(POOL, instanceId, guard("op-drain-" + instanceId, "drain"), member.generation());
    }

    /**
     * Registers every member's backend with the legacy active flag false, as the real client does.
     */
    private void registerBackendsInactive()
    {
        List<ProxyBackendConfiguration> backends = pools.members(POOL).stream()
                .map(member -> {
                    ProxyBackendConfiguration backend = backend(member.instanceId());
                    backend.setActive(false);
                    return backend;
                })
                .toList();
        when(backendManager.getAllBackends()).thenReturn(backends);
        when(backendManager.getActiveBackends(POOL)).thenReturn(List.of());
        backends.forEach(backend -> when(backendManager.getBackendByName(backend.getName())).thenReturn(Optional.of(backend)));
    }

    private void registerBackends()
    {
        List<ProxyBackendConfiguration> backends = pools.members(POOL).stream().map(member -> backend(member.instanceId())).toList();
        when(backendManager.getActiveBackends(POOL)).thenReturn(backends);
        when(backendManager.getAllBackends()).thenReturn(backends);
        backends.forEach(backend -> when(backendManager.getBackendByName(backend.getName())).thenReturn(Optional.of(backend)));
    }

    private void probeSucceeds()
    {
        StringResponse response = mock(StringResponse.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getBody()).thenAnswer(_ -> "{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node-i-1\",\"coordinatorId\":\"abcde\"}");
        when(httpClient.execute(any(), any())).thenAnswer(call -> {
            io.airlift.http.client.Request probe = call.getArgument(0);
            String host = probe.getUri().getHost();
            StringResponse identity = mock(StringResponse.class);
            when(identity.getStatusCode()).thenReturn(200);
            when(identity.getBody()).thenReturn("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node-"
                    + host.substring(0, host.indexOf('.')) + "\",\"coordinatorId\":\"abcde\"}");
            return identity;
        });
    }

    private void probeReturns(String body)
    {
        StringResponse response = mock(StringResponse.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getBody()).thenReturn(body);
        when(httpClient.execute(any(), any())).thenReturn(response);
    }

    private static HttpServletRequest request()
    {
        return request(Map.of("Authorization", List.of(AUTHORIZATION)));
    }

    private static HttpServletRequest request(Map<String, List<String>> suppliedHeaders)
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.putAll(suppliedHeaders);
        Map<String, Object> attributes = new HashMap<>();
        when(request.getMethod()).thenReturn("POST");
        when(request.getRequestURI()).thenReturn("/v1/statement");
        when(request.getHeaders(anyString())).thenAnswer(call -> Collections.enumeration(headers.getOrDefault(call.getArgument(0), List.of())));
        when(request.getHeader(anyString())).thenAnswer(call -> headers.getOrDefault(call.getArgument(0), List.of()).stream().findFirst().orElse(null));
        when(request.getHeaderNames()).thenAnswer(_ -> Collections.enumeration(headers.keySet()));
        when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
        doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1))).when(request).setAttribute(anyString(), any());
        return request;
    }
}
