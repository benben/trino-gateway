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
package io.trino.gateway.ha.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.sun.net.httpserver.HttpServer;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import io.airlift.tracing.TracingModule;
import io.trino.gateway.baseapp.BaseApp;
import io.trino.gateway.ha.config.ClusterStatsConfiguration;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.module.HaGatewayProviderModule;
import io.trino.gateway.ha.persistence.FlywayMigration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.trino.gateway.ha.config.ClusterStatsMonitorType.NOOP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The pooled lifecycle protocol over real HTTP, against a real PostgreSQL database and a real
 * coordinator identity endpoint.
 * <p>
 * This is the wire contract the operator's client is written against: exact paths, status codes,
 * error headers, JSON field names and replay behaviour. It exercises the whole production module
 * graph, not the service classes in isolation.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
class TestPoolResourceHttp
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ADMIN_TOKEN = "synthetic-admin-token-for-pool-http-tests";
    private static final String POOL = "pool-http";
    private static final String BASE = "/gateway/v1/pools/" + POOL;

    private final String schema = "pool_http_test_" + UUID.randomUUID().toString().replace("-", "");
    private Jdbi admin;
    private HttpServer coordinator;
    private HttpServer secondCoordinator;
    private Injector injector;
    private URI gateway;
    private HttpClient client;
    private boolean schemaCreated;

    @BeforeAll
    void startGateway()
            throws Exception
    {
        String url = requireNonNull(
                System.getenv("TX_STORE_TEST_JDBC_URL"),
                "TX_STORE_TEST_JDBC_URL is required: this test needs a real PostgreSQL database");
        String username = requireNonNull(System.getenv("TX_STORE_TEST_USERNAME"), "TX_STORE_TEST_USERNAME is required");
        String password = System.getenv().getOrDefault("TX_STORE_TEST_PASSWORD", "");
        admin = Jdbi.create(url, username, password);
        admin.useHandle(handle -> handle.execute("CREATE SCHEMA " + schema));
        schemaCreated = true;

        coordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/v1/info", exchange -> {
            byte[] body = "{\"coordinator\":true,\"starting\":false,\"nodeId\":\"pool-http-node\",\"coordinatorId\":\"abcde\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        coordinator.start();
        secondCoordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        secondCoordinator.createContext("/v1/info", exchange -> {
            byte[] body = "{\"coordinator\":true,\"starting\":false,\"nodeId\":\"pool-http-node-2\",\"coordinatorId\":\"fghij\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        secondCoordinator.start();

        HaGatewayConfiguration configuration = new HaGatewayConfiguration();
        DataStoreConfiguration database = new DataStoreConfiguration();
        database.setJdbcUrl(url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema);
        database.setUser(username);
        database.setPassword(password);
        database.setDriver("org.postgresql.Driver");
        configuration.setDataStore(database);
        ClusterStatsConfiguration monitor = new ClusterStatsConfiguration();
        monitor.setMonitorType(NOOP);
        configuration.setClusterStatsConfiguration(monitor);
        configuration.getTransactionAwareness().setEnabled(true);
        configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-pool-http-tests");
        configuration.getTransactionAwareness().setAdminToken(ADMIN_TOKEN);
        configuration.getTransactionAwareness().getPool().setEnabled(true);
        configuration.validate();
        FlywayMigration.migrate(database);

        injector = new Bootstrap(
                new NodeModule(),
                new HttpServerModule(),
                new JmxModule(),
                new MBeanModule(),
                new JsonModule(),
                new JaxrsModule(),
                new TracingModule("trino-gateway", "pool-http-test"),
                new HaGatewayProviderModule(configuration),
                new BaseApp(configuration))
                .setRequiredConfigurationProperties(Map.of(
                        "node.environment", "test",
                        "node.id", UUID.randomUUID().toString(),
                        "node.internal-address", "127.0.0.1",
                        "node.bind-ip", "127.0.0.1",
                        "http-server.http.port", "0",
                        "http-server.log.enabled", "false"))
                .initialize();
        gateway = injector.getInstance(HttpServerInfo.class).getHttpUri();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        // The pooled member's legacy registration exists and is inactive, exactly as the operator
        // creates it: activating it through the legacy route would make it routable uncertified.
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("pool-http-i-1");
        backend.setProxyTo("http://127.0.0.1:" + coordinator.getAddress().getPort());
        backend.setExternalUrl("https://pool-http-i-1.example.test");
        backend.setRoutingGroup(POOL);
        backend.setActive(false);
        injector.getInstance(GatewayBackendManager.class).addBackend(backend);
    }

    @AfterAll
    void stopGateway()
    {
        if (client != null) {
            client.close();
        }
        if (injector != null) {
            injector.getInstance(LifeCycleManager.class).stop();
        }
        if (coordinator != null) {
            coordinator.stop(0);
        }
        if (secondCoordinator != null) {
            secondCoordinator.stop(0);
        }
        if (schemaCreated) {
            admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
        }
    }

    @Test
    void theProtocolRefusesRequestsWithoutTheAdministrationToken()
            throws Exception
    {
        HttpResponse<String> missing = send("GET", BASE, null, null);
        assertThat(missing.statusCode()).isEqualTo(403);
        HttpResponse<String> wrong = send("GET", BASE, null, "Bearer not-the-token");
        assertThat(wrong.statusCode()).isEqualTo(403);
        assertThat(wrong.body()).doesNotContain(ADMIN_TOKEN);
    }

    @Test
    void anUnknownPoolIsReportedWithItsErrorCode()
            throws Exception
    {
        HttpResponse<String> response = get("/gateway/v1/pools/no-such-pool");
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_NOT_FOUND");
    }

    @Test
    void theWholeMemberLifecycleWorksOverHttpWithTheDocumentedFieldNames()
            throws Exception
    {
        JsonNode pool = body(put(BASE,
                """
                {"operationId":"op-http-1","stepId":"configure","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "apiMode":"POOLED","minServing":1,"desiredMembers":3,"maxSurge":1,"maxRepair":1,
                 "desiredRevision":"r-1","tenantAdmissionEnabled":false}
                """));
        assertThat(pool.path("protocolVersion").asInt()).isEqualTo(1);
        assertThat(pool.path("apiMode").asText()).isEqualTo("POOLED");
        assertThat(pool.path("controllerEpoch").asLong()).isEqualTo(1);
        assertThat(pool.path("counts").isObject()).isTrue();
        assertThat(pool.path("blocked").isArray()).isTrue();

        JsonNode registered = body(post(BASE + "/members",
                """
                {"operationId":"op-http-2","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-1","backendName":"pool-http-i-1","podUid":"pod-1","bootId":"boot-1","configRevision":"r-1"}
                """));
        assertThat(registered.path("phase").asText()).isEqualTo("PREPARING");
        assertThat(registered.path("eligible").asBoolean()).isFalse();
        // The Gateway observed the coordinator identity itself rather than taking it on assertion.
        assertThat(registered.path("nodeId").asText()).isEqualTo("pool-http-node");
        assertThat(registered.path("coordinatorId").asText()).isEqualTo("abcde");
        assertThat(registered.path("replayed").asBoolean()).isFalse();
        // Field names the operator's client decodes.
        assertThat(fieldNames(registered)).contains(
                "protocolVersion",
                "poolId",
                "instanceId",
                "incarnation",
                "backendName",
                "url",
                "externalUrl",
                "phase",
                "generation",
                "controllerEpoch",
                "podUid",
                "bootId",
                "nodeId",
                "coordinatorId",
                "configRevision",
                "certifiedRevision",
                "authRevision",
                "repair",
                "repairFor",
                "retirementKind",
                "pendingRequests",
                "openTransactions",
                "activeQueries",
                "readyToSeal",
                "drained",
                "eligible",
                "membershipGeneration",
                "replayed");

        // An identical replay resolves to the recorded result instead of a second effect.
        JsonNode replayed = body(post(BASE + "/members",
                """
                {"operationId":"op-http-2","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-1","backendName":"pool-http-i-1","podUid":"pod-1","bootId":"boot-1","configRevision":"r-1"}
                """));
        assertThat(replayed.path("replayed").asBoolean()).isTrue();
        assertThat(replayed.path("incarnation").asText()).isEqualTo(registered.path("incarnation").asText());

        // A changed intent under the same step identity is a conflict, never an overwrite.
        HttpResponse<String> conflict = post(BASE + "/members",
                """
                {"operationId":"op-http-2","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-2","backendName":"pool-http-i-1","podUid":"pod-2","bootId":"boot-2","configRevision":"r-1"}
                """);
        assertThat(conflict.statusCode()).isEqualTo(409);
        assertThat(conflict.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_INTENT_CHANGED");

        JsonNode admitted = body(post(BASE + "/members/i-1/admit",
                """
                {"operationId":"op-http-3","stepId":"admit","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "expectedGeneration":0,
                 "receipt":{"certificateHash":"%s","configRevision":"r-1","authRevision":"a-1",
                            "podUid":"pod-1","bootId":"boot-1","nodeId":"pool-http-node","coordinatorId":"abcde",
                            "readyWorkers":4,
                            "checks":["image","workers","catalog-revision","auth-revision","operational-connection"]}}
                """.formatted("a".repeat(64))));
        assertThat(admitted.path("phase").asText()).isEqualTo("ACTIVE");
        assertThat(admitted.path("eligible").asBoolean()).isTrue();
        assertThat(admitted.path("certifiedRevision").asText()).isEqualTo("r-1");

        JsonNode obligations = body(get(BASE + "/members/i-1/obligations"));
        assertThat(fieldNames(obligations)).contains("pendingRequests", "openTransactions", "activeQueries", "readyToSeal", "drained");
        assertThat(obligations.path("phase").asText()).isEqualTo("ACTIVE");

        JsonNode members = body(get(BASE + "/members"));
        assertThat(members.isArray()).isTrue();
        assertThat(members).anySatisfy(entry -> assertThat(entry.path("instanceId").asText()).isEqualTo("i-1"));

        // A planned drain of the last serving member is refused by the floor.
        HttpResponse<String> floor = post(BASE + "/members/i-1/drain",
                """
                {"operationId":"op-http-4","stepId":"drain","controllerEpoch":1,"ownerIdentity":"controller-a","expectedGeneration":1}
                """);
        assertThat(floor.statusCode()).isEqualTo(409);
        assertThat(floor.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_SERVING_FLOOR");

        JsonNode history = body(get(BASE + "/operations/op-http-2"));
        assertThat(history.path("operationId").asText()).isEqualTo("op-http-2");
        assertThat(history.path("steps").isArray()).isTrue();
        assertThat(history.path("steps").get(0).path("stepId").asText()).isEqualTo("register");
        assertThat(history.path("steps").get(0).path("outcome").asText()).isEqualTo("OK");
        assertThat(history.path("steps").get(0).path("result").path("instanceId").asText()).isEqualTo("i-1");
    }

    @Test
    void aDestructiveOverrideIsAcceptedWithTheClientsEmptyTerminationObject()
            throws Exception
    {
        put(BASE,
                """
                {"operationId":"op-lost-0","stepId":"configure","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "apiMode":"POOLED","minServing":1,"desiredMembers":3,"maxSurge":2,"maxRepair":1,"desiredRevision":"r-1"}
                """);
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("pool-http-i-lost");
        backend.setProxyTo("http://127.0.0.1:" + secondCoordinator.getAddress().getPort());
        backend.setRoutingGroup(POOL);
        backend.setActive(false);
        injector.getInstance(GatewayBackendManager.class).addBackend(backend);

        JsonNode member = body(post(BASE + "/members",
                """
                {"operationId":"op-lost-1","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-lost","backendName":"pool-http-i-lost","podUid":"pod-lost","bootId":"boot-lost","configRevision":"r-1"}
                """));
        long generation = member.path("generation").asLong();
        JsonNode suspected = body(post(BASE + "/members/i-lost/suspect",
                """
                {"operationId":"op-lost-2","stepId":"suspect","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "expectedGeneration":%d,"reason":"probe-timeout"}
                """.formatted(generation)));
        assertThat(suspected.path("phase").asText()).isEqualTo("SUSPECT");

        // A Go client marshals a value-typed termination proof as an empty object even for an
        // override, so the empty proof must not shadow the destructive-authorization path.
        HttpResponse<String> withoutAuthorization = post(BASE + "/members/i-lost/lost",
                """
                {"operationId":"op-lost-3","stepId":"lost","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "expectedGeneration":%d,"evidence":"DESTRUCTIVE_OVERRIDE",
                 "termination":{"podUid":"","bootId":"","nodeId":"","coordinatorId":"","source":""}}
                """.formatted(suspected.path("generation").asLong()));
        assertThat(withoutAuthorization.statusCode()).isEqualTo(409);
        assertThat(withoutAuthorization.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_EVIDENCE_REQUIRED");

        JsonNode lost = body(post(BASE + "/members/i-lost/lost",
                """
                {"operationId":"op-lost-4","stepId":"lost","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "expectedGeneration":%d,"evidence":"DESTRUCTIVE_OVERRIDE","destructiveAuthorization":true,
                 "reason":"operator authorized removal of a partitioned coordinator",
                 "termination":{"podUid":"","bootId":"","nodeId":"","coordinatorId":"","source":""}}
                """.formatted(suspected.path("generation").asLong())));
        assertThat(lost.path("phase").asText()).isEqualTo("LOST");
        assertThat(lost.path("drained").asBoolean()).isFalse();

        JsonNode receipt = body(get(BASE + "/members/i-lost/failure-receipt"));
        assertThat(receipt.path("evidence").asText()).isEqualTo("DESTRUCTIVE_OVERRIDE");
        assertThat(fieldNames(receipt)).contains("outstandingAdmissions", "outstandingTransactions", "outstandingQueries", "recordedAt");
    }

    @Test
    void aLostResponseIsResolvedByResendingTheIdenticalBodyAfterTheCoordinatorIsGone()
            throws Exception
    {
        put(BASE,
                """
                {"operationId":"op-replay-0","stepId":"configure","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "apiMode":"POOLED","minServing":1,"desiredMembers":4,"maxSurge":2,"maxRepair":1,"desiredRevision":"r-1"}
                """);
        HttpServer transient1 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        transient1.createContext("/v1/info", exchange -> {
            byte[] body = "{\"coordinator\":true,\"starting\":false,\"nodeId\":\"replay-node\",\"coordinatorId\":\"klmno\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        transient1.start();
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("pool-http-i-replay");
        backend.setProxyTo("http://127.0.0.1:" + transient1.getAddress().getPort());
        backend.setRoutingGroup(POOL);
        backend.setActive(false);
        injector.getInstance(GatewayBackendManager.class).addBackend(backend);

        String register =
                """
                {"operationId":"op-replay-1","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-replay","backendName":"pool-http-i-replay","podUid":"pod-replay","bootId":"boot-replay",
                 "configRevision":"r-1"}
                """;
        JsonNode first = body(post(BASE + "/members", register));
        assertThat(first.path("replayed").asBoolean()).isFalse();

        // The response was lost and the coordinator has since gone away. Resending the identical body
        // must return the recorded outcome rather than fail on a probe that can no longer succeed.
        transient1.stop(0);
        JsonNode replay = body(post(BASE + "/members", register));
        assertThat(replay.path("replayed").asBoolean()).isTrue();
        assertThat(replay.path("incarnation").asText()).isEqualTo(first.path("incarnation").asText());
        assertThat(replay.path("nodeId").asText()).isEqualTo("replay-node");

        // A first attempt at a step that never committed still reports the probe failure truthfully.
        HttpResponse<String> unreachable = post(BASE + "/members/i-replay/admit",
                """
                {"operationId":"op-replay-2","stepId":"admit","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "expectedGeneration":0,
                 "receipt":{"certificateHash":"%s","configRevision":"r-1","authRevision":"a-1",
                            "podUid":"pod-replay","bootId":"boot-replay","nodeId":"replay-node","coordinatorId":"klmno",
                            "readyWorkers":2,"checks":["image","workers","catalog-revision","auth-revision"]}}
                """.formatted("b".repeat(64)));
        assertThat(unreachable.statusCode()).isEqualTo(503);
        assertThat(unreachable.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_PROBE_UNAVAILABLE");
        assertThat(body(get(BASE + "/members/i-replay")).path("phase").asText()).isEqualTo("PREPARING");
    }

    @Test
    void aMemberRegisteredAgainstAnUnknownBackendIsRefused()
            throws Exception
    {
        HttpResponse<String> response = post(BASE + "/members",
                """
                {"operationId":"op-missing-1","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-missing","backendName":"not-registered","podUid":"pod","bootId":"boot","configRevision":"r-1"}
                """);
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_NOT_FOUND");
    }

    @Test
    void anEscapedInstanceIdentityRoundTripsThroughThePath()
            throws Exception
    {
        HttpResponse<String> response = get(BASE + "/members/i-does%3Anot%3Aexist");
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.headers().firstValue("X-Trino-Gateway-Error")).contains("POOL_NOT_FOUND");
    }

    private static List<String> fieldNames(JsonNode node)
    {
        List<String> names = new java.util.ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return List.copyOf(names);
    }

    private HttpResponse<String> get(String path)
            throws Exception
    {
        return send("GET", path, null, "Bearer " + ADMIN_TOKEN);
    }

    private HttpResponse<String> put(String path, String body)
            throws Exception
    {
        return send("PUT", path, body, "Bearer " + ADMIN_TOKEN);
    }

    private HttpResponse<String> post(String path, String body)
            throws Exception
    {
        return send("POST", path, body, "Bearer " + ADMIN_TOKEN);
    }

    private HttpResponse<String> send(String method, String path, String body, String authorization)
            throws Exception
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(gateway.resolve(path))
                .timeout(Duration.ofSeconds(20))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, UTF_8));
        if (body != null) {
            request.header("Content-Type", "application/json");
        }
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode body(HttpResponse<String> response)
            throws IOException
    {
        assertThat(response.statusCode())
                .withFailMessage("unexpected status %s with body %s", response.statusCode(), response.body())
                .isEqualTo(200);
        return JSON.readTree(response.body());
    }
}
