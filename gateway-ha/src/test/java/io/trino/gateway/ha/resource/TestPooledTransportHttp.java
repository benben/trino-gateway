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

import com.google.inject.Injector;
import com.sun.net.httpserver.HttpExchange;
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
import io.trino.gateway.ha.config.PoolLifecycleConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.module.HaGatewayProviderModule;
import io.trino.gateway.ha.persistence.FlywayMigration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.gateway.ha.config.ClusterStatsMonitorType.NOOP;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Pooled tenant traffic over the approved transport: HTTPS terminates at the Gateway and the hop to a
 * pooled coordinator is internal plain HTTP with trusted forwarded metadata.
 * <p>
 * The whole production module graph runs, tenant statements go through the real proxy, and a coordinator
 * double records exactly what arrives. It asserts the properties the transport decision depends on:
 * the Gateway's own forwarded protocol is what the coordinator sees, a client cannot inject forwarded
 * metadata, credentials are neither verified nor weakened by the Gateway, and query and transaction
 * continuations stay pinned to their member.
 * <p>
 * The Gateway under test listens on plain HTTP, so its own forwarded protocol here is {@code http};
 * in the deployed shape TLS terminates at the Gateway and the same code path sends {@code https}.
 * What is verified is that the value the coordinator sees is the Gateway's, never the client's.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
class TestPooledTransportHttp
{
    private static final String ADMIN_TOKEN = "synthetic-admin-token-for-pooled-transport-tests";
    private static final String POOL = "pool-transport";
    private static final String BASE = "/gateway/v1/pools/" + POOL;
    private static final String DOMAIN = "dw.example.test";
    private static final String TENANT = "org-7";
    private static final String WAREHOUSE_HOST = "warehouse-one." + DOMAIN;
    private final java.util.concurrent.atomic.AtomicInteger submissions = new java.util.concurrent.atomic.AtomicInteger();
    private static final String TRANSACTION = "00000000-0000-0000-0000-000000000042";

    private final String schema = "pool_transport_test_" + UUID.randomUUID().toString().replace("-", "");
    private final List<Exchange> exchanges = new CopyOnWriteArrayList<>();
    private Jdbi admin;
    private Jdbi database;
    private HttpServer coordinator;
    private Injector injector;
    private URI gateway;
    private boolean schemaCreated;

    private record Exchange(String method, String path, Map<String, List<String>> headers) {}

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
        String schemaUrl = url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema;
        database = Jdbi.create(schemaUrl, username, password);

        coordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/v1/info", exchange -> {
            record(exchange);
            respond(exchange,
                    HttpURLConnection.HTTP_OK,
                    "{\"coordinator\":true,\"starting\":false,\"nodeId\":\"transport-node\",\"coordinatorId\":\"abcde\"}",
                    Map.of());
        });
        coordinator.createContext("/v1/statement", exchange -> {
            record(exchange);
            // The coordinator authenticates for itself. The Gateway forwards the credential verbatim
            // and never decides authentication on its behalf.
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization == null || !authorization.equals(basic("alice", "correct-password"))) {
                // A real coordinator challenges, and an HTTP client requires the challenge header on a
                // 401, so the double has to send it too.
                respond(exchange,
                        HttpURLConnection.HTTP_UNAUTHORIZED,
                        "Access Denied",
                        Map.of("WWW-Authenticate", "Basic realm=\"Trino\""));
                return;
            }
            if (exchange.getRequestURI().getPath().equals("/v1/statement")) {
                // A distinct identity per submission, as a coordinator issues, so the Gateway records
                // one query per dispatch instead of colliding on one identity.
                String queryId = "20260918_120000_%05d_abcde".formatted(submissions.incrementAndGet());
                // A coordinator announces a started transaction only for a statement that opened one.
                // Claiming it again for a statement inside an existing transaction would be a protocol
                // violation, and the Gateway rejects it as one.
                Map<String, String> lifecycle = exchange.getRequestHeaders().containsKey("X-Trino-Transaction-Id")
                        ? Map.of()
                        : Map.of("X-Trino-Started-Transaction-Id", TRANSACTION);
                respond(exchange, HttpURLConnection.HTTP_OK, results(queryId, true), lifecycle);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String executing = path.substring("/v1/statement/executing/".length());
            respond(exchange, HttpURLConnection.HTTP_OK, results(executing.substring(0, executing.indexOf('/')), false), Map.of());
        });
        // An explicit pool, so a keep-alive connection from the proxy cannot stall the single
        // dispatcher thread the default executor would use.
        coordinator.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
        coordinator.start();

        HaGatewayConfiguration configuration = new HaGatewayConfiguration();
        DataStoreConfiguration dataStore = new DataStoreConfiguration();
        dataStore.setJdbcUrl(schemaUrl);
        dataStore.setUser(username);
        dataStore.setPassword(password);
        dataStore.setDriver("org.postgresql.Driver");
        configuration.setDataStore(dataStore);
        ClusterStatsConfiguration monitor = new ClusterStatsConfiguration();
        monitor.setMonitorType(NOOP);
        configuration.setClusterStatsConfiguration(monitor);
        configuration.getTransactionAwareness().setEnabled(true);
        configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-pooled-transport-tests");
        configuration.getTransactionAwareness().setAdminToken(ADMIN_TOKEN);
        configuration.getTransactionAwareness().getPool().setEnabled(true);
        configuration.getTransactionAwareness().getPool()
                .setTenantIdentitySource(PoolLifecycleConfiguration.TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL);
        configuration.getTransactionAwareness().getPool().setHostQualificationDomains(List.of(DOMAIN));
        // Pooled members are reached over internal HTTP, so their probe asserts the original protocol.
        // The setting is off by default and never applies to a legacy backend probe.
        configuration.getTransactionAwareness().getPool().setForwardedProtoHttps(true);
        configuration.validate();
        FlywayMigration.migrate(dataStore);

        injector = new Bootstrap(
                new NodeModule(),
                new HttpServerModule(),
                new JmxModule(),
                new MBeanModule(),
                new JsonModule(),
                new JaxrsModule(),
                new TracingModule("trino-gateway", "pooled-transport-test"),
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

        // Internal plain-HTTP endpoint, registered inactive: eligibility comes only from certified
        // admission, never from the legacy activation route.
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName("pool-transport-i-1");
        backend.setProxyTo("http://127.0.0.1:" + coordinator.getAddress().getPort());
        backend.setExternalUrl("https://tenant-a." + DOMAIN);
        backend.setRoutingGroup(POOL);
        backend.setActive(false);
        injector.getInstance(GatewayBackendManager.class).addBackend(backend);

        admin(BASE, "PUT",
                """
                {"operationId":"op-t-1","stepId":"configure","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "apiMode":"POOLED","minServing":1,"desiredMembers":1,"maxSurge":1,"maxRepair":1,
                 "desiredRevision":"r-1","tenantAdmissionEnabled":true}
                """);
        admin(BASE + "/members", "POST",
                """
                {"operationId":"op-t-2","stepId":"register","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "instanceId":"i-1","backendName":"pool-transport-i-1","podUid":"pod-1","bootId":"boot-1",
                 "configRevision":"r-1"}
                """);
        admin(BASE + "/members/i-1/admit", "POST",
                """
                {"operationId":"op-t-3","stepId":"admit","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "expectedGeneration":0,
                 "receipt":{"certificateHash":"%s","configRevision":"r-1","authRevision":"a-1",
                            "podUid":"pod-1","bootId":"boot-1","nodeId":"transport-node","coordinatorId":"abcde",
                            "readyWorkers":2,"checks":["image","workers","catalog-revision","auth-revision"]}}
                """.formatted("a".repeat(64)));
        // The controller publishes which principals belong to the tenant, over the same HTTP API. The
        // Gateway cannot derive them: the root login carries no separator and the tenant identifier is
        // not a prefix of any of them.
        admin(BASE + "/tenants/" + TENANT + "/principals", "PUT",
                """
                {"operationId":"op-t-4","stepId":"principals","controllerEpoch":1,"ownerIdentity":"controller-a",
                 "revision":"rev-1","principals":["warehouse-one","warehouse-one.alice"]}
                """);
        // Admission itself is what the publication barrier commits; this test is about transport, so
        // the admitted state is set directly rather than running a full publication.
        database.useHandle(handle -> handle.createUpdate(
                        """
                        INSERT INTO pool_tenant_admission (pool_id, tenant, state, admitted_revision, publication_id)
                        VALUES (:pool, :tenant, 'ADMITTED', 'r-1', 'pub-1')
                        ON CONFLICT (pool_id, tenant) DO UPDATE SET state = 'ADMITTED',
                          admitted_revision = 'r-1', publication_id = 'pub-1'
                        """)
                .bind("pool", POOL).bind("tenant", TENANT).execute());
    }

    @AfterAll
    void stopGateway()
    {
        if (injector != null) {
            injector.getInstance(LifeCycleManager.class).stop();
        }
        if (coordinator != null) {
            coordinator.stop(0);
        }
        if (schemaCreated) {
            admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
        }
    }

    @BeforeEach
    void resetExchanges()
    {
        exchanges.clear();
        // The double answers with one fixed query identity, and the Gateway records each dispatched
        // query in its history, so the recorded history is reset between tests as well.
        database.useHandle(handle -> handle.execute(
                "TRUNCATE transaction_admission, transaction_query_capability, transaction_query, transaction_binding, query_history CASCADE"));
        database.useHandle(handle -> handle.createUpdate("UPDATE transaction_backend SET state = 'ACTIVE' WHERE instance_id = 'i-1'").execute());
    }

    @Test
    void theCoordinatorSeesTheGatewaysOwnForwardedProtocolOverInternalHttp()
            throws Exception
    {
        Reply response = statement("alice", "correct-password", WAREHOUSE_HOST, Map.of());
        assertThat(response.status())
                .withFailMessage("status %s error %s body %s", response.status(), response.header("X-Trino-Gateway-Error"), response.body())
                .isEqualTo(200);

        Exchange statement = lastStatementExchange();
        // Exactly one forwarded protocol reaches the coordinator, and it is the Gateway's own scheme.
        assertThat(statement.headers().get("X-forwarded-proto")).containsExactly(gateway.getScheme());
        assertThat(statement.headers().get("X-forwarded-host")).hasSize(1);
        assertThat(statement.headers()).doesNotContainKey("Forwarded");
        // The credential is forwarded byte for byte: the Gateway does not verify it and does not weaken it.
        assertThat(statement.headers().get("Authorization")).containsExactly(basic("alice", "correct-password"));

        // The identity probe asserts the original protocol too, without asserting a forwarded host that
        // could host-qualify a management principal against the internal Service name.
        Exchange probe = exchanges.stream().filter(exchange -> exchange.path().equals("/v1/info")).reduce((_, b) -> b).orElseThrow();
        assertThat(probe.headers().get("X-forwarded-proto")).containsExactly("https");
        assertThat(probe.headers()).doesNotContainKey("X-forwarded-host");
        assertThat(probe.headers()).doesNotContainKey("Authorization");
    }

    @Test
    void aClientSuppliedForwardedHeaderIsRefusedByTheGatewayItself()
            throws Exception
    {
        // The Gateway is not configured to process forwarded headers, because TLS terminates at the
        // Gateway and it has no trusted upstream proxy. Its own server therefore refuses a
        // client-supplied X-Forwarded-* header outright, before routing, admission or any dispatch.
        for (String header : List.of("X-Forwarded-Host", "X-Forwarded-For", "X-Forwarded-Proto")) {
            Reply response = statement("alice", "correct-password", WAREHOUSE_HOST, Map.of(header, "warehouse-nine." + DOMAIN));
            assertThat(response.status())
                    .withFailMessage("%s produced status %s body %s", header, response.status(), response.body())
                    .isEqualTo(406);
        }
        assertThat(exchanges.stream().anyMatch(exchange -> exchange.path().startsWith("/v1/statement"))).isFalse();
        assertThat(openAdmissions()).isZero();
    }

    @Test
    void aClientSuppliedForwardedHeaderTheServerAcceptsNeverReachesTheCoordinator()
            throws Exception
    {
        // RFC 7239 Forwarded is accepted by the server, so the protection that matters is that the
        // pooled path removes it and the proxy substitutes the Gateway's own values. Otherwise a caller
        // could describe one host to the admission restriction and another to the authenticator.
        Reply response = statement("alice", "correct-password", WAREHOUSE_HOST, Map.of(
                "Forwarded", "host=warehouse-nine." + DOMAIN + ";proto=https"));
        assertThat(response.status())
                .withFailMessage("status %s error %s body %s", response.status(), response.header("X-Trino-Gateway-Error"), response.body())
                .isEqualTo(200);

        Exchange statement = lastStatementExchange();
        assertThat(statement.headers()).doesNotContainKey("Forwarded");
        assertThat(statement.headers().get("X-forwarded-proto")).containsExactly(gateway.getScheme());
        assertThat(statement.headers().get("X-forwarded-host")).doesNotContain("warehouse-nine." + DOMAIN);
    }

    @Test
    void anInjectedForwardedHostCannotMoveTheAdmissionRestrictionToAnAdmittedTenant()
            throws Exception
    {
        // The real routed host names an unadmitted tenant. A second identity header naming the admitted
        // tenant cannot rewrite the decision, because every candidate tenant must be admitted.
        Reply response = statement("alice", "correct-password", "warehouse-unpublished." + DOMAIN, Map.of(
                "X-Trino-User", "warehouse-one.alice"));
        assertThat(response.status()).isEqualTo(403);
        assertThat(response.header("X-Trino-Gateway-Error")).isEqualTo("TENANT_NOT_ADMITTED");
        assertThat(exchanges.stream().anyMatch(exchange -> exchange.path().startsWith("/v1/statement"))).isFalse();
        assertThat(openAdmissions()).isZero();
    }

    @Test
    void aMissingCredentialIsRefusedBeforeAnythingIsDispatched()
            throws Exception
    {
        Reply response = send("POST", "/v1/statement", "SELECT 1", WAREHOUSE_HOST, null, Map.of());
        assertThat(response.status()).isEqualTo(401);
        assertThat(exchanges.stream().anyMatch(exchange -> exchange.path().startsWith("/v1/statement"))).isFalse();
        assertThat(openAdmissions()).isZero();
    }

    @Test
    void aWrongCredentialIsDeniedByTheCoordinatorAndNotClaimedAsSuccess()
            throws Exception
    {
        Reply response = statement("alice", "wrong-password", WAREHOUSE_HOST, Map.of());
        assertThat(response.status())
                .withFailMessage("status %s error %s body %s", response.status(), response.header("X-Trino-Gateway-Error"), response.body())
                .isEqualTo(401);
        // The Gateway dispatched and forwarded the credential; the coordinator made the decision.
        Exchange statement = lastStatementExchange();
        assertThat(statement.headers().get("Authorization")).containsExactly(basic("alice", "wrong-password"));
        // No query or transaction binding is invented from a denied request.
        assertThat(count("transaction_query")).isZero();
        assertThat(count("transaction_binding")).isZero();
    }

    @Test
    void queryAndTransactionContinuationsStayPinnedWhileTheMemberDrains()
            throws Exception
    {
        Reply first = statement("alice", "correct-password", WAREHOUSE_HOST, Map.of());
        assertThat(first.status())
                .withFailMessage("status %s error %s body %s", first.status(), first.header("X-Trino-Gateway-Error"), first.body())
                .isEqualTo(200);
        String transactionState = database.withHandle(handle -> handle.createQuery(
                        "SELECT state FROM transaction_binding WHERE transaction_id = :id")
                .bind("id", TRANSACTION).mapTo(String.class).one());
        assertThat(transactionState).isEqualTo("OPEN");

        // The member stops taking new independent work.
        database.useHandle(handle -> handle.createUpdate("UPDATE transaction_backend SET state = 'DRAINING' WHERE instance_id = 'i-1'").execute());

        Reply newWork = statement("alice", "correct-password", WAREHOUSE_HOST, Map.of());
        assertThat(newWork.status()).isEqualTo(503);
        assertThat(newWork.header("X-Trino-Gateway-Error")).isEqualTo("ROUTING_STATE_NOT_ACTIVE");

        // The result continuation of the already dispatched query is still served by the same member.
        String continuationPath = continuationOf(dispatchedQueryId(first));
        Reply continuation = send("GET", continuationPath, null, WAREHOUSE_HOST, basic("alice", "correct-password"), Map.of());
        assertThat(continuation.status())
                .withFailMessage("status %s error %s body %s", continuation.status(), continuation.header("X-Trino-Gateway-Error"), continuation.body())
                .isEqualTo(200);
        assertThat(lastStatementExchange().path()).isEqualTo(continuationPath);

        // A statement inside the already bound transaction is still admitted to that member.
        Reply inTransaction = send(
                "POST",
                "/v1/statement",
                "SELECT 2",
                WAREHOUSE_HOST,
                basic("alice", "correct-password"),
                Map.of("X-Trino-Transaction-Id", TRANSACTION));
        assertThat(inTransaction.status()).isEqualTo(200);
        assertThat(lastStatementExchange().path()).isEqualTo("/v1/statement");
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    /**
     * The query identity the coordinator issued, read from the results the client received.
     */
    private static String dispatchedQueryId(Reply reply)
    {
        int start = reply.body().indexOf("\"id\":\"") + 6;
        return reply.body().substring(start, reply.body().indexOf('"', start));
    }

    private Exchange lastStatementExchange()
    {
        return exchanges.stream().filter(exchange -> exchange.path().startsWith("/v1/statement")).reduce((_, b) -> b).orElseThrow();
    }

    private long count(String table)
    {
        return database.withHandle(handle -> handle.createQuery("SELECT count(*) FROM " + table).mapTo(Long.class).one());
    }

    private long openAdmissions()
    {
        return database.withHandle(handle -> handle.createQuery(
                "SELECT count(*) FROM transaction_admission WHERE state <> 'COMPLETE'").mapTo(Long.class).one());
    }

    private void record(HttpExchange exchange)
            throws IOException
    {
        // The request body is drained before any response, so a refusal cannot leave an unread body on
        // a connection the proxy still intends to use.
        exchange.getRequestBody().readAllBytes();
        exchanges.add(new Exchange(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), Map.copyOf(exchange.getRequestHeaders())));
    }

    private static void respond(HttpExchange exchange, int status, String body, Map<String, String> headers)
            throws IOException
    {
        byte[] encoded = body.getBytes(UTF_8);
        headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        exchange.getResponseHeaders().add("Content-Type", status == HttpURLConnection.HTTP_OK ? "application/json" : "text/plain");
        exchange.sendResponseHeaders(status, encoded.length);
        exchange.getResponseBody().write(encoded);
        exchange.close();
    }

    private String results(String queryId, boolean more)
    {
        String next = more
                ? ",\"nextUri\":\"http://127.0.0.1:" + coordinator.getAddress().getPort() + continuationOf(queryId) + "\""
                : "";
        return "{\"id\":\"" + queryId + "\",\"stats\":{\"state\":\"" + (more ? "RUNNING" : "FINISHED") + "\"},\"data\":[]" + next + "}";
    }

    private static String continuationOf(String queryId)
    {
        return "/v1/statement/executing/" + queryId + "/capability/1";
    }

    private static String basic(String user, String password)
    {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(ISO_8859_1));
    }

    private Reply statement(String user, String password, String host, Map<String, String> extraHeaders)
            throws Exception
    {
        return send("POST", "/v1/statement", "SELECT 1", host, basic(user, password), extraHeaders);
    }

    /**
     * One real HTTP/1.1 request written on a socket.
     * <p>
     * The request line, {@code Host} and every forwarded header are under the test's control, which is
     * the whole point here: the routed host is what the restriction keys on, and a client-supplied
     * forwarded header must not be able to replace it.
     */
    private Reply send(String method, String path, String body, String host, String authorization, Map<String, String> extraHeaders)
            throws Exception
    {
        StringBuilder request = new StringBuilder()
                .append(method).append(' ').append(path).append(" HTTP/1.1\r\n")
                .append("Host: ").append(host).append("\r\n");
        if (path.startsWith("/v1/")) {
            // Selects the pool, never an individual member: the member is chosen by the authoritative
            // store. Header-free routing is a separate piece of work.
            request.append("X-Trino-Routing-Group: ").append(POOL).append("\r\n");
        }
        if (authorization != null) {
            request.append("Authorization: ").append(authorization).append("\r\n");
        }
        extraHeaders.forEach((name, value) -> request.append(name).append(": ").append(value).append("\r\n"));
        byte[] encoded = body == null ? new byte[0] : body.getBytes(UTF_8);
        if (body != null) {
            if (extraHeaders.keySet().stream().noneMatch(name -> name.equalsIgnoreCase("Content-Type"))) {
                request.append("Content-Type: text/plain\r\n");
            }
            request.append("Content-Length: ").append(encoded.length).append("\r\n");
        }
        request.append("\r\n");

        try (java.net.Socket socket = new java.net.Socket(gateway.getHost(), gateway.getPort())) {
            socket.setSoTimeout(30_000);
            socket.getOutputStream().write(request.toString().getBytes(ISO_8859_1));
            socket.getOutputStream().write(encoded);
            socket.getOutputStream().flush();
            return Reply.read(socket.getInputStream());
        }
    }

    /**
     * A parsed HTTP response: status, headers and body, with chunked and fixed-length framing.
     */
    private record Reply(int status, Map<String, List<String>> headers, String body)
    {
        static Reply read(java.io.InputStream stream)
                throws IOException
        {
            StringBuilder head = new StringBuilder();
            int previous = -1;
            while (!head.toString().endsWith("\r\n\r\n")) {
                int next = stream.read();
                if (next < 0) {
                    break;
                }
                head.append((char) next);
                previous = next;
            }
            if (previous < 0 && head.isEmpty()) {
                throw new IOException("the server closed the connection without a response");
            }
            String[] lines = head.toString().split("\r\n");
            int status = Integer.parseInt(lines[0].split(" ")[1]);
            Map<String, List<String>> headers = new java.util.LinkedHashMap<>();
            for (int index = 1; index < lines.length; index++) {
                int colon = lines[index].indexOf(':');
                if (colon > 0) {
                    headers.computeIfAbsent(lines[index].substring(0, colon).trim(), _ -> new java.util.ArrayList<>())
                            .add(lines[index].substring(colon + 1).trim());
                }
            }
            boolean chunked = headers.getOrDefault("Transfer-Encoding", List.of()).stream().anyMatch("chunked"::equalsIgnoreCase);
            String body;
            if (chunked) {
                body = readChunked(stream);
            }
            else {
                int length = headers.getOrDefault("Content-Length", List.of("0")).stream()
                        .findFirst().map(Integer::parseInt).orElse(0);
                byte[] payload = stream.readNBytes(length);
                body = new String(payload, ISO_8859_1);
            }
            return new Reply(status, Map.copyOf(headers), body);
        }

        /**
         * Reads one chunked body, so the response is framed rather than read to end of stream.
         */
        private static String readChunked(java.io.InputStream stream)
                throws IOException
        {
            StringBuilder body = new StringBuilder();
            while (true) {
                StringBuilder line = new StringBuilder();
                int next;
                while ((next = stream.read()) >= 0 && next != '\n') {
                    if (next != '\r') {
                        line.append((char) next);
                    }
                }
                int size = Integer.parseInt(line.toString().trim().split(";")[0], 16);
                if (size == 0) {
                    stream.readNBytes(2);
                    return body.toString();
                }
                body.append(new String(stream.readNBytes(size), ISO_8859_1));
                stream.readNBytes(2);
            }
        }

        String header(String name)
        {
            return headers.entrySet().stream()
                    .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                    .flatMap(entry -> entry.getValue().stream())
                    .findFirst().orElse(null);
        }
    }

    private void admin(String path, String method, String body)
            throws Exception
    {
        Reply reply = send(
                method,
                path,
                body,
                gateway.getHost() + ":" + gateway.getPort(),
                "Bearer " + ADMIN_TOKEN,
                Map.of("Content-Type", "application/json"));
        if (reply.status() != 200) {
            throw new IllegalStateException("administration call failed: " + reply.status() + " " + reply.body());
        }
    }
}
