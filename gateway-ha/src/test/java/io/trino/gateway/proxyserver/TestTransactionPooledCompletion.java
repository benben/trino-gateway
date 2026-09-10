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
package io.trino.gateway.proxyserver;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.GatewayCookieConfigurationPropertiesProvider;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.module.HaGatewayProviderModule;
import io.trino.gateway.ha.persistence.DatabaseDeadline;
import io.trino.gateway.ha.persistence.FlywayMigration;
import io.trino.gateway.ha.persistence.GatewayDataSource;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.QueryHistoryManager;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.transaction.TransactionAwarenessService;
import io.trino.gateway.ha.transaction.TransactionStore;
import io.trino.gateway.ha.transaction.TransactionStore.ResponseObservation;
import io.trino.gateway.proxyserver.ProxyResponseHandler.ProxyResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.TimeoutHandler;
import jakarta.ws.rs.core.Response;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.ArgumentCaptor;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestInstance(PER_CLASS)
@Isolated
class TestTransactionPooledCompletion
{
    private static final String QUERY = "20260910_120000_00001_abcde";
    private static final String PATH = "/v1/statement/executing/" + QUERY + "/capability/1";
    private JdbcDatabaseContainer<?> container;
    private Jdbi admin;
    private String url;
    private String username;
    private String password;

    @BeforeAll
    void setupDatabase()
    {
        url = System.getenv("TX_STORE_TEST_JDBC_URL");
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
    }

    @AfterAll
    void cleanupDatabase()
    {
        if (container != null) {
            container.close();
        }
    }

    @Test
    void timedOutClientStillPersistsThroughReservedFourthConnection()
            throws Exception
    {
        String schema = "pooled_completion_" + UUID.randomUUID().toString().replace("-", "");
        admin.useHandle(handle -> handle.execute("CREATE SCHEMA " + schema));
        try {
            HaGatewayConfiguration configuration = new HaGatewayConfiguration();
            DataStoreConfiguration database = new DataStoreConfiguration();
            String schemaUrl = url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema;
            database.setJdbcUrl(schemaUrl + "&ApplicationName=" + schema);
            database.setUser(username);
            database.setPassword(password);
            database.setDriver("org.postgresql.Driver");
            database.setMaximumPoolSize(4);
            database.setConnectionAcquisitionTimeoutMillis(250);
            configuration.setDataStore(database);
            configuration.getTransactionAwareness().setEnabled(true);
            configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-pooled-completion");
            configuration.getTransactionAwareness().setAdminToken("synthetic-admin-key-for-pooled-completion");
            configuration.getTransactionAwareness().setMaxInFlightRequests(1);
            configuration.getTransactionAwareness().setCompletionThreads(1);
            GatewayCookieConfigurationPropertiesProvider.getInstance().initialize(configuration.getGatewayCookieConfiguration());
            FlywayMigration.migrate(database);
            Jdbi observer = Jdbi.create(schemaUrl, username, password);
            try (GatewayDataSource source = new GatewayDataSource(configuration)) {
                Jdbi pooled = HaGatewayProviderModule.provideJdbi(source);
                TransactionStore store = new TransactionStore(pooled);
                store.ensureBackend("blue", "http://blue.example.test", null, "group", "node", "abcde");
                var initial = store.admitNew("blue", "owner", "group");
                String capability = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(PATH.getBytes(UTF_8)));
                store.recordResponse(initial.id(), new ResponseObservation(QUERY, null, false, false, 120, List.of(capability)));

                HttpClient monitor = mock(HttpClient.class);
                StringResponse info = mock(StringResponse.class);
                when(info.getStatusCode()).thenReturn(200);
                when(info.getBody()).thenReturn("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node\",\"coordinatorId\":\"abcde\"}");
                when(monitor.execute(any(), any())).thenReturn(info);
                TransactionAwarenessService service = new TransactionAwarenessService(configuration, pooled, mock(GatewayBackendManager.class), monitor, mock(RoutingGroupSelector.class));
                HttpClient proxy = mock(HttpClient.class);
                SettableFuture<ProxyResponse> raw = SettableFuture.create();
                doReturn(new TestingResponseFuture(raw)).when(proxy).executeAsync(any(), any());
                ProxyRequestHandler handler = new ProxyRequestHandler(proxy, mock(RoutingManager.class), mock(QueryHistoryManager.class), configuration);
                handler.setTransactionAwareness(service);
                try {
                    HttpServletRequest original = request();
                    var target = service.resolve(original, () -> { throw new AssertionError("Unexpected ordinary routing"); }, _ -> { throw new AssertionError("Unexpected backend selection"); });
                    UUID pending = observer.withHandle(handle -> handle.createQuery("SELECT admission_id FROM transaction_admission WHERE state = 'PENDING'").mapTo(UUID.class).one());
                    AsyncResponse client = mock(AsyncResponse.class);
                    doAnswer(_ -> {
                        doAnswer(_ -> { throw new IllegalStateException("Recycled servlet attributes"); }).when(original).getAttribute(anyString());
                        doAnswer(_ -> { throw new IllegalStateException("Recycled servlet method"); }).when(original).getMethod();
                        doAnswer(_ -> { throw new IllegalStateException("Recycled servlet URI"); }).when(original).getRequestURI();
                        return true;
                    }).when(client).resume(any(Response.class));
                    handler.getRequest(target.modifiedRequest(), client, target.routingDestination());
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    try (var one = DatabaseDeadline.withDeadline(deadline, true, pooled::open);
                            var two = DatabaseDeadline.withDeadline(deadline, true, pooled::open);
                            var three = DatabaseDeadline.withDeadline(deadline, true, pooled::open)) {
                        List<Integer> occupied = List.of(
                                one.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one(),
                                two.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one(),
                                three.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one());
                        assertThat(occupied).doesNotHaveDuplicates();
                        assertThatThrownBy(() -> DatabaseDeadline.withDeadline(deadline, true, pooled::open))
                                .hasStackTraceContaining("Gateway database admission capacity exhausted");
                        ArgumentCaptor<TimeoutHandler> timeout = ArgumentCaptor.forClass(TimeoutHandler.class);
                        verify(client).setTimeoutHandler(timeout.capture());
                        timeout.getValue().handleTimeout(client);
                        assertThat(raw.isCancelled()).isFalse();
                        assertThat(admissionState(observer, pending)).isEqualTo("PENDING");

                        raw.set(new ProxyResponse(200, ImmutableListMultimap.of(), "{\"id\":\"" + QUERY + "\",\"stats\":{\"state\":\"FINISHED\"}}"));
                        long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (!admissionState(observer, pending).equals("COMPLETE") && System.nanoTime() < waitUntil) {
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                        assertThat(admissionState(observer, pending)).isEqualTo("COMPLETE");
                        assertThat(store.getQuery(QUERY).orElseThrow().terminal()).isTrue();
                        try (var fourth = pooled.open()) {
                            int completionConnection = fourth.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one();
                            assertThat(occupied).doesNotContain(completionConnection);
                            long connections = admin.withHandle(handle -> handle.createQuery("SELECT count(*) FROM pg_stat_activity WHERE application_name = :name")
                                    .bind("name", schema).mapTo(Long.class).one());
                            assertThat(connections).isEqualTo(4);
                        }
                    }
                }
                finally {
                    raw.cancel(true);
                    handler.shutdown();
                    service.shutdown();
                }
            }
        }
        finally {
            admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
        }
    }

    private static String admissionState(Jdbi observer, UUID admission)
    {
        return observer.withHandle(handle -> handle.createQuery("SELECT state FROM transaction_admission WHERE admission_id = :id")
                .bind("id", admission).mapTo(String.class).one());
    }

    private static HttpServletRequest request()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        Map<String, Object> attributes = new HashMap<>();
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn(PATH);
        when(request.getHeaders(anyString())).thenAnswer(_ -> Collections.emptyEnumeration());
        when(request.getHeaderNames()).thenAnswer(_ -> Collections.emptyEnumeration());
        when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
        doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1))).when(request).setAttribute(anyString(), any());
        return request;
    }

    private static final class TestingResponseFuture
            extends ForwardingListenableFuture.SimpleForwardingListenableFuture<ProxyResponse>
            implements HttpResponseFuture<ProxyResponse>
    {
        private TestingResponseFuture(SettableFuture<ProxyResponse> delegate)
        {
            super(delegate);
        }

        @Override
        public String getState()
        {
            return isDone() ? "DONE" : "WAITING";
        }
    }
}
