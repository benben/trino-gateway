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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.node.NodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracing.TracingModule;
import io.trino.gateway.baseapp.BaseApp;
import io.trino.gateway.ha.config.ClusterStatsConfiguration;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.module.HaGatewayProviderModule;
import io.trino.gateway.ha.persistence.DatabaseDeadline;
import io.trino.gateway.ha.persistence.FlywayMigration;
import io.trino.gateway.ha.persistence.GatewayDataSource;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.weakref.jmx.guice.MBeanModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.trino.gateway.ha.config.ClusterStatsMonitorType.NOOP;
import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Isolated
class TestTransactionDatabasePool
{
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

    private HaGatewayConfiguration poolConfiguration()
    {
        HaGatewayConfiguration configuration = new HaGatewayConfiguration();
        configuration.setDataStore(new DataStoreConfiguration());
        configuration.getDataStore().setJdbcUrl(url);
        configuration.getDataStore().setUser(username);
        configuration.getDataStore().setPassword(password);
        configuration.getTransactionAwareness().setEnabled(true);
        return configuration;
    }

    @Test
    void statementTimeoutCancelsWorkAndReturnsConnection()
    {
        HaGatewayConfiguration configuration = poolConfiguration();
        configuration.getDataStore().setStatementTimeoutMillis(300);
        try (GatewayDataSource source = new GatewayDataSource(configuration)) {
            Jdbi database = HaGatewayProviderModule.provideJdbi(source);
            long start = System.nanoTime();
            assertThatThrownBy(() -> database.useHandle(handle -> handle.execute("SELECT pg_sleep(3)")))
                    .hasRootCauseInstanceOf(java.sql.SQLException.class)
                    .hasStackTraceContaining("statement timeout");
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(2000);
            int result = database.withHandle(handle -> handle.createQuery("SELECT 1").mapTo(Integer.class).one());
            assertThat(result).isEqualTo(1);
        }
    }

    @Test
    void absoluteBudgetPreventsAnotherStatementAndRestoresThreadContext()
    {
        try (GatewayDataSource source = new GatewayDataSource(poolConfiguration())) {
            Jdbi database = HaGatewayProviderModule.provideJdbi(source);
            java.util.concurrent.atomic.AtomicBoolean firstCompleted = new java.util.concurrent.atomic.AtomicBoolean();
            try (var handle = database.open()) {
                handle.execute("CREATE TEMP TABLE deadline_rollback (id int)");
                assertThatThrownBy(() -> handle.inTransaction(_ -> DatabaseDeadline.withDeadline(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100), true,
                        () -> {
                            handle.execute("INSERT INTO deadline_rollback VALUES (1)");
                            handle.execute("SELECT pg_sleep(0.2)");
                            firstCompleted.set(true);
                            return handle.createQuery("SELECT 2").mapTo(Integer.class).one();
                        })))
                        .hasStackTraceContaining("Gateway database phase deadline expired");
                assertThat(handle.createQuery("SELECT count(*) FROM deadline_rollback").mapTo(Integer.class).one()).isZero();
            }
            assertThat(firstCompleted).isTrue();
            int result = database.withHandle(handle -> handle.createQuery("SELECT 1").mapTo(Integer.class).one());
            assertThat(result).isEqualTo(1);
        }
    }

    @Test
    void remainingPhaseBudgetCancelsSingleLongStatement()
    {
        try (GatewayDataSource source = new GatewayDataSource(poolConfiguration())) {
            Jdbi database = HaGatewayProviderModule.provideJdbi(source);
            try (var handle = database.open()) {
                long start = System.nanoTime();
                assertThatThrownBy(() -> DatabaseDeadline.withDeadline(
                        start + TimeUnit.MILLISECONDS.toNanos(150),
                        false,
                        () -> handle.execute("SELECT pg_sleep(3)")))
                        .hasRootCauseInstanceOf(java.sql.SQLException.class)
                        .hasStackTraceContaining("canceling statement due to user request");
                assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(2500);
                assertThat(handle.createQuery("SELECT 1").mapTo(Integer.class).one()).isEqualTo(1);
            }
        }
    }

    @Test
    void admissionConnectionsLeaveCapacityForCompletion()
    {
        HaGatewayConfiguration configuration = poolConfiguration();
        configuration.getDataStore().setConnectionAcquisitionTimeoutMillis(250);
        try (GatewayDataSource source = new GatewayDataSource(configuration)) {
            Jdbi database = HaGatewayProviderModule.provideJdbi(source);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            try (var one = DatabaseDeadline.withDeadline(deadline, true, database::open);
                    var two = DatabaseDeadline.withDeadline(deadline, true, database::open);
                    var three = DatabaseDeadline.withDeadline(deadline, true, database::open)) {
                assertThatThrownBy(() -> DatabaseDeadline.withDeadline(deadline, true, database::open))
                        .hasStackTraceContaining("Gateway database admission capacity exhausted");
                int result = DatabaseDeadline.withDeadline(
                        deadline,
                        false,
                        () -> database.withHandle(handle -> handle.createQuery("SELECT 1").mapTo(Integer.class).one()));
                assertThat(result).isEqualTo(1);
            }
            try (var reused = DatabaseDeadline.withDeadline(deadline, true, database::open)) {
                assertThat(reused.createQuery("SELECT 1").mapTo(Integer.class).one()).isEqualTo(1);
            }
        }
    }

    @Test
    void closedPoolCannotOpenNewConnections()
            throws Exception
    {
        GatewayDataSource source = new GatewayDataSource(poolConfiguration());
        long backend;
        try (var connection = source.openConnection(); var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(result.next()).isTrue();
            backend = result.getLong(1);
        }
        source.close();
        assertThatThrownBy(source::openConnection).isInstanceOf(java.sql.SQLException.class);
        long sessions = admin.withHandle(handle -> handle.createQuery("SELECT count(*) FROM pg_stat_activity WHERE pid = :pid")
                .bind("pid", backend).mapTo(Long.class).one());
        assertThat(sessions).isZero();
    }

    @Test
    void disabledFeaturePreservesUnpooledDefaults()
    {
        HaGatewayConfiguration configuration = poolConfiguration();
        configuration.getTransactionAwareness().setEnabled(false);
        try (GatewayDataSource source = new GatewayDataSource(configuration)) {
            Jdbi database = HaGatewayProviderModule.provideJdbi(source);
            long first = database.withHandle(handle -> handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
            long second = database.withHandle(handle -> handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
            String timeout = database.withHandle(handle -> handle.createQuery("SHOW statement_timeout").mapTo(String.class).one());
            assertThat(second).isNotEqualTo(first);
            assertThat(timeout).isEqualTo("0");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"socketTimeout=0", "connectTimeout=0", "cancelSignalTimeout=0", "options=-c%20statement_timeout=0", "%73ocketTimeout=0", "SoCkEtTiMeOuT=0", "tcpKeepAlive=false"})
    void jdbcUrlCannotOverrideSafetySettings(String parameter)
    {
        HaGatewayConfiguration configuration = poolConfiguration();
        configuration.getDataStore().setJdbcUrl(url + (url.contains("?") ? "&" : "?") + parameter);
        assertThatThrownBy(() -> new GatewayDataSource(configuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JDBC URL overrides");
    }

    @ParameterizedTest
    @ValueSource(strings = {"pool-small", "pool-large", "acquisition", "connect", "socket", "statement", "lock", "driver"})
    void rejectsInvalidPoolLimitsBeforeConnecting(String invalid)
    {
        HaGatewayConfiguration configuration = poolConfiguration();
        DataStoreConfiguration database = configuration.getDataStore();
        switch (invalid) {
            case "pool-small" -> database.setMaximumPoolSize(1);
            case "pool-large" -> database.setMaximumPoolSize(101);
            case "acquisition" -> database.setConnectionAcquisitionTimeoutMillis(1);
            case "connect" -> database.setConnectTimeoutSeconds(0);
            case "socket" -> database.setSocketTimeoutSeconds(1);
            case "statement" -> database.setStatementTimeoutMillis(0);
            case "lock" -> database.setLockTimeoutMillis(5001);
            case "driver" -> database.setJdbcUrl("jdbc:mysql://localhost/unused");
            default -> throw new IllegalArgumentException(invalid);
        }
        assertThatThrownBy(() -> new GatewayDataSource(configuration)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void explicitPoolOptInWorksWithoutTransactionAwareness()
    {
        HaGatewayConfiguration configuration = poolConfiguration();
        configuration.getTransactionAwareness().setEnabled(false);
        configuration.getDataStore().setConnectionPoolEnabled(true);
        try (GatewayDataSource source = new GatewayDataSource(configuration)) {
            Jdbi database = HaGatewayProviderModule.provideJdbi(source);
            long first = database.withHandle(handle -> handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
            long second = database.withHandle(handle -> handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
            assertThat(second).isEqualTo(first);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"reuse", "settings", "capacity", "lock"})
    void productionDatabaseHasBoundedReusableConnections(String check)
            throws Exception
    {
        String schema = "transaction_pool_" + UUID.randomUUID().toString().replace("-", "");
        admin.useHandle(handle -> handle.execute("CREATE SCHEMA " + schema));
        try {
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
            {
                configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-bootstrap-only");
                configuration.getTransactionAwareness().setAdminToken("synthetic-admin-key-for-bootstrap-only");
            }
            configuration.validate();
            FlywayMigration.migrate(database);

            // Keep this list aligned with the production HaGatewayLauncher modules.
            Bootstrap bootstrap = new Bootstrap(
                    new NodeModule(),
                    new HttpServerModule(),
                    new JmxModule(),
                    new JmxHttpModule(),
                    new JmxOpenMetricsModule(),
                    new LogJmxModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new JaxrsModule(),
                    new TracingModule("trino-gateway", "bootstrap-test"),
                    new HaGatewayProviderModule(configuration),
                    new BaseApp(configuration));
            Injector injector = bootstrap.setRequiredConfigurationProperties(Map.of(
                    "node.environment", "test",
                    "node.id", UUID.randomUUID().toString(),
                    "node.internal-address", "127.0.0.1",
                    "node.bind-ip", "127.0.0.1",
                    "http-server.http.port", "0",
                    "http-server.log.enabled", "false")).initialize();
            long poolBackendPid = injector.getInstance(Jdbi.class).withHandle(handle -> handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
            try {
                Jdbi pooled = injector.getInstance(Jdbi.class);
                if (check.equals("reuse")) {
                    List<Long> backendPids = borrowWithBackgroundActivity(pooled);
                    assertThat(backendPids).hasSize(24);
                    assertThat(backendPids.stream().distinct().count()).isBetween(2L, 4L);
                }
                if (check.equals("settings")) {
                    String statementTimeout = pooled.withHandle(handle -> handle.createQuery("SHOW statement_timeout").mapTo(String.class).one());
                    String lockTimeout = pooled.withHandle(handle -> handle.createQuery("SHOW lock_timeout").mapTo(String.class).one());
                    assertThat(statementTimeout).isEqualTo("5s");
                    assertThat(lockTimeout).isEqualTo("250ms");
                }
                if (check.equals("capacity")) {
                    try (var one = pooled.open(); var two = pooled.open(); var three = pooled.open(); var four = pooled.open();
                            var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                        var extra = executor.submit(() -> pooled.withHandle(handle -> handle.createQuery("SELECT 1").mapTo(Integer.class).one()));
                        assertThatThrownBy(() -> extra.get(3, java.util.concurrent.TimeUnit.SECONDS))
                                .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    }
                }
                if (check.equals("lock")) {
                    TransactionStore store = new TransactionStore(pooled);
                    var backend = store.ensureBackend("blue", "http://blue.example.test", null, "group", "node", "abcde");
                    var admission = store.admitNew("blue", "owner", "group");
                    try (var blocker = admin.open(); var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                        blocker.begin();
                        blocker.createQuery("SELECT incarnation FROM " + schema + ".transaction_backend WHERE incarnation = :id FOR UPDATE")
                                .bind("id", backend.incarnation()).mapTo(UUID.class).one();
                        var update = executor.submit(() -> store.markUncertain(admission.id()));
                        try {
                            assertThatThrownBy(() -> update.get(2, java.util.concurrent.TimeUnit.SECONDS))
                                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                                    .hasStackTraceContaining("lock timeout");
                        }
                        finally {
                            blocker.rollback();
                            try {
                                update.get(3, java.util.concurrent.TimeUnit.SECONDS);
                            }
                            catch (java.util.concurrent.ExecutionException expected) {
                                assertThat(expected.getCause()).isInstanceOf(RuntimeException.class);
                            }
                        }
                    }
                    String admissionState = pooled.withHandle(handle -> handle.createQuery("SELECT state FROM transaction_admission WHERE admission_id = :id")
                            .bind("id", admission.id()).mapTo(String.class).one());
                    assertThat(admissionState).isEqualTo("PENDING");
                }
            }
            finally {
                injector.getInstance(LifeCycleManager.class).stop();
            }
            long liveSessions = admin.withHandle(handle -> handle.createQuery("SELECT count(*) FROM pg_stat_activity WHERE pid = :pid")
                    .bind("pid", poolBackendPid).mapTo(Long.class).one());
            assertThat(liveSessions).isZero();
        }
        finally {
            admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
        }
    }

    @Test
    void backgroundBorrowFixtureDetectsUnpooledConnections()
            throws Exception
    {
        List<Long> backendPids = borrowWithBackgroundActivity(admin);
        assertThat(backendPids).hasSize(24);
        assertThat(backendPids.stream().distinct().count()).isGreaterThan(4);
    }

    private static List<Long> borrowWithBackgroundActivity(Jdbi database)
            throws Exception
    {
        List<Long> backendPids = new ArrayList<>();
        try (var executor = Executors.newSingleThreadExecutor()) {
            for (int round = 0; round < 12; round++) {
                CompletableFuture<Long> backgroundPid = new CompletableFuture<>();
                CountDownLatch release = new CountDownLatch(1);
                var background = executor.submit(() -> {
                    try (var handle = database.open()) {
                        backgroundPid.complete(handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
                        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                    }
                    catch (Exception failure) {
                        backgroundPid.completeExceptionally(failure);
                        throw new RuntimeException(failure);
                    }
                });
                try {
                    long heldPid = backgroundPid.get(5, TimeUnit.SECONDS);
                    long foregroundPid = database.withHandle(handle -> handle.createQuery("SELECT pg_backend_pid()").mapTo(Long.class).one());
                    assertThat(foregroundPid).isNotEqualTo(heldPid);
                    backendPids.add(heldPid);
                    backendPids.add(foregroundPid);
                }
                finally {
                    release.countDown();
                    background.get(5, TimeUnit.SECONDS);
                }
            }
        }
        return backendPids;
    }
}
