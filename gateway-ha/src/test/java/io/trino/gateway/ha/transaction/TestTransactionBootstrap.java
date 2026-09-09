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

import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
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
import io.trino.gateway.ha.handler.RoutingTargetHandler;
import io.trino.gateway.ha.module.HaGatewayProviderModule;
import io.trino.gateway.ha.persistence.FlywayMigration;
import io.trino.gateway.ha.resource.TransactionResource;
import io.trino.gateway.proxyserver.ProxyRequestHandler;
import io.trino.gateway.proxyserver.RouteToBackendResource;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.weakref.jmx.guice.MBeanModule;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.trino.gateway.ha.config.ClusterStatsMonitorType.NOOP;
import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Isolated
class TestTransactionBootstrap
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void productionModulesStartWithoutJustInTimeBindings(boolean enabled)
            throws Exception
    {
        String schema = "transaction_bootstrap_" + UUID.randomUUID().toString().replace("-", "");
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
            configuration.getTransactionAwareness().setEnabled(enabled);
            if (enabled) {
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
            try {
                assertThat(injector.getInstance(TransactionAwarenessService.class).isEnabled()).isEqualTo(enabled);
                assertThat(injector.getInstance(TransactionResource.class)).isNotNull();
                assertThat(injector.getInstance(RoutingTargetHandler.class)).isNotNull();
                assertThat(injector.getInstance(ProxyRequestHandler.class)).isNotNull();
                assertThat(injector.getInstance(RouteToBackendResource.class)).isNotNull();
                assertThatThrownBy(() -> injector.getInstance(UnboundProbe.class))
                        .isInstanceOf(ConfigurationException.class).hasMessageContaining("not explicitly bound");
                try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
                    HttpRequest request = HttpRequest.newBuilder(injector.getInstance(HttpServerInfo.class).getHttpUri().resolve("/trino-gateway/livez"))
                            .timeout(Duration.ofSeconds(10)).GET().build();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.body()).isEqualTo("ok");
                }
            }
            finally {
                injector.getInstance(LifeCycleManager.class).stop();
            }
        }
        finally {
            admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
        }
    }

    public static class UnboundProbe {}
}
