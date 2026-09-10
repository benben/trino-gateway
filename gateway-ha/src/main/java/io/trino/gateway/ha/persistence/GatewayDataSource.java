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
package io.trino.gateway.ha.persistence;

import com.google.inject.Inject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import jakarta.annotation.PreDestroy;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.toIntExact;

public final class GatewayDataSource
        implements AutoCloseable
{
    private final DataStoreConfiguration configuration;
    private final HikariDataSource pool;
    private final Semaphore admissions;

    @Inject
    public GatewayDataSource(HaGatewayConfiguration configuration)
    {
        this.configuration = configuration.getDataStore();
        if (!configuration.getTransactionAwareness().isEnabled() && !this.configuration.isConnectionPoolEnabled()) {
            pool = null;
            admissions = null;
            return;
        }
        validate(this.configuration);
        HikariConfig settings = new HikariConfig();
        settings.setPoolName("gateway-database-" + UUID.randomUUID());
        settings.setJdbcUrl(this.configuration.getJdbcUrl());
        settings.setUsername(this.configuration.getUser());
        settings.setPassword(this.configuration.getPassword());
        settings.setMaximumPoolSize(this.configuration.getMaximumPoolSize());
        settings.setMinimumIdle(0);
        settings.setConnectionTimeout(this.configuration.getConnectionAcquisitionTimeoutMillis());
        settings.setValidationTimeout(250);
        settings.setIdleTimeout(60000);
        settings.setMaxLifetime(1800000);
        settings.setRegisterMbeans(true);
        settings.addDataSourceProperty("connectTimeout", this.configuration.getConnectTimeoutSeconds());
        settings.addDataSourceProperty("socketTimeout", this.configuration.getSocketTimeoutSeconds());
        settings.addDataSourceProperty("cancelSignalTimeout", 2);
        settings.addDataSourceProperty("tcpKeepAlive", true);
        settings.setConnectionInitSql("SET statement_timeout = " + this.configuration.getStatementTimeoutMillis() +
                "; SET lock_timeout = " + this.configuration.getLockTimeoutMillis());
        pool = new HikariDataSource(settings);
        admissions = new Semaphore(this.configuration.getMaximumPoolSize() - 1, true);
    }

    public Connection openConnection()
            throws SQLException
    {
        if (pool == null) {
            return DriverManager.getConnection(configuration.getJdbcUrl(), configuration.getUser(), configuration.getPassword());
        }
        long remaining = DatabaseDeadline.remainingNanos();
        boolean reserved = DatabaseDeadline.isAdmission();
        if (reserved) {
            try {
                long wait = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(configuration.getConnectionAcquisitionTimeoutMillis()));
                if (!admissions.tryAcquire(wait, TimeUnit.NANOSECONDS)) {
                    throw new SQLTransientConnectionException("Gateway database admission capacity exhausted");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLTransientConnectionException("Gateway database admission interrupted", e);
            }
        }
        Connection connection = null;
        try {
            DatabaseDeadline.remainingNanos();
            connection = pool.getConnection();
            DatabaseDeadline.remainingNanos();
            if (!reserved) {
                return connection;
            }
            Connection delegate = connection;
            AtomicBoolean released = new AtomicBoolean();
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (_, method, args) -> {
                try {
                    return method.invoke(delegate, args);
                }
                catch (InvocationTargetException e) {
                    throw e.getCause();
                }
                finally {
                    if (method.getName().equals("close") && released.compareAndSet(false, true)) {
                        admissions.release();
                    }
                }
            });
        }
        catch (SQLException | RuntimeException e) {
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (SQLException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            if (reserved) {
                admissions.release();
            }
            throw e;
        }
    }

    public SqlLogger deadlineLogger()
    {
        if (pool == null) {
            return SqlLogger.NOP_SQL_LOGGER;
        }
        return new SqlLogger()
        {
            @Override
            public void logBeforeExecution(StatementContext context)
            {
                try {
                    long remaining = DatabaseDeadline.remainingNanos();
                    long limit = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(configuration.getStatementTimeoutMillis()));
                    int seconds = toIntExact(Math.max(1, (limit + TimeUnit.SECONDS.toNanos(1) - 1) / TimeUnit.SECONDS.toNanos(1)));
                    context.getStatement().setQueryTimeout(seconds);
                }
                catch (SQLException e) {
                    throw new UnableToExecuteStatementException(e, context);
                }
            }
        };
    }

    @PreDestroy
    @Override
    public void close()
    {
        if (pool != null) {
            pool.close();
        }
    }

    private static void validate(DataStoreConfiguration configuration)
    {
        String url = configuration.getJdbcUrl();
        if (url == null || !url.startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException("Gateway connection pooling currently requires PostgreSQL");
        }
        if (configuration.getMaximumPoolSize() < 2 || configuration.getMaximumPoolSize() > 100 ||
                configuration.getConnectionAcquisitionTimeoutMillis() < 250 || configuration.getConnectionAcquisitionTimeoutMillis() > 60000 ||
                configuration.getConnectTimeoutSeconds() < 1 || configuration.getConnectTimeoutSeconds() > 60 ||
                configuration.getStatementTimeoutMillis() < 1 || configuration.getStatementTimeoutMillis() > 60000 ||
                configuration.getLockTimeoutMillis() < 1 || configuration.getLockTimeoutMillis() > configuration.getStatementTimeoutMillis() ||
                configuration.getSocketTimeoutSeconds() < 1 || configuration.getSocketTimeoutSeconds() > 120 ||
                configuration.getSocketTimeoutSeconds() * 1000L <= configuration.getStatementTimeoutMillis() + 2000L) {
            throw new IllegalArgumentException("Invalid Gateway database pool size or operation deadlines");
        }
        Set<String> reserved = Set.of("connecttimeout", "sockettimeout", "cancelsignaltimeout", "options", "tcpkeepalive");
        int query = url.indexOf('?');
        if (query >= 0) {
            for (String parameter : url.substring(query + 1).split("&")) {
                String key = URLDecoder.decode(parameter.split("=", 2)[0], StandardCharsets.UTF_8).toLowerCase(java.util.Locale.ROOT);
                if (reserved.contains(key)) {
                    throw new IllegalArgumentException("Configure database deadlines through dataStore fields, not JDBC URL overrides");
                }
            }
        }
    }
}
