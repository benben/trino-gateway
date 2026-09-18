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

import org.jdbi.v3.core.Handle;

import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;

final class TestDatabaseMigrationsPostgreSql
        extends BaseTestDatabaseMigrations
{
    public TestDatabaseMigrationsPostgreSql()
    {
        super(createPostgreSqlContainer(), "public");
    }

    @Override
    protected void createGatewaySchema()
    {
        String gatewayBackendTable =
                """
                CREATE TABLE gateway_backend (
                     name VARCHAR(256) PRIMARY KEY,
                     routing_group VARCHAR (256),
                     backend_url VARCHAR (256),
                     external_url VARCHAR (256),
                     active BOOLEAN
                );""";
        String queryHistoryTable =
                """
                CREATE TABLE query_history (
                     query_id VARCHAR(256) PRIMARY KEY,
                     query_text VARCHAR (256),
                     created bigint,
                     backend_url VARCHAR (256),
                     user_name VARCHAR(256),
                     source VARCHAR(256)
                );""";
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute(gatewayBackendTable);
        jdbiHandle.execute(queryHistoryTable);
        jdbiHandle.close();
    }

    @Override
    protected void verifyGatewaySchema()
    {
        super.verifyGatewaySchema();
        verifyResultSetCount("SELECT name FROM transaction_backend", 0);
        verifyResultSetCount("SELECT transaction_id FROM transaction_binding", 0);
        verifyResultSetCount("SELECT query_id FROM transaction_query", 0);
        verifyResultSetCount("SELECT query_id FROM transaction_query_capability", 0);
        verifyResultSetCount("SELECT admission_id FROM transaction_admission", 0);
        verifyResultSetCount("SELECT routing_group FROM transaction_route", 0);
        verifyResultSetCount("SELECT operation_id FROM transaction_rollout", 0);
        verifyResultSetCount("SELECT pool_id FROM pool", 0);
        verifyResultSetCount("SELECT operation_id FROM pool_operation", 0);
        verifyResultSetCount("SELECT incarnation::text FROM pool_member_certificate", 0);
        verifyResultSetCount("SELECT incarnation::text FROM pool_failure_receipt", 0);
        verifyResultSetCount("SELECT publication_id FROM pool_publication", 0);
        verifyResultSetCount("SELECT publication_id FROM pool_publication_receipt", 0);
        verifyResultSetCount("SELECT tenant FROM pool_tenant_admission", 0);
        verifyResultSetCount("SELECT principal FROM pool_tenant_principal", 0);
    }

    @Override
    protected void dropAllTables()
    {
        jdbi.useHandle(handle -> {
            handle.execute("DROP TABLE pool_tenant_principal");
            handle.execute("DROP TABLE pool_publication_receipt");
            handle.execute("DROP TABLE pool_publication");
            handle.execute("DROP TABLE pool_tenant_admission");
            handle.execute("DROP TABLE pool_failure_receipt");
            handle.execute("DROP TABLE pool_member_certificate");
            handle.execute("DROP TABLE pool_operation");
            handle.execute("DROP TABLE transaction_rollout");
            handle.execute("DROP TABLE transaction_route");
            handle.execute("DROP TABLE transaction_admission");
            handle.execute("DROP TABLE transaction_query_capability");
            handle.execute("DROP TABLE transaction_query");
            handle.execute("DROP TABLE transaction_binding");
            handle.execute("DROP TABLE transaction_backend");
            handle.execute("DROP TABLE pool");
        });
        super.dropAllTables();
    }
}
