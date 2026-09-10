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

import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.TransactionAwarenessConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTransactionAwarenessConfiguration
{
    @Test
    void testDisabledConfigurationDoesNotRequireNewSecrets()
    {
        TransactionAwarenessConfiguration config = new TransactionAwarenessConfiguration();
        assertThat(config.isEnabled()).isFalse();
        assertThat(config.getTerminalRetentionSeconds()).isEqualTo(120);
        config.validate(null);
    }

    @Test
    void testEnabledConfigurationRejectsMissingOrSharedKeys()
    {
        DataStoreConfiguration database = database("jdbc:postgresql://localhost/test");
        TransactionAwarenessConfiguration config = new TransactionAwarenessConfiguration();
        config.setEnabled(true);
        assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        config.setIdentityKey("synthetic-identity-key-for-tests-only");
        assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        config.setAdminToken(config.getIdentityKey());
        assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        config.setAdminToken("synthetic-admin-token-for-tests-only");
        config.validate(database);
    }

    @Test
    void testUnsupportedDatabaseAndRetentionAreRejected()
    {
        TransactionAwarenessConfiguration config = new TransactionAwarenessConfiguration();
        config.setEnabled(true);
        config.setIdentityKey("synthetic-identity-key-for-tests-only");
        config.setAdminToken("synthetic-admin-token-for-tests-only");
        assertThatThrownBy(() -> config.validate(database("jdbc:mysql://localhost/test"))).isInstanceOf(IllegalArgumentException.class);
        for (int seconds : new int[] {0, -1, 86401}) {
            config.setTerminalRetentionSeconds(seconds);
            assertThatThrownBy(() -> config.validate(database("jdbc:postgresql://localhost/test"))).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testRequestCapacityAndDeadlinesAreValidated()
    {
        TransactionAwarenessConfiguration config = new TransactionAwarenessConfiguration();
        config.setEnabled(true);
        config.setIdentityKey("synthetic-identity-key-for-tests-only");
        config.setAdminToken("synthetic-admin-token-for-tests-only");
        DataStoreConfiguration database = database("jdbc:postgresql://localhost/test");
        assertThat(config.getMaxInFlightRequests()).isEqualTo(16);
        assertThat(config.getCompletionThreads()).isEqualTo(4);
        assertThat(config.getRequestTimeoutMillis()).isEqualTo(120000);
        assertThat(config.getProcessInfoTimeoutMillis()).isEqualTo(5000);
        assertThat(config.getCompletionTimeoutMillis()).isEqualTo(10000);
        config.validate(database);
        for (int value : new int[] {0, -1, 10001}) {
            config.setMaxInFlightRequests(value);
            assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        }
        config.setMaxInFlightRequests(16);
        for (int value : new int[] {0, -1, 17}) {
            config.setCompletionThreads(value);
            assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        }
        config.setCompletionThreads(4);
        for (int value : new int[] {0, -1, 3600001}) {
            config.setRequestTimeoutMillis(value);
            assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        }
        config.setRequestTimeoutMillis(120000);
        for (int value : new int[] {0, -1, 120001}) {
            config.setProcessInfoTimeoutMillis(value);
            assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        }
        config.setProcessInfoTimeoutMillis(5000);
        for (int value : new int[] {0, -1, 60001}) {
            config.setCompletionTimeoutMillis(value);
            assertThatThrownBy(() -> config.validate(database)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static DataStoreConfiguration database(String url)
    {
        DataStoreConfiguration configuration = new DataStoreConfiguration();
        configuration.setJdbcUrl(url);
        return configuration;
    }
}
