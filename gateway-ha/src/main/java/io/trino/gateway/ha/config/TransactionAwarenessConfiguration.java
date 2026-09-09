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
package io.trino.gateway.ha.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonProperty.Access.WRITE_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TransactionAwarenessConfiguration
{
    private boolean enabled;
    private String identityKey;
    private String adminToken;
    private int terminalRetentionSeconds = 120;

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    @JsonProperty(access = WRITE_ONLY)
    public String getIdentityKey()
    {
        return identityKey;
    }

    public void setIdentityKey(String identityKey)
    {
        this.identityKey = identityKey;
    }

    @JsonProperty(access = WRITE_ONLY)
    public String getAdminToken()
    {
        return adminToken;
    }

    public void setAdminToken(String adminToken)
    {
        this.adminToken = adminToken;
    }

    public int getTerminalRetentionSeconds()
    {
        return terminalRetentionSeconds;
    }

    public void setTerminalRetentionSeconds(int terminalRetentionSeconds)
    {
        this.terminalRetentionSeconds = terminalRetentionSeconds;
    }

    public void validate(DataStoreConfiguration dataStore)
    {
        if (!enabled) {
            return;
        }
        if (dataStore == null || dataStore.getJdbcUrl() == null || !dataStore.getJdbcUrl().startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException("Transaction awareness requires PostgreSQL");
        }
        if (identityKey == null || identityKey.getBytes(UTF_8).length < 32) {
            throw new IllegalArgumentException("Transaction awareness requires a shared identityKey of at least 32 bytes");
        }
        if (adminToken == null || adminToken.getBytes(UTF_8).length < 32 || adminToken.equals(identityKey)) {
            throw new IllegalArgumentException("Transaction awareness requires a separate adminToken of at least 32 bytes");
        }
        if (terminalRetentionSeconds < 1 || terminalRetentionSeconds > 86400) {
            throw new IllegalArgumentException("terminalRetentionSeconds must be between 1 and 86400");
        }
    }
}
