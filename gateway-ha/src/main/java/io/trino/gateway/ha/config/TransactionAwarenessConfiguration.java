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
    private int maxInFlightRequests = 16;
    private int completionThreads = 4;
    private int requestTimeoutMillis = 120000;
    private int processInfoTimeoutMillis = 5000;
    private int completionTimeoutMillis = 10000;

    public int getMaxInFlightRequests()
    {
        return maxInFlightRequests;
    }

    public void setMaxInFlightRequests(int maxInFlightRequests)
    {
        this.maxInFlightRequests = maxInFlightRequests;
    }

    public int getCompletionThreads()
    {
        return completionThreads;
    }

    public void setCompletionThreads(int completionThreads)
    {
        this.completionThreads = completionThreads;
    }

    public int getRequestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int requestTimeoutMillis)
    {
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public int getProcessInfoTimeoutMillis()
    {
        return processInfoTimeoutMillis;
    }

    public void setProcessInfoTimeoutMillis(int processInfoTimeoutMillis)
    {
        this.processInfoTimeoutMillis = processInfoTimeoutMillis;
    }

    public int getCompletionTimeoutMillis()
    {
        return completionTimeoutMillis;
    }

    public void setCompletionTimeoutMillis(int completionTimeoutMillis)
    {
        this.completionTimeoutMillis = completionTimeoutMillis;
    }

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
        if (maxInFlightRequests < 1 || maxInFlightRequests > 10000 || completionThreads < 1 || completionThreads > maxInFlightRequests) {
            throw new IllegalArgumentException("Completion threads must be between 1 and maxInFlightRequests, which must not exceed 10000");
        }
        if (requestTimeoutMillis < 1 || requestTimeoutMillis > 3600000 || processInfoTimeoutMillis < 1 || processInfoTimeoutMillis > requestTimeoutMillis || completionTimeoutMillis < 1 || completionTimeoutMillis > 60000) {
            throw new IllegalArgumentException("Request, process probe, and completion timeouts must have bounded positive values");
        }
    }
}
