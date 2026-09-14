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

import java.net.URI;

import static com.fasterxml.jackson.annotation.JsonProperty.Access.WRITE_ONLY;

public class PrincipalRoutingConfiguration
{
    private boolean enabled;
    private String url;
    private String token;
    private int refreshIntervalSeconds = 5;
    private int maxStaleSeconds = 15;
    private int requestTimeoutMillis = 2000;
    private int maxEntries = 100000;

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    @JsonProperty(access = WRITE_ONLY)
    public String getToken()
    {
        return token;
    }

    public void setToken(String token)
    {
        this.token = token;
    }

    public int getRefreshIntervalSeconds()
    {
        return refreshIntervalSeconds;
    }

    public void setRefreshIntervalSeconds(int value)
    {
        refreshIntervalSeconds = value;
    }

    public int getMaxStaleSeconds()
    {
        return maxStaleSeconds;
    }

    public void setMaxStaleSeconds(int value)
    {
        maxStaleSeconds = value;
    }

    public int getRequestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int value)
    {
        requestTimeoutMillis = value;
    }

    public int getMaxEntries()
    {
        return maxEntries;
    }

    public void setMaxEntries(int value)
    {
        maxEntries = value;
    }

    public void validate()
    {
        if (!enabled) {
            return;
        }
        URI endpoint;
        try {
            endpoint = URI.create(url);
        }
        catch (RuntimeException error) {
            throw new IllegalArgumentException("Principal routing requires a valid endpoint");
        }
        if (!java.util.Set.of("http", "https").contains(endpoint.getScheme()) || endpoint.getHost() == null || endpoint.getUserInfo() != null || endpoint.getFragment() != null || endpoint.getQuery() != null) {
            throw new IllegalArgumentException("Principal routing requires an HTTP(S) endpoint without credentials, query or fragment");
        }
        if (token == null || token.isBlank() || token.chars().anyMatch(character -> character < 32 || character == 127)) {
            throw new IllegalArgumentException("Principal routing requires a nonempty service token");
        }
        if (refreshIntervalSeconds < 1 || refreshIntervalSeconds > 60 || maxStaleSeconds <= refreshIntervalSeconds || maxStaleSeconds > 300 || requestTimeoutMillis < 1 || requestTimeoutMillis >= refreshIntervalSeconds * 1000 || maxEntries < 1 || maxEntries > 100000) {
            throw new IllegalArgumentException("Invalid principal routing refresh, age, timeout or entry bounds");
        }
    }
}
