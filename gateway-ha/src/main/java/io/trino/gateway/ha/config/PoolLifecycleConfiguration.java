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

import java.util.List;

/**
 * Pooled member lifecycle configuration. Disabled by default: with {@code enabled = false} the
 * versioned pool API is absent and routing, admission and monitoring behave exactly as before.
 */
public class PoolLifecycleConfiguration
{
    /**
     * The restriction is absent. Nothing infers a tenant, and a pool cannot enable its admission gate.
     */
    public static final String TENANT_IDENTITY_NONE = "NONE";
    /**
     * The name the coordinator's password authenticator verifies: the Basic credential's user, host
     * qualified exactly as the coordinator qualifies it. The Gateway never verifies the credential
     * itself, and never derives a tenant from the name — the tenant comes from the
     * controller-published principal mapping.
     */
    public static final String TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL = "TRINO_BASIC_PRINCIPAL";

    public static final List<String> TENANT_IDENTITY_SOURCES = List.of(TENANT_IDENTITY_NONE, TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL);

    private boolean enabled;
    private String tenantIdentitySource = TENANT_IDENTITY_NONE;
    private List<String> hostQualificationDomains = List.of();
    private List<String> excludedHostLabels = List.of();
    private int maxPreDispatchCandidates = 3;
    private int certificateFreshnessSeconds = 300;
    private int operationRetentionDays = 30;
    private boolean forwardedProtoHttps;

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public String getTenantIdentitySource()
    {
        return tenantIdentitySource;
    }

    public void setTenantIdentitySource(String tenantIdentitySource)
    {
        this.tenantIdentitySource = tenantIdentitySource;
    }

    /**
     * True only when a verified tenant identity is actually available to the admission path.
     */
    public boolean hasVerifiedTenantIdentity()
    {
        return !TENANT_IDENTITY_NONE.equals(tenantIdentitySource);
    }

    /**
     * Domains under which a coordinator qualifies a Basic user with the routed host's leading label.
     * The restriction must recompute the same string the coordinator authenticates, so these must
     * equal the coordinator's own configured domains whenever the principal source is enabled.
     */
    public List<String> getHostQualificationDomains()
    {
        return hostQualificationDomains;
    }

    public void setHostQualificationDomains(List<String> hostQualificationDomains)
    {
        this.hostQualificationDomains = hostQualificationDomains == null ? List.of() : List.copyOf(hostQualificationDomains);
    }

    /**
     * Host labels under those domains that name something operational rather than a tenant. These must
     * equal the coordinator's own exclusions, so a request to an operational host qualifies the same
     * way on both sides.
     */
    public List<String> getExcludedHostLabels()
    {
        return excludedHostLabels;
    }

    public void setExcludedHostLabels(List<String> excludedHostLabels)
    {
        this.excludedHostLabels = excludedHostLabels == null ? List.of() : List.copyOf(excludedHostLabels);
    }

    public int getMaxPreDispatchCandidates()
    {
        return maxPreDispatchCandidates;
    }

    public void setMaxPreDispatchCandidates(int maxPreDispatchCandidates)
    {
        this.maxPreDispatchCandidates = maxPreDispatchCandidates;
    }

    public int getCertificateFreshnessSeconds()
    {
        return certificateFreshnessSeconds;
    }

    public void setCertificateFreshnessSeconds(int certificateFreshnessSeconds)
    {
        this.certificateFreshnessSeconds = certificateFreshnessSeconds;
    }

    public int getOperationRetentionDays()
    {
        return operationRetentionDays;
    }

    public void setOperationRetentionDays(int operationRetentionDays)
    {
        this.operationRetentionDays = operationRetentionDays;
    }

    /**
     * Whether a <em>pooled</em> member's identity probe asserts {@code X-Forwarded-Proto: https}.
     * <p>
     * A pooled member is reached over internal plain HTTP while TLS terminates at the Gateway, so a
     * coordinator that processes forwarded headers needs to be told the original protocol. Disabled by
     * default, and it applies only to probes of pooled members: probing of legacy backends is
     * byte-for-byte unchanged whether or not this is set. It relaxes nothing on the coordinator and
     * never substitutes for authentication.
     */
    public boolean isForwardedProtoHttps()
    {
        return forwardedProtoHttps;
    }

    public void setForwardedProtoHttps(boolean forwardedProtoHttps)
    {
        this.forwardedProtoHttps = forwardedProtoHttps;
    }

    public void validate(boolean transactionAwarenessEnabled)
    {
        if (!enabled) {
            return;
        }
        if (!transactionAwarenessEnabled) {
            throw new IllegalArgumentException("Pooled member lifecycle requires transaction awareness to be enabled");
        }
        if (tenantIdentitySource == null || !TENANT_IDENTITY_SOURCES.contains(tenantIdentitySource)) {
            throw new IllegalArgumentException("tenantIdentitySource must be one of " + TENANT_IDENTITY_SOURCES);
        }
        if (TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL.equals(tenantIdentitySource)) {
            // A coordinator may authenticate an unqualified name, so an empty domain list is valid: the
            // credential itself is then the only candidate. A malformed domain is not.
            for (String domain : hostQualificationDomains) {
                if (domain == null || domain.isBlank() || domain.startsWith(".") || domain.endsWith("..")) {
                    throw new IllegalArgumentException("hostQualificationDomains must contain absolute domains, "
                            + "so the restriction keys on the same principal the coordinator authenticates");
                }
            }
        }
        if (maxPreDispatchCandidates < 1 || maxPreDispatchCandidates > 10) {
            throw new IllegalArgumentException("maxPreDispatchCandidates must be between 1 and 10");
        }
        if (certificateFreshnessSeconds < 1 || certificateFreshnessSeconds > 3600) {
            throw new IllegalArgumentException("certificateFreshnessSeconds must be between 1 and 3600");
        }
        if (operationRetentionDays < 1 || operationRetentionDays > 3650) {
            throw new IllegalArgumentException("operationRetentionDays must be between 1 and 3650");
        }
    }
}
