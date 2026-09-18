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
     * Sources of a verified tenant identity for the final admission gate. {@code NONE} is the only
     * supported value today, so a pool cannot enable the tenant gate and the Gateway never pretends
     * that a request header identifies a tenant. No customer-visible client change is implied.
     */
    public static final String TENANT_IDENTITY_NONE = "NONE";
    /**
     * The name the coordinator's password authenticator verifies: the Basic credential's user, host
     * qualified exactly as the coordinator qualifies it. The Gateway never verifies the credential
     * itself; it only refuses to dispatch new work for a tenant that is not admitted yet.
     */
    public static final String TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL = "TRINO_BASIC_PRINCIPAL";

    public static final List<String> TENANT_IDENTITY_SOURCES = List.of(TENANT_IDENTITY_NONE, TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL);

    private boolean enabled;
    private String tenantIdentitySource = TENANT_IDENTITY_NONE;
    private String hostQualificationDomain;
    private int maxPreDispatchCandidates = 3;
    private int certificateFreshnessSeconds = 300;
    private int operationRetentionDays = 30;

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
     * Domain under which a coordinator qualifies a bare Basic user with the request host's leading
     * label. The restriction must recompute the same string the coordinator authenticates, so this is
     * required whenever the principal source is enabled.
     */
    public String getHostQualificationDomain()
    {
        return hostQualificationDomain;
    }

    public void setHostQualificationDomain(String hostQualificationDomain)
    {
        this.hostQualificationDomain = hostQualificationDomain;
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
        if (TENANT_IDENTITY_TRINO_BASIC_PRINCIPAL.equals(tenantIdentitySource)
                && (hostQualificationDomain == null || hostQualificationDomain.isBlank() || hostQualificationDomain.startsWith("."))) {
            throw new IllegalArgumentException("tenantIdentitySource TRINO_BASIC_PRINCIPAL requires hostQualificationDomain, "
                    + "so the restriction keys on the same principal the coordinator authenticates");
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
