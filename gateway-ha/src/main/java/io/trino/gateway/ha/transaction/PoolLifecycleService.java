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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.PoolLifecycleConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.transaction.PoolStore.Guard;
import io.trino.gateway.ha.transaction.PoolStore.PoolException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.jdbi.v3.core.Jdbi;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Request-facing half of the pooled member lifecycle protocol: canonical payload hashing for
 * idempotent steps, independent coordinator process verification, and error classification. It
 * reuses the existing transaction administration token; it adds no credential of its own.
 */
@Singleton
public class PoolLifecycleService
{
    private static final ObjectMapper JSON = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private final PoolLifecycleConfiguration config;
    private final PoolStore store;
    private final TransactionAwarenessService transactions;
    private final GatewayBackendManager backendManager;

    @Inject
    public PoolLifecycleService(HaGatewayConfiguration configuration, Jdbi jdbi, TransactionAwarenessService transactions, GatewayBackendManager backendManager)
    {
        this.config = configuration.getTransactionAwareness().getPool();
        this.store = new PoolStore(requireNonNull(jdbi, "jdbi is null"));
        this.transactions = requireNonNull(transactions, "transactions is null");
        this.backendManager = requireNonNull(backendManager, "backendManager is null");
    }

    public boolean isEnabled()
    {
        return transactions.isEnabled() && config.isEnabled();
    }

    /**
     * Reuses the existing administration token and the resource's own role check.
     */
    public void requireAdmin(HttpServletRequest request)
    {
        if (!isEnabled()) {
            throw poolError(404, "POOL_DISABLED", "The pooled member lifecycle protocol is disabled");
        }
        transactions.requireAdmin(request);
    }

    public PoolStore.PoolState configurePool(String poolId, JsonNode body)
    {
        return guarded(() -> store.configurePool(
                poolId,
                guard(body),
                new PoolStore.PoolSpec(
                        text(body, "apiMode"),
                        integer(body, "minServing"),
                        body.hasNonNull("desiredMembers") ? integer(body, "desiredMembers") : integer(body, "minServing"),
                        integer(body, "maxSurge"),
                        integer(body, "maxRepair"),
                        optionalText(body, "desiredRevision").orElse(""),
                        body.path("tenantAdmissionEnabled").asBoolean(false)),
                config.hasVerifiedTenantIdentity()));
    }

    public PoolStore.PoolState poolState(String poolId)
    {
        return guarded(() -> store.poolState(poolId).orElseThrow(() -> poolError(404, "POOL_NOT_FOUND", "Unknown pool")));
    }

    public List<PoolStore.Member> members(String poolId)
    {
        return guarded(() -> store.members(poolId));
    }

    public PoolStore.Member member(String poolId, String instanceId)
    {
        return guarded(() -> store.member(poolId, instanceId).orElseThrow(() -> poolError(404, "POOL_NOT_FOUND", "Unknown pool member")));
    }

    public PoolStore.Obligations obligations(String poolId, String instanceId)
    {
        return guarded(() -> store.obligations(poolId, instanceId).orElseThrow(() -> poolError(404, "POOL_NOT_FOUND", "Unknown pool member")));
    }

    public PoolStore.FailureReceipt failureReceipt(String poolId, String instanceId)
    {
        return guarded(() -> store.failureReceipt(poolId, instanceId)
                .orElseThrow(() -> poolError(404, "POOL_NOT_FOUND", "This member has no failure receipt")));
    }

    /**
     * Registers an unroutable PREPARING member. The endpoint is taken from the existing Gateway
     * backend registration, so a member can never be registered against an arbitrary endpoint, and
     * the coordinator process identity is observed by the Gateway rather than asserted by the caller.
     */
    public PoolStore.Member registerMember(String poolId, JsonNode body)
    {
        String backendName = text(body, "backendName");
        ProxyBackendConfiguration backend = registeredBackend(poolId, backendName);
        String url = trimSlashes(backend.getProxyTo());
        optionalText(body, "url").ifPresent(supplied -> {
            if (!trimSlashes(supplied).equals(url)) {
                throw poolError(409, "POOL_IDENTITY_CONFLICT", "The supplied endpoint does not match the registered backend");
            }
        });
        Guard guard = guard(body);
        // A lost response must be resolvable by resending the identical body. Probing first would turn
        // a replay into 503/409 whenever the coordinator has since restarted or gone away, exactly when
        // the operator most needs the recorded outcome.
        Optional<PoolStore.Member> replay = guarded(() -> store.replayedMember(poolId, guard));
        if (replay.isPresent()) {
            return replay.orElseThrow();
        }
        JsonNode info = probeIdentity(url);
        return guarded(() -> store.registerMember(poolId, guard, new PoolStore.MemberRegistration(
                text(body, "instanceId"),
                backendName,
                url,
                backend.getExternalUrl() == null ? url : trimSlashes(backend.getExternalUrl()),
                text(body, "podUid"),
                text(body, "bootId"),
                text(body, "configRevision"),
                optionalText(body, "repairFor").orElse(null),
                info.path("nodeId").asText(),
                info.path("coordinatorId").asText())));
    }

    /**
     * Certified activation. The receipt describes checks the operator performed against the candidate;
     * the Gateway records them verbatim and additionally verifies the live process identity itself.
     */
    public PoolStore.Member admitMember(String poolId, String instanceId, JsonNode body)
    {
        PoolStore.Member existing = member(poolId, instanceId);
        JsonNode receipt = body.path("receipt");
        if (!receipt.isObject()) {
            throw poolError(400, "POOL_VALIDATION", "The validation receipt is required");
        }
        Guard guard = guard(body);
        Optional<PoolStore.Member> replay = guarded(() -> store.replayedMember(poolId, guard));
        if (replay.isPresent()) {
            return replay.orElseThrow();
        }
        JsonNode info = probeIdentity(existing.url());
        List<String> checks = new ArrayList<>();
        receipt.path("checks").forEach(value -> checks.add(value.asText()));
        return guarded(() -> store.admitMember(
                poolId,
                instanceId,
                guard,
                generation(body),
                new PoolStore.ValidationReceipt(
                        text(receipt, "certificateHash"),
                        text(receipt, "configRevision"),
                        text(receipt, "authRevision"),
                        text(receipt, "podUid"),
                        text(receipt, "bootId"),
                        text(receipt, "nodeId"),
                        text(receipt, "coordinatorId"),
                        integer(receipt, "readyWorkers"),
                        List.copyOf(checks)),
                info.path("nodeId").asText(),
                info.path("coordinatorId").asText(),
                config.getCertificateFreshnessSeconds()));
    }

    public PoolStore.Member drainMember(String poolId, String instanceId, JsonNode body)
    {
        return guarded(() -> store.drainMember(poolId, instanceId, guard(body), generation(body)));
    }

    public PoolStore.Member sealMember(String poolId, String instanceId, JsonNode body)
    {
        return guarded(() -> store.sealMember(poolId, instanceId, guard(body), generation(body)));
    }

    public PoolStore.Member suspectMember(String poolId, String instanceId, JsonNode body)
    {
        return guarded(() -> store.suspectMember(poolId, instanceId, guard(body), generation(body), text(body, "reason")));
    }

    public PoolStore.Member lostMember(String poolId, String instanceId, JsonNode body)
    {
        String evidence = text(body, "evidence");
        JsonNode termination = body.path("termination");
        // A Go client marshals a value-typed termination proof as an empty object even for a
        // destructive override, so the proof is parsed only when the evidence actually claims one.
        PoolStore.Termination parsed = evidence.equals("PROCESS_TERMINATED") && termination.isObject()
                ? new PoolStore.Termination(
                text(termination, "podUid"),
                text(termination, "bootId"),
                text(termination, "nodeId"),
                text(termination, "coordinatorId"),
                text(termination, "source"),
                optionalText(termination, "observedAt").orElse(""))
                : null;
        return guarded(() -> store.lostMember(
                poolId,
                instanceId,
                guard(body),
                generation(body),
                evidence,
                parsed,
                body.path("destructiveAuthorization").asBoolean(false),
                optionalText(body, "reason").orElse(null)));
    }

    public PoolStore.Member retireMember(String poolId, String instanceId, JsonNode body)
    {
        return guarded(() -> store.retireMember(poolId, instanceId, guard(body), generation(body)));
    }

    public PoolStore.Member retiredMember(String poolId, String instanceId, JsonNode body)
    {
        return guarded(() -> store.retiredMember(poolId, instanceId, guard(body), generation(body), body.path("resourcesAbsent").asBoolean(false)));
    }

    public PoolStore.Publication openPublication(String poolId, JsonNode body)
    {
        return guarded(() -> store.openPublication(
                poolId,
                guard(body),
                text(body, "publicationId"),
                text(body, "tenant"),
                text(body, "targetRevision"),
                membershipGeneration(body),
                text(body, "payloadHash")));
    }

    public PoolStore.Publication recordPublicationReceipt(String poolId, String publicationId, JsonNode body)
    {
        return guarded(() -> store.recordPublicationReceipt(
                poolId,
                publicationId,
                guard(body),
                text(body, "instanceId"),
                text(body, "podUid"),
                text(body, "bootId"),
                text(body, "appliedRevision"),
                text(body, "authFingerprint")));
    }

    public PoolStore.Publication commitPublication(String poolId, String publicationId, JsonNode body)
    {
        return guarded(() -> store.commitPublication(poolId, publicationId, guard(body), membershipGeneration(body)));
    }

    public PoolStore.Publication abandonPublication(String poolId, String publicationId, JsonNode body)
    {
        return guarded(() -> store.abandonPublication(poolId, publicationId, guard(body)));
    }

    public PoolStore.Publication publication(String poolId, String publicationId)
    {
        return guarded(() -> store.publication(poolId, publicationId).orElseThrow(() -> poolError(404, "POOL_NOT_FOUND", "Unknown publication")));
    }

    public PoolStore.TenantAdmission tenantAdmission(String poolId, String tenant)
    {
        return guarded(() -> store.tenantAdmission(poolId, tenant)
                .orElseGet(() -> new PoolStore.TenantAdmission(PoolStore.PROTOCOL_VERSION, poolId, tenant, "PENDING", null, null, false)));
    }

    public PoolStore.TenantAdmission revokeTenant(String poolId, String tenant, JsonNode body)
    {
        return guarded(() -> store.revokeTenant(poolId, tenant, guard(body), text(body, "reason")));
    }

    /**
     * Backend names whose pooled member has irreversibly left service. Empty while the feature is
     * disabled, and deliberately tolerant of database failures: monitoring must not stop because the
     * lifecycle state is momentarily unreadable.
     */
    public java.util.Set<String> unmonitoredBackendNames()
    {
        if (!isEnabled()) {
            return java.util.Set.of();
        }
        try {
            return java.util.Set.copyOf(store.unmonitoredBackendNames());
        }
        catch (RuntimeException e) {
            return java.util.Set.of();
        }
    }

    public PoolStore.OperationHistory operationHistory(String poolId, String operationId)
    {
        return guarded(() -> store.operationHistory(poolId, operationId));
    }

    /**
     * Observes the coordinator process identity. A probe failure is an availability answer, never an
     * excuse to accept the operator's assertion about which process is running.
     */
    private JsonNode probeIdentity(String url)
    {
        try {
            return transactions.processIdentity(url);
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw poolError(503, "POOL_PROBE_UNAVAILABLE", "The coordinator process identity is unavailable");
        }
    }

    private ProxyBackendConfiguration registeredBackend(String poolId, String backendName)
    {
        ProxyBackendConfiguration backend = backendManager.getBackendByName(backendName)
                .orElseThrow(() -> poolError(404, "POOL_NOT_FOUND", "Unknown Gateway backend registration"));
        if (!poolId.equals(backend.getRoutingGroup())) {
            throw poolError(409, "POOL_IDENTITY_CONFLICT", "The backend belongs to another routing group");
        }
        return backend;
    }

    private Guard guard(JsonNode body)
    {
        if (body == null || !body.isObject()) {
            throw poolError(400, "POOL_VALIDATION", "A JSON object body is required");
        }
        return new Guard(
                text(body, "operationId"),
                text(body, "stepId"),
                longValue(body, "controllerEpoch"),
                canonicalHash(body),
                optionalText(body, "ownerIdentity").orElse(null));
    }

    private static long generation(JsonNode body)
    {
        return longValue(body, "expectedGeneration");
    }

    private static long membershipGeneration(JsonNode body)
    {
        return longValue(body, "expectedMembershipGeneration");
    }

    private static String text(JsonNode body, String field)
    {
        JsonNode value = body.path(field);
        if (!value.isTextual() || value.asText().isEmpty()) {
            throw poolError(400, "POOL_VALIDATION", "Field '" + field + "' is required");
        }
        return value.asText();
    }

    private static Optional<String> optionalText(JsonNode body, String field)
    {
        JsonNode value = body.path(field);
        return value.isTextual() && !value.asText().isEmpty() ? Optional.of(value.asText()) : Optional.empty();
    }

    private static int integer(JsonNode body, String field)
    {
        JsonNode value = body.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw poolError(400, "POOL_VALIDATION", "Field '" + field + "' must be an integer");
        }
        return value.asInt();
    }

    private static long longValue(JsonNode body, String field)
    {
        JsonNode value = body.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.asLong() < 0) {
            throw poolError(400, "POOL_VALIDATION", "Field '" + field + "' must be a non-negative integer");
        }
        return value.asLong();
    }

    /**
     * SHA-256 over the canonical form of the request body, so an identical replay resolves to the
     * recorded result and a changed intent under the same step identity is a conflict.
     */
    static String canonicalHash(JsonNode body)
    {
        try {
            byte[] canonical = JSON.writer(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .writeValueAsBytes(JSON.treeToValue(body, java.util.TreeMap.class));
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        }
        catch (JsonProcessingException e) {
            throw poolError(400, "POOL_VALIDATION", "The request body cannot be canonicalized");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String trimSlashes(String value)
    {
        return value.replaceAll("/+$", "");
    }

    private <T> T guarded(Supplier<T> action)
    {
        try {
            return action.get();
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (PoolException e) {
            throw poolError(e.code().status(), e.code().name(), e.getMessage());
        }
        catch (TransactionStore.StoreException e) {
            throw poolError(409, "ROUTING_STATE_" + e.code(), "The routing state rejected the operation: " + e.code());
        }
        catch (RuntimeException e) {
            throw poolError(503, "ROUTING_STATE_UNAVAILABLE", "The Gateway lifecycle state is unavailable; no lifecycle effect was applied");
        }
    }

    static WebApplicationException poolError(int status, String code, String message)
    {
        return new WebApplicationException(Response.status(status).type("text/plain")
                .header("X-Trino-Gateway-Error", code).entity(message).build());
    }
}
