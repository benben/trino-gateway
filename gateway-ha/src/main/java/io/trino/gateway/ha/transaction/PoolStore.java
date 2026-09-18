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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.annotation.Nullable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_APIMODE;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_EVIDENCE_REQUIRED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_IDENTITY_CONFLICT;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_INTENT_CHANGED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_IRREVERSIBLE;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_MEMBERSHIP_CHANGED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_NOT_CERTIFIED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_NOT_DRAINED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_NOT_FOUND;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_PHASE;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_PRINCIPAL_CONFLICT;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_PUBLICATION_BARRIER;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_RECEIPTS_INCOMPLETE;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_REPAIR_BUDGET;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_SERVING_FLOOR;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_STALE_EPOCH;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_STALE_GENERATION;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_SURGE_BUDGET;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_VALIDATION;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.TENANT_IDENTITY_UNVERIFIED;
import static java.util.Objects.requireNonNull;

/**
 * Authoritative pooled member lifecycle, membership publication barrier and tenant admission record.
 * <p>
 * Every mutation, eligibility read and minimum-serving check takes the existing
 * {@code transaction_route} row for the pool first, then member rows, so lifecycle transitions and
 * query admission are serialized against each other on one pool-level boundary.
 */
public final class PoolStore
{
    public static final int PROTOCOL_VERSION = 1;

    private static final ObjectMapper JSON = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    private static final int MAX_RESULT_BYTES = 65536;

    /**
     * Phases that occupy a live compute slot.
     * <p>
     * SUSPECT is included: a suspected coordinator is unproven, not absent. Its process may still be
     * running and serving bound transactions, so a failed probe must not silently free a compute slot
     * and let unbounded replacements be registered outside the repair budget. Only RETIRED and LOST
     * (which requires a termination receipt for the exact incarnation) stop consuming a slot.
     */
    private static final Set<String> LIVE_PHASES = Set.of("PREPARING", "ACTIVE", "DRAINING", "SEALED", "RETIRING", "SUSPECT");
    /**
     * Phases from which no transition back to ACTIVE is ever permitted.
     */
    private static final Set<String> IRREVERSIBLE_PHASES = Set.of("RETIRING", "RETIRED");
    /**
     * The validation a candidate must actually have passed before it can serve tenant work. The
     * Gateway does not perform these checks; it refuses to certify a member whose operator receipt
     * does not claim all of them, so an arbitrary or empty check list cannot stand in for validation.
     */
    private static final Set<String> REQUIRED_CHECKS = Set.of("image", "workers", "catalog-revision", "auth-revision");

    private static final List<String> ALL_PHASES = List.of("PREPARING", "ACTIVE", "DRAINING", "SEALED", "RETIRING", "RETIRED", "SUSPECT", "LOST");

    private final Jdbi jdbi;

    public PoolStore(Jdbi jdbi)
    {
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
    }

    public enum PoolErrorCode
    {
        POOL_NOT_FOUND(404),
        POOL_APIMODE(409),
        POOL_STALE_EPOCH(409),
        POOL_INTENT_CHANGED(409),
        POOL_STALE_GENERATION(409),
        POOL_PHASE(409),
        POOL_IRREVERSIBLE(409),
        POOL_SERVING_FLOOR(409),
        POOL_SURGE_BUDGET(409),
        POOL_REPAIR_BUDGET(409),
        POOL_NOT_CERTIFIED(409),
        POOL_NOT_DRAINED(409),
        POOL_PUBLICATION_BARRIER(409),
        POOL_MEMBERSHIP_CHANGED(409),
        POOL_RECEIPTS_INCOMPLETE(409),
        POOL_EVIDENCE_REQUIRED(409),
        POOL_IDENTITY_CONFLICT(409),
        POOL_PRINCIPAL_CONFLICT(409),
        POOL_VALIDATION(400),
        TENANT_IDENTITY_UNVERIFIED(409);

        private final int status;

        PoolErrorCode(int status)
        {
            this.status = status;
        }

        public int status()
        {
            return status;
        }
    }

    public static final class PoolException
            extends RuntimeException
    {
        private final PoolErrorCode code;

        public PoolException(PoolErrorCode code, String message)
        {
            super(message);
            this.code = requireNonNull(code, "code is null");
        }

        public PoolErrorCode code()
        {
            return code;
        }
    }

    /**
     * Idempotent step identity carried by every mutation.
     */
    public record Guard(String operationId, String stepId, long controllerEpoch, String payloadHash, @Nullable String ownerIdentity)
    {
        public Guard(String operationId, String stepId, long controllerEpoch, String payloadHash)
        {
            this(operationId, stepId, controllerEpoch, payloadHash, null);
        }

        public Guard
        {
            check(ownerIdentity == null || ownerIdentity.matches("[A-Za-z0-9_.:-]{1,256}"), POOL_VALIDATION, "ownerIdentity is invalid");
            check(operationId != null && operationId.matches("[A-Za-z0-9_.:-]{1,256}"), POOL_VALIDATION, "operationId is invalid");
            check(stepId != null && stepId.matches("[A-Za-z0-9_.:-]{1,64}"), POOL_VALIDATION, "stepId is invalid");
            check(controllerEpoch >= 0, POOL_VALIDATION, "controllerEpoch is invalid");
            check(payloadHash != null && payloadHash.matches("[0-9a-f]{64}"), POOL_VALIDATION, "payloadHash is invalid");
        }
    }

    public record PoolSpec(
            String apiMode,
            int minServing,
            int desiredMembers,
            int maxSurge,
            int maxRepair,
            String desiredRevision,
            boolean tenantAdmissionEnabled) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record PoolState(
            int protocolVersion,
            String poolId,
            String apiMode,
            long controllerEpoch,
            long membershipGeneration,
            int minServing,
            int desiredMembers,
            int maxSurge,
            int maxRepair,
            String desiredRevision,
            String admittedRevision,
            boolean tenantAdmissionEnabled,
            Map<String, Long> counts,
            long servingMembers,
            long liveMembers,
            long surgeInUse,
            long repairInUse,
            long openPublications,
            List<String> blocked,
            boolean replayed) {}

    public record MemberRegistration(
            String instanceId,
            String backendName,
            String url,
            String externalUrl,
            String podUid,
            String bootId,
            String configRevision,
            @Nullable String repairFor,
            String nodeId,
            String coordinatorId) {}

    /**
     * Operator-performed candidate validation receipt. The Gateway records it; it does not perform these checks.
     */
    public record ValidationReceipt(
            String certificateHash,
            String configRevision,
            String authRevision,
            String podUid,
            String bootId,
            String nodeId,
            String coordinatorId,
            int readyWorkers,
            List<String> checks)
    {
        public ValidationReceipt
        {
            checks = checks == null ? List.of() : List.copyOf(checks);
        }
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record Member(
            int protocolVersion,
            String poolId,
            String instanceId,
            UUID incarnation,
            String backendName,
            String url,
            String externalUrl,
            String phase,
            long generation,
            long controllerEpoch,
            String podUid,
            String bootId,
            String nodeId,
            String coordinatorId,
            @Nullable String configRevision,
            @Nullable String certifiedRevision,
            @Nullable String authRevision,
            boolean repair,
            @Nullable String repairFor,
            @Nullable String retirementKind,
            long pendingRequests,
            long openTransactions,
            long activeQueries,
            boolean readyToSeal,
            boolean drained,
            boolean eligible,
            long membershipGeneration,
            boolean replayed) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record Obligations(
            int protocolVersion,
            String instanceId,
            UUID incarnation,
            String phase,
            long generation,
            long pendingRequests,
            long openTransactions,
            long activeQueries,
            boolean readyToSeal,
            boolean drained) {}

    public record Termination(
            String podUid,
            String bootId,
            String nodeId,
            String coordinatorId,
            String source,
            String observedAt) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record FailureReceipt(
            int protocolVersion,
            String poolId,
            String instanceId,
            UUID incarnation,
            String evidence,
            JsonNode detail,
            long outstandingAdmissions,
            long outstandingTransactions,
            long outstandingQueries,
            String recordedAt) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record PublicationReceipt(
            String instanceId,
            UUID incarnation,
            String podUid,
            String bootId,
            String appliedRevision,
            String authFingerprint) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record Publication(
            int protocolVersion,
            String publicationId,
            String poolId,
            String tenant,
            String targetRevision,
            long membershipGeneration,
            String phase,
            List<String> requiredMembers,
            List<PublicationReceipt> receipts,
            List<String> missingMembers,
            String tenantState,
            @Nullable String admittedRevision,
            boolean replayed) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record TenantAdmission(
            int protocolVersion,
            String poolId,
            String tenant,
            String state,
            @Nullable String admittedRevision,
            @Nullable String publicationId,
            @Nullable String principalRevision,
            int principalCount,
            @Nullable String principalsHash,
            /**
             * The published logins, echoed only by the read path. A write result is recorded as an
             * idempotent step, so it must stay bounded no matter how many logins a tenant has; a
             * publication response therefore carries {@code principalCount} and
             * {@code principalsHash} instead and leaves this empty.
             */
            List<String> principals,
            boolean replayed)
    {
        public TenantAdmission
        {
            principals = principals == null ? List.of() : List.copyOf(principals);
        }
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record OperationStep(String stepId, String payloadHash, long controllerEpoch, String outcome, String recordedAt, JsonNode result) {}

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record OperationHistory(int protocolVersion, String operationId, List<OperationStep> steps) {}

    /**
     * An eligible routing candidate: ACTIVE in the authoritative store.
     */
    public record Candidate(String backendName, UUID incarnation, String instanceId, String url) {}

    // ------------------------------------------------------------------------------------------------
    // Pool configuration
    // ------------------------------------------------------------------------------------------------

    /**
     * Creates or updates the pool. The controller epoch is monotonic: it may be raised on takeover
     * but never lowered. This is the only call that may raise the epoch.
     */
    public PoolState configurePool(String poolId, Guard guard, PoolSpec spec, boolean verifiedTenantIdentityAvailable)
    {
        validatePoolId(poolId);
        check(List.of("LEGACY", "POOLED").contains(spec.apiMode()), POOL_VALIDATION, "apiMode must be LEGACY or POOLED");
        check(spec.minServing() >= 1 && spec.minServing() <= 1000, POOL_VALIDATION, "minServing must be between 1 and 1000");
        check(spec.desiredMembers() >= 1 && spec.desiredMembers() <= 1000, POOL_VALIDATION, "desiredMembers must be between 1 and 1000");
        check(spec.maxSurge() >= 0 && spec.maxSurge() <= 100, POOL_VALIDATION, "maxSurge must be between 0 and 100");
        check(spec.maxRepair() >= 0 && spec.maxRepair() <= 100, POOL_VALIDATION, "maxRepair must be between 0 and 100");
        validateRevision(spec.desiredRevision(), "desiredRevision");
        check(!spec.tenantAdmissionEnabled() || verifiedTenantIdentityAvailable,
                TENANT_IDENTITY_UNVERIFIED,
                "Tenant admission requires a configured verified tenant identity source");
        return jdbi.inTransaction(handle -> {
            TransactionStore.lockRoute(handle, poolId);
            ensurePoolRow(handle, poolId);
            Row pool = lockedPool(handle, poolId);
            // A recorded outcome is resolved first and applies nothing. For this call the recorded
            // authority must match: returning an earlier result to a controller presenting a different
            // epoch would report an authority it never acquired.
            Optional<JsonNode> replay = recordedStep(handle, poolId, guard, true);
            if (replay.isPresent()) {
                return decode(replay.orElseThrow(), PoolState.class, true);
            }
            check(guard.controllerEpoch() >= pool.epoch, POOL_STALE_EPOCH, "controllerEpoch is behind the recorded authority epoch");
            // Takeover is explicit: a controller that is not the recorded owner must raise the epoch.
            // An equal epoch presented by a different, or unidentified, controller is refused, so two
            // controllers cannot both mutate the pool at epoch N.
            check(pool.ownerIdentity == null || Objects.equals(pool.ownerIdentity, guard.ownerIdentity()) || guard.controllerEpoch() > pool.epoch,
                    POOL_STALE_EPOCH,
                    "Taking authority from another controller requires a strictly greater epoch");
            if (spec.apiMode().equals("LEGACY")) {
                check(countMembers(handle, poolId, LIVE_PHASES) == 0, POOL_APIMODE, "A pool with live members cannot return to LEGACY mode");
            }
            handle.createUpdate(
                            """
                            UPDATE pool SET epoch = :epoch, api_mode = :mode, min_serving = :minServing,
                              desired_members = :desired, max_surge = :surge, max_repair = :repair,
                              desired_revision = :revision, tenant_admission_enabled = :tenantAdmission,
                              owner_identity = coalesce(:owner, owner_identity)
                            WHERE pool_id = :id
                            """)
                    .bind("owner", guard.ownerIdentity())
                    .bind("epoch", guard.controllerEpoch()).bind("mode", spec.apiMode()).bind("minServing", spec.minServing())
                    .bind("desired", spec.desiredMembers()).bind("surge", spec.maxSurge()).bind("repair", spec.maxRepair())
                    .bind("revision", spec.desiredRevision()).bind("tenantAdmission", spec.tenantAdmissionEnabled())
                    .bind("id", poolId).execute();
            PoolState state = poolState(handle, poolId);
            recordStep(handle, poolId, guard, state);
            return state;
        });
    }

    public Optional<PoolState> poolState(String poolId)
    {
        return jdbi.withHandle(handle -> poolRow(handle, poolId).map(_ -> poolState(handle, poolId)));
    }

    // ------------------------------------------------------------------------------------------------
    // Member lifecycle
    // ------------------------------------------------------------------------------------------------

    /**
     * Registers an unroutable PREPARING member. Registration never produces an eligible ACTIVE member.
     */
    public Member registerMember(String poolId, Guard guard, MemberRegistration registration)
    {
        validateInstanceId(registration.instanceId());
        check(registration.backendName() != null && !registration.backendName().isBlank() && registration.backendName().length() <= 256,
                POOL_VALIDATION,
                "backendName is required");
        check(registration.url() != null && !registration.url().isBlank(), POOL_VALIDATION, "url is required");
        validateIdentityField(registration.podUid(), "podUid");
        validateIdentityField(registration.bootId(), "bootId");
        validateIdentityField(registration.nodeId(), "nodeId");
        validateIdentityField(registration.coordinatorId(), "coordinatorId");
        validateRevision(registration.configRevision(), "configRevision");
        if (registration.repairFor() != null) {
            validateInstanceId(registration.repairFor());
            check(!registration.repairFor().equals(registration.instanceId()), POOL_VALIDATION, "repairFor must name another instance");
        }
        String url = trimSlashes(registration.url());
        String externalUrl = registration.externalUrl() == null || registration.externalUrl().isBlank() ? url : trimSlashes(registration.externalUrl());
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            boolean repair = registration.repairFor() != null;
            if (repair) {
                Row target = memberRow(handle, poolId, registration.repairFor())
                        .orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "The repaired instance is not a member of this pool"));
                check(Set.of("SUSPECT", "LOST").contains(target.state) || "FAILED".equals(target.retirementKind),
                        POOL_REPAIR_BUDGET,
                        "Repair is only permitted for a suspect, lost or failure-retired member");
                check(countRepair(handle, poolId) < pool.maxRepair, POOL_REPAIR_BUDGET, "The pool repair budget is exhausted");
            }
            else {
                long live = countMembers(handle, poolId, LIVE_PHASES) - countRepair(handle, poolId);
                check(live + 1 <= (long) Math.max(pool.desiredMembers, pool.minServing) + pool.maxSurge,
                        POOL_SURGE_BUDGET,
                        "Registering this member would exceed the pool surge budget");
            }
            check(memberRow(handle, poolId, registration.instanceId()).isEmpty(), POOL_IDENTITY_CONFLICT, "This instance identity was already used");
            UUID incarnation = UUID.randomUUID();
            int inserted = handle.createUpdate(
                            """
                            INSERT INTO transaction_backend (incarnation, name, current_name, backend_url, external_url, routing_group,
                                node_id, coordinator_id, state, pool_id, instance_id, pod_uid, boot_id, config_revision, repair, repair_for)
                            VALUES (:incarnation, :name, :name, :url, :external, :group, :node, :coordinator, 'PREPARING',
                                :pool, :instance, :pod, :boot, :revision, :repair, :repairFor)
                            ON CONFLICT DO NOTHING
                            """)
                    .bind("incarnation", incarnation).bind("name", registration.backendName()).bind("url", url).bind("external", externalUrl)
                    .bind("group", poolId).bind("node", registration.nodeId()).bind("coordinator", registration.coordinatorId())
                    .bind("pool", poolId).bind("instance", registration.instanceId()).bind("pod", registration.podUid())
                    .bind("boot", registration.bootId()).bind("revision", registration.configRevision())
                    .bind("repair", repair).bind("repairFor", registration.repairFor())
                    .execute();
            check(inserted == 1, POOL_IDENTITY_CONFLICT, "The member name, endpoint or process identity already has another identity");
            return member(handle, poolId, registration.instanceId(), pool);
        });
    }

    /**
     * Certified activation: PREPARING to ACTIVE. Requires a validation receipt bound to the exact
     * process identity, generation CAS, and a receipt satisfying any open publication barrier.
     */
    public Member admitMember(String poolId, String instanceId, Guard guard, long expectedGeneration, ValidationReceipt receipt, String observedNodeId, String observedCoordinatorId, int certificateFreshnessSeconds)
    {
        check(receipt != null, POOL_VALIDATION, "The validation receipt is required");
        check(receipt.certificateHash() != null && receipt.certificateHash().matches("[0-9a-f]{64}"), POOL_VALIDATION, "certificateHash must be a lowercase SHA-256 hash");
        check(receipt.readyWorkers() >= 1, POOL_NOT_CERTIFIED, "A certified member needs at least one ready worker");
        validateRevision(receipt.configRevision(), "receipt.configRevision");
        validateRevision(receipt.authRevision(), "receipt.authRevision");
        check(receipt.checks().size() <= 64 && receipt.checks().stream().allMatch(value -> value != null && value.matches("[A-Za-z0-9_.:-]{1,64}")),
                POOL_VALIDATION,
                "receipt.checks entries are invalid");
        check(receipt.checks().containsAll(REQUIRED_CHECKS),
                POOL_NOT_CERTIFIED,
                "The receipt must claim every required candidate validation: " + REQUIRED_CHECKS);
        check(certificateFreshnessSeconds >= 1, POOL_VALIDATION, "The certificate freshness bound must be positive");
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(!IRREVERSIBLE_PHASES.contains(row.state), POOL_IRREVERSIBLE, "A retiring or retired member can never return to service");
            check(row.state.equals("PREPARING"), POOL_PHASE, "Only a preparing member can be admitted");
            check(Objects.equals(row.podUid, receipt.podUid()) && Objects.equals(row.bootId, receipt.bootId()),
                    POOL_NOT_CERTIFIED,
                    "The receipt does not identify the registered pod and boot identity");
            check(Objects.equals(row.nodeId, receipt.nodeId()) && Objects.equals(row.coordinatorId, receipt.coordinatorId()),
                    POOL_NOT_CERTIFIED,
                    "The receipt does not identify the registered coordinator process");
            check(Objects.equals(observedNodeId, receipt.nodeId()) && Objects.equals(observedCoordinatorId, receipt.coordinatorId()),
                    POOL_NOT_CERTIFIED,
                    "The observed coordinator process does not match the receipt");
            check(Objects.equals(row.configRevision, receipt.configRevision()),
                    POOL_NOT_CERTIFIED,
                    "The receipt applies to a different configuration revision");
            Optional<Row> barrier = openPublicationRow(handle, poolId);
            if (barrier.isPresent()) {
                check(receipt.configRevision().equals(barrier.orElseThrow().targetRevision),
                        POOL_PUBLICATION_BARRIER,
                        "A member joining during a publication must acknowledge its target revision");
            }
            // Freshness: a receipt already recorded for this incarnation and revision keeps its original
            // issue time, so an admission retried long after validation is refused rather than accepted
            // on stale evidence. Genuine revalidation produces a different receipt hash, which re-dates
            // the record; resending identical evidence cannot.
            check(handle.createQuery(
                                    """
                                    SELECT count(*) FROM pool_member_certificate
                                    WHERE incarnation = :incarnation AND config_revision = :revision
                                      AND certificate_hash = :hash
                                      AND issued_at < clock_timestamp() - make_interval(secs => :seconds)
                                    """)
                            .bind("incarnation", row.incarnation).bind("revision", receipt.configRevision())
                            .bind("hash", receipt.certificateHash()).bind("seconds", certificateFreshnessSeconds)
                            .mapTo(Long.class).one() == 0,
                    POOL_NOT_CERTIFIED,
                    "The recorded candidate validation is older than the configured freshness bound; revalidate the candidate");
            handle.createUpdate(
                            """
                            INSERT INTO pool_member_certificate (incarnation, config_revision, certificate_hash, auth_revision,
                                pod_uid, boot_id, node_id, coordinator_id, ready_workers, checks, operation_id)
                            VALUES (:incarnation, :revision, :hash, :auth, :pod, :boot, :node, :coordinator, :workers, CAST(:checks AS jsonb), :operation)
                            ON CONFLICT (incarnation, config_revision) DO UPDATE SET certificate_hash = EXCLUDED.certificate_hash,
                                auth_revision = EXCLUDED.auth_revision, ready_workers = EXCLUDED.ready_workers,
                                checks = EXCLUDED.checks, operation_id = EXCLUDED.operation_id,
                                issued_at = CASE WHEN pool_member_certificate.certificate_hash = EXCLUDED.certificate_hash
                                    THEN pool_member_certificate.issued_at ELSE clock_timestamp() END
                            """)
                    .bind("incarnation", row.incarnation).bind("revision", receipt.configRevision()).bind("hash", receipt.certificateHash())
                    .bind("auth", receipt.authRevision()).bind("pod", receipt.podUid()).bind("boot", receipt.bootId())
                    .bind("node", receipt.nodeId()).bind("coordinator", receipt.coordinatorId()).bind("workers", receipt.readyWorkers())
                    .bind("checks", encode(receipt.checks())).bind("operation", guard.operationId())
                    .execute();
            handle.createUpdate(
                            """
                            UPDATE transaction_backend SET state = 'ACTIVE', generation = generation + 1,
                              certified_revision = :revision, auth_revision = :auth
                            WHERE incarnation = :incarnation
                            """)
                    .bind("revision", receipt.configRevision()).bind("auth", receipt.authRevision()).bind("incarnation", row.incarnation).execute();
            if (barrier.isPresent()) {
                // A join during the barrier is itself a receipt, so it can never escape the barrier silently.
                insertPublicationReceipt(
                        handle,
                        barrier.orElseThrow().publicationId,
                        row.incarnation,
                        row.podUid,
                        row.bootId,
                        receipt.configRevision(),
                        receipt.certificateHash());
            }
            long membership = bumpMembership(handle, poolId);
            return member(handle, poolId, instanceId, pool.withMembership(membership));
        });
    }

    /**
     * Planned drain. Refused when it would take the pool below its minimum serving floor.
     */
    public Member drainMember(String poolId, String instanceId, Guard guard, long expectedGeneration)
    {
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(!IRREVERSIBLE_PHASES.contains(row.state), POOL_IRREVERSIBLE, "A retiring or retired member cannot be drained");
            check(row.state.equals("ACTIVE"), POOL_PHASE, "Only an active member can begin a planned drain");
            long active = countMembers(handle, poolId, Set.of("ACTIVE"));
            check(active - 1 >= pool.minServing, POOL_SERVING_FLOOR, "A planned drain cannot take the pool below its minimum serving count");
            handle.createUpdate("UPDATE transaction_backend SET state = 'DRAINING', generation = generation + 1 WHERE incarnation = :id")
                    .bind("id", row.incarnation).execute();
            long membership = bumpMembership(handle, poolId);
            return member(handle, poolId, instanceId, pool.withMembership(membership));
        });
    }

    /**
     * DRAINING to SEALED, only with zero remaining obligations.
     */
    public Member sealMember(String poolId, String instanceId, Guard guard, long expectedGeneration)
    {
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(!IRREVERSIBLE_PHASES.contains(row.state), POOL_IRREVERSIBLE, "A retiring or retired member cannot be sealed");
            check(row.state.equals("DRAINING"), POOL_PHASE, "Only a draining member can be sealed");
            Obligations obligations = obligations(handle, poolId, instanceId);
            check(obligations.pendingRequests() == 0 && obligations.openTransactions() == 0 && obligations.activeQueries() == 0,
                    POOL_NOT_DRAINED,
                    "The member still has pending admissions, open transactions or active or retained queries");
            handle.createUpdate("UPDATE transaction_backend SET state = 'SEALED', generation = generation + 1 WHERE incarnation = :id")
                    .bind("id", row.incarnation).execute();
            return member(handle, poolId, instanceId, pool);
        });
    }

    /**
     * Suspected failure. Blocks new independent admissions and authorizes nothing destructive.
     */
    public Member suspectMember(String poolId, String instanceId, Guard guard, long expectedGeneration, String reason)
    {
        check(reason != null && !reason.isBlank() && reason.length() <= 256, POOL_VALIDATION, "reason is required");
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(!IRREVERSIBLE_PHASES.contains(row.state), POOL_IRREVERSIBLE, "A retiring or retired member cannot become suspect");
            check(Set.of("PREPARING", "ACTIVE", "DRAINING", "SEALED").contains(row.state), POOL_PHASE, "This member cannot become suspect");
            boolean wasActive = row.state.equals("ACTIVE");
            handle.createUpdate("UPDATE transaction_backend SET state = 'SUSPECT', generation = generation + 1 WHERE incarnation = :id")
                    .bind("id", row.incarnation).execute();
            long membership = wasActive ? bumpMembership(handle, poolId) : pool.membershipGeneration;
            return member(handle, poolId, instanceId, pool.withMembership(membership));
        });
    }

    /**
     * Records an irreversible failure receipt for the exact process incarnation after authoritative
     * evidence that it terminated. Obligations are preserved, never reported as a successful drain.
     */
    public Member lostMember(String poolId, String instanceId, Guard guard, long expectedGeneration, String evidence, @Nullable Termination termination, boolean destructiveAuthorization, @Nullable String reason)
    {
        check(List.of("PROCESS_TERMINATED", "DESTRUCTIVE_OVERRIDE").contains(evidence), POOL_EVIDENCE_REQUIRED, "Unknown loss evidence");
        if (evidence.equals("PROCESS_TERMINATED")) {
            check(termination != null, POOL_EVIDENCE_REQUIRED, "Termination evidence is required");
            validateIdentityField(termination.podUid(), "termination.podUid");
            validateIdentityField(termination.bootId(), "termination.bootId");
            validateIdentityField(termination.nodeId(), "termination.nodeId");
            validateIdentityField(termination.coordinatorId(), "termination.coordinatorId");
            check(termination.source() != null && termination.source().matches("[A-Za-z0-9_.:-]{1,64}"), POOL_EVIDENCE_REQUIRED, "termination.source is required");
        }
        else {
            check(destructiveAuthorization, POOL_EVIDENCE_REQUIRED, "A possibly live process requires explicit destructive authorization");
            check(reason != null && !reason.isBlank() && reason.length() <= 256, POOL_EVIDENCE_REQUIRED, "A destructive override requires a reason");
        }
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(!IRREVERSIBLE_PHASES.contains(row.state), POOL_IRREVERSIBLE, "A retiring or retired member cannot be declared lost");
            check(row.state.equals("SUSPECT"), POOL_PHASE, "Only a suspect member can be declared lost");
            if (termination != null) {
                check(Objects.equals(row.podUid, termination.podUid()) && Objects.equals(row.bootId, termination.bootId())
                                && Objects.equals(row.nodeId, termination.nodeId()) && Objects.equals(row.coordinatorId, termination.coordinatorId()),
                        POOL_EVIDENCE_REQUIRED,
                        "Termination evidence must identify this exact process incarnation");
            }
            Obligations obligations = obligations(handle, poolId, instanceId);
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("evidence", evidence);
            if (termination != null) {
                detail.put("termination", termination);
            }
            if (reason != null) {
                detail.put("reason", reason);
            }
            detail.put("destructiveAuthorization", destructiveAuthorization);
            handle.createUpdate(
                            """
                            INSERT INTO pool_failure_receipt (incarnation, pool_id, operation_id, evidence, detail,
                                outstanding_admissions, outstanding_transactions, outstanding_queries)
                            VALUES (:incarnation, :pool, :operation, :evidence, CAST(:detail AS jsonb), :admissions, :transactions, :queries)
                            ON CONFLICT (incarnation) DO NOTHING
                            """)
                    .bind("incarnation", row.incarnation).bind("pool", poolId).bind("operation", guard.operationId())
                    .bind("evidence", evidence).bind("detail", encode(detail))
                    .bind("admissions", obligations.pendingRequests()).bind("transactions", obligations.openTransactions())
                    .bind("queries", obligations.activeQueries())
                    .execute();
            handle.createUpdate("UPDATE transaction_backend SET state = 'LOST', generation = generation + 1 WHERE incarnation = :id")
                    .bind("id", row.incarnation).execute();
            return member(handle, poolId, instanceId, pool);
        });
    }

    /**
     * Irreversible retirement claim for one exact incarnation. Accepts a sealed member, a lost member
     * with a failure receipt, or a preparing candidate proven never to have admitted work.
     */
    public Member retireMember(String poolId, String instanceId, Guard guard, long expectedGeneration)
    {
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(!IRREVERSIBLE_PHASES.contains(row.state), POOL_IRREVERSIBLE, "Retirement was already claimed for this incarnation");
            String kind;
            switch (row.state) {
                case "SEALED" -> kind = "DRAINED";
                case "LOST" -> {
                    check(failureReceiptRow(handle, row.incarnation).isPresent(), POOL_EVIDENCE_REQUIRED, "A lost member needs its failure receipt before retirement");
                    kind = "FAILED";
                }
                case "PREPARING" -> {
                    check(handle.createQuery("SELECT NOT EXISTS (SELECT 1 FROM transaction_admission WHERE incarnation = :id)")
                            .bind("id", row.incarnation).mapTo(Boolean.class).one(), POOL_NOT_DRAINED, "This candidate admitted work and is a serving incarnation");
                    kind = "DRAINED";
                }
                default -> throw new PoolException(POOL_PHASE, "Only a sealed, lost or never-admitted preparing member can be retired");
            }
            handle.createUpdate("UPDATE transaction_backend SET state = 'RETIRING', generation = generation + 1, retirement_kind = :kind WHERE incarnation = :id")
                    .bind("kind", kind).bind("id", row.incarnation).execute();
            return member(handle, poolId, instanceId, pool);
        });
    }

    /**
     * Completes retirement once the operator asserts the recorded resources are absent.
     */
    public Member retiredMember(String poolId, String instanceId, Guard guard, long expectedGeneration, boolean resourcesAbsent)
    {
        check(resourcesAbsent, POOL_EVIDENCE_REQUIRED, "Retirement completes only with an explicit resource-absence assertion");
        return inPool(poolId, guard, Member.class, (handle, pool) -> {
            requirePooled(pool);
            Row row = lockedMember(handle, poolId, instanceId);
            requireGeneration(row, expectedGeneration);
            check(row.state.equals("RETIRING"), POOL_PHASE, "Only a retiring member can complete retirement");
            handle.createUpdate("UPDATE transaction_backend SET state = 'RETIRED', generation = generation + 1 WHERE incarnation = :id")
                    .bind("id", row.incarnation).execute();
            return member(handle, poolId, instanceId, pool);
        });
    }

    // ------------------------------------------------------------------------------------------------
    // Publication barrier and tenant admission
    // ------------------------------------------------------------------------------------------------

    public Publication openPublication(String poolId, Guard guard, String publicationId, String tenant, String targetRevision, long expectedMembershipGeneration, String planHash)
    {
        validateOperationIdentity(publicationId, "publicationId");
        validateTenant(tenant);
        validateRevision(targetRevision, "targetRevision");
        check(planHash != null && planHash.matches("[0-9a-f]{64}"), POOL_VALIDATION, "payloadHash must be a lowercase SHA-256 hash");
        return inPool(poolId, guard, Publication.class, (handle, pool) -> {
            requirePooled(pool);
            check(pool.membershipGeneration == expectedMembershipGeneration, POOL_MEMBERSHIP_CHANGED, "The pool membership generation changed");
            List<String> required = activeInstanceIds(handle, poolId);
            check(required.size() >= pool.minServing, POOL_SERVING_FLOOR, "A publication requires at least the minimum serving membership");
            Optional<Row> existing = publicationRow(handle, publicationId);
            if (existing.isPresent()) {
                Row row = existing.orElseThrow();
                check(row.poolId.equals(poolId) && row.tenant.equals(tenant) && row.targetRevision.equals(targetRevision) && row.payloadHash.equals(planHash),
                        POOL_INTENT_CHANGED,
                        "This publication identity has a different immutable plan");
                return publication(handle, publicationId);
            }
            check(openPublicationRowForTenant(handle, poolId, tenant).isEmpty(), POOL_PUBLICATION_BARRIER, "This tenant already has an open publication");
            handle.createUpdate(
                            """
                            INSERT INTO pool_publication (publication_id, pool_id, tenant, target_revision, membership_generation,
                                phase, payload_hash, required_members, operation_id)
                            VALUES (:id, :pool, :tenant, :revision, :membership, 'OPEN', :hash, CAST(:required AS jsonb), :operation)
                            """)
                    .bind("id", publicationId).bind("pool", poolId).bind("tenant", tenant).bind("revision", targetRevision)
                    .bind("membership", pool.membershipGeneration).bind("hash", planHash).bind("required", encode(required))
                    .bind("operation", guard.operationId()).execute();
            handle.createUpdate(
                            """
                            INSERT INTO pool_tenant_admission (pool_id, tenant, state) VALUES (:pool, :tenant, 'PENDING')
                            ON CONFLICT (pool_id, tenant) DO NOTHING
                            """)
                    .bind("pool", poolId).bind("tenant", tenant).execute();
            return publication(handle, publicationId);
        });
    }

    /**
     * Records one member's application-loaded acknowledgement, bound to its exact process identity.
     */
    public Publication recordPublicationReceipt(String poolId, String publicationId, Guard guard, String instanceId, String podUid, String bootId, String appliedRevision, String authFingerprint)
    {
        validateInstanceId(instanceId);
        validateIdentityField(podUid, "podUid");
        validateIdentityField(bootId, "bootId");
        validateRevision(appliedRevision, "appliedRevision");
        check(authFingerprint != null && authFingerprint.matches("[0-9a-f]{64}"), POOL_VALIDATION, "authFingerprint must be a lowercase SHA-256 hash");
        return inPool(poolId, guard, Publication.class, (handle, pool) -> {
            requirePooled(pool);
            Row publication = requirePublication(handle, poolId, publicationId);
            check(publication.phase.equals("OPEN"), POOL_PHASE, "Only an open publication accepts receipts");
            check(appliedRevision.equals(publication.targetRevision), POOL_PUBLICATION_BARRIER, "The acknowledgement applies to a different revision");
            Row row = lockedMember(handle, poolId, instanceId);
            check(Objects.equals(row.podUid, podUid) && Objects.equals(row.bootId, bootId),
                    POOL_PUBLICATION_BARRIER,
                    "The acknowledgement does not identify this member's current process");
            check(Set.of("PREPARING", "ACTIVE").contains(row.state), POOL_PHASE, "Only a preparing or active member can acknowledge a publication");
            insertPublicationReceipt(handle, publicationId, row.incarnation, podUid, bootId, appliedRevision, authFingerprint);
            return publication(handle, publicationId);
        });
    }

    /**
     * Atomically opens the tenant admission gate against the collected receipts. One transaction under
     * the pool lock verifies the membership generation, receipt coverage of every active member and the
     * minimum serving count.
     */
    public Publication commitPublication(String poolId, String publicationId, Guard guard, long expectedMembershipGeneration)
    {
        return inPool(poolId, guard, Publication.class, (handle, pool) -> {
            requirePooled(pool);
            Row publication = requirePublication(handle, poolId, publicationId);
            if (publication.phase.equals("ADMITTED")) {
                return publication(handle, publicationId);
            }
            check(publication.phase.equals("OPEN"), POOL_PHASE, "An abandoned publication cannot be committed");
            check(pool.membershipGeneration == expectedMembershipGeneration && publication.membershipGeneration == expectedMembershipGeneration,
                    POOL_MEMBERSHIP_CHANGED,
                    "The pool membership generation changed during the publication");
            List<String> active = activeInstanceIds(handle, poolId);
            check(active.size() >= pool.minServing, POOL_SERVING_FLOOR, "The admitting membership fell below the minimum serving count");
            List<String> missing = missingMembers(handle, poolId, publicationId);
            check(missing.isEmpty(), POOL_RECEIPTS_INCOMPLETE, "Every active member must acknowledge the final revision");
            handle.createUpdate("UPDATE pool_publication SET phase = 'ADMITTED' WHERE publication_id = :id").bind("id", publicationId).execute();
            handle.createUpdate(
                            """
                            INSERT INTO pool_tenant_admission (pool_id, tenant, state, admitted_revision, publication_id, updated_at)
                            VALUES (:pool, :tenant, 'ADMITTED', :revision, :publication, clock_timestamp())
                            ON CONFLICT (pool_id, tenant) DO UPDATE SET state = 'ADMITTED', admitted_revision = :revision,
                              publication_id = :publication, updated_at = clock_timestamp()
                            """)
                    .bind("pool", poolId).bind("tenant", publication.tenant).bind("revision", publication.targetRevision)
                    .bind("publication", publicationId).execute();
            handle.createUpdate("UPDATE pool SET admitted_revision = :revision WHERE pool_id = :pool")
                    .bind("revision", publication.targetRevision).bind("pool", poolId).execute();
            return publication(handle, publicationId);
        });
    }

    public Publication abandonPublication(String poolId, String publicationId, Guard guard)
    {
        return inPool(poolId, guard, Publication.class, (handle, pool) -> {
            requirePooled(pool);
            Row publication = requirePublication(handle, poolId, publicationId);
            if (publication.phase.equals("ABANDONED")) {
                return publication(handle, publicationId);
            }
            check(!publication.phase.equals("ADMITTED"), POOL_IRREVERSIBLE, "A successfully opened admission gate cannot be retracted");
            handle.createUpdate("UPDATE pool_publication SET phase = 'ABANDONED' WHERE publication_id = :id").bind("id", publicationId).execute();
            return publication(handle, publicationId);
        });
    }

    /**
     * Publishes a tenant's authoritative principal set.
     * <p>
     * The restriction keys on the exact principal the coordinator authenticates. A tenant's logins are
     * one flat namespace produced by the controller's own projection, including a root login that
     * carries no separator, and they are not a function of the tenant identifier, so the Gateway must
     * be told them rather than deriving them. The set is replaced whole under the pool lock, and a
     * principal already bound to another tenant is a conflict rather than an ambiguous admission.
     */
    public TenantAdmission publishTenantPrincipals(String poolId, String tenant, Guard guard, String revision, List<String> principals)
    {
        validateTenant(tenant);
        check(revision != null && revision.matches("[A-Za-z0-9_.:-]{1,64}"), POOL_VALIDATION, "revision is invalid");
        check(principals != null && !principals.isEmpty() && principals.size() <= 10000, POOL_VALIDATION, "principals are required");
        principals.forEach(principal -> check(
                principal != null && !principal.isBlank() && principal.length() <= 512
                        && principal.codePoints().noneMatch(Character::isISOControl) && principal.indexOf(':') < 0,
                POOL_VALIDATION,
                "A principal must be a single-line name without a credential separator"));
        List<String> ordered = principals.stream().distinct().sorted().toList();
        return inPool(poolId, guard, TenantAdmission.class, (handle, pool) -> {
            requirePooled(pool);
            List<String> contested = handle.createQuery(
                            """
                            SELECT principal FROM pool_tenant_principal
                            WHERE pool_id = :pool AND tenant <> :tenant AND principal = ANY(:principals)
                            ORDER BY principal
                            """)
                    .bind("pool", poolId).bind("tenant", tenant)
                    .bindArray("principals", String.class, ordered.toArray(String[]::new))
                    .mapTo(String.class).list();
            check(contested.isEmpty(), POOL_PRINCIPAL_CONFLICT, "A published principal already belongs to another tenant");
            handle.createUpdate(
                            """
                            DELETE FROM pool_tenant_principal
                            WHERE pool_id = :pool AND tenant = :tenant AND NOT (principal = ANY(:principals))
                            """)
                    .bind("pool", poolId).bind("tenant", tenant)
                    .bindArray("principals", String.class, ordered.toArray(String[]::new))
                    .execute();
            handle.createUpdate(
                            """
                            INSERT INTO pool_tenant_principal (pool_id, principal, tenant, revision)
                            SELECT :pool, published.principal, :tenant, :revision
                            FROM unnest(:principals) AS published (principal)
                            ON CONFLICT (pool_id, principal) DO UPDATE SET tenant = EXCLUDED.tenant,
                              revision = EXCLUDED.revision, updated_at = clock_timestamp()
                            """)
                    .bind("pool", poolId).bind("tenant", tenant).bind("revision", revision)
                    .bindArray("principals", String.class, ordered.toArray(String[]::new))
                    .execute();
            handle.createUpdate(
                            """
                            INSERT INTO pool_tenant_admission (pool_id, tenant, state) VALUES (:pool, :tenant, 'PENDING')
                            ON CONFLICT (pool_id, tenant) DO NOTHING
                            """)
                    .bind("pool", poolId).bind("tenant", tenant).execute();
            return tenantAdmission(handle, poolId, tenant, false, false);
        });
    }

    public TenantAdmission revokeTenant(String poolId, String tenant, Guard guard, String reason)
    {
        validateTenant(tenant);
        check(reason != null && !reason.isBlank() && reason.length() <= 256, POOL_VALIDATION, "reason is required");
        return inPool(poolId, guard, TenantAdmission.class, (handle, pool) -> {
            requirePooled(pool);
            handle.createUpdate(
                            """
                            INSERT INTO pool_tenant_admission (pool_id, tenant, state, updated_at)
                            VALUES (:pool, :tenant, 'REVOKED', clock_timestamp())
                            ON CONFLICT (pool_id, tenant) DO UPDATE SET state = 'REVOKED', admitted_revision = NULL,
                              publication_id = NULL, updated_at = clock_timestamp()
                            """)
                    .bind("pool", poolId).bind("tenant", tenant).execute();
            handle.createUpdate("UPDATE pool_publication SET phase = 'ABANDONED' WHERE pool_id = :pool AND tenant = :tenant AND phase = 'OPEN'")
                    .bind("pool", poolId).bind("tenant", tenant).execute();
            return tenantAdmission(handle, poolId, tenant, false, false);
        });
    }

    // ------------------------------------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------------------------------------

    public List<Member> members(String poolId)
    {
        return jdbi.withHandle(handle -> {
            Row pool = poolRow(handle, poolId).orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool"));
            return handle.createQuery("SELECT instance_id FROM transaction_backend WHERE pool_id = :pool ORDER BY instance_id")
                    .bind("pool", poolId).mapTo(String.class).list().stream()
                    .map(instanceId -> member(handle, poolId, instanceId, pool))
                    .toList();
        });
    }

    /**
     * Returns the recorded result of an already committed member step, so a caller can resolve a lost
     * response without external probing. A step recorded under the same identity with a different
     * intent is still a conflict.
     */
    public Optional<Member> replayedMember(String poolId, Guard guard)
    {
        return jdbi.inTransaction(handle -> {
            if (poolRow(handle, poolId).isEmpty()) {
                return Optional.empty();
            }
            return recordedStep(handle, poolId, guard).map(document -> decode(document, Member.class, true));
        });
    }

    public Optional<Member> member(String poolId, String instanceId)
    {
        return jdbi.withHandle(handle -> poolRow(handle, poolId)
                .flatMap(pool -> memberRow(handle, poolId, instanceId).map(_ -> member(handle, poolId, instanceId, pool))));
    }

    public Optional<Obligations> obligations(String poolId, String instanceId)
    {
        return jdbi.withHandle(handle -> memberRow(handle, poolId, instanceId).map(_ -> obligations(handle, poolId, instanceId)));
    }

    public Optional<FailureReceipt> failureReceipt(String poolId, String instanceId)
    {
        return jdbi.withHandle(handle -> memberRow(handle, poolId, instanceId).flatMap(row -> handle.createQuery(
                        """
                        SELECT r.evidence, r.detail::text AS detail, r.outstanding_admissions, r.outstanding_transactions,
                          r.outstanding_queries, r.recorded_at
                        FROM pool_failure_receipt r WHERE r.incarnation = :id
                        """).bind("id", row.incarnation)
                .map((rs, _) -> new FailureReceipt(
                        PROTOCOL_VERSION,
                        poolId,
                        instanceId,
                        row.incarnation,
                        rs.getString("evidence"),
                        readTree(rs.getString("detail")),
                        rs.getLong("outstanding_admissions"),
                        rs.getLong("outstanding_transactions"),
                        rs.getLong("outstanding_queries"),
                        String.valueOf(rs.getObject("recorded_at"))))
                .findOne()));
    }

    public Optional<Publication> publication(String poolId, String publicationId)
    {
        return jdbi.withHandle(handle -> publicationRow(handle, publicationId)
                .filter(row -> row.poolId.equals(poolId))
                .map(_ -> publication(handle, publicationId)));
    }

    public Optional<TenantAdmission> tenantAdmission(String poolId, String tenant)
    {
        return jdbi.withHandle(handle -> handle.createQuery("SELECT 1 FROM pool_tenant_admission WHERE pool_id = :pool AND tenant = :tenant")
                .bind("pool", poolId).bind("tenant", tenant).mapTo(Integer.class).findOne()
                .map(_ -> tenantAdmission(handle, poolId, tenant, false, true)));
    }

    public OperationHistory operationHistory(String poolId, String operationId)
    {
        validateOperationIdentity(operationId, "operationId");
        List<OperationStep> steps = jdbi.withHandle(handle -> handle.createQuery(
                        """
                        SELECT step_id, payload_hash, epoch, outcome, recorded_at, result::text AS result
                        FROM pool_operation WHERE pool_id = :pool AND operation_id = :operation ORDER BY recorded_at, step_id
                        """).bind("pool", poolId).bind("operation", operationId)
                .map((rs, _) -> new OperationStep(
                        rs.getString("step_id"),
                        rs.getString("payload_hash"),
                        rs.getLong("epoch"),
                        rs.getString("outcome"),
                        String.valueOf(rs.getObject("recorded_at")),
                        readTree(rs.getString("result"))))
                .list());
        return new OperationHistory(PROTOCOL_VERSION, operationId, steps);
    }

    /**
     * Whether this routing group is pooled, and if so its ACTIVE members, read in one round trip.
     * <p>
     * One statement answers both questions so the request path does not pay two reads to discover
     * that a routing group is legacy. The result is a snapshot and is treated as advisory: the
     * admission itself re-validates the pool mode and the member's phase under the pool's share lock,
     * so a mode or phase change that races this read cannot admit work to an ineligible member. No
     * cross-replica cache is introduced, and no result is reused across requests.
     */
    public PoolRouting routing(String poolId)
    {
        return jdbi.withHandle(handle -> {
            List<Candidate> candidates = new ArrayList<>();
            boolean[] pooled = {false};
            handle.createQuery(
                            """
                            SELECT p.api_mode, b.current_name, b.incarnation, b.instance_id, b.backend_url
                            FROM pool p
                            LEFT JOIN transaction_backend b
                              ON b.pool_id = p.pool_id AND b.state = 'ACTIVE' AND b.current_name IS NOT NULL
                            WHERE p.pool_id = :pool
                            """).bind("pool", poolId)
                    .map((rs, _) -> {
                        pooled[0] = "POOLED".equals(rs.getString("api_mode"));
                        String name = rs.getString("current_name");
                        return name == null
                                ? Optional.<Candidate>empty()
                                : Optional.of(new Candidate(
                                name,
                                rs.getObject("incarnation", UUID.class),
                                rs.getString("instance_id"),
                                rs.getString("backend_url")));
                    })
                    .forEach(candidate -> candidate.ifPresent(candidates::add));
            return new PoolRouting(pooled[0], pooled[0] ? List.copyOf(candidates) : List.of());
        });
    }

    /**
     * An advisory routing snapshot: pool mode plus the members that were ACTIVE when it was read.
     */
    public record PoolRouting(boolean pooled, List<Candidate> candidates) {}

    /**
     * Candidates that are ACTIVE in the authoritative store. Cached health may only order these, and
     * the admission itself re-validates the phase under the pool's share lock.
     */
    public List<Candidate> eligibleCandidates(String poolId)
    {
        return routing(poolId).candidates();
    }

    /**
     * True when this routing group is a pool operating under the versioned member-lifecycle protocol.
     */
    public boolean isPooled(String poolId)
    {
        return routing(poolId).pooled();
    }

    /**
     * Backend names whose pooled member has left service for good and must not be monitored any more.
     */
    public List<String> unmonitoredBackendNames()
    {
        return jdbi.withHandle(handle -> handle.createQuery(
                """
                SELECT current_name FROM transaction_backend
                WHERE pool_id IS NOT NULL AND current_name IS NOT NULL AND state IN ('RETIRING', 'RETIRED', 'LOST')
                """).mapTo(String.class).list());
    }

    // ------------------------------------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------------------------------------

    private interface PoolAction<T>
    {
        T apply(Handle handle, Row pool);
    }

    /**
     * Runs one idempotent step inside a single transaction: pool row lock, epoch fence, replay
     * resolution, the effect itself, and the durable step receipt.
     */
    private <T> T inPool(String poolId, Guard guard, Class<T> resultType, PoolAction<T> action)
    {
        validatePoolId(poolId);
        return jdbi.inTransaction(handle -> {
            TransactionStore.lockRoute(handle, poolId);
            Row pool = poolRow(handle, poolId).orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool"));
            lockedPool(handle, poolId);
            // An already recorded outcome is resolved before the authority fence, and applies nothing.
            // Otherwise a step committed by a previous leader could never be read back after a takeover
            // raised the epoch, and the operation it belongs to could not be resumed. Reading back a
            // committed result is not a new effect, so it needs no current authority.
            Optional<JsonNode> replay = recordedStep(handle, poolId, guard);
            if (replay.isPresent()) {
                return decode(replay.orElseThrow(), resultType, true);
            }
            check(guard.controllerEpoch() == pool.epoch, POOL_STALE_EPOCH, "controllerEpoch does not match the recorded authority epoch");
            // Once a pool has a recorded owner, presenting it is mandatory: an omitted identity would
            // otherwise be a fallback that any equal-epoch caller could use to bypass the fence. A pool
            // that has never recorded an owner stays compatible with a client that does not send one.
            check(pool.ownerIdentity == null || Objects.equals(pool.ownerIdentity, guard.ownerIdentity()),
                    POOL_STALE_EPOCH,
                    "This controller identity does not hold the pool authority");
            T result = action.apply(handle, pool);
            recordStep(handle, poolId, guard, result);
            return result;
        });
    }

    private Optional<JsonNode> recordedStep(Handle handle, String poolId, Guard guard)
    {
        return recordedStep(handle, poolId, guard, false);
    }

    /**
     * @param authorityMustMatch for a step whose purpose is to hold or take authority. Such a step only
     *         resolves to its recorded result for the same authority that recorded it: replaying it to
     *         a different epoch would report an authority the caller never acquired. Ordinary effects
     *         resolve regardless of the presented epoch, which is what lets the next leader resume
     *         work its predecessor already committed.
     */
    private Optional<JsonNode> recordedStep(Handle handle, String poolId, Guard guard, boolean authorityMustMatch)
    {
        record Recorded(String payloadHash, long epoch, String result) {}

        return handle.createQuery(
                        """
                        SELECT payload_hash, epoch, result::text AS result FROM pool_operation
                        WHERE pool_id = :pool AND operation_id = :operation AND step_id = :step FOR UPDATE
                        """)
                .bind("pool", poolId).bind("operation", guard.operationId()).bind("step", guard.stepId())
                .map((rs, _) -> new Recorded(rs.getString("payload_hash"), rs.getLong("epoch"), rs.getString("result")))
                .findOne()
                .map(recorded -> {
                    check(recorded.payloadHash().equals(guard.payloadHash()), POOL_INTENT_CHANGED, "This operation step was recorded with a different intent");
                    check(!authorityMustMatch || recorded.epoch() == guard.controllerEpoch(),
                            POOL_INTENT_CHANGED,
                            "This step recorded a different authority; acquiring authority needs its own step identity");
                    return readTree(recorded.result());
                });
    }

    private void recordStep(Handle handle, String poolId, Guard guard, Object result)
    {
        String encoded = encode(result);
        check(encoded.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= MAX_RESULT_BYTES, POOL_VALIDATION, "The recorded step result exceeds its bound");
        handle.createUpdate(
                        """
                        INSERT INTO pool_operation (operation_id, step_id, pool_id, payload_hash, epoch, outcome, result)
                        VALUES (:operation, :step, :pool, :hash, :epoch, 'OK', CAST(:result AS jsonb))
                        """)
                .bind("operation", guard.operationId()).bind("step", guard.stepId()).bind("pool", poolId)
                .bind("hash", guard.payloadHash()).bind("epoch", guard.controllerEpoch()).bind("result", encoded)
                .execute();
    }

    private static void ensurePoolRow(Handle handle, String poolId)
    {
        handle.createUpdate("INSERT INTO pool (pool_id) VALUES (:id) ON CONFLICT DO NOTHING").bind("id", poolId).execute();
    }

    private static Row lockedPool(Handle handle, String poolId)
    {
        return handle.createQuery("SELECT * FROM pool WHERE pool_id = :id FOR UPDATE").bind("id", poolId)
                .map((rs, _) -> Row.pool(rs)).findOne().orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool"));
    }

    private static Optional<Row> poolRow(Handle handle, String poolId)
    {
        return handle.createQuery("SELECT * FROM pool WHERE pool_id = :id").bind("id", poolId).map((rs, _) -> Row.pool(rs)).findOne();
    }

    private static void requirePooled(Row pool)
    {
        check(pool.apiMode.equals("POOLED"), POOL_APIMODE, "This routing group is not in pooled mode");
    }

    private static Optional<Row> memberRow(Handle handle, String poolId, String instanceId)
    {
        return handle.createQuery("SELECT * FROM transaction_backend WHERE pool_id = :pool AND instance_id = :instance")
                .bind("pool", poolId).bind("instance", instanceId).map((rs, _) -> Row.member(rs)).findOne();
    }

    private static Row lockedMember(Handle handle, String poolId, String instanceId)
    {
        return handle.createQuery("SELECT * FROM transaction_backend WHERE pool_id = :pool AND instance_id = :instance FOR UPDATE")
                .bind("pool", poolId).bind("instance", instanceId).map((rs, _) -> Row.member(rs)).findOne()
                .orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool member"));
    }

    private static void requireGeneration(Row member, long expectedGeneration)
    {
        check(member.generation == expectedGeneration, POOL_STALE_GENERATION, "The member generation changed");
    }

    private static long countMembers(Handle handle, String poolId, Set<String> phases)
    {
        return handle.createQuery("SELECT count(*) FROM transaction_backend WHERE pool_id = :pool AND state = ANY(:phases)")
                .bind("pool", poolId).bindArray("phases", String.class, phases.toArray(String[]::new)).mapTo(Long.class).one();
    }

    private static long countRepair(Handle handle, String poolId)
    {
        return handle.createQuery("SELECT count(*) FROM transaction_backend WHERE pool_id = :pool AND repair AND state = ANY(:phases)")
                .bind("pool", poolId).bindArray("phases", String.class, LIVE_PHASES.toArray(String[]::new)).mapTo(Long.class).one();
    }

    private static List<String> activeInstanceIds(Handle handle, String poolId)
    {
        return handle.createQuery("SELECT instance_id FROM transaction_backend WHERE pool_id = :pool AND state = 'ACTIVE' ORDER BY instance_id")
                .bind("pool", poolId).mapTo(String.class).list();
    }

    private static long bumpMembership(Handle handle, String poolId)
    {
        return handle.createQuery("UPDATE pool SET membership_generation = membership_generation + 1 WHERE pool_id = :pool RETURNING membership_generation")
                .bind("pool", poolId).mapTo(Long.class).one();
    }

    private static PoolState poolState(Handle handle, String poolId)
    {
        Row pool = poolRow(handle, poolId).orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool"));
        Map<String, Long> counts = new LinkedHashMap<>();
        ALL_PHASES.forEach(phase -> counts.put(phase, 0L));
        handle.createQuery("SELECT state, count(*) AS total FROM transaction_backend WHERE pool_id = :pool GROUP BY state")
                .bind("pool", poolId).map((rs, _) -> Map.entry(rs.getString("state"), rs.getLong("total")))
                .list().forEach(entry -> counts.put(entry.getKey(), entry.getValue()));
        long serving = counts.getOrDefault("ACTIVE", 0L);
        long repair = countRepair(handle, poolId);
        long live = countMembers(handle, poolId, LIVE_PHASES) - repair;
        long openPublications = handle.createQuery("SELECT count(*) FROM pool_publication WHERE pool_id = :pool AND phase = 'OPEN'")
                .bind("pool", poolId).mapTo(Long.class).one();
        List<String> blocked = new ArrayList<>();
        if (pool.apiMode.equals("POOLED") && serving < pool.minServing) {
            blocked.add("SERVING_BELOW_FLOOR");
        }
        if (counts.getOrDefault("SUSPECT", 0L) > 0) {
            blocked.add("SUSPECT_MEMBER");
        }
        if (counts.getOrDefault("RETIRING", 0L) > 0) {
            blocked.add("RETIREMENT_IN_PROGRESS");
        }
        if (openPublications > 0) {
            blocked.add("PUBLICATION_OPEN");
        }
        return new PoolState(
                PROTOCOL_VERSION,
                poolId,
                pool.apiMode,
                pool.epoch,
                pool.membershipGeneration,
                pool.minServing,
                pool.desiredMembers,
                pool.maxSurge,
                pool.maxRepair,
                pool.desiredRevision,
                pool.admittedRevision,
                pool.tenantAdmissionEnabled,
                counts,
                serving,
                live,
                Math.max(0, live - Math.max(pool.desiredMembers, pool.minServing)),
                repair,
                openPublications,
                List.copyOf(blocked),
                false);
    }

    private static Member member(Handle handle, String poolId, String instanceId, Row pool)
    {
        Row row = memberRow(handle, poolId, instanceId).orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool member"));
        Obligations obligations = obligations(handle, poolId, instanceId);
        return new Member(
                PROTOCOL_VERSION,
                poolId,
                instanceId,
                row.incarnation,
                row.currentName,
                row.url,
                row.externalUrl,
                row.state,
                row.generation,
                pool.epoch,
                row.podUid,
                row.bootId,
                row.nodeId,
                row.coordinatorId,
                row.configRevision,
                row.certifiedRevision,
                row.authRevision,
                row.repair,
                row.repairFor,
                row.retirementKind,
                obligations.pendingRequests(),
                obligations.openTransactions(),
                obligations.activeQueries(),
                obligations.readyToSeal(),
                obligations.drained(),
                row.state.equals("ACTIVE"),
                pool.membershipGeneration,
                false);
    }

    private static Obligations obligations(Handle handle, String poolId, String instanceId)
    {
        Row row = memberRow(handle, poolId, instanceId).orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown pool member"));
        return handle.createQuery(TransactionStore.DRAIN_STATUS_SQL).bind("id", row.incarnation)
                .map((rs, _) -> {
                    String state = rs.getString("state");
                    long pending = rs.getLong("pending");
                    long transactions = rs.getLong("transactions");
                    long queries = rs.getLong("queries");
                    return new Obligations(
                            PROTOCOL_VERSION,
                            instanceId,
                            row.incarnation,
                            state,
                            rs.getLong("generation"),
                            pending,
                            transactions,
                            queries,
                            state.equals("DRAINING") && pending == 0 && transactions == 0 && queries == 0,
                            // A failure-retired member is never reported as successfully drained, however
                            // its obligations ended, and a lost process is never drained at all.
                            state.equals("SEALED") || (IRREVERSIBLE_PHASES.contains(state) && "DRAINED".equals(row.retirementKind)));
                }).one();
    }

    private static Optional<Row> failureReceiptRow(Handle handle, UUID incarnation)
    {
        return handle.createQuery("SELECT evidence FROM pool_failure_receipt WHERE incarnation = :id").bind("id", incarnation)
                .map((rs, _) -> Row.evidence(rs.getString("evidence"))).findOne();
    }

    private static void insertPublicationReceipt(Handle handle, String publicationId, UUID incarnation, String podUid, String bootId, String appliedRevision, String authFingerprint)
    {
        handle.createUpdate(
                        """
                        INSERT INTO pool_publication_receipt (publication_id, incarnation, pod_uid, boot_id, applied_revision, auth_fingerprint)
                        VALUES (:publication, :incarnation, :pod, :boot, :revision, :fingerprint)
                        ON CONFLICT (publication_id, incarnation) DO UPDATE SET pod_uid = EXCLUDED.pod_uid, boot_id = EXCLUDED.boot_id,
                          applied_revision = EXCLUDED.applied_revision, auth_fingerprint = EXCLUDED.auth_fingerprint, recorded_at = clock_timestamp()
                        """)
                .bind("publication", publicationId).bind("incarnation", incarnation).bind("pod", podUid).bind("boot", bootId)
                .bind("revision", appliedRevision).bind("fingerprint", authFingerprint).execute();
    }

    private static Optional<Row> publicationRow(Handle handle, String publicationId)
    {
        return handle.createQuery("SELECT * FROM pool_publication WHERE publication_id = :id FOR UPDATE").bind("id", publicationId)
                .map((rs, _) -> Row.publication(rs)).findOne();
    }

    private static Row requirePublication(Handle handle, String poolId, String publicationId)
    {
        Row row = publicationRow(handle, publicationId).orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown publication"));
        check(row.poolId.equals(poolId), POOL_NOT_FOUND, "Unknown publication");
        return row;
    }

    private static Optional<Row> openPublicationRow(Handle handle, String poolId)
    {
        return handle.createQuery("SELECT * FROM pool_publication WHERE pool_id = :pool AND phase = 'OPEN' ORDER BY created_at LIMIT 1")
                .bind("pool", poolId).map((rs, _) -> Row.publication(rs)).findOne();
    }

    private static Optional<Row> openPublicationRowForTenant(Handle handle, String poolId, String tenant)
    {
        return handle.createQuery("SELECT * FROM pool_publication WHERE pool_id = :pool AND tenant = :tenant AND phase = 'OPEN'")
                .bind("pool", poolId).bind("tenant", tenant).map((rs, _) -> Row.publication(rs)).findOne();
    }

    private static List<String> missingMembers(Handle handle, String poolId, String publicationId)
    {
        return handle.createQuery(
                """
                SELECT b.instance_id FROM transaction_backend b
                WHERE b.pool_id = :pool AND b.state = 'ACTIVE' AND NOT EXISTS (
                  SELECT 1 FROM pool_publication_receipt r JOIN pool_publication p ON p.publication_id = r.publication_id
                  WHERE r.publication_id = :publication AND r.incarnation = b.incarnation
                    AND r.pod_uid = b.pod_uid AND r.boot_id = b.boot_id AND r.applied_revision = p.target_revision)
                ORDER BY b.instance_id
                """).bind("pool", poolId).bind("publication", publicationId).mapTo(String.class).list();
    }

    private static Publication publication(Handle handle, String publicationId)
    {
        Row row = handle.createQuery("SELECT * FROM pool_publication WHERE publication_id = :id").bind("id", publicationId)
                .map((rs, _) -> Row.publication(rs)).findOne().orElseThrow(() -> new PoolException(POOL_NOT_FOUND, "Unknown publication"));
        List<PublicationReceipt> receipts = handle.createQuery(
                        """
                        SELECT b.instance_id, r.incarnation, r.pod_uid, r.boot_id, r.applied_revision, r.auth_fingerprint
                        FROM pool_publication_receipt r JOIN transaction_backend b ON b.incarnation = r.incarnation
                        WHERE r.publication_id = :id ORDER BY b.instance_id
                        """).bind("id", publicationId)
                .map((rs, _) -> new PublicationReceipt(
                        rs.getString("instance_id"),
                        rs.getObject("incarnation", UUID.class),
                        rs.getString("pod_uid"),
                        rs.getString("boot_id"),
                        rs.getString("applied_revision"),
                        rs.getString("auth_fingerprint")))
                .list();
        TenantAdmission admission = tenantAdmission(handle, row.poolId, row.tenant, false, false);
        return new Publication(
                PROTOCOL_VERSION,
                publicationId,
                row.poolId,
                row.tenant,
                row.targetRevision,
                row.membershipGeneration,
                row.phase,
                decodeStrings(row.requiredMembers),
                receipts,
                missingMembers(handle, row.poolId, publicationId),
                admission.state(),
                admission.admittedRevision(),
                false);
    }

    /**
     * @param includePrincipals only for a read. A result that is recorded as an idempotent step must
     *         not grow with a tenant's login count, so a write echoes the count and digest instead.
     */
    private static TenantAdmission tenantAdmission(Handle handle, String poolId, String tenant, boolean replayed, boolean includePrincipals)
    {
        record Summary(String revision, int count, String hash) {}

        Summary summary = handle.createQuery(
                        """
                        SELECT min(revision) AS revision, count(*) AS principal_count,
                          encode(sha256(coalesce(string_agg(principal, E'\\n' ORDER BY principal), '')::bytea), 'hex') AS principals_hash
                        FROM pool_tenant_principal WHERE pool_id = :pool AND tenant = :tenant
                        """)
                .bind("pool", poolId).bind("tenant", tenant)
                .map((rs, _) -> new Summary(rs.getString("revision"), rs.getInt("principal_count"), rs.getString("principals_hash")))
                .one();
        List<String> principals = includePrincipals && summary.count() > 0
                ? handle.createQuery(
                "SELECT principal FROM pool_tenant_principal WHERE pool_id = :pool AND tenant = :tenant ORDER BY principal")
                  .bind("pool", poolId).bind("tenant", tenant).mapTo(String.class).list()
                : List.of();
        String revision = summary.revision();
        int principalCount = summary.count();
        String principalsHash = principalCount == 0 ? null : summary.hash();
        return handle.createQuery("SELECT state, admitted_revision, publication_id FROM pool_tenant_admission WHERE pool_id = :pool AND tenant = :tenant")
                .bind("pool", poolId).bind("tenant", tenant)
                .map((rs, _) -> new TenantAdmission(
                        PROTOCOL_VERSION,
                        poolId,
                        tenant,
                        rs.getString("state"),
                        rs.getString("admitted_revision"),
                        rs.getString("publication_id"),
                        revision,
                        principalCount,
                        principalsHash,
                        principals,
                        replayed))
                .findOne()
                .orElseGet(() -> new TenantAdmission(
                        PROTOCOL_VERSION, poolId, tenant, "PENDING", null, null, revision, principalCount, principalsHash, principals, replayed));
    }

    /**
     * A narrow projection of the rows this store reads, so every query maps through one place.
     */
    private record Row(
            String poolId,
            long epoch,
            @Nullable String ownerIdentity,
            String apiMode,
            int minServing,
            int desiredMembers,
            int maxSurge,
            int maxRepair,
            long membershipGeneration,
            String desiredRevision,
            String admittedRevision,
            boolean tenantAdmissionEnabled,
            UUID incarnation,
            String currentName,
            String url,
            String externalUrl,
            String state,
            long generation,
            String nodeId,
            String coordinatorId,
            String podUid,
            String bootId,
            String configRevision,
            String certifiedRevision,
            String authRevision,
            String retirementKind,
            boolean repair,
            String repairFor,
            String publicationId,
            String tenant,
            String targetRevision,
            String phase,
            String payloadHash,
            String requiredMembers)
    {
        static Row pool(ResultSet rs)
                throws SQLException
        {
            return new Row(
                    rs.getString("pool_id"),
                    rs.getLong("epoch"),
                    rs.getString("owner_identity"),
                    rs.getString("api_mode"),
                    rs.getInt("min_serving"),
                    rs.getInt("desired_members"),
                    rs.getInt("max_surge"),
                    rs.getInt("max_repair"),
                    rs.getLong("membership_generation"),
                    rs.getString("desired_revision"),
                    rs.getString("admitted_revision"),
                    rs.getBoolean("tenant_admission_enabled"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
        }

        static Row member(ResultSet rs)
                throws SQLException
        {
            return new Row(
                    rs.getString("pool_id"),
                    0,
                    null,
                    "POOLED",
                    0,
                    0,
                    0,
                    0,
                    0,
                    null,
                    null,
                    false,
                    rs.getObject("incarnation", UUID.class),
                    rs.getString("current_name"),
                    rs.getString("backend_url"),
                    rs.getString("external_url"),
                    rs.getString("state"),
                    rs.getLong("generation"),
                    rs.getString("node_id"),
                    rs.getString("coordinator_id"),
                    rs.getString("pod_uid"),
                    rs.getString("boot_id"),
                    rs.getString("config_revision"),
                    rs.getString("certified_revision"),
                    rs.getString("auth_revision"),
                    rs.getString("retirement_kind"),
                    rs.getBoolean("repair"),
                    rs.getString("repair_for"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
        }

        static Row publication(ResultSet rs)
                throws SQLException
        {
            return new Row(
                    rs.getString("pool_id"),
                    0,
                    null,
                    "POOLED",
                    0,
                    0,
                    0,
                    0,
                    rs.getLong("membership_generation"),
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    null,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null,
                    rs.getString("publication_id"),
                    rs.getString("tenant"),
                    rs.getString("target_revision"),
                    rs.getString("phase"),
                    rs.getString("payload_hash"),
                    rs.getString("required_members"));
        }

        static Row evidence(String evidence)
        {
            return new Row(
                    null,
                    0,
                    null,
                    "POOLED",
                    0,
                    0,
                    0,
                    0,
                    0,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    evidence,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
        }

        Row withMembership(long membershipGeneration)
        {
            return new Row(
                    poolId,
                    epoch,
                    ownerIdentity,
                    apiMode,
                    minServing,
                    desiredMembers,
                    maxSurge,
                    maxRepair,
                    membershipGeneration,
                    desiredRevision,
                    admittedRevision,
                    tenantAdmissionEnabled,
                    incarnation,
                    currentName,
                    url,
                    externalUrl,
                    state,
                    generation,
                    nodeId,
                    coordinatorId,
                    podUid,
                    bootId,
                    configRevision,
                    certifiedRevision,
                    authRevision,
                    retirementKind,
                    repair,
                    repairFor,
                    publicationId,
                    tenant,
                    targetRevision,
                    phase,
                    payloadHash,
                    requiredMembers);
        }
    }

    private static String trimSlashes(String value)
    {
        return value.replaceAll("/+$", "");
    }

    private static void validatePoolId(String poolId)
    {
        check(poolId != null && !poolId.isBlank() && poolId.length() <= 256 && poolId.codePoints().noneMatch(Character::isISOControl),
                POOL_VALIDATION,
                "poolId is invalid");
    }

    private static void validateInstanceId(String instanceId)
    {
        check(instanceId != null && instanceId.matches("[A-Za-z0-9_.:-]{1,256}"), POOL_VALIDATION, "instanceId is invalid");
    }

    private static void validateOperationIdentity(String value, String field)
    {
        check(value != null && value.matches("[A-Za-z0-9_.:-]{1,256}"), POOL_VALIDATION, field + " is invalid");
    }

    private static void validateIdentityField(String value, String field)
    {
        check(value != null && !value.isBlank() && value.length() <= 256 && value.codePoints().noneMatch(Character::isISOControl),
                POOL_VALIDATION,
                field + " is required");
    }

    private static void validateRevision(String value, String field)
    {
        check(value != null && value.length() <= 128 && value.codePoints().noneMatch(Character::isISOControl), POOL_VALIDATION, field + " is invalid");
    }

    private static void validateTenant(String tenant)
    {
        check(tenant != null && !tenant.isBlank() && tenant.length() <= 256 && tenant.codePoints().noneMatch(Character::isISOControl),
                POOL_VALIDATION,
                "tenant is invalid");
    }

    private static String encode(Object value)
    {
        try {
            return JSON.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new PoolException(POOL_VALIDATION, "The pool document cannot be encoded");
        }
    }

    private static JsonNode readTree(String value)
    {
        try {
            return JSON.readTree(value);
        }
        catch (JsonProcessingException e) {
            throw new PoolException(POOL_VALIDATION, "A stored pool document is invalid");
        }
    }

    private static List<String> decodeStrings(String value)
    {
        JsonNode node = readTree(value);
        List<String> values = new ArrayList<>();
        node.forEach(element -> values.add(element.asText()));
        return List.copyOf(values);
    }

    private static <T> T decode(JsonNode document, Class<T> type, boolean replayed)
    {
        try {
            T value = JSON.treeToValue(document, type);
            return replayed ? markReplayed(value) : value;
        }
        catch (JsonProcessingException e) {
            throw new PoolException(POOL_VALIDATION, "A recorded step result is invalid");
        }
    }

    /**
     * Recorded results are returned verbatim, flagged so a caller can tell a replay from a fresh effect.
     */
    @SuppressWarnings("unchecked")
    private static <T> T markReplayed(T value)
    {
        Function<T, T> replay = switch (value) {
            case Member member -> _ -> (T) new Member(
                    member.protocolVersion(),
                    member.poolId(),
                    member.instanceId(),
                    member.incarnation(),
                    member.backendName(),
                    member.url(),
                    member.externalUrl(),
                    member.phase(),
                    member.generation(),
                    member.controllerEpoch(),
                    member.podUid(),
                    member.bootId(),
                    member.nodeId(),
                    member.coordinatorId(),
                    member.configRevision(),
                    member.certifiedRevision(),
                    member.authRevision(),
                    member.repair(),
                    member.repairFor(),
                    member.retirementKind(),
                    member.pendingRequests(),
                    member.openTransactions(),
                    member.activeQueries(),
                    member.readyToSeal(),
                    member.drained(),
                    member.eligible(),
                    member.membershipGeneration(),
                    true);
            case PoolState state -> _ -> (T) new PoolState(
                    state.protocolVersion(),
                    state.poolId(),
                    state.apiMode(),
                    state.controllerEpoch(),
                    state.membershipGeneration(),
                    state.minServing(),
                    state.desiredMembers(),
                    state.maxSurge(),
                    state.maxRepair(),
                    state.desiredRevision(),
                    state.admittedRevision(),
                    state.tenantAdmissionEnabled(),
                    state.counts(),
                    state.servingMembers(),
                    state.liveMembers(),
                    state.surgeInUse(),
                    state.repairInUse(),
                    state.openPublications(),
                    state.blocked(),
                    true);
            case Publication publication -> _ -> (T) new Publication(
                    publication.protocolVersion(),
                    publication.publicationId(),
                    publication.poolId(),
                    publication.tenant(),
                    publication.targetRevision(),
                    publication.membershipGeneration(),
                    publication.phase(),
                    publication.requiredMembers(),
                    publication.receipts(),
                    publication.missingMembers(),
                    publication.tenantState(),
                    publication.admittedRevision(),
                    true);
            case TenantAdmission admission -> _ -> (T) new TenantAdmission(
                    admission.protocolVersion(),
                    admission.poolId(),
                    admission.tenant(),
                    admission.state(),
                    admission.admittedRevision(),
                    admission.publicationId(),
                    admission.principalRevision(),
                    admission.principalCount(),
                    admission.principalsHash(),
                    admission.principals(),
                    true);
            default -> Function.identity();
        };
        return replay.apply(value);
    }

    private static void check(boolean condition, PoolErrorCode code, String message)
    {
        if (!condition) {
            throw new PoolException(code, message);
        }
    }
}
