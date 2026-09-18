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

import io.trino.gateway.ha.transaction.PoolStore.Guard;
import io.trino.gateway.ha.transaction.PoolStore.Member;
import io.trino.gateway.ha.transaction.PoolStore.PoolException;
import io.trino.gateway.ha.transaction.PoolStore.PoolSpec;
import io.trino.gateway.ha.transaction.PoolStore.Termination;
import io.trino.gateway.ha.transaction.PoolStore.ValidationReceipt;
import io.trino.gateway.ha.transaction.TransactionStore.ResponseObservation;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_APIMODE;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_EVIDENCE_REQUIRED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_IDENTITY_CONFLICT;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_INTENT_CHANGED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_IRREVERSIBLE;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_MEMBERSHIP_CHANGED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_NOT_CERTIFIED;
import static io.trino.gateway.ha.transaction.PoolStore.PoolErrorCode.POOL_NOT_DRAINED;
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
import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Pooled member lifecycle contract against a real PostgreSQL database.
 * <p>
 * {@code first} and {@code second} are independent store instances with their own connections,
 * standing in for two Gateway processes sharing one database. Concurrency assertions use both.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
class TestPoolStore
{
    private static final String POOL = "pool-a";
    private static final String CHECKS_HASH = "a".repeat(64);
    private static final String AUTH_FINGERPRINT = "b".repeat(64);
    private static final String PLAN_HASH = "c".repeat(64);
    private static final int FRESHNESS = 300;

    private final String schema = "pool_store_test_" + UUID.randomUUID().toString().replace("-", "");
    private JdbcDatabaseContainer<?> container;
    private Jdbi admin;
    private Jdbi database;
    private PoolStore first;
    private PoolStore second;
    private TransactionStore transactions;
    private boolean schemaCreated;
    private long epoch;

    @BeforeAll
    void setupDatabase()
            throws IOException
    {
        String url = System.getenv("TX_STORE_TEST_JDBC_URL");
        String username;
        String password;
        if (url == null) {
            container = createPostgreSqlContainer();
            container.start();
            url = container.getJdbcUrl();
            username = container.getUsername();
            password = container.getPassword();
        }
        else {
            username = requireNonNull(System.getenv("TX_STORE_TEST_USERNAME"), "TX_STORE_TEST_USERNAME is required");
            password = System.getenv().getOrDefault("TX_STORE_TEST_PASSWORD", "");
        }
        admin = Jdbi.create(url, username, password);
        admin.useHandle(handle -> handle.execute("CREATE SCHEMA " + schema));
        schemaCreated = true;
        database = Jdbi.create(url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema, username, password);
        for (String version : new String[] {
                "V5__transaction_awareness.sql", "V6__backend_incarnation_history.sql", "V7__query_capabilities.sql",
                "V8__drain_obligation_indexes.sql", "V9__cell_rollout_operations.sql", "V10__pool_member_lifecycle.sql",
                "V11__pool_tenant_principals.sql",
        }) {
            try (var migration = requireNonNull(getClass().getResourceAsStream("/postgresql/" + version))) {
                String sql = new String(migration.readAllBytes(), StandardCharsets.UTF_8);
                database.useHandle(handle -> handle.createScript(sql).execute());
            }
        }
        first = new PoolStore(database);
        second = new PoolStore(database);
        transactions = new TransactionStore(database);
    }

    @AfterAll
    void cleanupDatabase()
    {
        try {
            if (schemaCreated) {
                admin.useHandle(handle -> handle.execute("DROP SCHEMA " + schema + " CASCADE"));
            }
        }
        finally {
            if (container != null) {
                container.close();
            }
        }
    }

    @BeforeEach
    void resetLedger()
    {
        database.useHandle(handle -> handle.execute(
                """
                TRUNCATE pool_tenant_principal, pool_publication_receipt, pool_publication, pool_tenant_admission, pool_failure_receipt,
                  pool_member_certificate, pool_operation, pool, transaction_rollout, transaction_route,
                  transaction_admission, transaction_query_capability, transaction_query, transaction_binding, transaction_backend
                """));
        epoch = 7;
        configure(3, 3, 1, 1);
    }

    // --------------------------------------------------------------------------------------------
    // Registration and certified activation
    // --------------------------------------------------------------------------------------------

    @Test
    void registrationCreatesAnUnroutableMemberThatCannotAdmitWork()
    {
        Member member = register("i-1");
        assertThat(member.phase()).isEqualTo("PREPARING");
        assertThat(member.eligible()).isFalse();
        assertThat(member.certifiedRevision()).isNull();
        assertThat(first.eligibleCandidates(POOL)).isEmpty();
        assertThatThrownBy(() -> transactions.admitPooledMember(POOL, "backend-i-1", "owner"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.NOT_ACTIVE));
    }

    @Test
    void activationRequiresACertificateBoundToTheObservedProcessAndRevision()
    {
        Member member = register("i-1");
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-a", "admit"),
                member.generation(),
                receipt("r-1", "i-1"),
                "node-i-1",
                "other",
                FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-b", "admit"),
                member.generation(),
                receipt("r-other", "i-1"),
                "node-i-1",
                "coord-i-1",
                FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-c", "admit"),
                member.generation(),
                new ValidationReceipt(CHECKS_HASH, "r-1", "a-1", "pod-i-1", "boot-other", "node-i-1", "coord-i-1", 4, List.of("image", "workers", "catalog-revision", "auth-revision")),
                "node-i-1",
                "coord-i-1",
                FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        assertThat(first.member(POOL, "i-1").orElseThrow().phase()).isEqualTo("PREPARING");

        Member admitted = admit("i-1");
        assertThat(admitted.phase()).isEqualTo("ACTIVE");
        assertThat(admitted.eligible()).isTrue();
        assertThat(admitted.certifiedRevision()).isEqualTo("r-1");
        assertThat(admitted.generation()).isEqualTo(member.generation() + 1);
        assertThat(first.eligibleCandidates(POOL)).extracting(PoolStore.Candidate::instanceId).containsExactly("i-1");
    }

    @Test
    void aCertificateWithoutReadyWorkersCannotActivate()
    {
        Member member = register("i-1");
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-a", "admit"),
                member.generation(),
                new ValidationReceipt(CHECKS_HASH, "r-1", "a-1", "pod-i-1", "boot-i-1", "node-i-1", "coord-i-1", 0, List.of("image", "workers", "catalog-revision", "auth-revision")),
                "node-i-1",
                "coord-i-1",
                FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
    }

    @Test
    void theOperatorChecksAreRecordedVerbatimForReadBack()
    {
        register("i-1");
        admit("i-1");
        List<String> recorded = database.withHandle(handle -> handle.createQuery(
                        "SELECT checks::text FROM pool_member_certificate WHERE config_revision = 'r-1'")
                .mapTo(String.class).list());
        assertThat(recorded).hasSize(1);
        assertThat(recorded.getFirst()).contains("catalog-revision").contains("workers");
    }

    // --------------------------------------------------------------------------------------------
    // Idempotent steps, epochs and generations
    // --------------------------------------------------------------------------------------------

    @Test
    void anIdenticalReplayReturnsTheRecordedResultAndAChangedIntentConflicts()
    {
        Member member = register("i-1");
        Member replay = first.registerMember(POOL, guard("op-i-1-register", "register"), registration("i-1"));
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.incarnation()).isEqualTo(member.incarnation());
        assertThat(first.members(POOL)).hasSize(1);

        assertThatThrownBy(() -> second.registerMember(POOL, new Guard("op-i-1-register", "register", epoch, "d".repeat(64)), registration("i-2")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_INTENT_CHANGED));
        assertThat(first.members(POOL)).hasSize(1);
    }

    @Test
    void aLostResponseIsResolvedByOperationReadBackRatherThanANewIdentity()
    {
        Member member = register("i-1");
        PoolStore.OperationHistory history = second.operationHistory(POOL, "op-i-1-register");
        assertThat(history.steps()).hasSize(1);
        assertThat(history.steps().getFirst().stepId()).isEqualTo("register");
        assertThat(history.steps().getFirst().result().path("incarnation").asText()).isEqualTo(member.incarnation().toString());
        assertThat(history.steps().getFirst().outcome()).isEqualTo("OK");
    }

    @Test
    void aStaleControllerEpochCannotMutateAndTakeoverIsExplicit()
    {
        Member member = register("i-1");
        long stale = epoch;
        epoch = 9;
        configure(3, 3, 1, 1);
        assertThatThrownBy(() -> second.drainMember(POOL, "i-1", new Guard("op-stale", "drain", stale, PLAN_HASH), member.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
        assertThatThrownBy(() -> second.configurePool(POOL, new Guard("op-lower", "configure", 1, PLAN_HASH), new PoolSpec("POOLED", 3, 3, 1, 1, "r-1", false), false))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
        assertThat(first.poolState(POOL).orElseThrow().controllerEpoch()).isEqualTo(9);
    }

    @Test
    void aStaleGenerationCannotDriveATransition()
    {
        register("i-1");
        Member active = admit("i-1");
        assertThatThrownBy(() -> second.drainMember(POOL, "i-1", guard("op-x", "drain"), active.generation() - 1))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_GENERATION));
        assertThat(first.member(POOL, "i-1").orElseThrow().phase()).isEqualTo("ACTIVE");
    }

    // --------------------------------------------------------------------------------------------
    // Serving floor, surge and repair budgets
    // --------------------------------------------------------------------------------------------

    @Test
    void bootstrapAdmissionIsNotBlockedByTheMinimumServingFloor()
    {
        serving("i-1");
        serving("i-2");
        PoolStore.PoolState state = first.poolState(POOL).orElseThrow();
        assertThat(state.servingMembers()).isEqualTo(2);
        assertThat(state.blocked()).contains("SERVING_BELOW_FLOOR");
        assertThat(serving("i-3").phase()).isEqualTo("ACTIVE");
        assertThat(first.poolState(POOL).orElseThrow().blocked()).doesNotContain("SERVING_BELOW_FLOOR");
    }

    @Test
    void aPlannedDrainCannotTakeThePoolBelowItsServingFloor()
    {
        configure(2, 3, 1, 1);
        serving("i-1");
        serving("i-2");
        serving("i-3");
        assertThat(drain(first, "i-1", "op-drain-1").phase()).isEqualTo("DRAINING");
        assertThatThrownBy(() -> drain(first, "i-2", "op-drain-2"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SERVING_FLOOR));
        assertThat(first.member(POOL, "i-2").orElseThrow().phase()).isEqualTo("ACTIVE");
    }

    @Test
    void twoConcurrentPlannedDrainsFromSeparateProcessesCannotSpendTheSameCapacity()
            throws Exception
    {
        configure(2, 3, 1, 1);
        serving("i-1");
        serving("i-2");
        serving("i-3");
        try (var executor = Executors.newFixedThreadPool(2)) {
            CountDownLatch barrier = new CountDownLatch(1);
            var attempts = List.of(
                            executor.submit(() -> attemptDrain(first, "i-1", "op-race-1", barrier)),
                            executor.submit(() -> attemptDrain(second, "i-2", "op-race-2", barrier)))
                    .stream().toList();
            barrier.countDown();
            int winners = 0;
            for (var attempt : attempts) {
                if (attempt.get(30, TimeUnit.SECONDS)) {
                    winners++;
                }
            }
            assertThat(winners).isEqualTo(1);
        }
        assertThat(first.poolState(POOL).orElseThrow().servingMembers()).isEqualTo(2);
    }

    @Test
    void registrationIsBoundedByTheSurgeBudget()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        assertThat(register("i-4").phase()).isEqualTo("PREPARING");
        assertThatThrownBy(() -> register("i-5"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SURGE_BUDGET));
    }

    @Test
    void oneRepairIsAllowedBeyondTheSurgeBudgetAndOnlyForAFailedMember()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        register("i-4");
        assertThatThrownBy(() -> register("i-repair-1", "r-1", "i-1"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_REPAIR_BUDGET));
        suspect("i-1");
        Member repair = register("i-repair-1", "r-1", "i-1");
        assertThat(repair.repair()).isTrue();
        assertThat(repair.repairFor()).isEqualTo("i-1");
        suspect("i-2");
        assertThatThrownBy(() -> register("i-repair-2", "r-1", "i-2"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_REPAIR_BUDGET));
        assertThat(first.poolState(POOL).orElseThrow().repairInUse()).isEqualTo(1);
    }

    @Test
    void aSuspectedMemberStillConsumesItsLiveComputeSlot()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        register("i-4");
        // Budget is full: three desired plus one surge. A failed probe is not absence, so suspecting
        // members must not silently free slots for unbounded non-repair registrations.
        suspect("i-1");
        suspect("i-2");
        assertThatThrownBy(() -> register("i-5"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SURGE_BUDGET));
        assertThat(first.poolState(POOL).orElseThrow().liveMembers()).isEqualTo(4);
        // Recovering capacity goes through the bounded repair budget, once.
        assertThat(register("i-repair-1", "r-1", "i-1").repair()).isTrue();
        assertThatThrownBy(() -> register("i-repair-2", "r-1", "i-2"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_REPAIR_BUDGET));
    }

    @Test
    void aProvenLostMemberStopsConsumingItsSlotButASuspectedOneDoesNot()
    {
        configure(1, 1, 0, 0);
        serving("i-1");
        assertThatThrownBy(() -> register("i-2"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SURGE_BUDGET));
        suspect("i-1");
        assertThatThrownBy(() -> register("i-2"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SURGE_BUDGET));
        lost("i-1");
        assertThat(register("i-2").phase()).isEqualTo("PREPARING");
    }

    @Test
    void anEqualEpochControllerCannotTakeAuthorityFromTheRecordedOwner()
    {
        first.configurePool(
                POOL,
                new Guard("op-own", "configure", epoch, PLAN_HASH, "controller-a"),
                new PoolSpec("POOLED", 1, 3, 1, 1, "r-1", false),
                false);
        // Same epoch, different controller: refused, so two controllers cannot both act at epoch N.
        assertThatThrownBy(() -> second.configurePool(
                POOL,
                new Guard("op-steal", "configure", epoch, PLAN_HASH, "controller-b"),
                new PoolSpec("POOLED", 2, 3, 1, 1, "r-1", false),
                false))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
        assertThatThrownBy(() -> second.registerMember(POOL, new Guard("op-steal-member", "register", epoch, PLAN_HASH, "controller-b"), registration("i-1")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
        // The recorded owner still works, and an explicit takeover raises the epoch.
        assertThatCode(() -> first.registerMember(POOL, new Guard("op-own-member", "register", epoch, PLAN_HASH, "controller-a"), registration("i-1")))
                .doesNotThrowAnyException();
        second.configurePool(
                POOL,
                new Guard("op-takeover", "configure", epoch + 1, PLAN_HASH, "controller-b"),
                new PoolSpec("POOLED", 1, 3, 1, 1, "r-1", false),
                false);
        assertThat(first.poolState(POOL).orElseThrow().controllerEpoch()).isEqualTo(epoch + 1);
        assertThatThrownBy(() -> first.registerMember(POOL, new Guard("op-old-owner", "register", epoch, PLAN_HASH, "controller-a"), registration("i-2")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
    }

    @Test
    void aRecordedStepResolvesAfterAnotherControllerTakesAuthority()
    {
        // The intent hash excludes the authority envelope on purpose, so the next leader reissuing the
        // same step resolves what its predecessor committed instead of conflicting with it forever.
        Member member = first.registerMember(POOL, new Guard("op-resume", "register", epoch, PLAN_HASH, "controller-a"), registration("i-1"));
        epoch = epoch + 1;
        second.configurePool(
                POOL,
                new Guard("op-takeover", "configure", epoch, PLAN_HASH, "controller-b"),
                new PoolSpec("POOLED", 3, 3, 1, 1, "r-1", false),
                false);

        Member replay = second.registerMember(POOL, new Guard("op-resume", "register", epoch, PLAN_HASH, "controller-b"), registration("i-1"));
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.incarnation()).isEqualTo(member.incarnation());
        assertThat(first.members(POOL)).hasSize(1);

        // The old controller can still read back its own committed step, but cannot apply anything new.
        Member staleReplay = first.registerMember(POOL, new Guard("op-resume", "register", epoch - 1, PLAN_HASH, "controller-a"), registration("i-1"));
        assertThat(staleReplay.replayed()).isTrue();
        assertThatThrownBy(() -> first.registerMember(POOL, new Guard("op-stale-new", "register", epoch - 1, PLAN_HASH, "controller-a"), registration("i-2")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
        // A changed intent under a recorded step identity is still a conflict, whoever presents it.
        assertThatThrownBy(() -> second.registerMember(POOL, new Guard("op-resume", "register", epoch, "d".repeat(64), "controller-b"), registration("i-3")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_INTENT_CHANGED));
    }

    @Test
    void aConfigurationStepResolvesOnlyForTheAuthorityThatRecordedIt()
    {
        first.configurePool(
                POOL,
                new Guard("op-cfg", "configure", epoch, PLAN_HASH, "controller-a"),
                new PoolSpec("POOLED", 2, 3, 1, 1, "r-1", false),
                false);
        assertThat(first.poolState(POOL).orElseThrow().minServing()).isEqualTo(2);

        // The same controller retrying its own call resolves to the recorded result.
        PoolStore.PoolState replay = first.configurePool(
                POOL,
                new Guard("op-cfg", "configure", epoch, PLAN_HASH, "controller-a"),
                new PoolSpec("POOLED", 2, 3, 1, 1, "r-1", false),
                false);
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.controllerEpoch()).isEqualTo(epoch);

        // Reissued under a different authority it must NOT resolve: returning the earlier result would
        // report an epoch that was never acquired. Taking authority needs its own step identity.
        assertThatThrownBy(() -> second.configurePool(
                POOL,
                new Guard("op-cfg", "configure", epoch + 5, PLAN_HASH, "controller-b"),
                new PoolSpec("POOLED", 2, 3, 1, 1, "r-1", false),
                false))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_INTENT_CHANGED));
        assertThat(first.poolState(POOL).orElseThrow().controllerEpoch()).isEqualTo(epoch);

        // With its own step identity the takeover applies, and the new epoch is real.
        PoolStore.PoolState taken = second.configurePool(
                POOL,
                new Guard("op-cfg-takeover", "configure", epoch + 5, PLAN_HASH, "controller-b"),
                new PoolSpec("POOLED", 2, 3, 1, 1, "r-1", false),
                false);
        assertThat(taken.replayed()).isFalse();
        assertThat(taken.controllerEpoch()).isEqualTo(epoch + 5);
    }

    // --------------------------------------------------------------------------------------------
    // Authoritative principal mapping
    // --------------------------------------------------------------------------------------------

    @Test
    void aTenantsPrincipalSetIsPublishedWholeAndReadBack()
    {
        PoolStore.TenantAdmission published = first.publishTenantPrincipals(
                POOL,
                "org-1",
                guard("op-p1", "principals"),
                "rev-1",
                List.of("warehouse-one", "warehouse-one.alice", "warehouse-one.bob"));
        // A write result is recorded as an idempotent step, so it echoes the count and digest rather
        // than the set; the read path returns the set itself.
        assertThat(published.principalCount()).isEqualTo(3);
        assertThat(published.principals()).isEmpty();
        assertThat(published.principalRevision()).isEqualTo("rev-1");
        assertThat(published.state()).isEqualTo("PENDING");
        assertThat(first.tenantAdmission(POOL, "org-1").orElseThrow().principals())
                .containsExactly("warehouse-one", "warehouse-one.alice", "warehouse-one.bob");
        assertThat(first.tenantAdmission(POOL, "org-1").orElseThrow().principalsHash()).isEqualTo(published.principalsHash());

        // Republishing replaces the set: a removed login stops being dispatchable.
        PoolStore.TenantAdmission replaced = first.publishTenantPrincipals(
                POOL,
                "org-1",
                guard("op-p2", "principals"),
                "rev-2",
                List.of("warehouse-one", "warehouse-one.alice"));
        assertThat(replaced.principalCount()).isEqualTo(2);
        assertThat(replaced.principalRevision()).isEqualTo("rev-2");
        assertThat(second.tenantAdmission(POOL, "org-1").orElseThrow().principals())
                .containsExactly("warehouse-one", "warehouse-one.alice");
    }

    @Test
    void aTenantIdentifierUnrelatedToItsWarehouseNameIsHonoured()
    {
        // The tenant key is the controller's own identifier. It is not a prefix of any principal, and a
        // principal's leading label is not the tenant, so only the published mapping connects them.
        first.publishTenantPrincipals(
                POOL,
                "1f3a9c27-org",
                guard("op-p1", "principals"),
                "rev-1",
                List.of("warehouse-nine", "warehouse-nine.alice"));
        PoolStore.TenantAdmission admission = first.tenantAdmission(POOL, "1f3a9c27-org").orElseThrow();
        assertThat(admission.tenant()).isEqualTo("1f3a9c27-org");
        assertThat(admission.principals()).containsExactly("warehouse-nine", "warehouse-nine.alice");
    }

    @Test
    void aLargePrincipalSetIsPublishedWithoutHoldingThePoolLockPerPrincipal()
    {
        // The publication holds the same pool row lock that every query admission takes, so the number
        // of round trips must not grow with the tenant's login count. A realistic warehouse has one
        // bare login plus one per user.
        List<String> principals = new java.util.ArrayList<>();
        for (int warehouse = 0; warehouse < 500; warehouse++) {
            principals.add("warehouse-" + warehouse);
            for (int user = 0; user < 9; user++) {
                principals.add("warehouse-" + warehouse + ".user-" + user);
            }
        }
        // A duplicate must not become a second insert of the same key in one statement.
        principals.add("warehouse-0");
        assertThat(principals).hasSize(5001);

        java.util.List<String> executed = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        database.getConfig(org.jdbi.v3.core.statement.SqlStatements.class).setSqlLogger(
                new org.jdbi.v3.core.statement.SqlLogger()
                {
                    @Override
                    public void logAfterExecution(org.jdbi.v3.core.statement.StatementContext context)
                    {
                        executed.add(context.getRenderedSql() == null ? "" : context.getRenderedSql());
                    }
                });
        PoolStore.TenantAdmission published;
        try {
            published = first.publishTenantPrincipals(POOL, "org-1", guard("op-p1", "principals"), "rev-1", principals);
        }
        finally {
            database.getConfig(org.jdbi.v3.core.statement.SqlStatements.class)
                    .setSqlLogger(org.jdbi.v3.core.statement.SqlLogger.NOP_SQL_LOGGER);
        }

        long inserts = executed.stream().filter(sql -> sql.contains("INSERT INTO pool_tenant_principal")).count();
        assertThat(inserts).isEqualTo(1);
        // The whole publication, lock acquisition and read-back included, stays a fixed statement count.
        assertThat(executed).hasSizeLessThan(16);
        assertThat(published.principalCount()).isEqualTo(5000);
        assertThat(published.principalRevision()).isEqualTo("rev-1");
        // The recorded step result must stay bounded, so the write echoes a digest and the read path
        // returns the set. Both describe the same published mapping.
        PoolStore.TenantAdmission read = second.tenantAdmission(POOL, "org-1").orElseThrow();
        assertThat(read.principals()).hasSize(5000).contains("warehouse-0", "warehouse-499.user-8");
        assertThat(read.principalsHash()).isEqualTo(published.principalsHash());
        // Replaying the publication resolves from the recorded step instead of failing on its size.
        PoolStore.TenantAdmission replayed = second.publishTenantPrincipals(
                POOL, "org-1", guard("op-p1", "principals"), "rev-1", principals);
        assertThat(replayed.replayed()).isTrue();
        assertThat(replayed.principalsHash()).isEqualTo(published.principalsHash());

        // Replacement semantics are unchanged at this size: removed logins disappear, kept ones take the
        // new revision, and the set is exactly what was published last.
        PoolStore.TenantAdmission replaced = first.publishTenantPrincipals(
                POOL, "org-1", guard("op-p2", "principals"), "rev-2", List.of("warehouse-0", "warehouse-0.user-0", "warehouse-new"));
        assertThat(replaced.principalCount()).isEqualTo(3);
        assertThat(replaced.principalRevision()).isEqualTo("rev-2");
        assertThat(second.tenantAdmission(POOL, "org-1").orElseThrow().principals())
                .containsExactly("warehouse-0", "warehouse-0.user-0", "warehouse-new");
        // Publishing a set never admits the tenant.
        assertThat(replaced.state()).isEqualTo("PENDING");
    }

    @Test
    void aPrincipalCannotBelongToTwoTenantsOfOnePool()
    {
        first.publishTenantPrincipals(POOL, "org-1", guard("op-p1", "principals"), "rev-1", List.of("warehouse-one"));
        assertThatThrownBy(() -> first.publishTenantPrincipals(POOL, "org-2", guard("op-p2", "principals"), "rev-1", List.of("warehouse-one")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PRINCIPAL_CONFLICT));
        assertThat(first.tenantAdmission(POOL, "org-1").orElseThrow().principals()).containsExactly("warehouse-one");
    }

    @Test
    void aPrincipalThatCouldNotReachAPasswordFileIsRefused()
    {
        // A name with a credential separator or a control character could never be projected into the
        // coordinator's password file, so publishing it would bind a principal that cannot authenticate.
        assertThatThrownBy(() -> first.publishTenantPrincipals(
                POOL,
                "org-1",
                guard("op-p1", "principals"),
                "rev-1",
                List.of("warehouse-one:injected")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_VALIDATION));
        assertThatThrownBy(() -> first.publishTenantPrincipals(
                POOL,
                "org-1",
                guard("op-p2", "principals"),
                "rev-1",
                List.of("warehouse-one\nroot")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_VALIDATION));
        assertThatThrownBy(() -> first.publishTenantPrincipals(POOL, "org-1", guard("op-p3", "principals"), "rev-1", List.of()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_VALIDATION));
    }

    @Test
    void theSameStepIdentityInAnotherPoolIsNotAReplay()
    {
        register("i-1");
        database.useHandle(handle -> handle.execute("INSERT INTO transaction_route (routing_group) VALUES ('pool-b') ON CONFLICT DO NOTHING"));
        first.configurePool("pool-b", guard("op-configure-b", "configure"), new PoolSpec("POOLED", 1, 3, 1, 1, "r-1", false), false);
        Member other = first.registerMember("pool-b", guard("op-i-1-register", "register"),
                new PoolStore.MemberRegistration(
                        "i-1",
                        "backend-b-i-1",
                        "http://b-i-1.example.test",
                        "http://b-i-1.example.test",
                        "pod-b-i-1",
                        "boot-b-i-1",
                        "r-1",
                        null,
                        "node-b-i-1",
                        "coord-b-i-1"));
        assertThat(other.replayed()).isFalse();
        assertThat(other.poolId()).isEqualTo("pool-b");
        assertThat(first.member(POOL, "i-1").orElseThrow().incarnation()).isNotEqualTo(other.incarnation());
    }

    @Test
    void aStaleValidationReceiptCannotCertifyAMemberUntilItIsRevalidated()
    {
        Member member = register("i-1");
        // A first admission attempt records the operator's validation and then fails the process check.
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-first-try", "admit"),
                member.generation(),
                receipt("r-1", "i-1"),
                "node-i-1",
                "wrong",
                FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        // Age that recorded validation. Resending the identical receipt cannot re-date it.
        database.useHandle(handle -> handle.execute(
                "INSERT INTO pool_member_certificate (incarnation, config_revision, certificate_hash, auth_revision, pod_uid, boot_id,"
                        + " node_id, coordinator_id, ready_workers, checks, operation_id, issued_at)"
                        + " SELECT incarnation, 'r-1', '" + CHECKS_HASH + "', 'a-1', pod_uid, boot_id, node_id, coordinator_id, 4,"
                        + " '[\"image\"]'::jsonb, 'op-stale', clock_timestamp() - interval '2 hours'"
                        + " FROM transaction_backend WHERE instance_id = 'i-1'"
                        + " ON CONFLICT (incarnation, config_revision) DO UPDATE SET issued_at = clock_timestamp() - interval '2 hours'"));
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-stale-admit", "admit"),
                member.generation(),
                receipt("r-1", "i-1"),
                "node-i-1",
                "coord-i-1",
                300))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        assertThat(first.member(POOL, "i-1").orElseThrow().phase()).isEqualTo("PREPARING");

        // Genuine revalidation carries different evidence, so it re-dates the record and certifies.
        ValidationReceipt revalidated = new ValidationReceipt(
                "d".repeat(64),
                "r-1",
                "a-1",
                "pod-i-1",
                "boot-i-1",
                "node-i-1",
                "coord-i-1",
                4,
                List.of("image", "workers", "catalog-revision", "auth-revision"));
        assertThat(first.admitMember(POOL, "i-1", guard("op-fresh-admit", "admit"), member.generation(), revalidated, "node-i-1", "coord-i-1", 300).phase())
                .isEqualTo("ACTIVE");
    }

    @Test
    void aReceiptMissingARequiredCheckCannotCertifyAMember()
    {
        Member member = register("i-1");
        ValidationReceipt partial = new ValidationReceipt(
                CHECKS_HASH,
                "r-1",
                "a-1",
                "pod-i-1",
                "boot-i-1",
                "node-i-1",
                "coord-i-1",
                4,
                List.of("image", "workers"));
        assertThatThrownBy(() -> first.admitMember(POOL, "i-1", guard("op-partial", "admit"), member.generation(), partial, "node-i-1", "coord-i-1", FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        ValidationReceipt empty = new ValidationReceipt(CHECKS_HASH, "r-1", "a-1", "pod-i-1", "boot-i-1", "node-i-1", "coord-i-1", 4, List.of());
        assertThatThrownBy(() -> first.admitMember(POOL, "i-1", guard("op-empty", "admit"), member.generation(), empty, "node-i-1", "coord-i-1", FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_CERTIFIED));
        assertThat(first.member(POOL, "i-1").orElseThrow().phase()).isEqualTo("PREPARING");
    }

    @Test
    void aReplacementWithANewIdentityIsNotBlockedByAnEarlierRetirement()
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        serving("i-1");
        drainAndRetire("i-1");
        // Permanent identity and endpoint uniqueness must not deadlock replacement: the operator names
        // every instance-scoped object, including the coordinator Service, after a never-reused
        // instance identity, so a replacement arrives with a new identity and a new endpoint.
        Member replacement = register("i-1-replacement");
        assertThat(replacement.phase()).isEqualTo("PREPARING");
        assertThat(admit("i-1-replacement").phase()).isEqualTo("ACTIVE");
    }

    @Test
    void aFailedMemberIsReplacedThroughTheRepairBudgetAfterFailureRetirement()
    {
        configure(1, 1, 0, 1);
        serving("i-1");
        suspect("i-1");
        lost("i-1");
        Member failureRetiring = retire("i-1");
        assertThat(failureRetiring.retirementKind()).isEqualTo("FAILED");
        retired("i-1");
        Member repair = register("i-2", "r-1", "i-1");
        assertThat(repair.repair()).isTrue();
        assertThat(admit("i-2").phase()).isEqualTo("ACTIVE");
        // The failure receipt and its outstanding obligations survive the replacement.
        assertThat(first.failureReceipt(POOL, "i-1").orElseThrow().evidence()).isEqualTo("PROCESS_TERMINATED");
    }

    // --------------------------------------------------------------------------------------------
    // Drain, seal and irreversible retirement
    // --------------------------------------------------------------------------------------------

    @Test
    void sealingRequiresEveryObligationToEndAndADrainKeepsBoundWorkAdmissible()
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        serving("i-1");
        TransactionStore.Admission admission = transactions.admitPooledMember(POOL, "backend-i-1", "owner");
        transactions.recordResponse(admission.id(), new ResponseObservation("20260918_120000_00001_abcde", "00000000-0000-0000-0000-000000000001", false, false, 120));
        drain(first, "i-1", "op-drain");

        assertThatThrownBy(() -> seal("i-1"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_DRAINED));
        // New independent work stops, but a statement inside the bound transaction is still admitted.
        assertThatThrownBy(() -> transactions.admitPooledMember(POOL, "backend-i-1", "owner"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.NOT_ACTIVE));
        TransactionStore.Admission inTransaction = transactions.admitTransaction("00000000-0000-0000-0000-000000000001", "owner");
        assertThat(inTransaction.backend().name()).isEqualTo("backend-i-1");

        transactions.recordResponse(
                inTransaction.id(),
                new ResponseObservation("20260918_120000_00002_abcde", null, true, true, 0));
        transactions.recordResponse(
                transactions.admitQuery("20260918_120000_00001_abcde", Optional.of("owner"), Optional.empty()).id(),
                new ResponseObservation("20260918_120000_00001_abcde", null, false, true, 0));
        List<String> open = database.withHandle(handle -> handle.createQuery(
                "SELECT admission_id::text FROM transaction_admission WHERE state <> 'COMPLETE'").mapTo(String.class).list());
        assertThat(open).isEmpty();
        Member sealed = seal("i-1");
        assertThat(sealed.phase()).isEqualTo("SEALED");
        assertThat(sealed.drained()).isTrue();
    }

    @Test
    void aRetirementClaimIsIrreversibleAndItsIdentitiesAreNeverReused()
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        serving("i-1");
        drain(first, "i-1", "op-drain");
        seal("i-1");
        Member retiring = retire("i-1");
        assertThat(retiring.phase()).isEqualTo("RETIRING");
        assertThat(retiring.retirementKind()).isEqualTo("DRAINED");

        long generation = retiring.generation();
        assertThatThrownBy(() -> second.drainMember(POOL, "i-1", guard("op-resume-drain", "drain"), generation))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IRREVERSIBLE));
        assertThatThrownBy(() -> second.suspectMember(POOL, "i-1", guard("op-resume-suspect", "suspect"), generation, "probe"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IRREVERSIBLE));
        assertThatThrownBy(() -> second.admitMember(POOL, "i-1", guard("op-resume-admit", "admit"), generation, receipt("r-1", "i-1"), "node-i-1", "coord-i-1", FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IRREVERSIBLE));
        assertThatThrownBy(() -> second.retireMember(POOL, "i-1", guard("op-retire-again", "retire"), generation))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IRREVERSIBLE));

        assertThat(retired("i-1").phase()).isEqualTo("RETIRED");
        // A fresh operation identity cannot resurrect the instance identity either.
        assertThatThrownBy(() -> first.registerMember(POOL, guard("op-reuse-instance", "register"), registration("i-1")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IDENTITY_CONFLICT));
        assertThatThrownBy(() -> first.registerMember(POOL, guard("op-reuse-endpoint", "register"),
                new PoolStore.MemberRegistration(
                        "i-9",
                        "backend-i-9",
                        "http://i-1.example.test",
                        "http://i-1.example.test",
                        "pod-i-9",
                        "boot-i-9",
                        "r-1",
                        null,
                        "node-i-9",
                        "coord-i-9")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IDENTITY_CONFLICT));
    }

    @Test
    void retirementCompletionRequiresAnExplicitResourceAbsenceAssertion()
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        serving("i-1");
        drain(first, "i-1", "op-drain");
        seal("i-1");
        Member retiring = retire("i-1");
        assertThatThrownBy(() -> first.retiredMember(POOL, "i-1", guard("op-no-assertion", "retired"), retiring.generation(), false))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_EVIDENCE_REQUIRED));
        assertThat(first.member(POOL, "i-1").orElseThrow().phase()).isEqualTo("RETIRING");
    }

    @Test
    void aNeverAdmittedCandidateCanRetireButAnAdmittingOneCannot()
    {
        configure(1, 3, 1, 1);
        register("i-1");
        assertThat(retire("i-1").phase()).isEqualTo("RETIRING");

        serving("i-2");
        transactions.admitPooledMember(POOL, "backend-i-2", "owner");
        Member member = first.member(POOL, "i-2").orElseThrow();
        assertThatThrownBy(() -> first.retireMember(POOL, "i-2", guard("op-retire-active", "retire"), member.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PHASE));
    }

    // --------------------------------------------------------------------------------------------
    // Bounded failure recovery
    // --------------------------------------------------------------------------------------------

    @Test
    void lossNeedsEvidenceForTheExactIncarnationAndPreservesObligations()
    {
        configure(1, 3, 1, 1);
        serving("i-1");
        transactions.admitPooledMember(POOL, "backend-i-1", "owner");
        Member active = first.member(POOL, "i-1").orElseThrow();
        assertThatThrownBy(() -> first.lostMember(
                POOL,
                "i-1",
                guard("op-skip-suspect", "lost"),
                active.generation(),
                "PROCESS_TERMINATED",
                new Termination("pod-i-1", "boot-i-1", "node-i-1", "coord-i-1", "kubernetes-pod-absent", "now"),
                false,
                null))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PHASE));

        Member suspected = suspect("i-1");
        assertThat(suspected.phase()).isEqualTo("SUSPECT");
        assertThatThrownBy(() -> transactions.admitPooledMember(POOL, "backend-i-1", "owner"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.NOT_ACTIVE));

        assertThatThrownBy(() -> first.lostMember(
                POOL,
                "i-1",
                guard("op-wrong-boot", "lost"),
                suspected.generation(),
                "PROCESS_TERMINATED",
                new Termination("pod-i-1", "boot-other", "node-i-1", "coord-i-1", "kubernetes-pod-absent", "now"),
                false,
                null))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_EVIDENCE_REQUIRED));
        assertThatThrownBy(() -> first.lostMember(
                POOL,
                "i-1",
                guard("op-no-evidence", "lost"),
                suspected.generation(),
                "PROCESS_TERMINATED",
                null,
                false,
                null))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_EVIDENCE_REQUIRED));

        Member declared = lost("i-1");
        assertThat(declared.phase()).isEqualTo("LOST");
        assertThat(declared.drained()).isFalse();
        assertThat(declared.pendingRequests()).isEqualTo(1);
        PoolStore.FailureReceipt receipt = first.failureReceipt(POOL, "i-1").orElseThrow();
        assertThat(receipt.evidence()).isEqualTo("PROCESS_TERMINATED");
        assertThat(receipt.outstandingAdmissions()).isEqualTo(1);

        Member failureRetiring = retire("i-1");
        assertThat(failureRetiring.retirementKind()).isEqualTo("FAILED");
        assertThat(failureRetiring.drained()).isFalse();
    }

    @Test
    void aPossiblyLiveProcessNeedsExplicitDestructiveAuthorization()
    {
        configure(1, 3, 1, 1);
        serving("i-1");
        Member suspected = suspect("i-1");
        assertThatThrownBy(() -> first.lostMember(
                POOL,
                "i-1",
                guard("op-partition", "lost"),
                suspected.generation(),
                "DESTRUCTIVE_OVERRIDE",
                null,
                false,
                "network partition"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_EVIDENCE_REQUIRED));
        Member declared = first.lostMember(
                POOL,
                "i-1",
                guard("op-partition-authorized", "lost"),
                suspected.generation(),
                "DESTRUCTIVE_OVERRIDE",
                null,
                true,
                "operator authorized removal of a partitioned coordinator");
        assertThat(declared.phase()).isEqualTo("LOST");
        assertThat(first.failureReceipt(POOL, "i-1").orElseThrow().evidence()).isEqualTo("DESTRUCTIVE_OVERRIDE");
    }

    @Test
    void aSuspectMemberThatNeverRecoveredIsReplacedThroughTheDrainPath()
    {
        // Replacing a persistently unhealthy member must not require claiming its process ended. A
        // suspect drains, keeps everything pinned to it, and never returns to service.
        configure(2, 3, 1, 1);
        serving("i-keep-1");
        serving("i-keep-2");
        serving("i-1");
        TransactionStore.Admission admission = transactions.admitPooledMember(POOL, "backend-i-1", "owner");
        transactions.recordResponse(admission.id(), new ResponseObservation("20260918_120000_00007_abcde", null, false, false, 120));
        Member suspected = suspect("i-1");
        assertThat(suspected.phase()).isEqualTo("SUSPECT");
        assertThat(first.poolState(POOL).orElseThrow().blocked()).contains("SUSPECT_MEMBER");

        long membershipBeforeDrain = first.poolState(POOL).orElseThrow().membershipGeneration();
        Member draining = second.drainMember(POOL, "i-1", guard("op-suspect-drain", "drain"), suspected.generation());
        assertThat(draining.phase()).isEqualTo("DRAINING");
        assertThat(draining.eligible()).isFalse();
        assertThat(draining.generation()).isEqualTo(suspected.generation() + 1);
        // A suspect was already outside the serving set, so this is not a membership change.
        assertThat(first.poolState(POOL).orElseThrow().membershipGeneration()).isEqualTo(membershipBeforeDrain);
        PoolStore.PoolState afterDrain = second.poolState(POOL).orElseThrow();
        assertThat(afterDrain.servingMembers()).isEqualTo(2);
        assertThat(afterDrain.counts().get("SUSPECT")).isZero();
        assertThat(afterDrain.counts().get("DRAINING")).isEqualTo(1);
        assertThat(afterDrain.blocked()).doesNotContain("SUSPECT_MEMBER");

        // Pinned work survives, new independent work does not land there, and there is no way back.
        TransactionStore.Admission continued =
                transactions.admitQuery("20260918_120000_00007_abcde", Optional.of("owner"), Optional.empty());
        assertThat(continued.backend().name()).isEqualTo("backend-i-1");
        assertThat(second.eligibleCandidates(POOL)).extracting(PoolStore.Candidate::instanceId)
                .containsExactlyInAnyOrder("i-keep-1", "i-keep-2");
        assertThatThrownBy(() -> transactions.admitPooledMember(POOL, "backend-i-1", "owner"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class,
                        failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.NOT_ACTIVE));
        assertThatThrownBy(() -> first.admitMember(
                POOL,
                "i-1",
                guard("op-suspect-readmit", "admit"),
                draining.generation(),
                receipt("r-1", "i-1"),
                "node-i-1",
                "coord-i-1",
                FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PHASE));

        // It seals only once its obligations are really gone, so this never reports a drain it did not do.
        assertThatThrownBy(() -> seal("i-1"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_NOT_DRAINED));
        transactions.recordResponse(continued.id(), new ResponseObservation("20260918_120000_00007_abcde", null, false, true, 0));
        Member sealed = seal("i-1");
        assertThat(sealed.phase()).isEqualTo("SEALED");
        assertThat(sealed.drained()).isTrue();
        assertThat(retire("i-1").retirementKind()).isEqualTo("DRAINED");
    }

    @Test
    void drainingASuspectIsNotGatedByTheFloorWhileAPlannedDrainStillIs()
    {
        // A suspect holds no serving capacity, so its drain cannot be what takes the pool below the
        // floor. A planned drain of a member that is actually serving still is.
        configure(2, 3, 1, 1);
        serving("i-1");
        serving("i-2");
        serving("i-3");
        Member suspected = suspect("i-3");
        assertThat(first.poolState(POOL).orElseThrow().servingMembers()).isEqualTo(2);
        assertThatThrownBy(() -> drain(first, "i-2", "op-planned-below-floor"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SERVING_FLOOR));
        assertThat(first.drainMember(POOL, "i-3", guard("op-suspect-floor", "drain"), suspected.generation()).phase())
                .isEqualTo("DRAINING");
        assertThat(first.poolState(POOL).orElseThrow().servingMembers()).isEqualTo(2);
        assertThatThrownBy(() -> drain(second, "i-1", "op-planned-still-refused"))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SERVING_FLOOR));
    }

    @Test
    void onlyAnActiveOrSuspectMemberCanBeDrainedAndARetirementClaimStillCannotBe()
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        Member preparing = register("i-1");
        assertThatThrownBy(() -> first.drainMember(POOL, "i-1", guard("op-drain-preparing", "drain"), preparing.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PHASE));
        admit("i-1");
        Member draining = drain(first, "i-1", "op-drain-once");
        assertThatThrownBy(() -> first.drainMember(POOL, "i-1", guard("op-drain-twice", "drain"), draining.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PHASE));
        seal("i-1");
        Member retiring = retire("i-1");
        assertThatThrownBy(() -> first.drainMember(POOL, "i-1", guard("op-drain-retiring", "drain"), retiring.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IRREVERSIBLE));
    }

    @Test
    void aSuspectDrainIsFencedByGenerationAndAuthority()
    {
        configure(1, 3, 1, 1);
        serving("i-1");
        serving("i-keep");
        Member suspected = suspect("i-1");
        assertThatThrownBy(() -> second.drainMember(POOL, "i-1", guard("op-stale-generation", "drain"), suspected.generation() - 1))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_GENERATION));
        assertThatThrownBy(() -> second.drainMember(
                POOL, "i-1", new Guard("op-stale-epoch", "drain", epoch - 1, PLAN_HASH), suspected.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_STALE_EPOCH));
        assertThat(first.member(POOL, "i-1").orElseThrow().phase()).isEqualTo("SUSPECT");
        Member draining = second.drainMember(POOL, "i-1", guard("op-suspect-drain", "drain"), suspected.generation());
        // The recorded step resolves from either replica, so a lost response is not a second drain.
        Member replayed = first.drainMember(POOL, "i-1", guard("op-suspect-drain", "drain"), suspected.generation());
        assertThat(replayed.replayed()).isTrue();
        assertThat(replayed.generation()).isEqualTo(draining.generation());
    }

    @Test
    void aLostMemberNeedsItsFailureReceiptBeforeRetirement()
    {
        configure(1, 3, 1, 1);
        serving("i-1");
        Member suspected = suspect("i-1");
        database.useHandle(handle -> handle.createUpdate("UPDATE transaction_backend SET state = 'LOST' WHERE instance_id = 'i-1'").execute());
        assertThatThrownBy(() -> first.retireMember(POOL, "i-1", guard("op-retire-unreceipted", "retire"), suspected.generation()))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_EVIDENCE_REQUIRED));
    }

    // --------------------------------------------------------------------------------------------
    // Publication barrier and tenant admission
    // --------------------------------------------------------------------------------------------

    @Test
    void commitNeedsAReceiptFromEveryActiveMemberAndOpensTheGateAtomically()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        long membership = first.poolState(POOL).orElseThrow().membershipGeneration();
        PoolStore.Publication publication = first.openPublication(POOL, guard("op-pub", "open"), "pub-1", "tenant-a", "r-2", membership, PLAN_HASH);
        assertThat(publication.phase()).isEqualTo("OPEN");
        assertThat(publication.requiredMembers()).containsExactly("i-1", "i-2", "i-3");
        assertThat(publication.tenantState()).isEqualTo("PENDING");

        receiptFor("pub-1", "i-1", "r-2");
        receiptFor("pub-1", "i-2", "r-2");
        assertThatThrownBy(() -> second.commitPublication(POOL, "pub-1", guard("op-pub", "commit"), membership))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_RECEIPTS_INCOMPLETE));
        assertThat(first.tenantAdmission(POOL, "tenant-a").orElseThrow().state()).isEqualTo("PENDING");

        receiptFor("pub-1", "i-3", "r-2");
        PoolStore.Publication committed = first.commitPublication(POOL, "pub-1", guard("op-pub", "commit"), membership);
        assertThat(committed.phase()).isEqualTo("ADMITTED");
        assertThat(committed.missingMembers()).isEmpty();
        assertThat(first.tenantAdmission(POOL, "tenant-a").orElseThrow().state()).isEqualTo("ADMITTED");
        assertThat(first.tenantAdmission(POOL, "tenant-a").orElseThrow().admittedRevision()).isEqualTo("r-2");
        assertThat(first.poolState(POOL).orElseThrow().admittedRevision()).isEqualTo("r-2");

        PoolStore.Publication replay = second.commitPublication(POOL, "pub-1", guard("op-pub", "commit"), membership);
        assertThat(replay.phase()).isEqualTo("ADMITTED");
        assertThatThrownBy(() -> second.abandonPublication(POOL, "pub-1", guard("op-pub", "abandon")))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_IRREVERSIBLE));
    }

    @Test
    void aReceiptMustMatchTheMembersCurrentProcessAndTheTargetRevision()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        long membership = first.poolState(POOL).orElseThrow().membershipGeneration();
        first.openPublication(POOL, guard("op-pub", "open"), "pub-1", "tenant-a", "r-2", membership, PLAN_HASH);
        assertThatThrownBy(() -> first.recordPublicationReceipt(POOL, "pub-1", guard("op-pub", "stale-revision"), "i-1", "pod-i-1", "boot-i-1", "r-1", AUTH_FINGERPRINT))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PUBLICATION_BARRIER));
        assertThatThrownBy(() -> first.recordPublicationReceipt(POOL, "pub-1", guard("op-pub", "stale-process"), "i-1", "pod-i-1", "boot-other", "r-2", AUTH_FINGERPRINT))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PUBLICATION_BARRIER));
    }

    @Test
    void aMemberJoiningDuringTheBarrierMustAcknowledgeTheTargetRevision()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        long membership = first.poolState(POOL).orElseThrow().membershipGeneration();
        first.openPublication(POOL, guard("op-pub", "open"), "pub-1", "tenant-a", "r-2", membership, PLAN_HASH);
        register("i-4", "r-1", null);
        Member joining = first.member(POOL, "i-4").orElseThrow();
        assertThatThrownBy(() -> first.admitMember(POOL, "i-4", guard("op-join-stale", "admit"), joining.generation(), receipt("r-1", "i-4"), "node-i-4", "coord-i-4", FRESHNESS))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_PUBLICATION_BARRIER));

        database.useHandle(handle -> handle.createUpdate("UPDATE transaction_backend SET config_revision = 'r-2' WHERE instance_id = 'i-4'").execute());
        Member admitted = first.admitMember(POOL, "i-4", guard("op-join-fresh", "admit"), joining.generation(), receipt("r-2", "i-4"), "node-i-4", "coord-i-4", FRESHNESS);
        assertThat(admitted.phase()).isEqualTo("ACTIVE");
        // The join is itself a receipt, so it cannot slip past the barrier unacknowledged.
        assertThat(first.publication(POOL, "pub-1").orElseThrow().receipts())
                .extracting(PoolStore.PublicationReceipt::instanceId).contains("i-4");
        assertThatThrownBy(() -> first.commitPublication(POOL, "pub-1", guard("op-pub", "commit"), membership))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_MEMBERSHIP_CHANGED));
    }

    @Test
    void aMembershipChangeDuringThePublicationBlocksItsCommit()
    {
        configure(2, 3, 1, 1);
        serving("i-1");
        serving("i-2");
        serving("i-3");
        long membership = first.poolState(POOL).orElseThrow().membershipGeneration();
        first.openPublication(POOL, guard("op-pub", "open"), "pub-1", "tenant-a", "r-2", membership, PLAN_HASH);
        receiptFor("pub-1", "i-1", "r-2");
        receiptFor("pub-1", "i-2", "r-2");
        receiptFor("pub-1", "i-3", "r-2");
        drain(second, "i-3", "op-drain-during-publication");
        assertThatThrownBy(() -> first.commitPublication(POOL, "pub-1", guard("op-pub", "commit"), membership))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_MEMBERSHIP_CHANGED));
        assertThat(first.tenantAdmission(POOL, "tenant-a").orElseThrow().state()).isEqualTo("PENDING");
    }

    @Test
    void commitIsRefusedWhenTheAdmittingSetFallsBelowTheServingFloor()
    {
        serving("i-1");
        serving("i-2");
        assertThatThrownBy(() -> first.openPublication(
                POOL,
                guard("op-pub", "open"),
                "pub-1",
                "tenant-a",
                "r-2",
                first.poolState(POOL).orElseThrow().membershipGeneration(),
                PLAN_HASH))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_SERVING_FLOOR));
    }

    @Test
    void theTenantGateCannotBeEnabledWithoutAVerifiedTenantIdentity()
    {
        assertThatThrownBy(() -> first.configurePool(
                POOL,
                guard("op-gate", "configure"),
                new PoolSpec("POOLED", 3, 3, 1, 1, "r-1", true),
                false))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(TENANT_IDENTITY_UNVERIFIED));
        assertThat(first.poolState(POOL).orElseThrow().tenantAdmissionEnabled()).isFalse();
        assertThatCode(() -> first.configurePool(
                POOL,
                guard("op-gate-verified", "configure"),
                new PoolSpec("POOLED", 3, 3, 1, 1, "r-1", true),
                true)).doesNotThrowAnyException();
    }

    @Test
    void revocationClosesTheGateAndAbandonsAnOpenPublication()
    {
        serving("i-1");
        serving("i-2");
        serving("i-3");
        long membership = first.poolState(POOL).orElseThrow().membershipGeneration();
        first.openPublication(POOL, guard("op-pub", "open"), "pub-1", "tenant-a", "r-2", membership, PLAN_HASH);
        PoolStore.TenantAdmission revoked = first.revokeTenant(POOL, "tenant-a", guard("op-revoke", "revoke"), "security revocation");
        assertThat(revoked.state()).isEqualTo("REVOKED");
        assertThat(revoked.admittedRevision()).isNull();
        assertThat(first.publication(POOL, "pub-1").orElseThrow().phase()).isEqualTo("ABANDONED");
    }

    // --------------------------------------------------------------------------------------------
    // Coupling with legacy routing, admission and monitoring
    // --------------------------------------------------------------------------------------------

    @Test
    void legacyRoutingMutationsFailClosedOncePooled()
    {
        serving("i-1");
        assertThatThrownBy(() -> transactions.setRoute(POOL, "backend-i-1"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class, failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.CONFLICT));
        assertThatThrownBy(() -> transactions.clearRoute(POOL))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class, failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.CONFLICT));
        assertThatThrownBy(() -> transactions.beginDrain("backend-i-1"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class, failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.CONFLICT));
        assertThatThrownBy(() -> transactions.ensureBackend("backend-i-9", "http://i-9.example.test", null, POOL, "node-i-9", "coord-i-9"))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class, failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.CONFLICT));
        assertThatThrownBy(() -> transactions.admitNew("backend-i-1", "owner", POOL))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class, failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.CONFLICT));
    }

    @Test
    void aPooledModeChangeIsRefusedWhileLiveMembersExist()
    {
        serving("i-1");
        assertThatThrownBy(() -> first.configurePool(
                POOL,
                guard("op-legacy", "configure"),
                new PoolSpec("LEGACY", 3, 3, 1, 1, "r-1", false),
                false))
                .isInstanceOfSatisfying(PoolException.class, failure -> assertThat(failure.code()).isEqualTo(POOL_APIMODE));
    }

    @Test
    void anAdmissionCannotCommitAgainstAConcurrentlyDrainingMember()
            throws Exception
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        serving("i-1");
        try (var executor = Executors.newFixedThreadPool(2)) {
            CountDownLatch admitting = new CountDownLatch(1);
            var drainAttempt = executor.submit(() -> {
                admitting.await();
                return drain(second, "i-1", "op-drain-during-admission").phase();
            });
            var admissions = executor.submit(() -> {
                admitting.countDown();
                int accepted = 0;
                for (int attempt = 0; attempt < 40; attempt++) {
                    try {
                        transactions.admitPooledMember(POOL, "backend-i-1", "owner");
                        accepted++;
                    }
                    catch (TransactionStore.StoreException expected) {
                        assertThat(expected.code()).isEqualTo(TransactionStore.ErrorCode.NOT_ACTIVE);
                    }
                }
                return accepted;
            });
            assertThat(drainAttempt.get(30, TimeUnit.SECONDS)).isEqualTo("DRAINING");
            admissions.get(30, TimeUnit.SECONDS);
        }
        // Whatever interleaving occurred, no admission may exist that started after the drain committed.
        long admittedAfterDrain = database.withHandle(handle -> handle.createQuery(
                """
                SELECT count(*) FROM transaction_admission a JOIN transaction_backend b USING (incarnation)
                WHERE b.instance_id = 'i-1' AND b.state <> 'ACTIVE'
                  AND a.created_at > (SELECT max(recorded_at) FROM pool_operation WHERE step_id = 'drain')
                """).mapTo(Long.class).one());
        assertThat(admittedAfterDrain).isZero();
    }

    @Test
    void twoProcessesCannotBothActivateTheSameMember()
            throws Exception
    {
        register("i-1");
        Member member = first.member(POOL, "i-1").orElseThrow();
        try (var executor = Executors.newFixedThreadPool(2)) {
            CountDownLatch barrier = new CountDownLatch(1);
            var attempts = List.of(
                            executor.submit(() -> attemptAdmit(first, member.generation(), "op-admit-race-1", barrier)),
                            executor.submit(() -> attemptAdmit(second, member.generation(), "op-admit-race-2", barrier)))
                    .stream().toList();
            barrier.countDown();
            int winners = 0;
            for (var attempt : attempts) {
                if (attempt.get(30, TimeUnit.SECONDS)) {
                    winners++;
                }
            }
            assertThat(winners).isEqualTo(1);
        }
        assertThat(first.member(POOL, "i-1").orElseThrow().generation()).isEqualTo(member.generation() + 1);
        assertThat(first.poolState(POOL).orElseThrow().membershipGeneration()).isEqualTo(1);
    }

    @Test
    void retiredAndLostMembersLeaveMonitoringSelection()
    {
        configure(1, 3, 1, 1);
        serving("i-1");
        serving("i-2");
        serving("i-3");
        assertThat(first.unmonitoredBackendNames()).isEmpty();
        suspect("i-1");
        assertThat(first.unmonitoredBackendNames()).isEmpty();
        lost("i-1");
        assertThat(first.unmonitoredBackendNames()).containsExactly("backend-i-1");
        drain(first, "i-2", "op-drain-2");
        seal("i-2");
        retire("i-2");
        assertThat(first.unmonitoredBackendNames()).containsExactlyInAnyOrder("backend-i-1", "backend-i-2");
        retired("i-2");
        assertThat(second.unmonitoredBackendNames()).containsExactlyInAnyOrder("backend-i-1", "backend-i-2");
    }

    @Test
    void anIrreversiblyRetiredMemberRejectsContinuationsWhileADrainingOneAcceptsThem()
    {
        configure(1, 3, 1, 1);
        serving("i-keep");
        serving("i-1");
        TransactionStore.Admission admission = transactions.admitPooledMember(POOL, "backend-i-1", "owner");
        transactions.recordResponse(admission.id(), new ResponseObservation("20260918_120000_00003_abcde", null, false, false, 120));
        drain(first, "i-1", "op-drain");
        assertThatCode(() -> transactions.admitQuery("20260918_120000_00003_abcde", Optional.of("owner"), Optional.empty()))
                .doesNotThrowAnyException();
        database.useHandle(handle -> handle.createUpdate("UPDATE transaction_backend SET state = 'RETIRING' WHERE instance_id = 'i-1'").execute());
        assertThatThrownBy(() -> transactions.admitQuery("20260918_120000_00003_abcde", Optional.of("owner"), Optional.empty()))
                .isInstanceOfSatisfying(TransactionStore.StoreException.class, failure -> assertThat(failure.code()).isEqualTo(TransactionStore.ErrorCode.SEALED));
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private boolean attemptDrain(PoolStore store, String instanceId, String operationId, CountDownLatch barrier)
            throws InterruptedException
    {
        barrier.await();
        try {
            drain(store, instanceId, operationId);
            return true;
        }
        catch (PoolException e) {
            assertThat(e.code()).isEqualTo(POOL_SERVING_FLOOR);
            return false;
        }
    }

    private boolean attemptAdmit(PoolStore store, long generation, String operationId, CountDownLatch barrier)
            throws InterruptedException
    {
        barrier.await();
        try {
            store.admitMember(
                    POOL,
                    "i-1",
                    new Guard(operationId, "admit", epoch, PLAN_HASH),
                    generation,
                    receipt("r-1", "i-1"),
                    "node-i-1",
                    "coord-i-1",
                    FRESHNESS);
            return true;
        }
        catch (PoolException e) {
            assertThat(e.code()).isEqualTo(POOL_STALE_GENERATION);
            return false;
        }
    }

    private void receiptFor(String publicationId, String instanceId, String revision)
    {
        first.recordPublicationReceipt(
                POOL,
                publicationId,
                guard("op-pub", "receipt-" + instanceId),
                instanceId,
                "pod-" + instanceId,
                "boot-" + instanceId,
                revision,
                AUTH_FINGERPRINT);
    }

    private void configure(int minServing, int desired, int surge, int repair)
    {
        first.configurePool(
                POOL,
                guard("op-configure-" + epoch + "-" + minServing + "-" + desired + "-" + surge + "-" + repair, "configure"),
                new PoolSpec("POOLED", minServing, desired, surge, repair, "r-1", false),
                false);
    }

    private Guard guard(String operationId, String stepId)
    {
        return new Guard(operationId, stepId, epoch, PLAN_HASH);
    }

    private PoolStore.MemberRegistration registration(String instanceId)
    {
        return registration(instanceId, "r-1", null);
    }

    private PoolStore.MemberRegistration registration(String instanceId, String configRevision, String repairFor)
    {
        return new PoolStore.MemberRegistration(
                instanceId,
                "backend-" + instanceId,
                "http://" + instanceId + ".example.test",
                "http://" + instanceId + ".example.test",
                "pod-" + instanceId,
                "boot-" + instanceId,
                configRevision,
                repairFor,
                "node-" + instanceId,
                "coord-" + instanceId);
    }

    private ValidationReceipt receipt(String configRevision, String instanceId)
    {
        return new ValidationReceipt(
                CHECKS_HASH,
                configRevision,
                "a-1",
                "pod-" + instanceId,
                "boot-" + instanceId,
                "node-" + instanceId,
                "coord-" + instanceId,
                4,
                List.of("image", "workers", "catalog-revision", "auth-revision", "operational-connection"));
    }

    private Member register(String instanceId)
    {
        return register(instanceId, "r-1", null);
    }

    private Member register(String instanceId, String configRevision, String repairFor)
    {
        return first.registerMember(POOL, guard("op-" + instanceId + "-register", "register"), registration(instanceId, configRevision, repairFor));
    }

    private Member admit(String instanceId)
    {
        return admit(instanceId, "r-1");
    }

    private Member admit(String instanceId, String configRevision)
    {
        Member member = first.member(POOL, instanceId).orElseThrow();
        return first.admitMember(
                POOL,
                instanceId,
                guard("op-" + instanceId + "-admit-" + configRevision, "admit"),
                member.generation(),
                receipt(configRevision, instanceId),
                "node-" + instanceId,
                "coord-" + instanceId,
                FRESHNESS);
    }

    private Member serving(String instanceId)
    {
        register(instanceId);
        return admit(instanceId);
    }

    private Member drain(PoolStore store, String instanceId, String operationId)
    {
        Member member = store.member(POOL, instanceId).orElseThrow();
        return store.drainMember(POOL, instanceId, new Guard(operationId, "drain", epoch, PLAN_HASH), member.generation());
    }

    private Member seal(String instanceId)
    {
        Member member = first.member(POOL, instanceId).orElseThrow();
        return first.sealMember(POOL, instanceId, guard("op-" + instanceId + "-seal", "seal"), member.generation());
    }

    private Member suspect(String instanceId)
    {
        Member member = first.member(POOL, instanceId).orElseThrow();
        return first.suspectMember(POOL, instanceId, guard("op-" + instanceId + "-suspect", "suspect"), member.generation(), "probe-failed");
    }

    private Member lost(String instanceId)
    {
        Member member = first.member(POOL, instanceId).orElseThrow();
        return first.lostMember(
                POOL,
                instanceId,
                guard("op-" + instanceId + "-lost", "lost"),
                member.generation(),
                "PROCESS_TERMINATED",
                new Termination(
                        "pod-" + instanceId,
                        "boot-" + instanceId,
                        "node-" + instanceId,
                        "coord-" + instanceId,
                        "kubernetes-pod-absent",
                        "2026-09-18T12:00:00Z"),
                false,
                null);
    }

    private Member retire(String instanceId)
    {
        Member member = first.member(POOL, instanceId).orElseThrow();
        return first.retireMember(POOL, instanceId, guard("op-" + instanceId + "-retire", "retire"), member.generation());
    }

    private void drainAndRetire(String instanceId)
    {
        drain(first, instanceId, "op-" + instanceId + "-drain-retire");
        seal(instanceId);
        retire(instanceId);
        retired(instanceId);
    }

    private Member retired(String instanceId)
    {
        Member member = first.member(POOL, instanceId).orElseThrow();
        return first.retiredMember(POOL, instanceId, guard("op-" + instanceId + "-retired", "retired"), member.generation(), true);
    }
}
