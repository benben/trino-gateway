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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.gateway.ha.transaction.TransactionStore.Admission;
import io.trino.gateway.ha.transaction.TransactionStore.BackendRef;
import io.trino.gateway.ha.transaction.TransactionStore.ErrorCode;
import io.trino.gateway.ha.transaction.TransactionStore.ResponseObservation;
import io.trino.gateway.ha.transaction.TransactionStore.StoreException;
import org.jdbi.v3.core.Handle;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.CONFLICT;
import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.NOT_ACTIVE;
import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.NOT_DRAINED;
import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.NOT_FOUND;
import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.OWNER_MISMATCH;
import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.SEALED;
import static io.trino.gateway.ha.transaction.TransactionStore.ErrorCode.STALE_GENERATION;
import static io.trino.gateway.ha.util.TestcontainersUtils.createPostgreSqlContainer;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
class TestTransactionStore
{
    private final String schema = "transaction_store_test_" + UUID.randomUUID().toString().replace("-", "");
    private JdbcDatabaseContainer<?> container;
    private Jdbi admin;
    private Jdbi database;
    private TransactionStore first;
    private TransactionStore second;
    private boolean schemaCreated;

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
        for (String version : new String[] {"V5__transaction_awareness.sql", "V6__backend_incarnation_history.sql", "V7__query_capabilities.sql", "V8__drain_obligation_indexes.sql"}) {
            try (var migration = requireNonNull(getClass().getResourceAsStream("/postgresql/" + version))) {
                String sql = new String(migration.readAllBytes(), StandardCharsets.UTF_8);
                database.useHandle(handle -> handle.createScript(sql).execute());
            }
        }
        first = new TransactionStore(database);
        second = new TransactionStore(database);
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
        database.useHandle(handle -> handle.execute("TRUNCATE transaction_route, transaction_admission, transaction_query_capability, transaction_query, transaction_binding, transaction_backend"));
        first.ensureBackend("blue", "http://blue.example.test", "http://blue.example.test", "group", "blue-node", "blue-process");
        first.ensureBackend("green", "http://green.example.test", "http://green.example.test", "group", "green-node", "green-process");
    }

    @Test
    void unknownTransactionFailsWithoutAdmission()
    {
        expect(NOT_FOUND, () -> second.admitTransaction("unknown", "owner"));
        assertThat(first.drainStatus("blue").pendingRequests()).isZero();
    }

    @Test
    void transactionAndQueryPinAcrossStoreInstancesDuringDrain()
    {
        Admission start = startTransaction("transaction", "start");
        first.beginDrain("blue");
        Admission statement = second.admitTransaction("transaction", "owner");
        assertThat(statement.backend()).isEqualTo(start.backend());
        assertThat(first.getQuery("start").orElseThrow().transactionId()).isEqualTo("transaction");
        assertThat(second.drainStatus("blue").openTransactions()).isEqualTo(1);
        expect(NOT_ACTIVE, () -> first.admitNew("blue", "owner", "group"));
        expect(OWNER_MISMATCH, () -> second.admitTransaction("transaction", "other-owner"));
    }

    @Test
    void queryCapabilityRetainsOwnerAndRejectsContradictions()
    {
        startTransaction("transaction", "start");
        Admission continuation = second.admitQuery("start", Optional.empty(), Optional.empty());
        assertThat(continuation.ownerHash()).isEqualTo("owner");
        expect(OWNER_MISMATCH, () -> first.admitQuery("start", Optional.of("other"), Optional.empty()));
        expect(CONFLICT, () -> first.admitQuery("start", Optional.empty(), Optional.of("another-transaction")));
    }

    @Test
    void closedTransactionStillAllowsItsOutstandingResults()
    {
        startTransaction("transaction", "start");
        Admission commit = second.admitTransaction("transaction", "owner");
        second.recordResponse(commit.id(), new ResponseObservation("commit", null, true, false, 0));
        long generation = first.beginDrain("blue").generation();
        assertThat(first.getTransaction("transaction").orElseThrow().state()).isEqualTo("CLOSED");
        assertThat(first.drainStatus("blue").activeQueries()).isEqualTo(1);
        expect(CONFLICT, () -> first.admitTransaction("transaction", "owner"));
        expect(NOT_DRAINED, () -> first.seal("blue", generation));
        Admission result = first.admitQuery("commit", Optional.empty(), Optional.of("transaction"));
        first.recordResponse(result.id(), new ResponseObservation("commit", null, true, true, 0));
        assertThat(first.seal("blue", generation).drained()).isTrue();
    }

    @Test
    void startReplayCannotReopenTombstone()
    {
        Admission start = startTransaction("transaction", "start");
        Admission commit = second.admitTransaction("transaction", "owner");
        second.recordResponse(commit.id(), new ResponseObservation("commit", null, true, true, 0));
        first.recordResponse(start.id(), new ResponseObservation("start", "transaction", false, true, 0));
        Admission replay = first.admitQuery("start", Optional.empty(), Optional.empty());
        first.recordResponse(replay.id(), new ResponseObservation("start", "transaction", false, true, 0));
        assertThat(first.getTransaction("transaction").orElseThrow().state()).isEqualTo("CLOSED");
        Admission different = first.admitNew("blue", "owner", "group");
        expect(CONFLICT, () -> first.recordResponse(different.id(), new ResponseObservation("different-start", "transaction", false, true, 0)));
        assertThat(first.getQuery("different-start")).isEmpty();
        assertThat(first.drainStatus("blue").pendingRequests()).isEqualTo(1);
    }

    @Test
    void duplicateCallbacksAreIdempotentAndCannotChangeOutcome()
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        ResponseObservation observation = new ResponseObservation("query", null, false, true, 120);
        first.recordResponse(admission.id(), observation);
        String firstRetention = retention("query");
        second.recordResponse(admission.id(), observation);
        second.markUncertain(admission.id());
        assertThat(retention("query")).isEqualTo(firstRetention);
        assertThat(first.drainStatus("blue").pendingRequests()).isZero();
        expect(CONFLICT, () -> first.recordResponse(admission.id(), new ResponseObservation("different", null, false, true, 120)));
        expect(CONFLICT, () -> first.rejectAdmission(admission.id()));
    }

    @Test
    void uncertainAdmissionRemainsBlockingUntilKnownOutcome()
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        first.markUncertain(admission.id());
        long generation = second.beginDrain("blue").generation();
        expect(NOT_DRAINED, () -> first.seal("blue", generation));
        second.recordResponse(admission.id(), new ResponseObservation("late", null, false, true, 0));
        assertThat(first.seal("blue", generation).drained()).isTrue();
    }

    @Test
    void definiteRejectionIsIdempotent()
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        first.rejectAdmission(admission.id());
        second.rejectAdmission(admission.id());
        second.markUncertain(admission.id());
        assertThat(first.drainStatus("blue").pendingRequests()).isZero();
        expect(CONFLICT, () -> first.recordResponse(admission.id(), new ResponseObservation("late", null, false, true, 0)));
    }

    @Test
    void rejectedContinuationDoesNotEraseQueryOrTransactionObligations()
    {
        Admission start = first.admitNew("blue", "owner", "group");
        first.recordResponse(start.id(), new ResponseObservation("start", "transaction", false, false, 0));
        Admission continuation = second.admitQuery("start", Optional.of("owner"), Optional.of("transaction"));
        assertThat(first.drainStatus("blue").pendingRequests()).isEqualTo(1);
        first.rejectAdmission(continuation.id());
        var status = second.beginDrain("blue");
        assertThat(status.pendingRequests()).isZero();
        assertThat(status.openTransactions()).isEqualTo(1);
        assertThat(status.activeQueries()).isEqualTo(1);
        assertThat(status.readyToSeal()).isFalse();
        assertThat(first.getQuery("start").orElseThrow().terminal()).isFalse();
        assertThat(first.getTransaction("transaction").orElseThrow().state()).isEqualTo("OPEN");
        expect(NOT_DRAINED, () -> second.seal("blue", status.generation()));
    }

    @Test
    void settledCancellationPreservesConcurrentPollAndTerminalRetention()
    {
        Admission start = first.admitNew("blue", "owner", "group");
        first.recordResponse(start.id(), new ResponseObservation("start", "transaction", false, true, 120));
        String originalRetention = retention("start");
        Admission poll = first.admitQuery("start", Optional.of("owner"), Optional.of("transaction"));
        Admission cancellation = second.admitQuery("start", Optional.of("owner"), Optional.of("transaction"));
        second.rejectAdmission(cancellation.id());
        var status = first.beginDrain("blue");
        assertThat(status.pendingRequests()).isEqualTo(1);
        assertThat(status.openTransactions()).isEqualTo(1);
        assertThat(status.activeQueries()).isEqualTo(1);
        assertThat(first.getQuery("start").orElseThrow().terminal()).isTrue();
        assertThat(retention("start")).isEqualTo(originalRetention);
        expect(NOT_DRAINED, () -> second.seal("blue", status.generation()));
        first.rejectAdmission(poll.id());
        assertThat(second.drainStatus("blue").pendingRequests()).isZero();
        assertThat(second.drainStatus("blue").activeQueries()).isEqualTo(1);
        assertThat(second.getTransaction("transaction").orElseThrow().state()).isEqualTo("OPEN");
        assertThat(retention("start")).isEqualTo(originalRetention);
    }

    @Test
    void terminalWindowAndLateContinuationFenceSealing()
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        first.recordResponse(admission.id(), new ResponseObservation("query", null, false, true, 120));
        long generation = first.beginDrain("blue").generation();
        assertThat(first.drainStatus("blue").activeQueries()).isEqualTo(1);
        expect(NOT_DRAINED, () -> second.seal("blue", generation));
        database.useHandle(handle -> handle.execute("UPDATE transaction_query SET retain_until = clock_timestamp() - INTERVAL '1 second'"));
        assertThat(first.drainStatus("blue").readyToSeal()).isTrue();
        Admission continuation = second.admitQuery("query", Optional.empty(), Optional.empty());
        expect(NOT_DRAINED, () -> first.seal("blue", generation));
        second.recordResponse(continuation.id(), new ResponseObservation("query", null, false, true, 0));
        assertThat(first.seal("blue", generation).drained()).isTrue();
        expect(SEALED, () -> first.admitQuery("query", Optional.empty(), Optional.empty()));
    }

    @Test
    void lifecycleGenerationRejectsStaleSealAndResume()
    {
        long generation = first.beginDrain("blue").generation();
        assertThat(second.beginDrain("blue").generation()).isEqualTo(generation);
        long resumed = second.resume("blue", generation).generation();
        assertThat(resumed).isGreaterThan(generation);
        expect(STALE_GENERATION, () -> first.seal("blue", generation));
        expect(STALE_GENERATION, () -> first.resume("blue", generation));
        expect(NOT_DRAINED, () -> first.seal("blue", resumed));
    }

    @Test
    void sealingInvalidatesPreviouslyPreparedResumeAndSeal()
    {
        long draining = first.beginDrain("blue").generation();
        var sealed = first.seal("blue", draining);
        expect(STALE_GENERATION, () -> second.resume("blue", draining));
        expect(STALE_GENERATION, () -> second.seal("blue", draining));
        assertThat(sealed.generation()).isEqualTo(draining + 1);
        assertThat(second.seal("blue", sealed.generation())).isEqualTo(sealed);
        expect(NOT_ACTIVE, () -> first.admitNew("blue", "owner", "group"));
        assertThat(second.resume("blue", sealed.generation()).generation()).isEqualTo(sealed.generation() + 1);
        assertThat(first.admitNew("blue", "owner", "group")).isNotNull();
    }

    @Test
    void competingSealAndResumeShareOneGenerationFence()
            throws Exception
    {
        try (var executor = Executors.newFixedThreadPool(2)) {
            for (int iteration = 0; iteration < 20; iteration++) {
                long generation = first.beginDrain("blue").generation();
                CountDownLatch start = new CountDownLatch(1);
                var sealed = executor.submit(() -> attemptLifecycle(start, () -> first.seal("blue", generation)));
                var resumed = executor.submit(() -> attemptLifecycle(start, () -> second.resume("blue", generation)));
                start.countDown();
                boolean sealWon = sealed.get(5, TimeUnit.SECONDS);
                boolean resumeWon = resumed.get(5, TimeUnit.SECONDS);
                assertThat(new boolean[] {sealWon, resumeWon}).containsExactlyInAnyOrder(true, false);
                var current = first.drainStatus("blue");
                assertThat(current.generation()).isEqualTo(generation + 1);
                assertThat(current.state()).isEqualTo(sealWon ? "SEALED" : "ACTIVE");
                if (sealWon) {
                    expect(NOT_ACTIVE, () -> second.admitNew("blue", "owner", "group"));
                    second.resume("blue", current.generation());
                }
            }
        }
    }

    private static boolean attemptLifecycle(CountDownLatch start, Runnable operation)
            throws InterruptedException
    {
        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            operation.run();
            return true;
        }
        catch (StoreException failure) {
            assertThat(failure.code()).isEqualTo(STALE_GENERATION);
            return false;
        }
    }

    @Test
    void backendConfigurationAndProcessIdentityAreImmutable()
    {
        BackendRef original = first.getBackend("blue").orElseThrow();
        assertThat(second.ensureBackend("blue", original.url(), original.externalUrl(), "group", "blue-node", "blue-process")).isEqualTo(original);
        expect(CONFLICT, () -> second.ensureBackend("blue", "http://replacement.example.test", original.externalUrl(), "group", "blue-node", "blue-process"));
        expect(CONFLICT, () -> second.ensureBackend("blue", original.url(), original.externalUrl(), "group", "blue-node", "new-process"));
        assertThat(first.getBackend("blue").orElseThrow()).isEqualTo(original);
    }

    @Test
    void backendAliasesCannotCreateIndependentDrainLedgers()
    {
        BackendRef blue = first.getBackend("blue").orElseThrow();
        expect(CONFLICT, () -> second.ensureBackend("alias", blue.url() + "/", blue.externalUrl(), "group", "another-node", "another-process"));
        expect(CONFLICT, () -> second.ensureBackend("alias", "http://alias.example.test", blue.externalUrl(), "group", blue.nodeId(), blue.coordinatorId()));
        assertThat(first.getBackend("alias")).isEmpty();
        assertThat(second.ensureBackend("blue", blue.url() + "/", blue.externalUrl() + "/", "group", blue.nodeId(), blue.coordinatorId())).isEqualTo(blue);
    }

    @Test
    void queryCollisionDoesNotRebindOrReleaseAdmission()
    {
        Admission blue = first.admitNew("blue", "owner", "group");
        Admission green = second.admitNew("green", "owner", "group");
        first.recordResponse(blue.id(), new ResponseObservation("collision", null, false, false, 0));
        expect(CONFLICT, () -> second.recordResponse(green.id(), new ResponseObservation("collision", null, false, false, 0)));
        assertThat(first.getQuery("collision").orElseThrow().backend().name()).isEqualTo("blue");
        assertThat(first.drainStatus("green").pendingRequests()).isEqualTo(1);
    }

    @Test
    void groupOverrideIsSharedAndAdmissionUsesItsTarget()
    {
        first.setRoute("group", "green");
        assertThat(second.getRoute("group")).contains("green");
        Admission admission = second.admitNew("blue", "owner", "group");
        assertThat(admission.backend().name()).isEqualTo("green");
        first.clearRoute("group");
        assertThat(second.getRoute("group")).isEmpty();
        assertThat(second.admitNew("blue", "owner", "group").backend().name()).isEqualTo("blue");
        expect(CONFLICT, () -> first.setRoute("other-group", "blue"));
        second.beginDrain("green");
        expect(NOT_ACTIVE, () -> first.setRoute("group", "green"));
    }

    @Test
    void malformedResponseDoesNotSettleAdmission()
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        expect(CONFLICT, () -> first.recordResponse(admission.id(), new ResponseObservation(null, null, false, true, 0)));
        expect(CONFLICT, () -> first.recordResponse(admission.id(), new ResponseObservation("query", "transaction", true, true, 0)));
        expect(CONFLICT, () -> first.recordResponse(admission.id(), new ResponseObservation("query", null, true, true, 0)));
        assertThat(first.drainStatus("blue").pendingRequests()).isEqualTo(1);
        assertThat(first.getQuery("query")).isEmpty();
    }

    @Test
    void admissionWaitsForBackendFenceAndObservesCommittedDrain()
            throws Exception
    {
        try (Handle fence = database.open(); var executor = Executors.newSingleThreadExecutor()) {
            fence.begin();
            fence.createQuery("SELECT name FROM transaction_backend WHERE name = 'blue' FOR UPDATE").mapTo(String.class).one();
            CountDownLatch started = new CountDownLatch(1);
            var admission = executor.submit(() -> {
                started.countDown();
                return second.admitNew("blue", "owner", "group");
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            fence.execute("UPDATE transaction_backend SET state = 'DRAINING', generation = generation + 1 WHERE name = 'blue'");
            fence.commit();
            assertThatThrownBy(() -> admission.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(StoreException.class)
                    .satisfies(error -> assertThat(((StoreException) error.getCause()).code()).isEqualTo(NOT_ACTIVE));
            assertThat(first.drainStatus("blue").pendingRequests()).isZero();
        }
    }

    @Test
    void admissionWaitsForGroupFenceAndUsesCommittedOverride()
            throws Exception
    {
        first.clearRoute("group");
        try (Handle fence = database.open(); var executor = Executors.newSingleThreadExecutor()) {
            fence.begin();
            fence.createQuery("SELECT routing_group FROM transaction_route WHERE routing_group = 'group' FOR UPDATE").mapTo(String.class).one();
            CountDownLatch started = new CountDownLatch(1);
            var admission = executor.submit(() -> {
                started.countDown();
                return second.admitNew("blue", "owner", "group");
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            fence.execute("UPDATE transaction_route SET backend_name = 'green', generation = generation + 1 WHERE routing_group = 'group'");
            fence.commit();
            assertThat(admission.get(5, TimeUnit.SECONDS).backend().name()).isEqualTo("green");
        }
    }

    @Test
    void queryTerminalStateCannotRegress()
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        first.recordResponse(admission.id(), new ResponseObservation("query", null, false, false, 0));
        Admission late = first.admitQuery("query", Optional.empty(), Optional.empty());
        Admission terminal = second.admitQuery("query", Optional.empty(), Optional.empty());
        second.recordResponse(terminal.id(), new ResponseObservation("query", null, false, true, 0));
        first.recordResponse(late.id(), new ResponseObservation("query", null, false, false, 0));
        assertThat(first.getQuery("query").orElseThrow().terminal()).isTrue();
    }

    @Test
    void sealedSlotCanBeReusedWithoutRebindingHistoricalQueries()
    {
        Admission start = startTransaction("transaction", "start");
        Admission commit = second.admitTransaction("transaction", "owner");
        second.recordResponse(commit.id(), new ResponseObservation("commit", null, true, true, 0));
        long generation = first.seal("blue", first.beginDrain("blue").generation()).generation();
        BackendRef old = first.getBackend("blue").orElseThrow();
        BackendRef replacement = replacement(old, "new-process");
        var replaced = second.reincarnate("blue", old.incarnation(), generation, replacement);
        assertThat(replaced.state()).isEqualTo("DRAINING");
        assertThat(replaced.generation()).isGreaterThan(generation);
        assertThat(first.getBackend("blue").orElseThrow().incarnation()).isEqualTo(replacement.incarnation());
        assertThat(first.getQuery("start").orElseThrow().backend().incarnation()).isEqualTo(old.incarnation());
        assertThat(first.getTransaction("transaction").orElseThrow().backend().incarnation()).isEqualTo(old.incarnation());
        expect(SEALED, () -> first.admitQuery("start", Optional.empty(), Optional.empty()));
        expect(CONFLICT, () -> first.admitTransaction("transaction", "owner"));
        expect(NOT_ACTIVE, () -> first.admitNew("blue", "owner", "group"));
        second.resume("blue", replaced.generation());
        assertThat(first.admitNew("blue", "owner", "group").backend().incarnation()).isEqualTo(replacement.incarnation());
        first.recordResponse(start.id(), new ResponseObservation("start", "transaction", false, true, 0));
        first.markUncertain(start.id());
        assertThat(first.getTransaction("transaction").orElseThrow().state()).isEqualTo("CLOSED");
    }

    @Test
    void replacementRequiresSealAndCurrentGeneration()
    {
        BackendRef old = first.getBackend("blue").orElseThrow();
        BackendRef replacement = replacement(old, "new-process");
        expect(NOT_DRAINED, () -> first.reincarnate("blue", old.incarnation(), 0, replacement));
        long draining = first.beginDrain("blue").generation();
        expect(NOT_DRAINED, () -> first.reincarnate("blue", old.incarnation(), draining, replacement));
        long generation = first.seal("blue", draining).generation();
        expect(STALE_GENERATION, () -> first.reincarnate("blue", UUID.randomUUID(), generation, replacement));
        expect(STALE_GENERATION, () -> first.reincarnate("blue", old.incarnation(), generation - 1, replacement));
        var replaced = first.reincarnate("blue", old.incarnation(), generation, replacement);
        expect(STALE_GENERATION, () -> second.reincarnate("blue", old.incarnation(), generation, replacement));
        expect(STALE_GENERATION, () -> second.resume("blue", generation));
        expect(STALE_GENERATION, () -> second.seal("blue", generation));
        assertThat(replaced.generation()).isGreaterThan(generation);
    }

    @Test
    void replacementRejectsSameProcessAndActiveAliasesAtomically()
    {
        BackendRef old = first.getBackend("blue").orElseThrow();
        long generation = first.seal("blue", first.beginDrain("blue").generation()).generation();
        expect(CONFLICT, () -> first.reincarnate("blue", old.incarnation(), generation, replacement(old, old.coordinatorId())));
        BackendRef green = first.getBackend("green").orElseThrow();
        BackendRef alias = new BackendRef("blue", UUID.randomUUID(), green.url(), old.externalUrl(), "group", old.nodeId(), "new-process");
        expect(CONFLICT, () -> first.reincarnate("blue", old.incarnation(), generation, alias));
        BackendRef processAlias = new BackendRef("blue", UUID.randomUUID(), old.url(), old.externalUrl(), "group", green.nodeId(), green.coordinatorId());
        expect(CONFLICT, () -> first.reincarnate("blue", old.incarnation(), generation, processAlias));
        assertThat(first.getBackend("blue").orElseThrow()).isEqualTo(old);
        assertThat(first.drainStatus("blue").state()).isEqualTo("SEALED");
    }

    @Test
    void routeMustLeaveOldSlotBeforeReplacement()
    {
        first.setRoute("group", "blue");
        BackendRef old = first.getBackend("blue").orElseThrow();
        long generation = first.seal("blue", first.beginDrain("blue").generation()).generation();
        expect(CONFLICT, () -> first.reincarnate("blue", old.incarnation(), generation, replacement(old, "new-process")));
        first.setRoute("group", "green");
        assertThat(first.reincarnate("blue", old.incarnation(), generation, replacement(old, "new-process")).state()).isEqualTo("DRAINING");
        assertThat(first.admitNew("blue", "owner", "group").backend().name()).isEqualTo("green");
    }

    @Test
    void repeatedBlueGreenCyclesPreserveHistoryAndIncreaseGenerations()
    {
        long lastGeneration = -1;
        for (int cycle = 0; cycle < 3; cycle++) {
            String name = cycle % 2 == 0 ? "blue" : "green";
            BackendRef old = first.getBackend(name).orElseThrow();
            Admission query = first.admitNew(name, "owner", "group");
            first.recordResponse(query.id(), new ResponseObservation("cycle-" + cycle, null, false, true, 0));
            long generation = first.seal(name, first.beginDrain(name).generation()).generation();
            var replaced = first.reincarnate(name, old.incarnation(), generation, replacement(old, "process-" + cycle));
            assertThat(replaced.generation()).isGreaterThan(generation);
            if (name.equals("blue")) {
                assertThat(replaced.generation()).isGreaterThan(lastGeneration);
                lastGeneration = replaced.generation();
            }
            first.resume(name, replaced.generation());
            expect(SEALED, () -> second.admitQuery(queryId(query.id()), Optional.empty(), Optional.empty()));
        }
        long incarnations = database.withHandle(handle -> handle.createQuery("SELECT count(*) FROM transaction_backend").mapTo(Long.class).one());
        assertThat(incarnations).isEqualTo(5);
    }

    @Test
    void versionSixPreservesExistingRoutesAndAffinity()
            throws IOException
    {
        UUID incarnation = UUID.randomUUID();
        String upgradeSchema = "transaction_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        try (Handle handle = database.open()) {
            handle.begin();
            try {
                handle.execute("CREATE SCHEMA " + upgradeSchema);
                handle.execute("SET LOCAL search_path TO " + upgradeSchema);
                try (var migration = requireNonNull(getClass().getResourceAsStream("/postgresql/V5__transaction_awareness.sql"))) {
                    handle.createScript(new String(migration.readAllBytes(), StandardCharsets.UTF_8)).execute();
                }
                handle.createUpdate("INSERT INTO transaction_backend (incarnation, name, backend_url, external_url, routing_group, node_id, coordinator_id, state) VALUES (:id, 'blue', 'http://blue.example.test', 'http://blue.example.test', 'group', 'node', 'process', 'ACTIVE')")
                        .bind("id", incarnation).execute();
                handle.createUpdate("INSERT INTO transaction_binding (transaction_id, owner_hash, incarnation, start_query_id, state) VALUES ('transaction', 'owner', :id, 'query', 'OPEN')")
                        .bind("id", incarnation).execute();
                handle.createUpdate("INSERT INTO transaction_query (query_id, owner_hash, incarnation, transaction_id) VALUES ('query', 'owner', :id, 'transaction')")
                        .bind("id", incarnation).execute();
                handle.createUpdate("INSERT INTO transaction_admission (admission_id, incarnation, owner_hash, transaction_id, query_id, state) VALUES (:admission, :id, 'owner', 'transaction', 'query', 'UNCERTAIN')")
                        .bind("admission", UUID.randomUUID()).bind("id", incarnation).execute();
                handle.execute("INSERT INTO transaction_route (routing_group, backend_name) VALUES ('group', 'blue')");
                try (var migration = requireNonNull(getClass().getResourceAsStream("/postgresql/V6__backend_incarnation_history.sql"))) {
                    handle.createScript(new String(migration.readAllBytes(), StandardCharsets.UTF_8)).execute();
                }
                assertThat(handle.createQuery("SELECT current_name FROM transaction_backend WHERE incarnation = :id").bind("id", incarnation).mapTo(String.class).one()).isEqualTo("blue");
                assertThat(handle.createQuery("SELECT b.incarnation FROM transaction_route r JOIN transaction_backend b ON b.current_name = r.backend_name").mapTo(UUID.class).one()).isEqualTo(incarnation);
                assertThat(handle.createQuery("SELECT b.incarnation FROM transaction_admission a JOIN transaction_query q ON q.query_id = a.query_id JOIN transaction_binding t ON t.transaction_id = q.transaction_id JOIN transaction_backend b ON b.incarnation = t.incarnation WHERE a.state = 'UNCERTAIN'")
                        .mapTo(UUID.class).one()).isEqualTo(incarnation);
            }
            finally {
                handle.rollback();
            }
        }
    }

    @Test
    void competingReplacementsCannotRebindAnOldContinuation()
            throws Exception
    {
        BackendRef old = first.getBackend("blue").orElseThrow();
        Admission query = first.admitNew("blue", "owner", "group");
        first.recordResponse(query.id(), new ResponseObservation("old-query", null, false, true, 0));
        long generation = first.seal("blue", first.beginDrain("blue").generation()).generation();
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(3)) {
            var replacementOne = executor.submit(() -> attemptReplacement(start, old, generation, "replacement-one"));
            var replacementTwo = executor.submit(() -> attemptReplacement(start, old, generation, "replacement-two"));
            var continuation = executor.submit(() -> {
                assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                expect(SEALED, () -> second.admitQuery("old-query", Optional.empty(), Optional.empty()));
                return first.getQuery("old-query").orElseThrow().backend().incarnation();
            });
            start.countDown();
            assertThat(new boolean[] {replacementOne.get(5, TimeUnit.SECONDS), replacementTwo.get(5, TimeUnit.SECONDS)})
                    .containsExactlyInAnyOrder(true, false);
            assertThat(continuation.get(5, TimeUnit.SECONDS)).isEqualTo(old.incarnation());
            assertThat(first.getBackend("blue").orElseThrow().incarnation()).isNotEqualTo(old.incarnation());
            assertThat(first.drainStatus("blue").generation()).isEqualTo(generation + 1);
        }
    }

    private boolean attemptReplacement(CountDownLatch start, BackendRef old, long generation, String coordinatorId)
            throws InterruptedException
    {
        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            second.reincarnate("blue", old.incarnation(), generation, replacement(old, coordinatorId));
            return true;
        }
        catch (StoreException expected) {
            assertThat(expected.code()).isIn(STALE_GENERATION, NOT_FOUND);
            return false;
        }
    }

    private String queryId(UUID admissionId)
    {
        return database.withHandle(handle -> handle.createQuery("SELECT query_id FROM transaction_admission WHERE admission_id = :id").bind("id", admissionId).mapTo(String.class).one());
    }

    @Test
    void advertisedCapabilityPersistsAcrossStoreInstances()
    {
        String hash = "a".repeat(64);
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), new ResponseObservation("query", null, false, false, 0, List.of(hash)));
        assertThat(capabilities("query")).containsExactly(hash);
        Admission continuation = second.admitQuery("query", Optional.empty(), Optional.empty(), Optional.of(hash));
        assertThat(continuation.ownerHash()).isEqualTo("owner");
        assertThat(continuation.backend().incarnation()).isEqualTo(initial.backend().incarnation());
    }

    @Test
    void wrongOrCrossQueryCapabilitiesCannotCreateAdmissions()
    {
        String firstHash = "a".repeat(64);
        String secondHash = "b".repeat(64);
        Admission one = first.admitNew("blue", "owner", "group");
        Admission two = first.admitNew("blue", "owner", "group");
        first.recordResponse(one.id(), new ResponseObservation("one", null, false, false, 0, List.of(firstHash)));
        first.recordResponse(two.id(), new ResponseObservation("two", null, false, false, 0, List.of(secondHash)));
        expect(NOT_FOUND, () -> second.admitQuery("one", Optional.empty(), Optional.empty(), Optional.of(secondHash)));
        expect(NOT_FOUND, () -> second.admitQuery("one", Optional.empty(), Optional.empty(), Optional.of("c".repeat(64))));
        assertThat(first.drainStatus("blue").pendingRequests()).isZero();
        assertThat(first.drainStatus("blue").activeQueries()).isEqualTo(2);
    }

    @Test
    void completedCallbackCannotPublishDifferentCapabilities()
    {
        String firstHash = "a".repeat(64);
        String changedHash = "b".repeat(64);
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), new ResponseObservation("query", null, false, false, 0, List.of(firstHash)));
        expect(CONFLICT, () -> second.recordResponse(initial.id(), new ResponseObservation("query", null, false, false, 0, List.of(changedHash))));
        assertThat(capabilities("query")).containsExactly(firstHash);
    }

    @Test
    void capabilityCollectionIsImmutableAndCanonicalForReplays()
    {
        String firstHash = "a".repeat(64);
        String secondHash = "b".repeat(64);
        List<String> supplied = new ArrayList<>(List.of(secondHash, firstHash, secondHash));
        ResponseObservation observation = new ResponseObservation("query", null, false, false, 0, supplied);
        supplied.clear();
        assertThat(observation.capabilityHashes()).containsExactly(firstHash, secondHash);
        assertThatThrownBy(() -> observation.capabilityHashes().add("c".repeat(64))).isInstanceOf(UnsupportedOperationException.class);
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), observation);
        second.recordResponse(initial.id(), new ResponseObservation("query", null, false, false, 0, List.of(firstHash, secondHash)));
        assertThat(capabilities("query")).containsExactly(firstHash, secondHash);
    }

    @Test
    void responseConflictRollsBackCapabilitiesAndQueryBinding()
    {
        Admission initial = first.admitNew("blue", "owner", "group");
        expect(CONFLICT, () -> first.recordResponse(initial.id(), new ResponseObservation("query", null, true, false, 0, List.of("a".repeat(64)))));
        assertThat(first.getQuery("query")).isEmpty();
        assertThat(capabilities("query")).isEmpty();
        assertThat(first.drainStatus("blue").pendingRequests()).isEqualTo(1);
    }

    @Test
    void laterObservationsRetainPreviousCapabilitiesAndBindLateTransactions()
    {
        String firstHash = "a".repeat(64);
        String secondHash = "b".repeat(64);
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), new ResponseObservation("query", null, false, false, 0, List.of(firstHash)));
        Admission continuation = second.admitQuery("query", Optional.empty(), Optional.empty(), Optional.of(firstHash));
        second.recordResponse(continuation.id(), new ResponseObservation("query", "transaction", false, false, 0, List.of(secondHash)));
        assertThat(capabilities("query")).containsExactly(firstHash, secondHash);
        assertThat(first.getTransaction("transaction").orElseThrow().state()).isEqualTo("OPEN");
        assertThat(first.admitQuery("query", Optional.empty(), Optional.empty(), Optional.of(firstHash)).transactionId()).isEqualTo("transaction");
        assertThat(second.admitQuery("query", Optional.empty(), Optional.empty(), Optional.of(secondHash)).transactionId()).isEqualTo("transaction");
    }

    @Test
    void malformedCapabilityHashesCannotPartiallyRecordResponses()
    {
        Admission initial = first.admitNew("blue", "owner", "group");
        for (String hash : List.of("", "a".repeat(63), "a".repeat(65), "g".repeat(64), "A".repeat(64))) {
            expect(CONFLICT, () -> first.recordResponse(initial.id(), new ResponseObservation("query", "transaction", false, false, 0, List.of(hash))));
        }
        assertThat(first.getQuery("query")).isEmpty();
        assertThat(first.getTransaction("transaction")).isEmpty();
        assertThat(first.drainStatus("blue").pendingRequests()).isEqualTo(1);
    }

    @Test
    void recordedCapabilityCannotBypassHistoricalSealedIncarnation()
    {
        String hash = "a".repeat(64);
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), new ResponseObservation("query", null, false, true, 0, List.of(hash)));
        long generation = first.seal("blue", first.beginDrain("blue").generation()).generation();
        var replaced = first.reincarnate("blue", initial.backend().incarnation(), generation, replacement(initial.backend(), "new-process"));
        first.resume("blue", replaced.generation());
        expect(SEALED, () -> second.admitQuery("query", Optional.empty(), Optional.empty(), Optional.of(hash)));
        assertThat(capabilities("query")).containsExactly(hash);
        assertThat(first.drainStatus("blue").pendingRequests()).isZero();
    }

    @Test
    void capabilityCheckWaitsForAtomicPublicationUnderBackendFence()
            throws Exception
    {
        String hash = "a".repeat(64);
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), new ResponseObservation("query", null, false, false, 0));
        try (var executor = Executors.newSingleThreadExecutor(); Handle fence = database.open()) {
            fence.begin();
            fence.createQuery("SELECT name FROM transaction_backend WHERE incarnation = :id FOR UPDATE")
                    .bind("id", initial.backend().incarnation()).mapTo(String.class).one();
            CountDownLatch started = new CountDownLatch(1);
            var continuation = executor.submit(() -> {
                started.countDown();
                return second.admitQuery("query", Optional.empty(), Optional.empty(), Optional.of(hash));
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> continuation.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            fence.createUpdate("INSERT INTO transaction_query_capability (query_id, capability_hash) VALUES ('query', :hash)")
                    .bind("hash", hash).execute();
            fence.commit();
            assertThat(continuation.get(5, TimeUnit.SECONDS).queryId()).isEqualTo("query");
        }
    }

    @Test
    void legacyQueryWithoutRecordedCapabilitiesFailsClosed()
    {
        Admission initial = first.admitNew("blue", "owner", "group");
        first.recordResponse(initial.id(), new ResponseObservation("legacy", null, false, false, 0));
        expect(NOT_FOUND, () -> second.admitQuery("legacy", Optional.empty(), Optional.empty(), Optional.of("a".repeat(64))));
        assertThat(first.drainStatus("blue").pendingRequests()).isZero();
        first.recordResponse(initial.id(), new ResponseObservation("legacy", null, false, false, 0));
        assertThat(second.admitQuery("legacy", Optional.of("owner"), Optional.empty()).queryId()).isEqualTo("legacy");
    }

    @Test
    void databaseFailureDuringCapabilityPublicationRollsBackEntireObservation()
    {
        String firstHash = "a".repeat(64);
        String rejectedHash = "b".repeat(64);
        Admission initial = first.admitNew("blue", "owner", "group");
        database.useHandle(handle -> handle.execute("ALTER TABLE transaction_query_capability ADD CONSTRAINT test_capability_failure CHECK (capability_hash <> repeat('b', 64))"));
        try {
            assertThatThrownBy(() -> first.recordResponse(initial.id(), new ResponseObservation("query", "transaction", false, false, 0, List.of(firstHash, rejectedHash))))
                    .isInstanceOf(RuntimeException.class).hasMessageContaining("test_capability_failure");
            assertThat(first.getQuery("query")).isEmpty();
            assertThat(first.getTransaction("transaction")).isEmpty();
            assertThat(capabilities("query")).isEmpty();
            assertThat(first.drainStatus("blue").pendingRequests()).isEqualTo(1);
        }
        finally {
            database.useHandle(handle -> handle.execute("ALTER TABLE transaction_query_capability DROP CONSTRAINT test_capability_failure"));
        }
    }

    @Test
    void drainStatusReadsOutstandingWorkWithoutScanningCompletedHistory()
            throws IOException
    {
        UUID backend = first.getBackend("blue").orElseThrow().incarnation();
        database.useHandle(handle -> {
            handle.createUpdate(
                    """
                    INSERT INTO transaction_admission (admission_id, incarnation, owner_hash, state, observation)
                    SELECT md5(n::text)::uuid, :backend, 'owner', 'COMPLETE', 'finished'
                    FROM generate_series(1, 40000) n
                    """).bind("backend", backend).execute();
            handle.createUpdate(
                    """
                    INSERT INTO transaction_binding (transaction_id, owner_hash, incarnation, start_query_id, state)
                    SELECT 'closed-' || n, 'owner', :backend, 'history', 'CLOSED'
                    FROM generate_series(1, 40000) n
                    """).bind("backend", backend).execute();
            handle.createUpdate(
                    """
                    INSERT INTO transaction_query (query_id, owner_hash, incarnation, terminal, retain_until)
                    SELECT 'expired-' || n, 'owner', :backend, TRUE, statement_timestamp() - interval '1 day'
                    FROM generate_series(1, 40000) n
                    """).bind("backend", backend).execute();
            handle.createUpdate(
                    """
                    INSERT INTO transaction_binding (transaction_id, owner_hash, incarnation, start_query_id, state)
                    VALUES ('open', 'owner', :backend, 'running', 'OPEN')
                    """).bind("backend", backend).execute();
            handle.createUpdate(
                    """
                    INSERT INTO transaction_query (query_id, owner_hash, incarnation, terminal, retain_until)
                    VALUES ('running', 'owner', :backend, FALSE, NULL),
                           ('retained', 'owner', :backend, TRUE, statement_timestamp() + interval '1 day')
                    """).bind("backend", backend).execute();
            handle.execute("ANALYZE transaction_admission, transaction_binding, transaction_query, transaction_backend");
        });
        first.admitNew("blue", "owner", "group");
        first.markUncertain(first.admitNew("blue", "owner", "group").id());
        var status = first.drainStatus("blue");
        assertThat(status.pendingRequests()).isEqualTo(2);
        assertThat(status.openTransactions()).isEqualTo(1);
        assertThat(status.activeQueries()).isEqualTo(2);
        String explain = database.withHandle(handle -> handle.createQuery("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + TransactionStore.DRAIN_STATUS_SQL)
                .bind("id", backend).mapTo(String.class).one());
        var plan = new ObjectMapper().readTree(explain).get(0).path("Plan");
        long blocks = plan.path("Shared Hit Blocks").asLong() + plan.path("Shared Read Blocks").asLong();
        assertThat(blocks).as("Drain status must not read completed history: %s", explain).isLessThan(64);
    }

    private List<String> capabilities(String queryId)
    {
        return database.withHandle(handle -> handle.createQuery("SELECT capability_hash FROM transaction_query_capability WHERE query_id = :query ORDER BY capability_hash")
                .bind("query", queryId).mapTo(String.class).list());
    }

    private static BackendRef replacement(BackendRef old, String coordinatorId)
    {
        return new BackendRef(old.name(), UUID.randomUUID(), old.url(), old.externalUrl(), old.routingGroup(), old.nodeId(), coordinatorId);
    }

    private Admission startTransaction(String transactionId, String queryId)
    {
        Admission admission = first.admitNew("blue", "owner", "group");
        first.recordResponse(admission.id(), new ResponseObservation(queryId, transactionId, false, true, 0));
        return admission;
    }

    private String retention(String queryId)
    {
        return database.withHandle(handle -> handle.createQuery("SELECT retain_until::text FROM transaction_query WHERE query_id = :id")
                .bind("id", queryId).mapTo(String.class).one());
    }

    private static void expect(ErrorCode code, Runnable operation)
    {
        assertThatThrownBy(operation::run).isInstanceOf(StoreException.class)
                .satisfies(error -> assertThat(((StoreException) error).code()).isEqualTo(code));
    }
}
