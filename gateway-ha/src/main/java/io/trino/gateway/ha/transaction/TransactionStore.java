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

import jakarta.annotation.Nullable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public final class TransactionStore
{
    static final String DRAIN_STATUS_SQL =
            """
            SELECT b.state, b.generation,
              (SELECT count(*) FROM transaction_admission a WHERE a.incarnation = b.incarnation AND a.state <> 'COMPLETE') AS pending,
              (SELECT count(*) FROM transaction_binding t WHERE t.incarnation = b.incarnation AND t.state = 'OPEN') AS transactions,
              (SELECT count(*) FROM transaction_query q WHERE q.incarnation = b.incarnation AND NOT q.terminal)
                + (SELECT count(*) FROM transaction_query q WHERE q.incarnation = b.incarnation AND q.terminal AND q.retain_until > statement_timestamp()) AS queries
            FROM transaction_backend b WHERE b.incarnation = :id
            """;

    private final Jdbi jdbi;

    public TransactionStore(Jdbi jdbi)
    {
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
    }

    public record BackendRef(String name, UUID incarnation, String url, String externalUrl, String routingGroup, @Nullable String nodeId, @Nullable String coordinatorId) {}

    public record Admission(UUID id, BackendRef backend, String ownerHash, @Nullable String transactionId, @Nullable String queryId) {}

    public record ResponseObservation(@Nullable String queryId, @Nullable String startedTxId, boolean clear, boolean terminal, int retryWindowSeconds, List<String> capabilityHashes)
    {
        public ResponseObservation
        {
            capabilityHashes = List.copyOf(capabilityHashes).stream().distinct().sorted().toList();
        }

        public ResponseObservation(@Nullable String queryId, @Nullable String startedTxId, boolean clear, boolean terminal, int retryWindowSeconds)
        {
            this(queryId, startedTxId, clear, terminal, retryWindowSeconds, List.of());
        }
    }

    public record QueryBinding(String queryId, String ownerHash, BackendRef backend, @Nullable String transactionId, boolean terminal) {}

    public record TransactionBinding(String transactionId, String ownerHash, BackendRef backend, String startQueryId, String state) {}

    public record DrainStatus(String name, UUID incarnation, String state, long generation, long pendingRequests, long openTransactions, long activeQueries, boolean readyToSeal, boolean drained) {}

    public enum ErrorCode
    {
        NOT_FOUND, OWNER_MISMATCH, CONFLICT, SEALED, NOT_DRAINED, NOT_ACTIVE, STALE_GENERATION
    }

    public static final class StoreException
            extends RuntimeException
    {
        private final ErrorCode code;

        public StoreException(ErrorCode code, String message)
        {
            super(message);
            this.code = requireNonNull(code, "code is null");
        }

        public ErrorCode code()
        {
            return code;
        }
    }

    public BackendRef ensureBackend(String name, String url, @Nullable String externalUrl, String routingGroup, @Nullable String nodeId, @Nullable String coordinatorId)
    {
        return ensureBackend(new BackendRef(name, UUID.randomUUID(), url, externalUrl == null ? url : externalUrl, routingGroup, nodeId, coordinatorId));
    }

    public BackendRef ensureBackend(BackendRef proposed)
    {
        BackendRef canonical = new BackendRef(
                proposed.name(),
                proposed.incarnation(),
                proposed.url().replaceAll("/+$", ""),
                proposed.externalUrl().replaceAll("/+$", ""),
                proposed.routingGroup(),
                proposed.nodeId(),
                proposed.coordinatorId());
        return jdbi.inTransaction(handle -> {
            handle.createUpdate(
                            """
                            INSERT INTO transaction_backend (incarnation, name, current_name, backend_url, external_url, routing_group, node_id, coordinator_id, state)
                            VALUES (:id, :name, :name, :url, :external, :group, :node, :coordinator, 'ACTIVE')
                            ON CONFLICT DO NOTHING
                            """)
                    .bind("id", canonical.incarnation()).bind("name", canonical.name()).bind("url", canonical.url())
                    .bind("external", canonical.externalUrl()).bind("group", canonical.routingGroup())
                    .bind("node", canonical.nodeId()).bind("coordinator", canonical.coordinatorId()).execute();
            check(findBackend(handle, canonical.name()).isPresent(), ErrorCode.CONFLICT, "Backend endpoint or process already has another identity");
            BackendRef actual = lockBackend(handle, canonical.name());
            check(actual.url().equals(canonical.url()) && actual.externalUrl().equals(canonical.externalUrl()) &&
                            actual.routingGroup().equals(canonical.routingGroup()) && Objects.equals(actual.nodeId(), canonical.nodeId()) &&
                            Objects.equals(actual.coordinatorId(), canonical.coordinatorId()),
                    ErrorCode.CONFLICT,
                    "Backend incarnation configuration changed");
            return actual;
        });
    }

    public Optional<BackendRef> getBackend(String name)
    {
        return jdbi.withHandle(handle -> findBackend(handle, name));
    }

    public Optional<QueryBinding> getQuery(String queryId)
    {
        return jdbi.withHandle(handle -> findQuery(handle, queryId));
    }

    public Optional<TransactionBinding> getTransaction(String transactionId)
    {
        return jdbi.withHandle(handle -> findTransaction(handle, transactionId));
    }

    public Admission admitNew(String candidateName, String ownerHash, String routingGroup)
    {
        return jdbi.inTransaction(handle -> {
            lockRoute(handle, routingGroup);
            String selected = findRoute(handle, routingGroup).orElse(candidateName);
            BackendRef backend = lockBackend(handle, selected);
            check(backend.routingGroup().equals(routingGroup), ErrorCode.CONFLICT, "Backend belongs to another routing group");
            check(backendState(handle, backend).equals("ACTIVE"), ErrorCode.NOT_ACTIVE, "Backend does not accept new statements");
            return insertAdmission(handle, backend, ownerHash, null, null);
        });
    }

    public Admission admitTransaction(String transactionId, String ownerHash)
    {
        return jdbi.inTransaction(handle -> {
            TransactionBinding initial = findTransaction(handle, transactionId).orElseThrow(() -> missing("transaction"));
            BackendRef backend = lockIncarnation(handle, initial.backend().incarnation());
            TransactionBinding binding = findTransaction(handle, transactionId).orElseThrow(() -> missing("transaction"));
            checkOwner(binding.ownerHash(), ownerHash);
            check(binding.state().equals("OPEN"), ErrorCode.CONFLICT, "Transaction is closed");
            checkNotSealed(handle, backend);
            return insertAdmission(handle, backend, ownerHash, transactionId, null);
        });
    }

    public Admission admitQuery(String queryId, Optional<String> ownerHash, Optional<String> transactionId)
    {
        return admitQuery(queryId, ownerHash, transactionId, Optional.empty());
    }

    public Admission admitQuery(String queryId, Optional<String> ownerHash, Optional<String> transactionId, Optional<String> capabilityHash)
    {
        return jdbi.inTransaction(handle -> {
            QueryBinding initial = findQuery(handle, queryId).orElseThrow(() -> missing("query"));
            BackendRef backend = lockIncarnation(handle, initial.backend().incarnation());
            QueryBinding binding = findQuery(handle, queryId).orElseThrow(() -> missing("query"));
            ownerHash.ifPresent(owner -> checkOwner(binding.ownerHash(), owner));
            transactionId.ifPresent(transaction -> check(transaction.equals(binding.transactionId()), ErrorCode.CONFLICT, "Query and transaction disagree"));
            capabilityHash.ifPresent(hash -> {
                check(hash.matches("[0-9a-f]{64}"), ErrorCode.NOT_FOUND, "Unknown query continuation capability");
                check(handle.createQuery("SELECT EXISTS (SELECT 1 FROM transaction_query_capability WHERE query_id = :query AND capability_hash = :hash)")
                        .bind("query", queryId).bind("hash", hash).mapTo(Boolean.class).one(), ErrorCode.NOT_FOUND, "Unknown query continuation capability");
            });
            checkNotSealed(handle, backend);
            return insertAdmission(handle, backend, binding.ownerHash(), binding.transactionId(), queryId);
        });
    }

    public void recordResponse(UUID admissionId, ResponseObservation observation)
    {
        check(observation.retryWindowSeconds() >= 0, ErrorCode.CONFLICT, "Retry window cannot be negative");
        check(!(observation.clear() && observation.startedTxId() != null), ErrorCode.CONFLICT, "Response both starts and clears a transaction");
        observation.capabilityHashes().forEach(hash -> check(hash.matches("[0-9a-f]{64}"), ErrorCode.CONFLICT, "Capability must be a lowercase SHA-256 hash"));
        jdbi.useTransaction(handle -> {
            Admission admission = lockAdmissionBackend(handle, admissionId);
            String fingerprint = fingerprint(observation);
            if (isComplete(handle, admissionId)) {
                check(fingerprint.equals(recordedObservation(handle, admissionId)), ErrorCode.CONFLICT, "Admission response changed");
                return;
            }
            String queryId = observation.queryId() == null ? admission.queryId() : observation.queryId();
            check(queryId != null && !queryId.isEmpty(), ErrorCode.CONFLICT, "Response has no query identity");
            check(admission.queryId() == null || admission.queryId().equals(queryId), ErrorCode.CONFLICT, "Response query identity changed");
            Optional<QueryBinding> previous = findQuery(handle, queryId);
            previous.ifPresent(query -> {
                checkOwner(query.ownerHash(), admission.ownerHash());
                check(query.backend().incarnation().equals(admission.backend().incarnation()), ErrorCode.CONFLICT, "Query backend changed");
            });

            String transactionId = admission.transactionId();
            if (previous.isPresent()) {
                String previousTransaction = previous.orElseThrow().transactionId();
                check(transactionId == null || previousTransaction == null || transactionId.equals(previousTransaction), ErrorCode.CONFLICT, "Query transaction changed");
                if (transactionId == null) {
                    transactionId = previousTransaction;
                }
            }
            if (observation.startedTxId() != null) {
                check(transactionId == null || transactionId.equals(observation.startedTxId()), ErrorCode.CONFLICT, "Response starts a different transaction");
                bindStartedTransaction(handle, observation.startedTxId(), admission, queryId);
                transactionId = observation.startedTxId();
            }
            bindQuery(handle, queryId, admission, transactionId);
            for (String hash : observation.capabilityHashes()) {
                handle.createUpdate("INSERT INTO transaction_query_capability (query_id, capability_hash) VALUES (:query, :hash) ON CONFLICT DO NOTHING")
                        .bind("query", queryId).bind("hash", hash).execute();
            }
            if (observation.clear()) {
                check(transactionId != null, ErrorCode.CONFLICT, "Response clears an unknown transaction");
                TransactionBinding transaction = findTransaction(handle, transactionId).orElseThrow(() -> missing("transaction"));
                checkOwner(transaction.ownerHash(), admission.ownerHash());
                check(transaction.backend().incarnation().equals(admission.backend().incarnation()), ErrorCode.CONFLICT, "Transaction backend changed");
                handle.createUpdate("UPDATE transaction_binding SET state = 'CLOSED' WHERE transaction_id = :id").bind("id", transactionId).execute();
            }
            if (observation.terminal()) {
                handle.createUpdate(
                        """
                        UPDATE transaction_query SET terminal = TRUE,
                          retain_until = GREATEST(retain_until, clock_timestamp() + make_interval(secs => :seconds))
                        WHERE query_id = :id
                        """).bind("seconds", observation.retryWindowSeconds()).bind("id", queryId).execute();
            }
            handle.createUpdate(
                    """
                    UPDATE transaction_admission SET state = 'COMPLETE', observation = :observation, query_id = :query, transaction_id = :transaction
                    WHERE admission_id = :id
                    """).bind("id", admissionId).bind("observation", fingerprint).bind("query", queryId).bind("transaction", transactionId).execute();
        });
    }

    public void markUncertain(UUID admissionId)
    {
        jdbi.useTransaction(handle -> {
            lockAdmissionBackend(handle, admissionId);
            handle.createUpdate("UPDATE transaction_admission SET state = 'UNCERTAIN' WHERE admission_id = :id AND state <> 'COMPLETE'")
                    .bind("id", admissionId).execute();
        });
    }

    public void rejectAdmission(UUID admissionId)
    {
        jdbi.useTransaction(handle -> {
            lockAdmissionBackend(handle, admissionId);
            if (isComplete(handle, admissionId)) {
                check("REJECTED".equals(recordedObservation(handle, admissionId)), ErrorCode.CONFLICT, "Admission already has a different outcome");
                return;
            }
            handle.createUpdate("UPDATE transaction_admission SET state = 'COMPLETE', observation = 'REJECTED' WHERE admission_id = :id")
                    .bind("id", admissionId).execute();
        });
    }

    public DrainStatus beginDrain(String name)
    {
        return jdbi.inTransaction(handle -> {
            BackendRef backend = lockBackend(handle, name);
            handle.createUpdate("UPDATE transaction_backend SET state = 'DRAINING', generation = generation + 1 WHERE incarnation = :id AND state = 'ACTIVE'")
                    .bind("id", backend.incarnation()).execute();
            return status(handle, backend);
        });
    }

    public DrainStatus drainStatus(String name)
    {
        return jdbi.inTransaction(handle -> status(handle, lockBackend(handle, name)));
    }

    public DrainStatus seal(String name, long generation)
    {
        return jdbi.inTransaction(handle -> {
            BackendRef backend = lockBackend(handle, name);
            DrainStatus before = status(handle, backend);
            check(before.generation() == generation, ErrorCode.STALE_GENERATION, "Backend generation changed");
            if (before.drained()) {
                return before;
            }
            check(before.readyToSeal(), ErrorCode.NOT_DRAINED, "Backend still has work or is not draining");
            handle.createUpdate("UPDATE transaction_backend SET state = 'SEALED', generation = generation + 1 WHERE incarnation = :id").bind("id", backend.incarnation()).execute();
            return status(handle, backend);
        });
    }

    public DrainStatus resume(String name, long generation)
    {
        return jdbi.inTransaction(handle -> {
            BackendRef backend = lockBackend(handle, name);
            DrainStatus before = status(handle, backend);
            check(before.generation() == generation, ErrorCode.STALE_GENERATION, "Backend generation changed");
            handle.createUpdate("UPDATE transaction_backend SET state = 'ACTIVE', generation = generation + 1 WHERE incarnation = :id")
                    .bind("id", backend.incarnation()).execute();
            return status(handle, backend);
        });
    }

    public long setRoute(String routingGroup, String backendName)
    {
        return jdbi.inTransaction(handle -> {
            lockRoute(handle, routingGroup);
            BackendRef backend = lockBackend(handle, backendName);
            check(backend.routingGroup().equals(routingGroup), ErrorCode.CONFLICT, "Backend belongs to another routing group");
            check(backendState(handle, backend).equals("ACTIVE"), ErrorCode.NOT_ACTIVE, "Route target does not accept new statements");
            return handle.createQuery(
                    """
                    UPDATE transaction_route SET backend_name = :backend, generation = generation + 1
                    WHERE routing_group = :group RETURNING generation
                    """).bind("backend", backendName).bind("group", routingGroup).mapTo(Long.class).one();
        });
    }

    public DrainStatus reincarnate(String name, UUID expectedIncarnation, long expectedGeneration, BackendRef proposed)
    {
        check(name.equals(proposed.name()), ErrorCode.CONFLICT, "Replacement logical name changed");
        check(proposed.nodeId() != null && !proposed.nodeId().isBlank() && proposed.coordinatorId() != null && !proposed.coordinatorId().isBlank(),
                ErrorCode.CONFLICT,
                "Replacement requires a verified coordinator identity");
        return jdbi.inTransaction(handle -> {
            BackendRef previous = lockBackend(handle, name);
            DrainStatus before = status(handle, previous);
            check(previous.incarnation().equals(expectedIncarnation) && before.generation() == expectedGeneration,
                    ErrorCode.STALE_GENERATION,
                    "Backend incarnation or generation changed");
            check(before.drained() && before.pendingRequests() == 0 && before.openTransactions() == 0 && before.activeQueries() == 0,
                    ErrorCode.NOT_DRAINED,
                    "Replacement requires a sealed backend without outstanding work");
            check(!previous.incarnation().equals(proposed.incarnation()) &&
                            !(Objects.equals(previous.nodeId(), proposed.nodeId()) && Objects.equals(previous.coordinatorId(), proposed.coordinatorId())),
                    ErrorCode.CONFLICT,
                    "Replacement must identify a different coordinator process");
            check(previous.routingGroup().equals(proposed.routingGroup()), ErrorCode.CONFLICT, "Replacement routing group changed");
            check(!handle.createQuery("SELECT EXISTS (SELECT 1 FROM transaction_route WHERE backend_name = :name)")
                    .bind("name", name).mapTo(Boolean.class).one(), ErrorCode.CONFLICT, "Remove the route to this backend before replacement");
            handle.createUpdate("UPDATE transaction_backend SET current_name = NULL WHERE incarnation = :id").bind("id", previous.incarnation()).execute();
            int inserted = handle.createUpdate(
                            """
                            INSERT INTO transaction_backend (incarnation, name, current_name, backend_url, external_url, routing_group, node_id, coordinator_id, state, generation)
                            VALUES (:id, :name, :name, :url, :external, :group, :node, :coordinator, 'DRAINING', :generation)
                            ON CONFLICT DO NOTHING
                            """).bind("id", proposed.incarnation()).bind("name", name).bind("url", proposed.url().replaceAll("/+$", ""))
                    .bind("external", proposed.externalUrl().replaceAll("/+$", "")).bind("group", proposed.routingGroup())
                    .bind("node", proposed.nodeId()).bind("coordinator", proposed.coordinatorId()).bind("generation", Math.incrementExact(before.generation())).execute();
            check(inserted == 1, ErrorCode.CONFLICT, "Replacement endpoint or process already has another identity");
            return status(handle, lockBackend(handle, name));
        });
    }

    public long clearRoute(String routingGroup)
    {
        return jdbi.inTransaction(handle -> {
            lockRoute(handle, routingGroup);
            return handle.createQuery(
                    """
                    UPDATE transaction_route SET backend_name = NULL, generation = generation + 1
                    WHERE routing_group = :group RETURNING generation
                    """).bind("group", routingGroup).mapTo(Long.class).one();
        });
    }

    public Optional<String> getRoute(String routingGroup)
    {
        return jdbi.withHandle(handle -> findRoute(handle, routingGroup));
    }

    private static void lockRoute(Handle handle, String routingGroup)
    {
        handle.createUpdate("INSERT INTO transaction_route (routing_group) VALUES (:group) ON CONFLICT DO NOTHING").bind("group", routingGroup).execute();
        handle.createQuery("SELECT routing_group FROM transaction_route WHERE routing_group = :group FOR UPDATE")
                .bind("group", routingGroup).mapTo(String.class).one();
    }

    private static Optional<String> findRoute(Handle handle, String routingGroup)
    {
        return handle.createQuery("SELECT backend_name FROM transaction_route WHERE routing_group = :group AND backend_name IS NOT NULL")
                .bind("group", routingGroup).mapTo(String.class).findOne();
    }

    private static Admission insertAdmission(Handle handle, BackendRef backend, String ownerHash, @Nullable String transactionId, @Nullable String queryId)
    {
        check(ownerHash != null && !ownerHash.isEmpty(), ErrorCode.OWNER_MISMATCH, "Missing owner binding");
        UUID id = UUID.randomUUID();
        handle.createUpdate(
                        """
                        INSERT INTO transaction_admission (admission_id, incarnation, owner_hash, transaction_id, query_id, state)
                        VALUES (:id, :backend, :owner, :transaction, :query, 'PENDING')
                        """).bind("id", id).bind("backend", backend.incarnation()).bind("owner", ownerHash)
                .bind("transaction", transactionId).bind("query", queryId).execute();
        return new Admission(id, backend, ownerHash, transactionId, queryId);
    }

    private static Admission lockAdmissionBackend(Handle handle, UUID admissionId)
    {
        Admission admission = handle.createQuery(
                        """
                        SELECT a.admission_id, a.owner_hash, a.transaction_id, a.query_id, b.*
                        FROM transaction_admission a JOIN transaction_backend b USING (incarnation)
                        WHERE admission_id = :id
                        """).bind("id", admissionId)
                .map((rs, _) -> new Admission(rs.getObject("admission_id", UUID.class), backend(rs), rs.getString("owner_hash"), rs.getString("transaction_id"), rs.getString("query_id")))
                .findOne().orElseThrow(() -> missing("admission"));
        lockIncarnation(handle, admission.backend().incarnation());
        return admission;
    }

    private static void bindStartedTransaction(Handle handle, String transactionId, Admission admission, String queryId)
    {
        handle.createUpdate(
                """
                INSERT INTO transaction_binding (transaction_id, owner_hash, incarnation, start_query_id, state)
                VALUES (:id, :owner, :backend, :query, 'OPEN') ON CONFLICT DO NOTHING
                """).bind("id", transactionId).bind("owner", admission.ownerHash()).bind("backend", admission.backend().incarnation()).bind("query", queryId).execute();
        TransactionBinding transaction = findTransaction(handle, transactionId).orElseThrow(() -> missing("transaction"));
        checkOwner(transaction.ownerHash(), admission.ownerHash());
        check(transaction.backend().incarnation().equals(admission.backend().incarnation()) && transaction.startQueryId().equals(queryId),
                ErrorCode.CONFLICT,
                "Transaction start binding changed");
    }

    private static void bindQuery(Handle handle, String queryId, Admission admission, @Nullable String transactionId)
    {
        handle.createUpdate(
                """
                INSERT INTO transaction_query (query_id, owner_hash, incarnation, transaction_id)
                VALUES (:id, :owner, :backend, :transaction) ON CONFLICT DO NOTHING
                """).bind("id", queryId).bind("owner", admission.ownerHash()).bind("backend", admission.backend().incarnation()).bind("transaction", transactionId).execute();
        QueryBinding query = findQuery(handle, queryId).orElseThrow(() -> missing("query"));
        checkOwner(query.ownerHash(), admission.ownerHash());
        check(query.backend().incarnation().equals(admission.backend().incarnation()), ErrorCode.CONFLICT, "Query backend changed");
        check(query.transactionId() == null || query.transactionId().equals(transactionId), ErrorCode.CONFLICT, "Query transaction changed");
        if (query.transactionId() == null && transactionId != null) {
            handle.createUpdate("UPDATE transaction_query SET transaction_id = :transaction WHERE query_id = :id")
                    .bind("transaction", transactionId).bind("id", queryId).execute();
        }
    }

    private static DrainStatus status(Handle handle, BackendRef backend)
    {
        return handle.createQuery(DRAIN_STATUS_SQL).bind("id", backend.incarnation())
                .map((rs, _) -> {
                    String state = rs.getString("state");
                    long pending = rs.getLong("pending");
                    long transactions = rs.getLong("transactions");
                    long queries = rs.getLong("queries");
                    return new DrainStatus(
                            backend.name(),
                            backend.incarnation(),
                            state,
                            rs.getLong("generation"),
                            pending,
                            transactions,
                            queries,
                            state.equals("DRAINING") && pending == 0 && transactions == 0 && queries == 0,
                            state.equals("SEALED"));
                }).one();
    }

    private static BackendRef lockBackend(Handle handle, String name)
    {
        return handle.createQuery("SELECT * FROM transaction_backend WHERE current_name = :name FOR UPDATE").bind("name", name)
                .map((rs, _) -> backend(rs)).findOne().orElseThrow(() -> missing("backend"));
    }

    private static BackendRef lockIncarnation(Handle handle, UUID incarnation)
    {
        return handle.createQuery("SELECT * FROM transaction_backend WHERE incarnation = :id FOR UPDATE").bind("id", incarnation)
                .map((rs, _) -> backend(rs)).findOne().orElseThrow(() -> missing("backend incarnation"));
    }

    private static Optional<BackendRef> findBackend(Handle handle, String name)
    {
        return handle.createQuery("SELECT * FROM transaction_backend WHERE current_name = :name").bind("name", name)
                .map((rs, _) -> backend(rs)).findOne();
    }

    private static Optional<QueryBinding> findQuery(Handle handle, String queryId)
    {
        return handle.createQuery("SELECT q.*, b.* FROM transaction_query q JOIN transaction_backend b USING (incarnation) WHERE query_id = :id")
                .bind("id", queryId).map((rs, _) -> new QueryBinding(rs.getString("query_id"), rs.getString("owner_hash"), backend(rs), rs.getString("transaction_id"), rs.getBoolean("terminal"))).findOne();
    }

    private static Optional<TransactionBinding> findTransaction(Handle handle, String transactionId)
    {
        return handle.createQuery("SELECT t.transaction_id, t.owner_hash, t.start_query_id, t.state AS transaction_state, b.* FROM transaction_binding t JOIN transaction_backend b USING (incarnation) WHERE transaction_id = :id")
                .bind("id", transactionId).map((rs, _) -> new TransactionBinding(rs.getString("transaction_id"), rs.getString("owner_hash"), backend(rs), rs.getString("start_query_id"), rs.getString("transaction_state"))).findOne();
    }

    private static BackendRef backend(ResultSet rs)
            throws SQLException
    {
        return new BackendRef(
                rs.getString("name"),
                rs.getObject("incarnation", UUID.class),
                rs.getString("backend_url"),
                rs.getString("external_url"),
                rs.getString("routing_group"),
                rs.getString("node_id"),
                rs.getString("coordinator_id"));
    }

    private static String backendState(Handle handle, BackendRef backend)
    {
        return handle.createQuery("SELECT state FROM transaction_backend WHERE incarnation = :id").bind("id", backend.incarnation()).mapTo(String.class).one();
    }

    private static void checkNotSealed(Handle handle, BackendRef backend)
    {
        check(!backendState(handle, backend).equals("SEALED"), ErrorCode.SEALED, "Backend is sealed");
    }

    private static boolean isComplete(Handle handle, UUID admissionId)
    {
        return handle.createQuery("SELECT state = 'COMPLETE' FROM transaction_admission WHERE admission_id = :id").bind("id", admissionId).mapTo(Boolean.class).one();
    }

    private static String recordedObservation(Handle handle, UUID admissionId)
    {
        return handle.createQuery("SELECT observation FROM transaction_admission WHERE admission_id = :id").bind("id", admissionId).mapTo(String.class).one();
    }

    private static String fingerprint(ResponseObservation observation)
    {
        String fingerprint = encode(observation.queryId()) + encode(observation.startedTxId()) + observation.clear() + ":" + observation.terminal() + ":" + observation.retryWindowSeconds();
        return observation.capabilityHashes().isEmpty() ? fingerprint : fingerprint + ":capabilities:" + String.join(",", observation.capabilityHashes());
    }

    private static String encode(@Nullable String value)
    {
        return value == null ? "-1:" : value.length() + ":" + value;
    }

    private static void checkOwner(String expected, String actual)
    {
        check(expected.equals(actual), ErrorCode.OWNER_MISMATCH, "Owner binding does not match");
    }

    private static StoreException missing(String kind)
    {
        return new StoreException(ErrorCode.NOT_FOUND, "Unknown " + kind + " binding");
    }

    private static void check(boolean condition, ErrorCode code, String message)
    {
        if (!condition) {
            throw new StoreException(code, message);
        }
    }
}
