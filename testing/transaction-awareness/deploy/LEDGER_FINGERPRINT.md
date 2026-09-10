# Aggregate ledger preservation checks

`ledger_fingerprint.sql` is one read-only PostgreSQL query. Run it through the
approved observer connection with a read-only session and bounded statement
timeout. It requires the transaction ledger migrations through V8 and does not
install an extension or modify ledger records.

The result contains counts, SHA256 fingerprints, backend names and lifecycle
state, a UTC statement-start timestamp, and terminal-retention counts. The
timestamp identifies statement start, which can precede snapshot acquisition;
it does not report scan completion. Raw query, admission, transaction, owner, and backend-incarnation
identifiers are not returned.
Fingerprints still represent private operational data: obtain approval for
their destination and keep collected evidence out of public repositories.

The codec orders rows by their identity field with PostgreSQL `C` collation.
Each ASCII field becomes its decimal byte length, a colon, and its value.
Concatenate fields and rows without additional separators, then SHA256 the
UTF-8 bytes. An empty set hashes the empty byte string. The reference tests
check this encoding against a real PostgreSQL ledger with synthetic records.

Field order is `query_id, incarnation, current_name` for nonterminal queries;
`admission_id, incarnation, state` for incomplete admissions; and
`transaction_id, incarnation, state` for open transactions. Incomplete admissions
include both PENDING and UNCERTAIN states. Backend identity uses
`current_name, incarnation`, ordered by `current_name`; lifecycle state and
generation are separate and do not change that identity fingerprint.

Reject results with `ascii_valid=false`, missing fields, inconsistent counts,
unexpected owner changes, or new obligations. Historical incarnations referenced
by nonterminal queries without a current name deliberately fail this bounded
codec's validity check; do not treat their omitted encoded values as an empty
ledger. Backend incarnations
may register lazily, so establish and validate the expected manifest before a
measurement. A matching count alone does not prove preservation.

Capture two stable snapshots around each approved trial. Fingerprints provide
cryptographic equality evidence, not recovery or proof that a query completed.
Never delete uncertain records to make a comparison pass.

The existing disposable-tooling CI job runs the PostgreSQL tests with its
local service. For a separately approved local fixture, set
`TX_OBSERVER_LOCAL_PG=yes` and the standard `PGHOST`, `PGPORT`, `PGDATABASE`,
and `PGUSER` variables, then run `python3 -m unittest test_ledger_fingerprint`.
The integration tests require a loopback host and create only a unique temporary
schema, which they remove afterwards. They must not target a live ledger.
