# Single-case orchestration and preservation guards

These helpers prepare one explicitly approved disposable benchmark case. They do
not authorize database access, credential transfer, Kubernetes changes, or a full
matrix. Keep runtime case configuration, generated objects, and evidence private.

`matrix_case.run` reuses `OpenLoop` and `Checkpoints`. Its configuration includes
the case identifier, source commit, image digest, expected replica count, direct
pod URLs, UIDs, and immutable spec image pins, routing-group source/target
identities, HTTP rate, duration,
warmup, and an optional scheduled UTC start. The caller must obtain a fresh,
verified pod inventory; validating supplied metadata does not verify live pods.

Supported bounds are 2/20/100 replicas, 100/1000 aggregate HTTP requests per
second, 10/60/300-second measurements, and zero or ten seconds of warmup.
Every source and target needs a globally distinct fixture response identity,
including across groups. Every Gateway URL must address a
distinct pod IP over HTTPS on port 8443. The existing TLS helper verifies the
configured certificate identity; do not replace these URLs with a Service.

The runner performs warmup, starts one checkpoint transaction per measured
group, measures load, then finishes the checkpoints. Warmup failure stops before
starting checkpoint transactions. Invalid measured throughput remains invalid,
but the runner still attempts the normal checkpoint proof and known-transaction
ROLLBACK. Unknown query results and failed checkpoints are not purged, cancelled,
or retried. Exceptions report only the phase and exception type, not private
response bodies or credentials.

Transport validity and target acceptance are separate. The target passes only
when actual in-window successful HTTP rate reaches at least 99% of the configured
target. The receipt records the actual rate, target, threshold fraction, required
rate, and acceptance result. This tolerance does not turn a lower observed rate
into an exact-target claim. A clean protocol checkpoint cannot convert unmet
throughput into success; it can establish whether subsequent authorized testing
is safe after the external preservation checks also pass.

Checkpoint preparation has a 60-second process alarm; completion has a shared
600-second alarm for at most two groups. An alarm can interrupt a client while
server work remains uncertain. It does not establish server-side cancellation.
The normal helper's drain, seal, resume, and route restoration remain unchanged.
The runner never performs unconditional cleanup after a failed checkpoint.

`deploy.render_matrix_case.compose` extends the existing load Job renderer with
the four source modules, runtime case configuration, and an existing synthetic
admin Secret reference. It creates only a ConfigMap and a Job. The Job retains
2 CPU/2 GiB, no API token, no retries, no preemption, and a finite deadline. Its
maximum deadline is 1,990 seconds, including startup margin, scheduled-start
waiting, both continuation-cleanup allowances, and checkpoint phases. Supply
reviewed `load_open_loop.py`, `protocol.py`, `load_checkpoints.py`, and
`matrix_case.py` contents. Do not embed credential values in the source bundle.

The Job returns success when its own throughput and checkpoint checks pass.
Its receipt still has `valid_run: false` and `external_postguard_required: true`:
the orchestration cannot observe the database directly. Overall case validity
also requires independently collected, current before/after ledger evidence,
unchanged pod/fixture identities, and adequate client/database metric coverage.
Do not turn a successful Job into a capacity claim without these external gates.

## Cutover during timed load

The optional `cutover_during_load: {offset_seconds: 30}` profile uses twenty
Gateways, one routing group, a ten-second warmup, and sixty seconds of aggregate
traffic. It accepts exactly 100 HTTP RPS with 128 aggregate client slots, or
1,000 HTTP RPS with 512 slots. Both profiles require eight client processes and
the same warmup, timing, ownership and validity checks. A lower-rate case does
not establish higher-rate capacity or change another case's validity.
The caller binds both fixture pod URLs and process fingerprints
to a fresh, independently verified inventory. Both backends are already running;
this measures routing cutover, not backend startup or deployment orchestration.

After warmup, the controller opens a source transaction and retains a separate
source result continuation. During measurement it switches routing to the target
before draining the source. Draining first would reject new work while the route
still names the draining backend. After a verified cutover acknowledgement, the
controller checks the old transaction and a new query through every Gateway,
then consumes the retained continuation through another Gateway. All proofs must
finish with at least ten seconds of background traffic remaining.

The load client checks each continuation against its initial query identifier
and verified coordinator owner. Initial POST responses completed before cutover
must belong to the source. POSTs dispatched after acknowledgement must belong to
the target. Requests overlapping the cutover operation may use either owner;
that allowance never permits a continuation to change owner.

Receipts retain actual cutover request/acknowledgement times, endpoint proof
intervals, and separate control HTTP counts. Request-start cohorts report exact
pooled latency percentiles and errors before, during, and after the operation.
They retain late-start and late-completion counts. An empty cohort has null
percentiles, not zero latency. Raw identifiers remain in memory, and raw numeric
samples are discarded before receipt output. Later auditors cannot reconstruct
the percentiles from compact receipts alone.

The control thread has a bounded deadline and short, remaining-budget-capped
HTTP calls. Unknown acknowledgements, failed proofs or an active control thread
prevent restoration. Normal transaction rollback, final drain/seal, resume and
route restoration happen only after the completed background phase and verified
control outcome. All original client, throughput and external preservation gates
remain required. A successful routing proof does not repair a failed load gate.

This mode adds `load_cutover.py` and `load_cutover_aggregate.py` to the six-source
multi-process bundle. It creates no additional Gateway replicas or backends.

## Aggregate ledger evidence

`matrix_guard.compare` accepts already-authorized server-side aggregate
manifests. It neither connects to a database nor exports data. Export permission
still applies to derived fingerprints. Do not substitute transformed output for
a denied export or collect raw identifiers without permission.

The manifest contains `ascii_valid`, counted SHA256 objects for nonterminal
queries, pending admissions, and open transactions, per-incarnation query groups,
an ACTIVE backend manifest with generation and incarnation hash, the backend-set
hash, and retained-terminal counts. Each before/after side needs at least two
stable samples. The collector must separately prove freshness and associate the
samples with this exact case; this pure guard cannot authenticate their origin.
Samples must use explicit UTC timestamps, accepting `Z` or a zero UTC offset.
Each sequence strictly increases, and the first post sample must be later than
the last pre sample. A final case guard also rejects post evidence taken before
case completion. Reusing or duplicating observations does not establish safety.

Canonical fingerprinting sorts rows by the first identity field using bytewise
ordering. Each ASCII field becomes its decimal byte length, a colon, then the
field value. Fields and rows are concatenated before SHA256. Empty sets use
SHA256 of the empty byte string. Query fields are identity, incarnation, backend
name; admission/transaction fields are identity, incarnation, state. Backend-set
fields are name and incarnation. `ascii_fingerprint` provides the local reference
codec. Require an actual PostgreSQL/reference-codec equality check before using
a collector's manifest; unit tests alone do not prove its SQL serialization.

The guard preserves exact historical query digests and counts, rejects any new
query groups, pending admissions, open transactions, or terminal retention, and
keeps the historical owner's incarnation and lifecycle generation unchanged.
Normal checkpoints may advance other existing backends' generations. Incarnation
replacement or decreasing generations always fail. Successful guards permit
preserved historical obligations; they do not claim that owner is drained.

Lazy backend initialization must be explicit: `allow_initialized` names exactly
the approved newly initialized backends. `require_complete=False` is only for
that setup transition. Establish the complete inventory before normal matrix
cases, which require exact backend inventory without additional initialization.
No helper changes backend state to satisfy a guard.

`matrix_guard.finalize` combines workload results with the ledger comparison and
private host-collected pod snapshots. Each pod snapshot has `sample_time` and
`pods`; every pod provides its name, UID, role, `spec_image`,
`observed_image_id`, restart
count, Ready flag, phase, and deletion timestamp. Fixture entries also identify
their fixture name. Include every Gateway and all four fixtures, even for a hot
single-group case. The Gateway spec pin must match the measured image index.
Its observed image ID can identify the platform-specific child manifest instead;
do not require index and child digests to match. Preserve both fields unchanged
per pod, including when Gateway pods use different architectures. All pod
UIDs and restart counts must also remain unchanged. This catches a fixture
container restart even when its synthetic process identity did not change.
An existing fixture may use a tagged spec image, but its exact spec image and
observed image ID must both stay unchanged. Do not restart a preserved fixture
just to replace its tag with a digest. Gateway spec images must remain pinned.

The last pre-ledger sample and pre-pod snapshot must precede case start by at
most 120 seconds. The first post-ledger sample and post-pod snapshot must follow
case completion by at most 120 seconds. Callers may tighten this bound, not widen
it. Missing, future-dated pre-evidence, premature post-evidence, and stale samples
fail closed. This assumes sufficiently synchronized host/database clocks and
does not establish clock synchronization itself. Collectors remain responsible
for obtaining these observations from the authorized environment.

Only this combined finalizer can produce `valid_run: true`. A clean preservation
result with inadequate throughput still returns `valid_run: false`; it does not
authorize another case or a larger replica count.
The configured case rate, measurement target and successful rate, and acceptance
fields must agree. Missing measurements, contradictory values, non-finite rates,
or a changed 99% threshold cannot produce a valid result.

Run local tests from this directory:

```sh
python3 -m unittest -v test_matrix_case test_load_open_loop test_load_checkpoints test_protocol_tls
```

These tests establish local orchestration and rejection behavior, not a completed
load matrix or permission to run one. They make no throughput or cost claim.
