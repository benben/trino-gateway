# Regressions found during implementation review

Date: 2026-09-09. These are additional red tests against earlier implementation
checkpoints, not the original upstream baseline. See [BASELINE.md](BASELINE.md)
for the pre-implementation evidence. Private runtime output remains outside Git.

| Gap | Genuine red result | Required correction |
| --- | --- | --- |
| Unknown result capability reaches a backend | Four live HTTP tests: three forged GET/HEAD/DELETE failures, one valid replay control passed | Persist advertised paths before client exposure and reject unknown paths before dispatch |
| Legacy API hides lifecycle conflict as HTTP 404 | Four resource tests: two failures, two ordinary-404 controls passed | Preserve the deliberate HTTP 409 response |
| Duplicate JSON fields can hide a continuation or overwrite identity | 74 adapter tests: four failures, 70 controls passed | Reject duplicate fields before any ledger operation |
| Missing continuation with a nonterminal or missing state appears complete | 83 adapter tests: seven failures, 76 controls passed | Require FINISHED or FAILED when no continuation remains |
| A resume prepared before sealing can succeed after sealing | 39 PostgreSQL tests: two failures, 37 controls passed | Advance generation on sealing; reject stale lifecycle requests |
| Acknowledged cancellation leaves unnecessary permanent uncertainty | 90 adapter tests: three failures, 87 controls passed | Settle only the bound DELETE request; preserve query, transaction and retention state |

The terminal-state invariant was checked against Trino tag 483. Executing results
omit their continuation only for a failed query or final query information.
Final query information requires a done state. Queued results retain their
continuation unless dispatch failed. The only done states are FINISHED and FAILED.
The reverse implication is invalid: FINISHED results can still have a continuation
for buffered data or acknowledgment.

The generation regression includes concurrent seal/resume attempts using the
same observed generation. Exactly one may succeed. A fresh, intentional resume
after sealing remains possible; the external deployment controller must serialize
that decision with stopping the coordinator.

These targeted receipts do not replace the final multi-process acceptance run.

After adding terminal-state validation, the four duplicate-field fixtures were
strengthened to end with an otherwise valid FINISHED state. A mutation run that
removed duplicate detection still failed all four cases; restoring it passed all
83 adapter cases at that checkpoint. Another validation must not mask the parser
regression.
