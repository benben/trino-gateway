# Targeted review regression oracles

Use focused regressions to test each safety invariant without letting another
validation failure mask it. Keep execution outcomes and mutation-test receipts
private. These oracles complement the complete verification procedure in
[VALIDATION.md](VALIDATION.md).

| Invariant | Required oracle |
| --- | --- |
| Unknown result capability cannot reach a backend | Pair forged GET, HEAD, and DELETE requests with a valid advertised-replay control and inspect backend dispatch |
| Deliberate lifecycle conflict remains HTTP 409 | Exercise legacy activation conflicts and ordinary missing-backend responses separately |
| Duplicate JSON cannot overwrite identity or continuation state | Use otherwise valid terminal responses and reject duplicate fields before any ledger mutation |
| Missing continuation does not imply completion | Require FINISHED or FAILED when no continuation remains; reject missing or nonterminal state |
| Stale lifecycle commands cannot undo sealing | Race commands with the same observed generation and verify sealing advances that generation |
| Acknowledged cancellation settles only its request | Preserve the query, transaction, concurrent request, and retention obligations after DELETE 204 |
| Servlet recycling cannot lose completion context | Recycle the servlet at response delivery and timeout; verify stable admission and lease handling |
| Independent queries can progress concurrently | Hold one query's database fence and verify unrelated work can complete while that fence remains held |
| Conflicting callbacks cannot overwrite one another | Block one callback after it acquires its admission fence and verify a conflicting callback waits and rejects |
| One replica's recovery does not imply fleet recovery | Delay another replica's database response and require every replica within one shared deadline |

## Protocol details

Trino can retain a continuation for buffered data or acknowledgment even when
its state is FINISHED. Do not apply the reverse implication that every FINISHED
response is terminal for Gateway accounting. Consult the matching Trino source
when adding or changing protocol states.

A fresh intentional resume after sealing is different from a stale resume
prepared before sealing. The deployment controller must serialize a new resume
decision with stopping or replacing the coordinator.

For duplicate-field parser tests, use a valid final state so terminal-state
validation cannot mask missing duplicate detection. A targeted mutation should
disable only the validation under review; restore the implementation before
running the full verification gates.

Test identity hash collisions without treating colliding strings as the same
query or transaction. Query and transaction lock namespaces must remain separate
so collisions cannot reverse the global lock acquisition order.
