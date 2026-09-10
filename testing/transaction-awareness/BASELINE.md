# Baseline comparison procedure

Use a baseline to distinguish an existing behavior gap from a test-fixture
failure. Keep execution results, artifact identities, timings, logs, and
environment details in private evidence, not this repository or public comments.

## Prepare a controlled comparison

Select an explicitly pinned upstream or pre-change image. Use an authorized,
disposable environment with independent Gateway processes sharing PostgreSQL
and independent backend processes. Give each run fresh credentials and verified
TLS. Preserve any unresolved state from previous runs instead of resetting it
to make a comparison appear clean.

Verify backend health and database access through every Gateway process before
running feature assertions. A readiness deadline must cover the configured
health-monitor interval. A successful readiness check through one replica does
not establish recovery of another replica's database connections.

## Separate controls from feature assertions

Run the baseline controls first:

```sh
cd testing/transaction-awareness
python3 -m unittest -v test_gateway.BaselineControls
```

Then select the behavior under review, or run the complete initial contract:

```sh
python3 -m unittest -v test_gateway.TransactionContract
python3 -m unittest -v test_adversarial
```

Distinguish assertion failures from authentication, connectivity, configuration,
and fixture-timing errors. A missing feature API establishes a missing capability;
it does not independently demonstrate incorrect transaction routing. For routing
assertions, inspect the backend identity and whether a request reached a backend,
not only its HTTP status.

## Strengthen the oracle

Trino can return an HTTP success response containing a query error. A rollback
response alone is also insufficient to prove that the request reached the owner.
Check the original coordinator identity, returned query identity, and advertised
continuation behavior when validating transaction affinity.

Include protocol variants such as lifecycle headers returned on continuation
pages, trailing slashes on statement requests, and HEAD heartbeats. Use the
matching Trino source and client implementation to define expected behavior.

Repeat relevant assertions with real Trino and an unmodified supported client.
Do not rewrite client continuation URLs or override encoding negotiation to hide
a routing or compatibility problem. Follow [VALIDATION.md](VALIDATION.md) for
the remaining verification gates.

A genuine regression should fail before its implementation changes and pass
afterward without expected-failure annotations or skipped assertions. Keep that
comparison private, including unsuccessful attempts and fixture corrections.
