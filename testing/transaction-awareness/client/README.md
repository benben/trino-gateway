# Native JDBC compatibility

This fixture uses the unmodified Trino JDBC 483 driver. It does not intercept
HTTP, replace continuation URLs, or manually supply transaction identifiers.
The driver manages transaction headers, query polling, commit, and rollback.
Both the driver and the administrative HTTP client verify TLS certificates and
hostnames. Credentials come from the private lab environment, not this source.

Provide a local JDBC 483 JAR and a JDK compatible with that driver. No dependency
download, image publication, or cluster rollout occurs in this test.

```sh
export TX_GATEWAY_URLS=https://localhost:<first-port>,https://localhost:<second-port>
export TX_REAL_BACKEND_URLS=https://localhost:<blue-port>,https://localhost:<green-port>
export TX_TRUST_STORE=<private-runtime-directory>/tls/truststore.p12
export TX_TRUST_STORE_PASSWORD_FILE=<private-runtime-directory>/tls/store-password
export TX_ALLOW_FIXTURE_MUTATION=yes
java --class-path <local-trino-jdbc-483.jar> \
  testing/transaction-awareness/client/NativeJdbc.java --baseline
java --class-path <local-trino-jdbc-483.jar> \
  testing/transaction-awareness/client/NativeJdbc.java
```

Feature-enabled Gateways also require `TX_ADMIN_TOKEN` from the private lab
environment file. Export `TX_TRINO_USER` and `TX_TRINO_PASSWORD` from the private
runtime `trino.env` file for both modes. Optional `TX_REAL_BACKEND_NAMES` and `TX_REAL_ROUTING_GROUP`
use the same defaults as the real HTTP fixture.

Run this alone, between other fixture suites: it activates and deactivates the
two real backends. The baseline proves native autocommit and explicit transaction
completion. The full test also proves fresh connections reach a different
coordinator while the existing transaction remains on its original coordinator.
It repeats for each Gateway and restores the first backend afterwards.
Activation waits until fresh JDBC queries report the selected backend's node ID
on both Gateways. Backend IDs come from their verified HTTPS `/v1/info` responses.
Feature mode uses the transaction-aware cutover API; upstream baseline mode uses
the legacy activation API. The token selects the feature mode.

This is client protocol coverage, not a coordinator recovery or cross-replica
continuation failover claim. The JDBC driver keeps its configured Gateway URL.
Separate HTTP tests exercise continuation handling on another Gateway replica.
