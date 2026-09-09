# Trino Gateway

A load balancer, proxy server, and configurable routing gateway for multiple
[Trino](https://trino.io) clusters.

<img src="./docs/assets/logos/trino-gateway-h.png"/>

Find out more details from our documentation at 
[trinodb.github.io/trino-gateway](https://trinodb.github.io/trino-gateway)

This fork also develops opt-in PostgreSQL-backed transaction affinity and safe
backend draining. See the [design](docs/design/transaction-awareness.md),
[operational boundaries](testing/transaction-awareness/OPERATIONS.md), and
[repeatable test suite](testing/transaction-awareness/README.md). This work does
not constitute a production deployment or transaction replication.

<img src="./docs/assets/logos/trino-gateway-v.png"/>
