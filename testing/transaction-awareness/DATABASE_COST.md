# Database measurements and cost estimates

Keep raw receipts outside this public repository. Resource names, credentials,
endpoints, and actual environment settings are runtime inputs only.

Record the immutable Gateway image, replica count, workload, and actual UTC
measurement window for every case. Measure an idle baseline with the same
Gateway image, database configuration, replica count, and observer cadence.
Keep database configuration fixed during each measurement.

The HTTP load receipt records attempts, successes, errors, and per-second
minimum, maximum, and duration-weighted average rates. These are HTTP requests:
statement POSTs and continuation GETs, not a count of SQL statements or
database operations. Warmup and cleanup have separate accounting.

Run the [connection observer](deploy/OBSERVE_DATABASE.md) across each idle and
load window. It uses one persistent read-only database connection and reports
sampled connection minima, maxima, averages, states, and wait types. Its own
connection is excluded from client counts; its counter overhead still exists.

`database_metrics.py capture` reads the single Serverless writer's CloudWatch
metrics. It records configured minimum/maximum ACUs separately from observed
capacity minimum/maximum/average and integrated ACU-seconds. It also collects
database connections, CPU, free memory, commit throughput, and network traffic.
The collector rejects multi-instance clusters; those require summing capacity
across instances, not taking a cluster average.

```sh
python3 database_metrics.py capture \
  --profile '<runtime-profile>' --region '<region>' \
  --cluster '<isolated-cluster>' --instance '<isolated-writer>' \
  --case-id '<unique-case>' --gateway-image '<image>@sha256:<digest>' \
  --replicas 20 --start '<UTC-start>' --end '<UTC-end>' \
  --output /private/tmp/database-case.json

python3 database_metrics.py report \
  --capture /private/tmp/database-case.json \
  --load /private/tmp/load-case.json --idle /private/tmp/database-idle.json \
  --acu-hour-usd '<verified-regional-rate>' \
  --io-million-usd '<verified-storage-mode-rate>' \
  --output /private/tmp/database-cost-case.json
```

Use at least five-minute UTC-aligned windows for I/O cost comparisons. The load
client supports `--measurement-start-utc`; retain its actual timestamps and
reported drift. The cost report allows at most one second of boundary drift
and includes that error explicitly. Short throughput tests remain useful but
cannot establish an exact I/O cost per case. Capture a following idle tail:
asynchronous writes and cleanup can move database work outside the load window.

CloudWatch capacity uses 60-second buckets; incomplete capacity sample counts
prevent a compute cost claim. Connection metrics are more sparsely sampled:
neither CloudWatch nor the one-second observer proves an instantaneous peak.
Missing buckets remain missing. Retrieve into a new private file after metrics
publish; never replace missing data with zero.

Aurora volume read/write metrics are five-minute operation counts. The collector
uses the cluster dimension and `Sum`, not a requests-per-second gauge. It emits
I/O cost only for complete aligned five-minute coverage. These counts differ
from PostgreSQL logical reads, WAL bytes, commits, and SQL calls.

Compute estimate = integrated ACU-seconds / 3600 × supplied ACU-hour price.
I/O estimate = observed volume operations / 1,000,000 × supplied I/O price.
Supply zero I/O price only when the verified storage mode includes I/O.
Record the price source and observation date with private run metadata.

Report allocated cost per attempted and successful HTTP request, together with
the error rate and valid-run flag. A failed throughput run can still incur cost;
its unit cost must not be presented as healthy-system capacity. Compare allocated
compute against the matching idle baseline separately. A zero allocation delta
at the capacity floor does not mean requests cause no database work.

These are shared-window estimates, not an AWS invoice or per-query CPU bill.
Storage, backups, transfer, Gateway compute, discounts, and tax are excluded.
Background work and rejected requests contribute to the successful-request
denominator's allocated cost. Idle I/O and tail I/O should be reported separately.

References:

- [Aurora CloudWatch metrics](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraMonitoring.Metrics.html)
- [Aurora monitoring](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/monitoring-cloudwatch.html)
- [Serverless capacity settings](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2.setting-capacity.html)
- [Aurora pricing](https://aws.amazon.com/rds/aurora/pricing/)
- [PostgreSQL statistics](https://www.postgresql.org/docs/17/monitoring-stats.html)
