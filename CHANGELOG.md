# Changelog

## 2.0.0

The collection engine was rewritten to be safe on production clusters and to report exactly what was read. Internals are described in [Documentation/ARCHITECTURE.md](Documentation/ARCHITECTURE.md).

### Fixed: data that was silently missed
- Every topic consumer was killed after a fixed 25 s (`timeout` around the consumer), whether or not the topic had been read. The "smart-stop" never worked because partition and offset were not printed. Each partition is now read with an exact, bounded consumer, and completeness is verified.
- `--max-age` was applied after reading every topic from the beginning, so the 25 s budget was spent on old data. The window is now converted to per-partition start offsets (`offsetsForTimes`).
- `--max-messages` kept the oldest messages. It now keeps the newest N per partition.
- Record keys and headers were not searched. Ids that only appeared in the key were missed.
- A single non-UTF-8 record aborted the rest of a topic. Decoding now uses `errors=replace`.
- The consumer's stderr was never drained and could block the consumer.
- A device id with dashes did not match the same id without dashes (or vice versa). Both UUID spellings now match, and `--match` adds extra identifiers.
- Events carried a fake partition (`0`) and offset (line number). They now carry the real partition, offset, key and headers.

### Production safety
- Kafka CLI JVMs run with a small heap, SerialGC, C1 only, under `nice`, and with JMX and `-javaagent` stripped. Previously they used the default heap inside the broker container and could clash with the broker's JMX/exporter ports.
- The pod's memory working set is measured first. Parallelism is reduced (or the run refused) when another JVM could OOM-kill the pod, and broker pods are limited to 2 concurrent JVMs.
- No consumer groups, commits or rebalances. `client.id=komnitrace` makes the load identifiable on the brokers.
- Every in-pod read is bounded (`--max-messages`, `--timeout-ms`, `timeout`). Nothing keeps running after Ctrl-C or a dropped connection.
- Added `--dry-run` and a scan budget (`--max-total-messages`, `--yes`).
- The per-topic "accessibility test" consumer, which cost one extra JVM per topic, was removed.

### Performance
- Offsets for all topics take 2–4 JVM launches in total, instead of 2 per topic.
- Records are filtered inside the pod (`awk`, falling back to `grep`), so only matches cross `kubectl exec`/SSH.
- One multiplexed SSH connection carries all commands. Workers are threads instead of processes, and the longest work units are scheduled first.

### Security
- The remote command is built with `shlex.join`; the old ad-hoc quoting broke on quotes, `$` or `;`.
- The SSH password is passed through `sshpass -e` (`SSHPASS` env var) instead of argv, where it was visible in `ps`.
- Host keys are verified (`accept-new`) by default; `--ssh-insecure` skips verification.
- Key setup is opt-in (`--ssh-setup-keys`) and uses ed25519. `~/.ssh/config` is no longer modified. An existing working key is now actually reused.
- Kafka content is HTML-escaped in reports (XSS fix).
- Payloads are no longer logged at INFO. The log moved to the output directory.

### New options
`--since`, `--until`, `--match`, `--topic-regex`, `--exclude-regex`, `--dry-run`, `--max-total-messages`, `--yes`, `--partition-timeout`, `--partitions-per-exec`, `--tool-heap`, `--client-id`, `--kafka-bin-dir`, `--kafka-client-config` (SASL/SSL), `--kubectl`, `--context`, `--no-remote-filter`, `--unsafe`, `--ssh-insecure`, `--verbose`.

### New outputs
- `coverage_<device>.json`: per-partition expected vs read, last offset, status, retention and cap flags.
- `plan_<device>.json`: the scan plan.

### Migration notes
- `--max-messages` is now per partition and keeps the newest messages.
- `--skip-offsets` is accepted but ignored.
- `--no-graphs` is an alias of `--no-reports`.
- SSH keys are no longer installed automatically; pass `--ssh-setup-keys` once if you want one. Password auth works with any `--max-parallel` thanks to multiplexing.
- Against a broker pod, parallelism is capped at 2. Point `--kafka-pod` to a client pod, or use `--unsafe`.
- Python 3.8+ is required. `bokeh`/`numpy` are no longer used.
