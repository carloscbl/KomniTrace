# KomniTrace internals

This document explains how a trace runs, why each design decision was made, and how to diagnose or extend it. For usage see the [README](../README.md).

## Pipeline

```
 local machine                         SSH host (optional)             target pod (Kafka CLI tools)
 ─────────────                         ───────────────────             ────────────────────────────
 KomniTracer
   Transport.open() ── ssh -M (once) ──▶ master connection
   probe()          ── kubectl exec ──────────────────────────────────▶ PROBE_SCRIPT (no JVM)
   fetch_offsets()  ── kubectl exec ──────────────────────────────────▶ kafka-get-offsets × 2–4
   build_plan()       (pure Python)
   _run_unit() × N  ── kubectl exec ──────────────────────────────────▶ per-partition consumers | awk
   _finish()          events / coverage / plan JSON, then reports
```

Every remote command is `kubectl exec <pod> -- sh -c '<preamble>\n<script>'`. Over SSH, that argv is `shlex.join`-ed and multiplexed over a single master connection (`ControlMaster=yes` once, `ControlMaster=no` afterwards).

### 1. Transport (`Transport`)

- **Local mode.** It runs `kubectl` directly. `--kubectl` accepts multi-word commands such as `k3s kubectl`.
- **SSH mode.** It opens one master with `ssh -M -N -f -o ControlPersist=900`. The master's stderr goes to a temp file, never to a pipe: a backgrounded master keeps inherited pipes open, which would hang `subprocess.run`.
  - Later commands authenticate through the master, so password auth (`sshpass -e`, password read from the `SSHPASS` env var) works with parallelism.
  - The socket lives in a private temp directory (`ControlPath=<tmp>/%C`, which keeps the path short and avoids collisions) and is removed on exit.
- **Host keys.** The default is `StrictHostKeyChecking=accept-new`; `--ssh-insecure` restores the old "trust everything" behaviour.
- **Dedicated key (`SSHKeyManager`).** An existing `~/.ssh/komni_trace_<host>` key is reused when it still works. A new one is installed only with `--ssh-setup-keys`. `~/.ssh/config` is never written. `--ssh-clean-keys` removes the key and the block that older versions appended to `~/.ssh/config`.

### 2. Probe (`PROBE_SCRIPT`)

The probe uses shell builtins only, with no JVM. It reports:

| Key | Meaning | Used for |
|-----|---------|----------|
| `get_offsets`, `consumer`, `topics`, `run_class` | Kafka scripts found (with or without `.sh`) | Choosing the modern or legacy offsets path |
| `awk`, `grep` | In-pod filter available | `awk` filter, then `grep -F`, then local filtering |
| `timeout` | Hard per-partition cap is possible | Wrapping each consumer in `timeout` |
| `mem_limit`, `mem_used`, `mem_inactive_file` | cgroup v2/v1 memory | Parallelism guard |
| `broker` | A `kafka.Kafka` process runs in the pod | Broker guard |

Every script starts with `pod_preamble()`, which:

- extends `PATH` with the usual Kafka bin dirs (or `--kafka-bin-dir`);
- defines `kt_bin` to resolve a tool with or without `.sh`;
- exports `JMX_PORT=` and strips `-javaagent:` from `KAFKA_OPTS`, so tool JVMs never bind the broker's JMX or exporter ports;
- sets a small heap (`-Xms32m -Xmx<tool-heap>`) and `-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:+ExitOnOutOfMemoryError`;
- defines `KT_NICE="nice -n 10"` when `nice` is available.

### 3. Parallelism guards (`safe_parallelism`)

1. **SSH:** at most 8 sessions (sshd's default `MaxSessions` is 10 per connection).
2. **Broker pod:** at most 2 concurrent JVMs unless `--unsafe`.
3. **Memory:** `working_set = usage − inactive_file`, which is how kubelet accounts for OOM and eviction; page cache does not count. Each tool JVM is estimated at `tool_heap + 128 MiB`, and the tool uses at most 50% of the free headroom.
   - If not even one JVM fits, the run is refused, because it could OOM-kill the pod (a broker, in the worst case).

### 4. Offsets (`fetch_offsets`)

- **Modern path (Kafka ≥ 3.0).** One `kubectl exec` runs `kafka-get-offsets --time X` once per time spec, each preceded by a `KTMARK|<label>` line:
  - `earliest` (`-2`) and `latest` (`-1`);
  - `since` (window start, in ms) and `until` (window end, in ms), only when those bounds exist.

  Without `--topics`, all non-internal topics are queried in a single call; with `--topics`, an anchored regex is passed. If `--bootstrap-server` is rejected, the query is retried with `--broker-list`.
- **Legacy path (< 3.0).** It lists topics with `kafka-topics --list`, then runs `kafka-run-class kafka.tools.GetOffsetShell` per topic and time spec, all inside one exec.
- **Output parsing.** The format is `topic:partition:offset`. An empty offset (no record at or after the timestamp) is stored as `None`.

### 5. Plan (`build_plan`)

For each partition:

```
start = max(offset_for_time(since), earliest)   # since: --since or now - --max-age
end   = min(offset_for_time(until), latest)     # until: --until or "latest"
skip if end <= start, or if no record exists at/after `since`
if --max-messages and end - start > N: start = end - N  (newest N; flagged capped)
retention_hit = earliest > 0 and offset_for_time(since) <= earliest
```

The full plan is written to `plan_<device>.json` and summarised per topic in the log. Plans above `--max-total-messages` stop here unless `--yes`, and `--dry-run` always stops here.

### 6. Scan (`_unit_script`, `_run_unit`)

Partitions of one topic are grouped into work units of up to `--partitions-per-exec` partitions. Units are sorted longest first and run on a `ThreadPoolExecutor`; the work is I/O-bound, so threads are enough. Each unit is one `kubectl exec`:

```sh
{
for spec in P:OFFSET:COUNT ...; do
  echo "KTMARK|start|P" >&2; echo "KTMARK|p|P"
  timeout <partition-timeout> nice -n 10 kafka-console-consumer \
      --partition P --offset OFFSET --max-messages COUNT --timeout-ms <timeout*1000> \
      --consumer-property client.id=komnitrace --consumer-property enable.auto.commit=false \
      --property print.timestamp/partition/offset/headers/key=true --property key.separator='|KT|'
  echo "KTMARK|end|P|$?" >&2
done
} | awk '<filter>'
```

- **Every in-pod process is bounded.** It stops when it reaches the exact message count, after `--timeout` seconds without records, or when the hard `timeout` fires. The client can disappear (Ctrl-C, SSH drop) without leaving consumers behind.
- **No consumer group** is joined or committed, and nothing is tailed live.
- **The `awk` filter.** Search terms come from the `KT_TERMS` environment variable, so they need no awk escaping. It prints each matching line preceded by `KTMARK|p|P`. It remembers the last `Offset:` seen per partition and emits `KTMARK|last|P|OFFSET`, which drives the coverage verdict.
- **The `grep -F -i` fallback** passes the `KTMARK|` lines through but cannot report the last offset.
- **Local side.**
  - **stdout:** markers and records. Records are re-checked against the terms and parsed by `parse_record`. Columns that older CLIs do not print are tolerated, and the value is always the last column, so it may contain the separator.
  - **stderr:** `KTMARK|start/end`, `Processed a total of N messages`, and error lines. A dedicated thread drains it, so a full pipe can never block the consumer.
- **Watchdog.** A per-unit timer kills the local process after `partition_timeout × partitions + 120 s`.

### 7. Coverage verdicts

| Status | Condition |
|--------|-----------|
| `complete` | `consumed ≥ expected`, or the last offset read is within 3 of `end − 1`. The difference is offset gaps (compaction holes, or transaction markers that usually close a transactional partition) and is noted. |
| `incomplete` | Fewer records than planned, and the end was not reached. Usually `--timeout` expired; the note shows the last offset reached. |
| `hard_timeout` | Killed by `--partition-timeout` (rc 124). |
| `error` | The consumer never reported a count (auth, connectivity, missing tool...). The note carries the last error line. |
| `cancelled` / `not_run` | Interrupted before running. |

`coverage_<device>.json` keeps every field of the verdict: `earliest`, `latest`, `start`, `end`, `expected`, `consumed`, `last_offset`, `matches`, `rc`, `status`, `note`, `capped` and `retention_hit`.

### 8. Matching

`build_search_terms` combines:

- the device id;
- the other UUID spelling (32-hex ⇄ dashed);
- every `--match` term;
- the JSON `\/` escape variant of any term containing `/`.

Matching is a case-insensitive substring search over the whole printed line, so it covers the value, key and headers. Values are JSON-decoded only after they match. A value spread over several lines can only be matched on the line that holds the term; such lines are logged at debug level and skipped.

## Output schema

`events_<device>.json` is a list of objects with these fields:

| Field | Description |
|-------|-------------|
| `timestamp` | ISO local time (record CreateTime or LogAppendTime) |
| `topic`, `partition`, `offset` | Real coordinates of the record |
| `key`, `headers` | `null` when absent |
| `event_type` | First present of `event_type`, `action_type`, `eventType`, `actionType`, `type`, `action`, `operation`, `method` |
| `message` | Raw value |
| `raw_data` | Parsed JSON, `{"raw_message": ...}` if the value is not JSON, or `{"value": ...}` for non-object JSON |

## Extending

- **Another backend** (e.g. `kcat`, which can read a whole topic from a timestamp in one lightweight process): add a probe key, then an alternative to `_unit_script` that emits the same marker protocol. Coverage and parsing stay unchanged.
- **New guard:** add it in `safe_parallelism` and log the reason, so users understand why parallelism dropped.

## Testing without a cluster

Put a fake `kubectl` first in `PATH` that runs the `sh -c` script locally (or `docker exec <kafka container>` for a real local broker), together with fake `kafka-get-offsets.sh` / `kafka-console-consumer.sh` scripts that print the formats described above. All of the behaviour described here was validated that way, including against Apache Kafka 4.0.
