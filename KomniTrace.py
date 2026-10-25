#!/usr/bin/env python3
"""
KomniTrace: Kubernetes Kafka Event Tracer

Traces every Kafka record that mentions a device (or any other identifier)
across the topics of a Kafka cluster running in Kubernetes, and renders the
result as JSON + HTML reports.

Collection model (production-safe by design):
- Offsets for ALL topics are resolved with a handful of JVM launches
  (earliest / latest / window start / window end) instead of one per topic.
- The time window is translated to per-partition start offsets with
  offsetsForTimes, so only the requested window is read from the brokers
  (old segments outside the window are never touched).
- Each partition is read with an exact, bounded consumer
  (--partition/--offset/--max-messages + inactivity guard + hard timeout):
  no consumer groups, no rebalances, no commits, no live tailing, and every
  in-pod process terminates by itself even if the client disappears.
- Filtering happens inside the pod (grep -F) so only matching records travel
  through kubectl exec / SSH.
- Tool JVMs run with a small heap, SerialGC, nice, and JMX/javaagents stripped;
  pod memory headroom is checked before choosing the parallelism.
- Per-partition coverage (expected vs consumed messages) is reported, so gaps
  in what was actually read are visible instead of silent.

Usage:
    python KomniTrace.py --namespace my-namespace --device-id "12345"
"""

import argparse
import json
import logging
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import orjson
    JSON_LOADS = orjson.loads
    JSON_DECODE_ERROR = orjson.JSONDecodeError
except ImportError:
    JSON_LOADS = json.loads
    JSON_DECODE_ERROR = json.JSONDecodeError

try:
    from komni_reports import KomniReportGenerator
except ImportError:
    KomniReportGenerator = None


logger = logging.getLogger("komnitrace")

# Field separator for kafka-console-consumer output (applies between every printed column)
FIELD_SEP = "|KT|"
# Marker lines emitted by the in-pod scripts
MARK = "KTMARK|"
TS_RE = re.compile(r'^(CreateTime|LogAppendTime):(-?\d+)$')
OFFSET_LINE_RE = re.compile(r'^([A-Za-z0-9._-]+):(\d+):(-?\d*)$')
PROCESSED_RE = re.compile(r'Processed a total of (\d+) messages')
OFFSET_COL_RE = re.compile(r'\|KT\|Offset:(\d+)\|KT\|')
AWK_FILTER = r'''awk 'BEGIN { n = split(tolower(ENVIRON["KT_TERMS"]), T, "\n") }
/^KTMARK[|]p[|]/ { if (last != "") print "KTMARK|last|" cur "|" last; cur = substr($0, 10); last = ""; next }
{ if (match($0, /[|]KT[|]Offset:[0-9]+[|]KT[|]/)) last = substr($0, RSTART + 11, RLENGTH - 15)
  l = tolower($0); for (i = 1; i <= n; i++) if (index(l, T[i])) { print "KTMARK|p|" cur; print; break } }
END { if (last != "") print "KTMARK|last|" cur "|" last }'
'''
HEX32_RE = re.compile(r'^[0-9a-fA-F]{32}$')
UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

# Estimated non-heap footprint of a Kafka CLI JVM (metaspace, code cache, threads)
JVM_OVERHEAD_MB = 128
# Default sshd MaxSessions is 10 per multiplexed connection; keep room for the control session
SSH_MAX_SESSIONS = 8
# Trailing offsets that may be transaction control records rather than missing data
TRAILING_MARKER_TOLERANCE = 3


def setup_logging(output_dir: str, verbose: bool):
    os.makedirs(output_dir, exist_ok=True)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    file_handler = logging.FileHandler(os.path.join(output_dir, 'komni_trace.log'))
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)
    # Root handlers so komni_reports logs land in the same places; only our logger goes down to DEBUG
    root = logging.getLogger()
    root.handlers[:] = [console, file_handler]
    root.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)


# --------------------------------------------------------------------------------------
# Configuration and data model
# --------------------------------------------------------------------------------------

@dataclass
class KafkaEvent:
    """Represents a Kafka event with metadata"""
    timestamp: datetime
    topic: str
    device_id: str
    event_type: str
    message: str
    raw_data: Dict[str, Any]
    partition: Optional[int] = None
    offset: Optional[int] = None
    key: Optional[str] = None
    headers: Optional[str] = None


@dataclass
class TraceConfig:
    """Configuration for the trace operation"""
    namespace: str
    device_id: str
    kafka_broker: str
    max_age_hours: int = 24 * 8
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    max_parallel_topics: int = 4
    output_dir: str = "./trace_output"
    consumer_inactivity_timeout: int = 15  # seconds without records before a partition read gives up
    partition_timeout: int = 900  # hard wall-clock cap per partition read (enforced inside the pod)
    topic_filter: Optional[str] = None
    topic_regex: Optional[str] = None
    exclude_regex: Optional[str] = r'^_'
    topic_list: Optional[List[str]] = None
    max_messages: Optional[int] = None  # per partition, newest messages win
    max_total_messages: int = 20_000_000
    extra_terms: List[str] = field(default_factory=list)
    remote_filter: bool = True
    partitions_per_exec: int = 16
    ssh_host: Optional[str] = None
    ssh_user: Optional[str] = None
    ssh_password: Optional[str] = None
    ssh_key: Optional[str] = None
    ssh_setup_keys: bool = False
    ssh_no_key_setup: bool = False
    ssh_clean_keys: bool = False
    ssh_insecure: bool = False
    kafka_pod_selector: str = "deployment/kafka"
    kafka_container: Optional[str] = None
    kafka_bin_dir: Optional[str] = None
    kafka_client_config: Optional[str] = None  # path INSIDE the pod (SASL/SSL properties)
    kubectl: str = "kubectl"
    kube_context: Optional[str] = None
    tool_heap: str = "256m"
    client_id: str = "komnitrace"
    assume_yes: bool = False
    dry_run: bool = False
    unsafe: bool = False


@dataclass
class PartitionTask:
    topic: str
    partition: int
    earliest: int
    latest: int
    start: int
    end: int
    capped: bool = False
    retention_hit: bool = False

    @property
    def expected(self) -> int:
        return max(0, self.end - self.start)


@dataclass
class PartitionResult:
    topic: str
    partition: int
    earliest: int
    latest: int
    start: int
    end: int
    expected: int
    consumed: Optional[int] = None
    last_offset: Optional[int] = None
    matches: int = 0
    rc: Optional[int] = None
    status: str = "not_run"
    note: str = ""
    capped: bool = False
    retention_hit: bool = False


# --------------------------------------------------------------------------------------
# SSH key management (opt-in; multiplexing already removes the need for keys)
# --------------------------------------------------------------------------------------

class SSHKeyManager:
    """Manages a dedicated SSH key per host. Only installs keys when explicitly asked to."""

    def __init__(self, config: TraceConfig):
        self.config = config
        self.ssh_dir = Path.home() / '.ssh'
        self.key_name = f"komni_trace_{config.ssh_host.replace('.', '_')}"
        self.private_key_path = self.ssh_dir / self.key_name
        self.public_key_path = self.ssh_dir / f"{self.key_name}.pub"
        self.config_path = self.ssh_dir / 'config'

    def _host_key_opts(self) -> List[str]:
        if self.config.ssh_insecure:
            return ['-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null']
        return ['-o', 'StrictHostKeyChecking=accept-new']

    def _password_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env['SSHPASS'] = self.config.ssh_password or ''
        return env

    def generate_ssh_key(self) -> bool:
        if self.private_key_path.exists():
            logger.info(f"SSH key already exists: {self.private_key_path}")
            return True
        self.ssh_dir.mkdir(mode=0o700, exist_ok=True)
        logger.info(f"Generating ed25519 SSH key for {self.config.ssh_host}...")
        cmd = ['ssh-keygen', '-t', 'ed25519', '-f', str(self.private_key_path), '-N', '',
               '-C', f'komni_trace_{self.config.ssh_host}_{int(time.time())}']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error(f"Failed to generate SSH key: {result.stderr.strip()}")
            return False
        self.private_key_path.chmod(0o600)
        return True

    def copy_public_key_to_remote(self) -> bool:
        if not self.public_key_path.exists() or not shutil.which('sshpass'):
            logger.error("Public key missing or sshpass not installed; cannot install key")
            return False
        public_key = self.public_key_path.read_text().strip()
        setup_cmd = (
            "umask 077; mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys && "
            f"(grep -qxF {shlex.quote(public_key)} ~/.ssh/authorized_keys || "
            f"echo {shlex.quote(public_key)} >> ~/.ssh/authorized_keys)"
        )
        cmd = ['sshpass', '-e', 'ssh', *self._host_key_opts(),
               '-o', 'ConnectTimeout=10', '-o', 'PreferredAuthentications=password,keyboard-interactive',
               '-o', 'PubkeyAuthentication=no', '-o', 'NumberOfPasswordPrompts=1', '-o', 'LogLevel=ERROR',
               f"{self.config.ssh_user}@{self.config.ssh_host}", setup_cmd]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=45, env=self._password_env())
        if result.returncode != 0:
            logger.error(f"Failed to install public key: {result.stderr.strip()}")
            return False
        logger.info("Public key installed on remote host")
        return True

    def key_works(self) -> bool:
        if not self.private_key_path.exists():
            return False
        cmd = ['ssh', '-i', str(self.private_key_path), *self._host_key_opts(),
               '-o', 'ConnectTimeout=10', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
               '-o', 'PreferredAuthentications=publickey', '-o', 'LogLevel=ERROR',
               f"{self.config.ssh_user}@{self.config.ssh_host}", 'echo komni_key_ok']
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        except subprocess.TimeoutExpired:
            return False
        return result.returncode == 0 and 'komni_key_ok' in result.stdout

    def clean_existing_keys(self):
        """Remove the dedicated key and any legacy ~/.ssh/config block written by older versions"""
        for path in (self.private_key_path, self.public_key_path):
            if path.exists():
                path.unlink()
                logger.info(f"Removed {path}")
        if self.config_path.exists():
            marker = f"# KomniTrace optimized config for {self.config.ssh_host}"
            lines = self.config_path.read_text().split('\n')
            new_lines, skip = [], False
            for line in lines:
                stripped = line.strip()
                if stripped == marker:
                    skip = True
                    continue
                if skip and stripped.startswith('Host ') and not stripped.endswith(self.config.ssh_host):
                    skip = False
                if skip and stripped.startswith('# KomniTrace optimized config for'):
                    skip = False
                if not skip:
                    new_lines.append(line)
            self.config_path.write_text('\n'.join(new_lines))

    def resolve(self) -> Optional[str]:
        """Return a working key path, installing one only when --ssh-setup-keys is set."""
        if self.config.ssh_clean_keys:
            self.clean_existing_keys()
        if self.key_works():
            logger.info(f"Reusing working SSH key {self.private_key_path}")
            return str(self.private_key_path)
        if not self.config.ssh_setup_keys or not self.config.ssh_password:
            return None
        if self.generate_ssh_key() and self.copy_public_key_to_remote() and self.key_works():
            logger.info("SSH key installed and verified")
            return str(self.private_key_path)
        logger.warning("SSH key setup failed, continuing with password authentication")
        return None


def _check_ssh_dependencies(config: TraceConfig) -> bool:
    if shutil.which('ssh') is None:
        logger.error("OpenSSH client (ssh) is required for remote execution")
        return False
    if config.ssh_password and shutil.which('sshpass') is None:
        logger.error("Password authentication requires sshpass (e.g. apt-get install sshpass) or use --ssh-key")
        return False
    return True


# --------------------------------------------------------------------------------------
# Command transport: local kubectl, or kubectl on a remote host through one multiplexed SSH connection
# --------------------------------------------------------------------------------------

class Transport:
    def __init__(self, config: TraceConfig):
        self.config = config
        self.remote = bool(config.ssh_host and config.ssh_user)
        self.control_dir = tempfile.mkdtemp(prefix="kt-ssh-") if self.remote else None
        self.master_open = False
        self.env = os.environ.copy()
        if config.ssh_password:
            self.env['SSHPASS'] = config.ssh_password
        self._procs = set()
        self._lock = threading.Lock()

    # --- SSH ---
    def _ssh_base(self, master: bool = False) -> List[str]:
        c = self.config
        cmd = []
        if c.ssh_password and not c.ssh_key:
            cmd += ['sshpass', '-e']
        cmd += ['ssh']
        if c.ssh_key:
            cmd += ['-i', os.path.expanduser(c.ssh_key), '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes']
        if c.ssh_insecure:
            cmd += ['-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null']
        else:
            cmd += ['-o', 'StrictHostKeyChecking=accept-new']
        cmd += ['-o', f'ControlPath={self.control_dir}/%C',
                '-o', 'ControlMaster=yes' if master else 'ControlMaster=no',
                '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=4',
                '-o', 'Compression=yes', '-o', 'LogLevel=ERROR']
        return cmd

    def open(self):
        if not self.remote:
            if shutil.which(shlex.split(self.config.kubectl)[0]) is None:
                raise RuntimeError(f"'{self.config.kubectl}' not found locally; use --ssh-host or --kubectl")
            return
        if not _check_ssh_dependencies(self.config):
            raise RuntimeError("Missing SSH tooling")
        if not self.config.ssh_key and not self.config.ssh_no_key_setup:
            key = SSHKeyManager(self.config).resolve()
            if key:
                self.config.ssh_key = key
        # One authenticated master connection; every later command is multiplexed over it
        # (no re-authentication, so password auth works with parallelism too).
        # stderr goes to a file: a backgrounded master would keep a pipe open forever.
        target = f"{self.config.ssh_user}@{self.config.ssh_host}"
        with tempfile.TemporaryFile() as err:
            cmd = self._ssh_base(master=True) + ['-o', 'ControlPersist=900', '-N', '-f', target]
            try:
                rc = subprocess.run(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=err,
                                    env=self.env, timeout=40).returncode
            except subprocess.TimeoutExpired:
                rc = -1
            err.seek(0)
            err_text = err.read().decode(errors='replace').strip()
        if rc == 0:
            self.master_open = True
            logger.info(f"SSH master connection established to {target} (multiplexed)")
        else:
            hint = ""
            if "REMOTE HOST IDENTIFICATION HAS CHANGED" in err_text or "Host key verification failed" in err_text:
                hint = " (host key changed: fix ~/.ssh/known_hosts or pass --ssh-insecure for lab hosts)"
            elif "Permission denied" in err_text:
                hint = " (check user/password/key)"
            raise RuntimeError(f"Cannot open SSH connection to {target}: {err_text or rc}{hint}")

    def close(self):
        with self._lock:
            procs = list(self._procs)
        for p in procs:
            _kill(p)
        if self.remote and self.master_open:
            subprocess.run(['ssh', '-o', f'ControlPath={self.control_dir}/%C', '-O', 'exit',
                            f"{self.config.ssh_user}@{self.config.ssh_host}"],
                           capture_output=True, timeout=10)
            self.master_open = False
        if self.control_dir:
            shutil.rmtree(self.control_dir, ignore_errors=True)

    # --- execution ---
    def wrap(self, cmd: List[str]) -> List[str]:
        if not self.remote:
            return cmd
        return self._ssh_base() + [f"{self.config.ssh_user}@{self.config.ssh_host}", shlex.join(cmd)]

    def run(self, cmd: List[str], timeout: int) -> subprocess.CompletedProcess:
        logger.debug(f"run (timeout={timeout}s): {_redact(cmd)}")
        return subprocess.run(self.wrap(cmd), capture_output=True, text=True, encoding='utf-8',
                              errors='replace', timeout=timeout, env=self.env, stdin=subprocess.DEVNULL)

    def popen(self, cmd: List[str]) -> subprocess.Popen:
        logger.debug(f"popen: {_redact(cmd)}")
        proc = subprocess.Popen(self.wrap(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                stdin=subprocess.DEVNULL, text=True, encoding='utf-8', errors='replace',
                                bufsize=1, env=self.env)
        with self._lock:
            self._procs.add(proc)
        return proc

    def forget(self, proc: subprocess.Popen):
        with self._lock:
            self._procs.discard(proc)

    # --- kubectl ---
    def kubectl_exec(self, script: str) -> List[str]:
        c = self.config
        cmd = shlex.split(c.kubectl)
        if c.kube_context:
            cmd += ['--context', c.kube_context]
        cmd += ['exec', '-n', c.namespace, c.kafka_pod_selector]
        if c.kafka_container:
            cmd += ['-c', c.kafka_container]
        return cmd + ['--', 'sh', '-c', script]


def _redact(cmd: List[str]) -> str:
    text = ' '.join(cmd)
    return text if len(text) < 400 else text[:400] + '...'


def _kill(proc: subprocess.Popen):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


# --------------------------------------------------------------------------------------
# In-pod shell snippets
# --------------------------------------------------------------------------------------

def pod_preamble(config: TraceConfig) -> str:
    extra = "/opt/kafka/bin:/opt/bitnami/kafka/bin:/usr/local/kafka/bin:/opt/confluent/bin"
    path = f'{shlex.quote(config.kafka_bin_dir)}:"$PATH":{extra}' if config.kafka_bin_dir else f'"$PATH":{extra}'
    jvm_opts = "-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:+ExitOnOutOfMemoryError -Djava.awt.headless=true"
    return "\n".join([
        f"export PATH={path}",
        'kt_bin() { for n in "$1.sh" "$1"; do if command -v "$n" >/dev/null 2>&1; then echo "$n"; return 0; fi; done; return 127; }',
        # Tool JVMs must not bind the broker's JMX port nor load its javaagents (jmx exporter ports, extra memory)
        "export JMX_PORT=",
        'if [ -n "$KAFKA_OPTS" ]; then KAFKA_OPTS=$(printf "%s" "$KAFKA_OPTS" | sed "s/-javaagent:[^ ]*//g"); export KAFKA_OPTS; fi',
        f"export KAFKA_HEAP_OPTS={shlex.quote('-Xms32m -Xmx' + config.tool_heap)}",
        f"export KAFKA_JVM_PERFORMANCE_OPTS={shlex.quote(jvm_opts)}",
        'KT_NICE=; if command -v nice >/dev/null 2>&1; then KT_NICE="nice -n 10"; fi',
    ])


PROBE_SCRIPT = r'''
echo "KTPROBE|get_offsets|$(kt_bin kafka-get-offsets)"
echo "KTPROBE|consumer|$(kt_bin kafka-console-consumer)"
echo "KTPROBE|topics|$(kt_bin kafka-topics)"
echo "KTPROBE|run_class|$(kt_bin kafka-run-class)"
echo "KTPROBE|grep|$(command -v grep)"
if [ "$(KT_T=AB awk 'BEGIN{print tolower(ENVIRON["KT_T"])}' 2>/dev/null)" = ab ]; then echo "KTPROBE|awk|yes"; fi
if timeout 2 true >/dev/null 2>&1; then echo "KTPROBE|timeout|yes"; fi
if [ -r /sys/fs/cgroup/memory.max ]; then
  echo "KTPROBE|mem_limit|$(cat /sys/fs/cgroup/memory.max)"
  echo "KTPROBE|mem_used|$(cat /sys/fs/cgroup/memory.current)"
  echo "KTPROBE|mem_inactive_file|$(awk '$1 == "inactive_file" {print $2}' /sys/fs/cgroup/memory.stat 2>/dev/null)"
elif [ -r /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
  echo "KTPROBE|mem_limit|$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)"
  echo "KTPROBE|mem_used|$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes)"
  echo "KTPROBE|mem_inactive_file|$(awk '$1 == "total_inactive_file" {print $2}' /sys/fs/cgroup/memory/memory.stat 2>/dev/null)"
fi
for f in /proc/[0-9]*/cmdline; do
  [ "$f" = "/proc/$$/cmdline" ] && continue
  if tr '\000' ' ' < "$f" 2>/dev/null | grep -q 'kafka[.]Kafka '; then echo "KTPROBE|broker|yes"; break; fi
done
'''


# --------------------------------------------------------------------------------------
# Search terms and record parsing
# --------------------------------------------------------------------------------------

def build_search_terms(device_id: str, extra: List[str]) -> List[str]:
    """Device id plus the spellings other services commonly use for the same id."""
    terms = [device_id]
    if HEX32_RE.match(device_id):
        d = device_id.lower()
        terms.append(f"{d[:8]}-{d[8:12]}-{d[12:16]}-{d[16:20]}-{d[20:]}")
    elif UUID_RE.match(device_id):
        terms.append(device_id.replace('-', ''))
    terms += extra
    out = []
    for t in terms:
        for v in (t, t.replace('/', '\\/')):  # JSON encoders may escape '/'
            if v and v.lower() not in {o.lower() for o in out}:
                out.append(v)
    return out


def parse_record(line: str, partition_hint: Optional[int]) -> Optional[Dict[str, Any]]:
    """Parse '<TsType>:<ts>|KT|Partition:p|KT|Offset:o|KT|<headers>|KT|<key>|KT|<value>'.
    Columns unsupported by older Kafka CLIs are simply absent."""
    parts = line.split(FIELD_SEP)
    m = TS_RE.match(parts[0])
    if not m and parts[0] != 'NO_TIMESTAMP':
        return None
    rec = {'ts_type': m.group(1) if m else None, 'ts': int(m.group(2)) if m else None,
           'partition': partition_hint, 'offset': None, 'headers': None, 'key': None}
    i = 1
    if i < len(parts) and parts[i].startswith('Partition:') and parts[i][10:].isdigit():
        rec['partition'] = int(parts[i][10:])
        i += 1
    if i < len(parts) and parts[i].startswith('Offset:') and parts[i][7:].isdigit():
        rec['offset'] = int(parts[i][7:])
        i += 1
    rest = parts[i:]
    if len(rest) >= 3:
        rec['headers'], rec['key'], rec['value'] = rest[0], rest[1], FIELD_SEP.join(rest[2:])
    elif len(rest) == 2:
        rec['key'], rec['value'] = rest
    else:
        rec['value'] = rest[0] if rest else ''
    return rec


def _extract_event_type(data: Any) -> str:
    if not isinstance(data, dict):
        return 'unknown'
    for key in ('event_type', 'action_type', 'eventType', 'actionType', 'type', 'action', 'operation', 'method'):
        value = data.get(key)
        if value and isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    if 'message' in data:
        return 'log_message'
    if 'error' in data or 'exception' in data:
        return 'error'
    return 'unknown'


# --------------------------------------------------------------------------------------
# Tracer
# --------------------------------------------------------------------------------------

class KomniTracer:
    """Plans and executes a bounded, window-scoped scan of the cluster"""

    def __init__(self, config: TraceConfig):
        self.config = config
        self.transport = Transport(config)
        self.events: List[KafkaEvent] = []
        self.results: List[PartitionResult] = []
        self.caps: Dict[str, str] = {}
        self.terms = build_search_terms(config.device_id, config.extra_terms)
        self._terms_lower = [t.lower() for t in self.terms]
        self._stop = threading.Event()
        self.window_start, self.window_end = self._window()
        os.makedirs(config.output_dir, exist_ok=True)

    def __enter__(self):
        self.transport.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        self._stop.set()
        self.transport.close()

    def _window(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        start = self.config.since
        if start is None and self.config.max_age_hours > 0:
            start = datetime.now() - timedelta(hours=self.config.max_age_hours)
        return start, self.config.until

    def _exec(self, script: str, timeout: int) -> subprocess.CompletedProcess:
        full = pod_preamble(self.config) + "\n" + script
        return self.transport.run(self.transport.kubectl_exec(full), timeout=timeout)

    # --- capability probe & safety ---
    def probe(self):
        result = self._exec(PROBE_SCRIPT, timeout=60)
        for line in result.stdout.splitlines():
            if line.startswith('KTPROBE|'):
                _, k, v = (line.split('|', 2) + [''])[:3]
                self.caps[k] = v.strip()
        if not self.caps:
            raise RuntimeError(f"kubectl exec into {self.config.kafka_pod_selector} failed: "
                               f"{result.stderr.strip()[-800:]}")
        logger.info("Pod capabilities: " + ", ".join(f"{k}={v or '-'}" for k, v in sorted(self.caps.items())
                                                     if not k.startswith('mem_')))
        if not self.caps.get('consumer'):
            raise RuntimeError("kafka-console-consumer not found in the pod (use --kafka-bin-dir or another --kafka-pod)")
        if self.config.remote_filter and not self.caps.get('grep'):
            logger.warning("grep not available in the pod: filtering client-side (all records cross the wire)")
            self.config.remote_filter = False

    def safe_parallelism(self, requested: int) -> int:
        c = self.config
        parallel = max(1, requested)
        reasons = []
        if self.transport.remote and parallel > SSH_MAX_SESSIONS:
            parallel = SSH_MAX_SESSIONS
            reasons.append(f"sshd MaxSessions (multiplexed) -> {SSH_MAX_SESSIONS}")
        if self.caps.get('broker') == 'yes':
            logger.warning(f"{c.kafka_pod_selector} is a running Kafka BROKER: tool JVMs share its memory cgroup. "
                           "Prefer --kafka-pod pointing to a Kafka client/toolbox pod.")
            if parallel > 2 and not c.unsafe:
                parallel = 2
                reasons.append("target is a broker pod -> 2 (override with --unsafe)")
        limit, used = _to_int(self.caps.get('mem_limit')), _to_int(self.caps.get('mem_used'))
        if used is not None:
            # Working set like kubelet's eviction/OOM accounting: reclaimable page cache does not count
            used -= _to_int(self.caps.get('mem_inactive_file')) or 0
        if limit and used is not None and limit < (1 << 50):
            per_jvm = (_mem_to_mb(c.tool_heap) + JVM_OVERHEAD_MB) * 1024 * 1024
            headroom = limit - used
            allowed = int((headroom * 0.5) // per_jvm)
            logger.info(f"Pod memory: limit={limit >> 20}MiB working_set={used >> 20}MiB headroom={headroom >> 20}MiB, "
                        f"~{per_jvm >> 20}MiB per tool JVM")
            if allowed < 1 and not c.unsafe:
                raise RuntimeError(
                    f"Pod has only {headroom >> 20}MiB memory headroom; launching a tool JVM risks an OOMKill of "
                    f"{c.kafka_pod_selector}. Use a client pod (--kafka-pod), a smaller --tool-heap, or --unsafe.")
            if allowed < parallel and not c.unsafe:
                parallel = max(1, allowed)
                reasons.append(f"memory headroom -> {parallel}")
        if reasons:
            logger.warning(f"Parallelism reduced from {requested} to {parallel}: " + "; ".join(reasons))
        return parallel

    # --- offsets ---
    def _offset_cmd_args(self, legacy_flag: bool) -> List[str]:
        c = self.config
        args = ['--broker-list' if legacy_flag else '--bootstrap-server', c.kafka_broker]
        if c.kafka_client_config:
            args += ['--command-config', c.kafka_client_config]
        return args

    def fetch_offsets(self, topics_regex: Optional[str]) -> Dict[str, Dict[str, Dict[int, Optional[int]]]]:
        """Returns {label: {topic: {partition: offset}}} for labels earliest/latest/since/until"""
        specs = [('earliest', '-2'), ('latest', '-1')]
        if self.window_start:
            specs.append(('since', str(int(self.window_start.timestamp() * 1000))))
        if self.window_end:
            specs.append(('until', str(int(self.window_end.timestamp() * 1000))))
        if self.caps.get('get_offsets'):
            for legacy in (False, True):
                out = self._fetch_offsets_modern(specs, topics_regex, legacy)
                if out is not None:
                    return out
            raise RuntimeError("kafka-get-offsets failed (see log)")
        if self.caps.get('run_class') and self.caps.get('topics'):
            return self._fetch_offsets_legacy(specs, topics_regex)
        raise RuntimeError("Neither kafka-get-offsets nor kafka-run-class/kafka-topics are available in the pod")

    def _fetch_offsets_modern(self, specs, topics_regex, legacy_flag):
        base = ' '.join(shlex.quote(a) for a in self._offset_cmd_args(legacy_flag))
        topic_arg = f"--topic {shlex.quote(topics_regex)}" if topics_regex else "--exclude-internal-topics"
        lines = ['GO=$(kt_bin kafka-get-offsets)']
        for label, t in specs:
            lines.append(f'echo "{MARK}{label}"; $KT_NICE "$GO" {base} {topic_arg} --time {t} || exit $?')
        result = self._exec("\n".join(lines), timeout=300)
        if result.returncode != 0:
            logger.debug(f"kafka-get-offsets rc={result.returncode}: {result.stderr.strip()[-1500:]}")
            if not legacy_flag and 'recognized option' in result.stderr:
                return None
            logger.error(f"kafka-get-offsets failed: {result.stderr.strip()[-800:]}")
            return None
        return _parse_offsets(result.stdout)

    def _fetch_offsets_legacy(self, specs, topics_regex):
        c = self.config
        list_args = ['--bootstrap-server', c.kafka_broker, '--list']
        if c.kafka_client_config:
            list_args += ['--command-config', c.kafka_client_config]
        res = self._exec('$KT_NICE "$(kt_bin kafka-topics)" ' + ' '.join(shlex.quote(a) for a in list_args), 120)
        topics = [t.strip() for t in res.stdout.splitlines() if t.strip() and not t.startswith('_')]
        if topics_regex:
            rx = re.compile(topics_regex)
            topics = [t for t in topics if rx.fullmatch(t)]
        base = ' '.join(shlex.quote(a) for a in self._offset_cmd_args(True))
        lines = ['RC=$(kt_bin kafka-run-class)']
        for label, t in specs:
            lines.append(f'echo "{MARK}{label}"')
            for topic in topics:
                lines.append(f'$KT_NICE "$RC" kafka.tools.GetOffsetShell {base} --topic {shlex.quote(topic)} --time {t}')
        result = self._exec("\n".join(lines), timeout=120 + 10 * len(topics) * len(specs))
        return _parse_offsets(result.stdout)

    # --- planning ---
    def select_topics(self, latest: Dict[str, Dict[int, Optional[int]]]) -> List[str]:
        c = self.config
        topics = sorted(latest)
        if c.topic_list:
            missing = [t for t in c.topic_list if t not in latest]
            if missing:
                logger.warning(f"Requested topics not found in cluster: {missing}")
            topics = [t for t in c.topic_list if t in latest]
        if c.topic_filter:
            topics = [t for t in topics if c.topic_filter in t]
        if c.topic_regex:
            topics = [t for t in topics if re.search(c.topic_regex, t)]
        if c.exclude_regex and not c.topic_list:
            topics = [t for t in topics if not re.search(c.exclude_regex, t)]
        return topics

    def build_plan(self, offsets, topics: List[str]) -> List[PartitionTask]:
        c = self.config
        tasks = []
        for topic in topics:
            for p, latest in sorted(offsets['latest'].get(topic, {}).items()):
                if latest is None or latest <= 0:
                    continue
                earliest = offsets['earliest'].get(topic, {}).get(p) or 0
                start, end = earliest, latest
                retention_hit = False
                if 'since' in offsets:
                    s = offsets['since'].get(topic, {}).get(p)
                    if s is None or s < 0:  # no record at/after window start
                        continue
                    start = max(s, earliest)
                    retention_hit = earliest > 0 and s <= earliest
                if 'until' in offsets:
                    u = offsets['until'].get(topic, {}).get(p)
                    if u is not None and u >= 0:
                        end = min(u, latest)
                if end <= start:
                    continue
                capped = False
                if c.max_messages and end - start > c.max_messages:
                    start, capped = end - c.max_messages, True
                tasks.append(PartitionTask(topic, p, earliest, latest, start, end, capped, retention_hit))
        return tasks

    def log_plan(self, topics: List[str], tasks: List[PartitionTask]):
        per_topic: Dict[str, List[PartitionTask]] = {}
        for t in tasks:
            per_topic.setdefault(t.topic, []).append(t)
        total = sum(t.expected for t in tasks)
        w0 = self.window_start.isoformat(sep=' ', timespec='seconds') if self.window_start else 'beginning'
        w1 = self.window_end.isoformat(sep=' ', timespec='seconds') if self.window_end else 'now'
        logger.info(f"Window: {w0} -> {w1}; search terms: {self.terms}")
        logger.info(f"Plan: {len(per_topic)}/{len(topics)} topics with data in window, "
                    f"{len(tasks)} partitions, {total:,} messages to scan")
        for topic, ts in sorted(per_topic.items(), key=lambda kv: -sum(t.expected for t in kv[1])):
            flags = []
            if any(t.capped for t in ts):
                flags.append("CAPPED by --max-messages")
            if any(t.retention_hit for t in ts):
                flags.append("window older than retention (oldest data already deleted)")
            logger.info(f"  {topic}: {len(ts)} partitions, {sum(t.expected for t in ts):,} msgs"
                        + (f"  [{'; '.join(flags)}]" if flags else ""))

    # --- execution ---
    def _units(self, tasks: List[PartitionTask]) -> List[List[PartitionTask]]:
        per_topic: Dict[str, List[PartitionTask]] = {}
        for t in tasks:
            per_topic.setdefault(t.topic, []).append(t)
        units = []
        n = max(1, self.config.partitions_per_exec)
        for ts in per_topic.values():
            for i in range(0, len(ts), n):
                units.append(ts[i:i + n])
        # Longest first: better wall time with a bounded worker pool
        units.sort(key=lambda u: -sum(t.expected for t in u))
        return units

    def _unit_script(self, unit: List[PartitionTask]) -> str:
        c = self.config
        topic = unit[0].topic
        consumer_args = [
            '--bootstrap-server', c.kafka_broker, '--topic', topic,
            '--timeout-ms', str(c.consumer_inactivity_timeout * 1000),
            '--consumer-property', f'client.id={c.client_id}',
            '--consumer-property', 'enable.auto.commit=false',
            '--consumer-property', 'max.partition.fetch.bytes=1048576',
            '--consumer-property', 'fetch.max.bytes=8388608',
            '--property', 'print.timestamp=true', '--property', 'print.partition=true',
            '--property', 'print.offset=true', '--property', 'print.headers=true',
            '--property', 'print.key=true', '--property', f'key.separator={FIELD_SEP}',
        ]
        if c.kafka_client_config:
            consumer_args += ['--consumer.config', c.kafka_client_config]
        args = ' '.join(shlex.quote(a) for a in consumer_args)
        specs = ' '.join(f"{t.partition}:{t.start}:{t.expected}" for t in unit)
        timeout = f"timeout {c.partition_timeout}" if self.caps.get('timeout') else ""
        prelude = ""
        if c.remote_filter and self.caps.get('awk'):
            prelude = f"export KT_TERMS={shlex.quote(chr(10).join(self.terms))}\n"
            sink = AWK_FILTER.strip()
        elif c.remote_filter:
            greps = ' '.join(f"-e {shlex.quote(t)}" for t in self.terms + [MARK])
            sink = f"grep -F -i {greps}"
        else:
            sink = "cat"
        return prelude + f'''C=$(kt_bin kafka-console-consumer) || {{ echo "{MARK}fatal|consumer not found" >&2; exit 127; }}
{{
for spec in {specs}; do
  p=${{spec%%:*}}; r=${{spec#*:}}; o=${{r%%:*}}; n=${{r#*:}}
  echo "{MARK}start|$p" >&2; echo "{MARK}p|$p"
  {timeout} $KT_NICE "$C" {args} --partition "$p" --offset "$o" --max-messages "$n"
  echo "{MARK}end|$p|$?" >&2
done
}} | {sink}
exit 0'''

    def _run_unit(self, unit: List[PartitionTask]) -> Tuple[List[Dict[str, Any]], List[PartitionResult]]:
        c = self.config
        results = {t.partition: PartitionResult(t.topic, t.partition, t.earliest, t.latest, t.start, t.end,
                                                t.expected, capped=t.capped, retention_hit=t.retention_hit)
                   for t in unit}
        events: List[Dict[str, Any]] = []
        if self._stop.is_set():
            return events, list(results.values())
        cmd = self.transport.kubectl_exec(pod_preamble(c) + "\n" + self._unit_script(unit))
        proc = self.transport.popen(cmd)
        errors: Dict[int, List[str]] = {}
        stderr_tail: List[str] = []

        def read_stderr():
            current = None
            for line in proc.stderr:
                line = line.rstrip('\n')
                if line.startswith(MARK):
                    f = line[len(MARK):].split('|')
                    if f[0] == 'start' and f[1].isdigit():
                        current = int(f[1])
                        results[current].status = 'running'
                    elif f[0] == 'end' and len(f) > 2 and f[1].isdigit():
                        results[int(f[1])].rc = int(f[2]) if f[2].lstrip('-').isdigit() else None
                    continue
                m = PROCESSED_RE.search(line)
                if m and current is not None:
                    results[current].consumed = int(m.group(1))
                elif line.strip():
                    if current is not None and ('ERROR' in line or 'Exception' in line):
                        errors.setdefault(current, []).append(line.strip())
                    stderr_tail.append(line)
                    del stderr_tail[:-20]

        t_err = threading.Thread(target=read_stderr, daemon=True)
        t_err.start()
        deadline = c.partition_timeout * len(unit) + 120
        watchdog = threading.Timer(deadline, lambda: _kill(proc))
        watchdog.start()
        current_partition = None
        try:
            for line in proc.stdout:
                if self._stop.is_set():
                    _kill(proc)
                    break
                line = line.rstrip('\n')
                if line.startswith(MARK):
                    f = line[len(MARK):].split('|')
                    if f[0] == 'p' and f[1].isdigit():
                        current_partition = int(f[1])
                    elif f[0] == 'last' and f[1].isdigit() and f[2].isdigit() and int(f[1]) in results:
                        results[int(f[1])].last_offset = int(f[2])
                    continue
                if not line:
                    continue
                if not c.remote_filter and current_partition in results:
                    m = OFFSET_COL_RE.search(line)
                    if m:
                        results[current_partition].last_offset = int(m.group(1))
                low = line.lower()
                if not any(t in low for t in self._terms_lower):
                    continue
                rec = parse_record(line, current_partition)
                if rec is None:
                    logger.debug(f"Unparseable matching line (multi-line value?): {line[:200]}")
                    continue
                ev = self._to_event(unit[0].topic, rec)
                if ev is not None:
                    events.append(ev)
                    if rec['partition'] in results:
                        results[rec['partition']].matches += 1
            proc.wait()
        finally:
            watchdog.cancel()
            _kill(proc)
            t_err.join(timeout=5)
            self.transport.forget(proc)

        for r in results.values():
            errs = errors.get(r.partition, [])
            timed_out = any('TimeoutException' in e for e in errs)
            if r.status == 'not_run':
                r.note = (stderr_tail[-1] if stderr_tail else 'not started')[:300]
                if self._stop.is_set():
                    r.status, r.note = 'cancelled', ''
            elif r.consumed is not None and r.consumed >= r.expected:
                r.status = 'complete'
            elif r.last_offset is not None and r.last_offset >= r.end - 1 - TRAILING_MARKER_TOLERANCE:
                # Offsets never delivered as records: compaction holes or transaction control markers
                # (a transactional partition usually ends with a commit marker)
                r.status = 'complete'
                r.note = f'{r.expected - (r.consumed or 0)} offsets were gaps (compaction / transaction markers)'
            elif r.rc == 124:
                r.status, r.note = 'hard_timeout', f'killed after --partition-timeout {c.partition_timeout}s'
            elif r.consumed is not None and timed_out:
                r.status = 'incomplete'
                reached = f', stopped at offset {r.last_offset}' if r.last_offset is not None else ''
                r.note = (f'no records for {c.consumer_inactivity_timeout}s after {r.consumed}/{r.expected}{reached}: '
                          'slow broker or offset gaps; raise --timeout')
            elif r.consumed is not None:
                r.status = 'incomplete'
            else:
                r.status = 'error'
                r.note = (errs[-1] if errs else (stderr_tail[-1] if stderr_tail else f'rc={r.rc}'))[:300]
        return events, list(results.values())

    def _to_event(self, topic: str, rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if rec['ts'] is None or rec['ts'] < 0:
            ts = None
        else:
            ts = datetime.fromtimestamp(rec['ts'] / 1000)
        # CreateTime is producer-supplied and not monotonic: keep the window exact client-side
        if ts is not None:
            if self.window_start and ts < self.window_start:
                return None
            if self.window_end and ts >= self.window_end:
                return None
        value = rec['value']
        try:
            data = JSON_LOADS(value)
        except (JSON_DECODE_ERROR, ValueError, TypeError):
            data = {"raw_message": value}
        if not isinstance(data, dict):
            data = {"value": data}
        key = None if rec['key'] in (None, 'null') else rec['key']
        headers = None if rec['headers'] in (None, 'NO_HEADERS') else rec['headers']
        return {
            "timestamp": (ts or datetime.fromtimestamp(0)).isoformat(), "topic": topic,
            "device_id": self.config.device_id, "event_type": _extract_event_type(data),
            "message": value, "raw_data": data, "partition": rec['partition'], "offset": rec['offset'],
            "key": key, "headers": headers,
        }

    def run_trace(self) -> List[KafkaEvent]:
        c = self.config
        self.probe()
        parallel = self.safe_parallelism(c.max_parallel_topics)

        topics_regex = None
        if c.topic_list:
            topics_regex = '(' + '|'.join(re.escape(t) for t in c.topic_list) + ')'
        logger.info("Resolving offsets for all topics...")
        t0 = time.time()
        offsets = self.fetch_offsets(topics_regex)
        topics = self.select_topics(offsets.get('latest', {}))
        logger.info(f"Offsets resolved in {time.time() - t0:.1f}s for {len(offsets.get('latest', {}))} topics; "
                    f"{len(topics)} selected")
        tasks = self.build_plan(offsets, topics)
        self.log_plan(topics, tasks)
        total = sum(t.expected for t in tasks)
        self._write_json(f"plan_{c.device_id}.json", [asdict(t) for t in tasks])
        if c.dry_run:
            logger.info("Dry run: nothing consumed")
            return []
        if total > c.max_total_messages and not c.assume_yes:
            raise RuntimeError(
                f"Plan reads {total:,} messages (> --max-total-messages {c.max_total_messages:,}). Narrow it with "
                "--max-age/--since/--until/--topics/--topic-regex/--max-messages, or confirm with --yes.")
        if not tasks:
            logger.warning("No partitions have data in the requested window")
            return []

        units = self._units(tasks)
        logger.info(f"Scanning with {parallel} parallel kubectl exec sessions ({len(units)} work units)")
        raw_events: List[Dict[str, Any]] = []
        started = time.time()
        done_msgs = 0
        executor = ThreadPoolExecutor(max_workers=parallel)
        futures = {executor.submit(self._run_unit, u): u for u in units}
        try:
            for i, fut in enumerate(as_completed(futures), 1):
                unit = futures[fut]
                try:
                    evs, res = fut.result()
                except Exception as e:
                    logger.error(f"Work unit {unit[0].topic} failed: {e}", exc_info=True)
                    evs, res = [], [PartitionResult(t.topic, t.partition, t.earliest, t.latest, t.start, t.end,
                                                    t.expected, status='error', note=str(e)[:300]) for t in unit]
                raw_events.extend(evs)
                self.results.extend(res)
                done_msgs += sum(t.expected for t in unit)
                bad = [r for r in res if r.status != 'complete']
                logger.info(f"[{i}/{len(units)}] {unit[0].topic} p{unit[0].partition}-{unit[-1].partition}: "
                            f"{len(evs)} matches, {sum(r.consumed or 0 for r in res):,}/"
                            f"{sum(r.expected for r in res):,} msgs"
                            + (f", {len(bad)} partitions NOT complete" if bad else "")
                            + f" | {done_msgs * 100 // max(total, 1)}% planned msgs, {time.time() - started:.0f}s")
        except KeyboardInterrupt:
            logger.warning("Interrupted: stopping workers (in-pod consumers are bounded and exit on their own)")
            self._stop.set()
            for f in futures:
                f.cancel()
            self.transport.close()
            raise
        finally:
            executor.shutdown(wait=True)
            self._finish(raw_events, tasks)
        return self.events

    def _finish(self, raw_events: List[Dict[str, Any]], tasks: List[PartitionTask]):
        seen_results = {(r.topic, r.partition) for r in self.results}
        for t in tasks:
            if (t.topic, t.partition) not in seen_results:
                self.results.append(PartitionResult(t.topic, t.partition, t.earliest, t.latest, t.start, t.end,
                                                    t.expected, status='cancelled'))
        seen, events = set(), []
        for e in raw_events:
            k = (e['topic'], e['partition'], e['offset'], e['message'] if e['offset'] is None else None)
            if k in seen:
                continue
            seen.add(k)
            events.append(KafkaEvent(timestamp=datetime.fromisoformat(e['timestamp']), topic=e['topic'],
                                     device_id=e['device_id'], event_type=e['event_type'], message=e['message'],
                                     raw_data=e['raw_data'], partition=e['partition'], offset=e['offset'],
                                     key=e['key'], headers=e['headers']))
        events.sort(key=lambda ev: (ev.timestamp, ev.topic, ev.partition or 0, ev.offset or 0))
        self.events = events
        self.results.sort(key=lambda r: (r.topic, r.partition))
        self._write_json(f"coverage_{self.config.device_id}.json", [asdict(r) for r in self.results])
        self.log_coverage()

    def log_coverage(self):
        if not self.results:
            return
        by_status: Dict[str, int] = {}
        for r in self.results:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        expected = sum(r.expected for r in self.results)
        consumed = sum(min(r.consumed or 0, r.expected) for r in self.results)
        logger.info(f"Coverage: {consumed:,}/{expected:,} planned messages read "
                    f"({consumed * 100 / max(expected, 1):.2f}%), partitions: {by_status}")
        problems = [r for r in self.results if r.status != 'complete']
        for r in problems[:30]:
            logger.warning(f"  GAP {r.topic}[{r.partition}] {r.status}: read {r.consumed or 0}/{r.expected} "
                           f"(offsets {r.start}-{r.end}) {r.note}")
        if len(problems) > 30:
            logger.warning(f"  ... {len(problems) - 30} more, see coverage_{self.config.device_id}.json")
        retention = sorted({r.topic for r in self.results if r.retention_hit})
        if retention:
            logger.warning(f"Retention already deleted part of the window in: {retention}")
        capped = sorted({r.topic for r in self.results if r.capped})
        if capped:
            logger.warning(f"Only the newest --max-messages per partition were read in: {capped}")

    def _write_json(self, name: str, data: Any):
        path = os.path.join(self.config.output_dir, name)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        return path


def _parse_offsets(stdout: str) -> Dict[str, Dict[str, Dict[int, Optional[int]]]]:
    out: Dict[str, Dict[str, Dict[int, Optional[int]]]] = {}
    label = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith(MARK):
            label = line[len(MARK):]
            out.setdefault(label, {})
            continue
        m = OFFSET_LINE_RE.match(line)
        if m and label:
            off = m.group(3)
            out[label].setdefault(m.group(1), {})[int(m.group(2))] = int(off) if off not in ('',) else None
    return out


def _to_int(v: Optional[str]) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except ValueError:
        return None


def _mem_to_mb(v: str) -> int:
    m = re.fullmatch(r'(\d+)([kKmMgG]?)', v.strip())
    if not m:
        return 256
    n, unit = int(m.group(1)), m.group(2).lower()
    return {'k': n // 1024, 'm': n, 'g': n * 1024}.get(unit, n // (1024 * 1024))


# --------------------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------------------

def event_to_dict(e: KafkaEvent) -> Dict[str, Any]:
    return {'timestamp': e.timestamp.isoformat(), 'topic': e.topic, 'device_id': e.device_id,
            'event_type': e.event_type, 'message': e.message, 'raw_data': e.raw_data,
            'partition': e.partition, 'offset': e.offset, 'key': e.key, 'headers': e.headers}


def load_events_from_json(json_file: str) -> List[KafkaEvent]:
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            events_data = JSON_LOADS(f.read())
        events = [KafkaEvent(timestamp=datetime.fromisoformat(e['timestamp']), topic=e['topic'],
                             device_id=e['device_id'], event_type=e['event_type'], message=e['message'],
                             raw_data=e['raw_data'], partition=e.get('partition'), offset=e.get('offset'),
                             key=e.get('key'), headers=e.get('headers'))
                  for e in events_data]
        logger.info(f"Loaded {len(events)} events from {json_file}")
        return events
    except Exception as e:
        logger.error(f"Error loading events from {json_file}: {e}")
        return []


def generate_reports(events: List[KafkaEvent], config: TraceConfig) -> str:
    """Render every visualization plus the HTML report; returns the HTML report path"""
    if not KomniReportGenerator:
        logger.error("komni_reports.py could not be imported. Cannot generate reports.")
        return ""
    logger.info("Generating visualizations and HTML report...")
    generator = KomniReportGenerator(events, config)
    graph_paths = {}
    for name, fn in (('timeline', generator.generate_timeline_graph),
                     ('swimlane', generator.generate_timeline_swimlane),
                     ('flow', generator.generate_event_flow_diagram),
                     ('compact', generator.generate_compact_timeline)):
        try:
            path = fn()
            if path:
                graph_paths[name] = path
                logger.info(f"Generated {name} graph: {path}")
        except Exception as e:
            logger.warning(f"Failed to generate {name} graph: {e}")
    report_path = ""
    try:
        report_path = generator.generate_html_report(graph_paths)
        logger.info(f"Generated HTML report with {len(graph_paths)} visualizations")
    except Exception as e:
        logger.error(f"Failed to generate HTML report: {e}")
    try:
        plotly_path = generator.generate_plotly_interactive_timeline()
        if plotly_path:
            logger.info(f"Generated interactive Plotly timeline: {plotly_path}")
    except Exception as e:
        logger.warning(f"Failed to generate interactive Plotly timeline: {e}")
    return report_path


def generate_report_only(json_file: str, device_id: str, output_dir: str = "./trace_output"):
    logger.info(f"Report-only mode: Loading events from {json_file}")
    events = load_events_from_json(json_file)
    if not events:
        logger.error("No events loaded from JSON file")
        return
    config = TraceConfig(namespace="report-only", device_id=device_id, kafka_broker="", output_dir=output_dir)
    os.makedirs(output_dir, exist_ok=True)
    report_path = generate_reports(events, config)
    print("\n" + "=" * 60)
    print(f"Report generation completed: {len(events)} events for device {device_id}")
    if report_path:
        print(f"  HTML Report: {report_path}")
    print("=" * 60)


def _parse_dt(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date/time '{value}' (use ISO, e.g. 2025-09-10T15:00)")
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def main():
    parser = argparse.ArgumentParser(
        description='KomniTrace - Kafka Event Tracer for Kubernetes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Last 8 days (default window), all topics, local kubectl
  python KomniTrace.py -n my-namespace -d b40754c2bc034c9c9b29ec8e86a0edf7

  # Remote kubectl through SSH (one multiplexed connection, password works with parallelism)
  SSHPASS=secret python KomniTrace.py -n my-ns -d 12345 --ssh-host 10.0.0.1 --ssh-user root

  # Exact window, specific topics, also match the account id
  python KomniTrace.py -n my-ns -d 12345 --since 2025-09-10T08:00 --until 2025-09-10T12:00 \\
    --topics as-provision-topic device-events-topic --match ACCOUNT-42

  # See what would be read (topics, partitions, message counts) without consuming
  python KomniTrace.py -n my-ns -d 12345 --dry-run

  # Run the Kafka CLI tools in a client pod instead of a broker (recommended in production)
  python KomniTrace.py -n my-ns -d 12345 --kafka-pod deploy/kafka-client --kafka-broker kafka:9092

  # Report from an existing events JSON
  python KomniTrace.py --report-only --events-json trace_output/events_12345.json -d 12345
        """
    )
    g = parser.add_argument_group('mode')
    g.add_argument('--report-only', action='store_true', help='Generate report from an existing events JSON file')
    g.add_argument('--events-json', help='Events JSON for --report-only')
    g.add_argument('--dry-run', action='store_true', help='Resolve offsets and print the scan plan without consuming')

    g = parser.add_argument_group('search')
    g.add_argument('--device-id', '-d', required=True, help='Device ID to search for (UUID with/without dashes both match)')
    g.add_argument('--match', action='append', default=[], metavar='TERM',
                   help='Additional term to match (repeatable): account id, MAC, IP, encrypted id...')
    g.add_argument('--max-age', '-a', type=int, default=24 * 8, help='Window: last N hours (default 192, 0 = all retained data)')
    g.add_argument('--since', type=_parse_dt, help='Window start (ISO datetime, overrides --max-age)')
    g.add_argument('--until', type=_parse_dt, help='Window end (ISO datetime, default now)')

    g = parser.add_argument_group('kafka / kubernetes')
    g.add_argument('--namespace', '-n', help='Kubernetes namespace where Kafka is running')
    g.add_argument('--kafka-broker', '-b', default='kafka:9092', help='Bootstrap server as seen from the pod')
    g.add_argument('--kafka-pod', default='deployment/kafka', help='Pod/deployment where Kafka CLI tools run (prefer a client pod)')
    g.add_argument('--kafka-container', help='Container name inside --kafka-pod')
    g.add_argument('--kafka-bin-dir', help='Directory with Kafka CLI scripts inside the pod (auto-detected otherwise)')
    g.add_argument('--kafka-client-config', help='Client properties file INSIDE the pod (SASL/SSL)')
    g.add_argument('--kubectl', default='kubectl', help="kubectl command (e.g. 'k3s kubectl', 'oc')")
    g.add_argument('--context', dest='kube_context', help='kubeconfig context')
    g.add_argument('--topics', nargs='+', help='Explicit list of topics')
    g.add_argument('--topic-filter', '-f', help='Only topics containing this string')
    g.add_argument('--topic-regex', help='Only topics matching this regex')
    g.add_argument('--exclude-regex', default=r'^_', help="Skip topics matching this regex (default '^_')")

    g = parser.add_argument_group('load and safety')
    g.add_argument('--max-parallel', '-p', type=int, default=4, help='Concurrent kubectl exec sessions / tool JVMs (default 4)')
    g.add_argument('--timeout', type=int, default=15, help='Seconds without records before a partition read stops (default 15)')
    g.add_argument('--partition-timeout', type=int, default=900, help='Hard cap per partition read, enforced in the pod (default 900)')
    g.add_argument('--max-messages', '-m', type=int, default=None, help='Per partition: read only the newest N messages of the window')
    g.add_argument('--max-total-messages', type=int, default=20_000_000, help='Refuse plans larger than this unless --yes (default 20M)')
    g.add_argument('--partitions-per-exec', type=int, default=16, help='Partitions read sequentially per kubectl exec (default 16)')
    g.add_argument('--tool-heap', default='256m', help='Max heap for Kafka CLI JVMs in the pod (default 256m)')
    g.add_argument('--client-id', default='komnitrace', help='Kafka client.id (lets operators identify/throttle the scan)')
    g.add_argument('--no-remote-filter', action='store_true', help='Do not grep inside the pod (stream all records)')
    g.add_argument('--yes', '-y', action='store_true', help='Accept plans above --max-total-messages')
    g.add_argument('--unsafe', action='store_true', help='Disable memory/broker parallelism guards')
    # Accepted so old command lines keep working; offsets are always resolved now
    g.add_argument('--skip-offsets', action='store_true', help=argparse.SUPPRESS)

    g = parser.add_argument_group('ssh')
    g.add_argument('--ssh-host', help='Host where kubectl runs')
    g.add_argument('--ssh-user', help='SSH username')
    g.add_argument('--ssh-password', help='SSH password (prefer the SSHPASS env var: argv is visible in ps)')
    g.add_argument('--ssh-key', help='SSH private key file')
    g.add_argument('--ssh-setup-keys', action='store_true', help='Generate and install a dedicated key on the host')
    g.add_argument('--ssh-no-key-setup', action='store_true', help='Do not look for/reuse a KomniTrace key')
    g.add_argument('--ssh-clean-keys', action='store_true', help='Remove the KomniTrace key (and legacy ~/.ssh/config block)')
    g.add_argument('--ssh-insecure', action='store_true', help='Disable host key checking (lab hosts that get reinstalled)')

    g = parser.add_argument_group('output')
    g.add_argument('--output-dir', '-o', default='./trace_output', help='Output directory')
    g.add_argument('--no-reports', '--no-graphs', dest='no_reports', action='store_true', help='Only save events JSON')
    g.add_argument('--verbose', '-v', action='store_true', help='Debug logging on the console')

    args = parser.parse_args()
    setup_logging(args.output_dir, args.verbose)

    if args.report_only:
        if not args.events_json or not os.path.exists(args.events_json):
            parser.error("--report-only needs an existing --events-json file")
        generate_report_only(args.events_json, args.device_id, args.output_dir)
        return
    if not args.namespace:
        parser.error("--namespace is required when not using --report-only")
    if args.ssh_host and not args.ssh_user:
        parser.error("--ssh-user is required with --ssh-host")

    config = TraceConfig(
        namespace=args.namespace, device_id=args.device_id, kafka_broker=args.kafka_broker,
        max_age_hours=args.max_age, since=args.since, until=args.until,
        max_parallel_topics=args.max_parallel, output_dir=args.output_dir,
        consumer_inactivity_timeout=args.timeout, partition_timeout=args.partition_timeout,
        topic_filter=args.topic_filter, topic_regex=args.topic_regex, exclude_regex=args.exclude_regex or None,
        topic_list=args.topics, max_messages=args.max_messages, max_total_messages=args.max_total_messages,
        extra_terms=args.match, remote_filter=not args.no_remote_filter,
        partitions_per_exec=args.partitions_per_exec,
        ssh_host=args.ssh_host, ssh_user=args.ssh_user,
        ssh_password=args.ssh_password or (os.environ.get('SSHPASS') if args.ssh_host else None),
        ssh_key=args.ssh_key, ssh_setup_keys=args.ssh_setup_keys, ssh_no_key_setup=args.ssh_no_key_setup,
        ssh_clean_keys=args.ssh_clean_keys, ssh_insecure=args.ssh_insecure,
        kafka_pod_selector=args.kafka_pod, kafka_container=args.kafka_container,
        kafka_bin_dir=args.kafka_bin_dir, kafka_client_config=args.kafka_client_config,
        kubectl=args.kubectl, kube_context=args.kube_context, tool_heap=args.tool_heap,
        client_id=args.client_id, assume_yes=args.yes, dry_run=args.dry_run, unsafe=args.unsafe,
    )
    logger.info(f"Starting KomniTrace for device_id: {config.device_id}")

    # SIGTERM (e.g. CI cancel) behaves like Ctrl-C so children are cleaned up
    def _on_sigterm(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, _on_sigterm)
    tracer = None
    interrupted = False
    try:
        with KomniTracer(config) as tracer:
            tracer.run_trace()
    except KeyboardInterrupt:
        logger.warning("Trace interrupted; partial results (if any) are saved below")
        interrupted = True
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(2)

    if config.dry_run or tracer is None:
        return
    events = tracer.events
    events_file = os.path.join(config.output_dir, f"events_{config.device_id}.json")
    with open(events_file, 'w', encoding='utf-8') as f:
        json.dump([event_to_dict(e) for e in events], f, indent=2, ensure_ascii=False)
    logger.info(f"Events saved to: {events_file}")
    if not events:
        logger.warning("No events found for the specified device_id")
        sys.exit(130 if interrupted else 0)
    report_path = "" if args.no_reports else generate_reports(events, config)

    incomplete = [r for r in tracer.results if r.status != 'complete']
    print("\n" + "=" * 60)
    print(f"KomniTrace finished: {len(events)} events for device {config.device_id}")
    print(f"  Events JSON: {events_file}")
    print(f"  Coverage:    {os.path.join(config.output_dir, f'coverage_{config.device_id}.json')}")
    if report_path:
        print(f"  HTML Report: {report_path}")
    if incomplete:
        print(f"  WARNING: {len(incomplete)} partitions were not fully read (see coverage file)")
    print("=" * 60)
    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    main()
