#!/usr/bin/env python3
"""bench-isolation.py: measures the wall-clock cost of the primitives an
eval harness could use to get a fresh, isolated database per test.

Stdlib only (plus sdk/python on sys.path — no pip install, no PyPI/npm
network access) except for the `docker` CLI, used only for the Postgres
rows. Everything the script writes lives under one `tempfile` directory
that is removed when the run ends; it touches nothing else on disk besides
that temp dir and (via `docker`) throwaway containers/databases it also
tears down itself.

Primitives measured, at each of --sizes (MB), over --iters repetitions
(median and p95 reported, in milliseconds):

  1. `offshoot fork` + `open` + `close` via the Python SDK, against a
     database seeded from a SQLite file of the given size (imported via
     `Client.create(db, from_path=...)`).
  2. `offshoot fork` alone (no session opened) — the pure branch cost.
  3. `sqlite3.Connection.backup()` of the seed file into a fresh file.
  4. `shutil.copyfile` of the seed file.
  5. Postgres `CREATE DATABASE trial TEMPLATE seed` inside a local `docker
     run -d postgres:16` container, seeded to approximately the same size
     via `generate_series` (sized by `pg_total_relation_size`).

A sixth row, `postgres cold container start` (docker run -d postgres:16
until pg_isready), is measured once, fixed at 3 iterations
(--cold-container-iters), independent of --sizes — starting a container
has nothing to do with the seed's size.

Two more overhead figures are measured and printed alongside the table
(not as per-size rows, since neither depends on seed size):
  - `docker exec no-op (SELECT 1)`: the fixed cost of the same
    `docker exec ... psql` round trip the template-clone row pays,
    isolating per-command overhead from that row's absolute numbers.
    Measured over the same --iters count, against the container already
    running for the template-clone row.
  - `pg_isready -> queryable` gap: within the cold-container primitive,
    the time from `pg_isready` first succeeding to the first successful
    `SELECT 1` (see `_wait_pg_queryable`) — pg_isready can report ready
    during the image entrypoint's temporary setup-only server, before the
    real server is actually queryable.

Postgres rows (5, 6, and the two overhead figures above) are skipped
cleanly, with a note in the output, when the `docker` CLI or the local
`postgres:16` image is unavailable — this script never attempts a network
pull.

Run `--dry-run` to validate tooling (offshoot binary present, sqlite3
stdlib module, docker + postgres:16 image) and print the plan without
doing any timed work.
"""
from __future__ import annotations

import argparse
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "sdk" / "python"))
import offshoot  # noqa: E402  (must follow the sys.path insert above)

DEFAULT_BIN = REPO / "bin" / "offshoot-bench"
COLD_CONTAINER_ITERS_DEFAULT = 3
PG_CONTAINER_ENV = [
    "-e", "POSTGRES_PASSWORD=pg",
    "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
]
PG_READY_TIMEOUT_S = 30


def eprint(*a: object) -> None:
    print(*a, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------
# Stats
# --------------------------------------------------------------------------

def percentile(values: list[float], pct: float) -> float:
    """Linear-interpolation percentile (no numpy; stdlib only)."""
    if not values:
        return float("nan")
    data = sorted(values)
    k = (len(data) - 1) * (pct / 100.0)
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return data[int(k)]
    return data[f] + (data[c] - data[f]) * (k - f)


def stats_ms(samples_sec: list[float], pct: float = 95.0) -> tuple[float, float]:
    ms = [s * 1000.0 for s in samples_sec]
    return percentile(ms, 50), percentile(ms, pct)


def fmt_stat(stat: tuple[float, float] | None) -> str:
    if stat is None:
        return "skipped"
    median, p95 = stat
    return f"{median:.2f} / {p95:.2f}"


# --------------------------------------------------------------------------
# SQLite seed + primitives 3/4
# --------------------------------------------------------------------------

def build_sqlite_seed(path: Path, size_mb: int) -> int:
    """A single table of size_mb one-MiB random blobs. Returns the actual
    file size in bytes (should be within a few KB of size_mb MiB)."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)")
        with conn:
            for _ in range(size_mb):
                conn.execute("INSERT INTO blobs (data) VALUES (randomblob(1024*1024))")
        conn.execute("VACUUM")
    finally:
        conn.close()
    return os.path.getsize(path)


def bench_sqlite_backup(seed_path: Path, iters: int, work_dir: Path) -> list[float]:
    # dst_conn is opened with SQLite's default `synchronous=FULL`, so
    # `.backup()` fsyncs the destination as it commits — it asks the OS for
    # durability; bench_copyfile below does not. Left as-is: this models
    # what harness authors actually call, not an apples-to-apples I/O test.
    samples = []
    for i in range(iters):
        dst = work_dir / f"backup-{i}.db"
        src_conn = sqlite3.connect(seed_path)
        dst_conn = sqlite3.connect(dst)
        try:
            t0 = time.perf_counter()
            src_conn.backup(dst_conn)
            t1 = time.perf_counter()
        finally:
            src_conn.close()
            dst_conn.close()
        dst.unlink(missing_ok=True)
        samples.append(t1 - t0)
    return samples


def bench_copyfile(seed_path: Path, iters: int, work_dir: Path) -> list[float]:
    # shutil.copyfile never fsyncs the destination — no durability is asked
    # of the OS here, unlike bench_sqlite_backup above. Left as-is: this
    # models what harness authors actually call.
    samples = []
    for i in range(iters):
        dst = work_dir / f"copy-{i}.db"
        t0 = time.perf_counter()
        shutil.copyfile(seed_path, dst)
        t1 = time.perf_counter()
        dst.unlink(missing_ok=True)
        samples.append(t1 - t0)
    return samples


# --------------------------------------------------------------------------
# offshoot daemon + primitives 1/2
# --------------------------------------------------------------------------

class Daemon:
    """A daemon on a temp store+socket. Mirrors sdk/python/tests/test_client.py's
    DaemonFixture (same spawn pattern: `offshoot -store S init`, then
    `offshoot -store S serve -socket SOCK`, poll for the socket file)."""

    def __init__(self, binpath: Path, work_dir: Path):
        self.dir = work_dir
        self.bin = binpath
        self.store = self.dir / "store"
        subprocess.run([str(self.bin), "-store", str(self.store), "init"],
                        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.sock = str(self.dir / "d.sock")
        self.proc = subprocess.Popen(
            [str(self.bin), "-store", str(self.store), "serve", "-socket", self.sock],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 10
        while not os.path.exists(self.sock):
            if time.time() > deadline:
                raise RuntimeError("daemon did not start: " +
                                    self.proc.stderr.read().decode(errors="replace"))
            if self.proc.poll() is not None:
                raise RuntimeError(self.proc.stderr.read().decode(errors="replace"))
            time.sleep(0.05)

    def stop(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc.stderr.close()


def bench_offshoot_fork_open_close(sock: str, db: str, iters: int) -> list[float]:
    samples = []
    with offshoot.connect(sock) as c:
        for i in range(iters):
            branch = f"t1-{i}"
            t0 = time.perf_counter()
            c.fork(db, "main", branch)
            s = c.open(db, branch)
            s.close()
            t1 = time.perf_counter()
            samples.append(t1 - t0)
            c.destroy(db, branch)  # untimed cleanup
    return samples


def bench_offshoot_fork_alone(sock: str, db: str, iters: int) -> list[float]:
    samples = []
    with offshoot.connect(sock) as c:
        for i in range(iters):
            branch = f"t2-{i}"
            t0 = time.perf_counter()
            c.fork(db, "main", branch)
            t1 = time.perf_counter()
            samples.append(t1 - t0)
            c.destroy(db, branch)  # untimed cleanup
    return samples


# --------------------------------------------------------------------------
# Docker / Postgres
# --------------------------------------------------------------------------

def docker_available() -> tuple[bool, str]:
    if shutil.which("docker") is None:
        return False, "docker CLI not found on PATH"
    try:
        subprocess.run(["docker", "info"], check=True, timeout=10,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        return False, f"docker daemon not reachable ({e})"
    try:
        subprocess.run(["docker", "image", "inspect", "postgres:16"], check=True, timeout=10,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        return False, "postgres:16 image not present locally (no network pull attempted)"
    return True, ""


def _docker_exec_psql(cid: str, sql: str, db: str = "postgres") -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", cid, "psql", "-U", "postgres", "-d", db, "-tAc", sql],
        check=True, capture_output=True, text=True)


def _wait_pg_ready(cid: str, deadline: float) -> None:
    """Waits for `pg_isready` alone — exactly the cold-container primitive's
    definition. NOT sufficient, by itself, to safely run SQL against the
    container: the official postgres image's entrypoint runs a *temporary*
    server for initdb's setup scripts, which pg_isready can observe as
    "accepting connections" moments before it's stopped and the real server
    restarts — see _wait_pg_queryable, used everywhere this script goes on
    to actually run psql."""
    while True:
        r = subprocess.run(["docker", "exec", cid, "pg_isready", "-U", "postgres"],
                            capture_output=True, text=True)
        if r.returncode == 0:
            return
        if time.time() > deadline:
            raise RuntimeError("postgres container never became ready")
        time.sleep(0.05)


def _wait_pg_queryable(cid: str, deadline: float) -> None:
    """Waits for pg_isready, then for an actual `SELECT 1` to succeed —
    see _wait_pg_ready's docstring for why pg_isready alone is not enough
    before running real SQL against a freshly started container."""
    _wait_pg_ready(cid, deadline)
    while True:
        r = subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-tAc", "SELECT 1"],
                            capture_output=True, text=True)
        if r.returncode == 0:
            return
        if time.time() > deadline:
            raise RuntimeError("postgres container never became queryable: " + r.stderr.strip())
        time.sleep(0.1)


def start_postgres_container() -> str:
    out = subprocess.run(
        ["docker", "run", "-d", "--rm", *PG_CONTAINER_ENV, "postgres:16"],
        check=True, capture_output=True, text=True)
    cid = out.stdout.strip()
    try:
        _wait_pg_queryable(cid, time.time() + PG_READY_TIMEOUT_S)
    except Exception:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True)
        raise
    return cid


def stop_postgres_container(cid: str) -> None:
    subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


def seed_postgres(cid: str, dbname: str, size_mb: int) -> int:
    """Builds `dbname` with one table (`blobs`) sized to approximately
    size_mb via generate_series, verified (and, if off by more than 15%,
    corrected once) via pg_total_relation_size. Storage is forced EXTERNAL
    (out-of-line, uncompressed) so TOAST compression of the repeated-md5
    filler text can't silently shrink the table below the target. Returns
    the actual measured size in bytes."""
    subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                     f"DROP DATABASE IF EXISTS {dbname}"],
                    check=True, capture_output=True, text=True)
    subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                     f"CREATE DATABASE {dbname}"],
                    check=True, capture_output=True, text=True)
    subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-d", dbname, "-c",
                     "CREATE TABLE blobs (data text); "
                     "ALTER TABLE blobs ALTER COLUMN data SET STORAGE EXTERNAL;"],
                    check=True, capture_output=True, text=True)

    target_bytes = size_mb * 1024 * 1024
    row_bytes = 8192  # 8 KiB filler text per row
    repeat_n = row_bytes // 32  # md5(random()::text) is 32 hex chars

    def fill(rows: int) -> None:
        subprocess.run(
            ["docker", "exec", cid, "psql", "-U", "postgres", "-d", dbname, "-c",
             f"TRUNCATE blobs; INSERT INTO blobs "
             f"SELECT repeat(md5(random()::text), {repeat_n}) FROM generate_series(1, {rows});"],
            check=True, capture_output=True, text=True)

    rows = max(1, target_bytes // row_bytes)
    fill(rows)
    actual = int(_docker_exec_psql(cid, "SELECT pg_total_relation_size('blobs')", db=dbname).stdout.strip())
    if actual > 0 and abs(actual - target_bytes) / target_bytes > 0.15:
        rows = max(1, int(rows * (target_bytes / actual)))
        fill(rows)
        actual = int(_docker_exec_psql(cid, "SELECT pg_total_relation_size('blobs')", db=dbname).stdout.strip())
    return actual


def bench_postgres_template(cid: str, seed_db: str, iters: int) -> list[float]:
    samples = []
    for i in range(iters):
        trial = f"trial_{i}"
        subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                         f"DROP DATABASE IF EXISTS {trial}"],
                        check=True, capture_output=True, text=True)
        t0 = time.perf_counter()
        subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                         f"CREATE DATABASE {trial} TEMPLATE {seed_db}"],
                        check=True, capture_output=True, text=True)
        t1 = time.perf_counter()
        samples.append(t1 - t0)
        subprocess.run(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                         f"DROP DATABASE {trial}"],
                        check=True, capture_output=True, text=True)
    return samples


def bench_docker_exec_noop(cid: str, iters: int) -> list[float]:
    """`docker exec ... psql ... SELECT 1` no-op round trip — the exact
    exec path bench_postgres_template's CREATE/DROP DATABASE calls use,
    isolating that row's fixed per-command overhead from its absolute
    numbers. Measured against an already-running, already-queryable
    container (no container start cost included)."""
    samples = []
    for _ in range(iters):
        t0 = time.perf_counter()
        _docker_exec_psql(cid, "SELECT 1")
        t1 = time.perf_counter()
        samples.append(t1 - t0)
    return samples


def bench_cold_container(iters: int) -> tuple[list[float], list[float]]:
    """Returns (cold_start_samples, ready_to_queryable_gap_samples): the
    second list is, per iteration, the time from `pg_isready` first
    succeeding to the first successful `SELECT 1` (via `_wait_pg_queryable`,
    which re-checks pg_isready — already satisfied — before looping on
    SELECT 1), isolating the setup-server/real-server handoff gap
    `_wait_pg_ready`'s docstring describes."""
    cold_samples = []
    gap_samples = []
    for _ in range(iters):
        t0 = time.perf_counter()
        out = subprocess.run(
            ["docker", "run", "-d", "--rm", *PG_CONTAINER_ENV, "postgres:16"],
            check=True, capture_output=True, text=True)
        cid = out.stdout.strip()
        try:
            _wait_pg_ready(cid, time.time() + PG_READY_TIMEOUT_S)
            t1 = time.perf_counter()
            _wait_pg_queryable(cid, time.time() + PG_READY_TIMEOUT_S)
            t2 = time.perf_counter()
        finally:
            subprocess.run(["docker", "rm", "-f", cid], capture_output=True)
        cold_samples.append(t1 - t0)
        gap_samples.append(t2 - t1)
    return cold_samples, gap_samples


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

PRIMITIVES = [
    ("fork_open_close", "`offshoot fork` + `open` + `close` (SDK)"),
    ("fork_alone", "`offshoot fork` alone (no session)"),
    ("sqlite_backup", "`sqlite3.Connection.backup()`"),
    ("shutil_copy", "`shutil.copyfile`"),
    ("pg_template", "Postgres `CREATE DATABASE trial TEMPLATE seed`"),
]


def dry_run(sizes: list[int], iters: int, cold_iters: int, offshoot_bin: Path) -> None:
    bin_ok = offshoot_bin.exists() and os.access(offshoot_bin, os.X_OK)
    pg_ok, pg_reason = docker_available()

    print("=== bench-isolation: dry run (no timed work performed) ===")
    print(f"offshoot binary : {offshoot_bin} ({'found, executable' if bin_ok else 'MISSING or not executable'})")
    print(f"sqlite3 module  : ok (stdlib, {sqlite3.sqlite_version})")
    print(f"docker          : {'available' if pg_ok else 'unavailable — ' + pg_reason}")
    print()
    print(f"sizes (MB)      : {', '.join(str(s) for s in sizes)}")
    print(f"iters (per primitive) : {iters}")
    print(f"cold-container  : {cold_iters} iterations (fixed; size-independent)")
    print()
    print("Plan:")
    for _, label in PRIMITIVES:
        skip = "" if (pg_ok or "offshoot" in label.lower() or "sqlite" in label.lower() or "copyfile" in label.lower()) else f" -- SKIPPED: {pg_reason}"
        print(f"  - {label}, per size{skip}")
    cold_skip = "" if pg_ok else f" -- SKIPPED: {pg_reason}"
    print(f"  - Postgres cold container start, N={cold_iters}, size-independent{cold_skip}")
    if not bin_ok:
        print()
        print(f"NOTE: build the binary first: go build -o {offshoot_bin} ./cmd/offshoot")
        sys.exit(1)


def run(sizes: list[int], iters: int, cold_iters: int, offshoot_bin: Path) -> dict:
    results: dict = {k: {} for k, _ in PRIMITIVES}
    pg_ok, pg_reason = docker_available()
    pg_actual_bytes: dict[int, int] = {}
    sqlite_actual_bytes: dict[int, int] = {}
    pg_cold = None
    docker_exec_noop = None
    pg_ready_gap = None

    with tempfile.TemporaryDirectory(prefix="offshoot-bench-") as tmp_s:
        tmp = Path(tmp_s)

        for size in sizes:
            eprint(f"== size {size} MB ==")
            size_dir = tmp / f"size-{size}"
            size_dir.mkdir()
            seed_path = size_dir / "seed.db"

            eprint("  building sqlite seed...")
            actual_bytes = build_sqlite_seed(seed_path, size)
            sqlite_actual_bytes[size] = actual_bytes
            eprint(f"    seed file: {actual_bytes / (1024 * 1024):.2f} MiB")

            eprint("  sqlite3 backup()...")
            results["sqlite_backup"][size] = stats_ms(bench_sqlite_backup(seed_path, iters, size_dir))

            eprint("  shutil.copyfile...")
            results["shutil_copy"][size] = stats_ms(bench_copyfile(seed_path, iters, size_dir))

            eprint("  offshoot daemon: starting...")
            store_dir = size_dir / "offshoot"
            store_dir.mkdir()
            d = Daemon(offshoot_bin, store_dir)
            try:
                with offshoot.connect(d.sock) as c:
                    c.create("eval-bench", from_path=seed_path)
                eprint("  offshoot fork+open+close...")
                results["fork_open_close"][size] = stats_ms(
                    bench_offshoot_fork_open_close(d.sock, "eval-bench", iters))
                eprint("  offshoot fork alone...")
                results["fork_alone"][size] = stats_ms(
                    bench_offshoot_fork_alone(d.sock, "eval-bench", iters))
            finally:
                d.stop()

        if pg_ok:
            eprint("== postgres ==")
            eprint("  starting container...")
            cid = start_postgres_container()
            try:
                for size in sizes:
                    dbname = f"seed_{size}mb"
                    eprint(f"  seeding {dbname} (target {size} MB)...")
                    actual = seed_postgres(cid, dbname, size)
                    pg_actual_bytes[size] = actual
                    eprint(f"    actual: {actual / (1024 * 1024):.2f} MiB")
                    eprint("  CREATE DATABASE ... TEMPLATE...")
                    results["pg_template"][size] = stats_ms(bench_postgres_template(cid, dbname, iters))

                eprint("  docker exec no-op (SELECT 1)...")
                docker_exec_noop = stats_ms(bench_docker_exec_noop(cid, iters), pct=90)
            finally:
                stop_postgres_container(cid)

            eprint("  cold container start...")
            cold_samples, gap_samples = bench_cold_container(cold_iters)
            pg_cold = stats_ms(cold_samples)
            pg_ready_gap = stats_ms(gap_samples, pct=90)
        else:
            eprint(f"== postgres: skipped ({pg_reason}) ==")
            for size in sizes:
                results["pg_template"][size] = None

    return {
        "results": results,
        "pg_ok": pg_ok,
        "pg_reason": pg_reason,
        "pg_actual_bytes": pg_actual_bytes,
        "sqlite_actual_bytes": sqlite_actual_bytes,
        "pg_cold": pg_cold,
        "docker_exec_noop": docker_exec_noop,
        "pg_ready_gap": pg_ready_gap,
        "sizes": sizes,
        "iters": iters,
        "cold_iters": cold_iters,
    }


def render(report: dict) -> str:
    sizes = report["sizes"]
    results = report["results"]
    lines = []

    header = "| Primitive | " + " | ".join(f"{s} MB" for s in sizes) + " |"
    sep = "|---" * (len(sizes) + 1) + "|"
    lines.append(header)
    lines.append(sep)
    for key, label in PRIMITIVES:
        row = [label]
        for size in sizes:
            row.append(fmt_stat(results[key].get(size)))
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    if report["pg_ok"]:
        median, p95 = report["pg_cold"]
        lines.append(f"Postgres cold container start (`docker run -d postgres:16` until "
                      f"`pg_isready`; N={report['cold_iters']}, size-independent): "
                      f"**{median:.2f} / {p95:.2f} ms** (median / p95)")
    else:
        lines.append(f"Postgres cold container start: skipped ({report['pg_reason']})")
    lines.append("")

    if report["pg_ok"]:
        median, p90 = report["docker_exec_noop"]
        lines.append(f"docker exec no-op (SELECT 1) overhead (same `docker exec ... psql` "
                      f"round trip the template-clone row above pays; N={report['iters']}, "
                      f"size-independent): **{median:.2f} / {p90:.2f} ms** (median / p90)")
    else:
        lines.append(f"docker exec no-op (SELECT 1) overhead: skipped ({report['pg_reason']})")
    lines.append("")

    if report["pg_ok"]:
        median, p90 = report["pg_ready_gap"]
        lines.append(f"`pg_isready` -> first successful `SELECT 1` gap (measured inside the "
                      f"cold-container start above; N={report['cold_iters']}, "
                      f"size-independent): **{median:.2f} / {p90:.2f} ms** (median / p90)")
    else:
        lines.append(f"`pg_isready` -> queryable gap: skipped ({report['pg_reason']})")
    lines.append("")

    lines.append("Values are `median / p95` milliseconds over "
                  f"{report['iters']} iterations (3 for the cold-container row), except the "
                  "docker-exec-no-op and pg_isready-gap lines above, which report "
                  "`median / p90`.")
    if report["pg_actual_bytes"]:
        actual_notes = ", ".join(
            f"{s} MB target -> {b / (1024 * 1024):.1f} MiB actual"
            for s, b in sorted(report["pg_actual_bytes"].items()))
        lines.append(f"Postgres seed sizes (`pg_total_relation_size`): {actual_notes}.")
    if report["sqlite_actual_bytes"]:
        actual_notes = ", ".join(
            f"{s} MB target -> {b / (1024 * 1024):.1f} MiB actual"
            for s, b in sorted(report["sqlite_actual_bytes"].items()))
        lines.append(f"SQLite seed sizes (actual file size): {actual_notes}.")
    lines.append("")

    lines.append("**Caveats:**")
    lines.append("- Single host, single run, not a fleet average: all rows were measured "
                  "back-to-back on the same machine (see the machine line above this table "
                  "in the docs) with no other load; run-to-run variance on a laptop is real.")
    lines.append("- Apple Silicon macOS: the offshoot daemon and SQLite rows run natively, "
                  "but Postgres only runs in Docker Desktop's Linux VM here — its numbers "
                  "include a virtualization/VM-boundary tax a native Linux host would not pay.")
    lines.append("- offshoot uses its local-directory store backend (not S3) for this run.")
    lines.append("- No network access is used or required by this script; the postgres:16 "
                  "image must already be present locally or the Postgres rows are skipped.")
    if not report["pg_ok"]:
        lines.append(f"- Postgres rows were skipped entirely on this run: {report['pg_reason']}.")
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Benchmark per-test database-isolation primitives "
                    "(offshoot fork, sqlite copy/backup, Postgres template clone/cold-start).")
    ap.add_argument("--sizes", default="10,100", help="comma-separated seed sizes in MB (default: 10,100)")
    ap.add_argument("--iters", type=int, default=20, help="iterations for primitives 1-5 (default: 20)")
    ap.add_argument("--cold-container-iters", type=int, default=COLD_CONTAINER_ITERS_DEFAULT,
                     help=f"iterations for the postgres cold-container row (default: {COLD_CONTAINER_ITERS_DEFAULT})")
    ap.add_argument("--offshoot-bin", default=None,
                     help="path to the offshoot binary (default: $OFFSHOOT_BIN or bin/offshoot-bench)")
    ap.add_argument("--dry-run", action="store_true",
                     help="validate tooling and print the plan; perform no timed work")
    args = ap.parse_args()

    sizes = [int(s.strip()) for s in args.sizes.split(",") if s.strip()]
    offshoot_bin = Path(args.offshoot_bin or os.environ.get("OFFSHOOT_BIN") or DEFAULT_BIN)

    if args.dry_run:
        dry_run(sizes, args.iters, args.cold_container_iters, offshoot_bin)
        return

    if not (offshoot_bin.exists() and os.access(offshoot_bin, os.X_OK)):
        eprint(f"offshoot binary not found or not executable: {offshoot_bin}")
        eprint(f"build it with: go build -o {offshoot_bin} ./cmd/offshoot")
        sys.exit(1)

    report = run(sizes, args.iters, args.cold_container_iters, offshoot_bin)
    print()
    print(render(report))


if __name__ == "__main__":
    main()
