#!/usr/bin/env python3
"""run.py: a runnable pass^k eval loop over offshoot, tau2-bench-style.

The shape this demonstrates (see docs/recipes/eval-harnesses.md for the
narrative version): seed a database once, fork an identical copy per
attempt, let a "stub agent" (here, a deterministic migration -- swap in a
real LLM call and this loop is unchanged) act on its own private copy, grade
the result with `offshoot diff` against a golden reference, then throw the
fork away. Running the same task `k` times and requiring ALL k attempts to
pass (pass^k) is a much stricter bar than pass@1 (at least one of k passes)
-- and a much better proxy for "would I trust this in production," where an
agent doesn't get to keep re-rolling until it gets lucky.

Every fork below carries `meta={"task": ..., "trial": ...}` -- offshoot
branch metadata, visible in `offshoot branches evals` and stored on the
branch's ref -- which is the concrete thing this example exists to show off:
an eval harness can label *why* a branch exists (which task, which trial,
which run) without inventing its own out-of-band bookkeeping.

Stdlib only (plus sdk/python on sys.path, exactly like scripts/bench-
isolation.py -- no pip install; the SDK isn't published yet, see
docs/status.md's publish-pipeline row). Spawns its own throwaway daemon on
a temp store + unix socket (same spawn pattern as
sdk/python/tests/test_client.py's DaemonFixture and scripts/bench-
isolation.py's Daemon), runs entirely in one process with no threads, and
exits 0 whether or not every task passes -- this is a demonstration of the
loop and its output, not a CI gate (see docs/ci-recipes.md for wiring an
eval loop's exit code into CI instead).

    OFFSHOOT_BIN=./bin/offshoot-bench PYTHONPATH=../../sdk/python \\
        python3 run.py --k 4 --tasks 5
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "sdk" / "python"))
import offshoot  # noqa: E402  (must follow the sys.path insert above)

HERE = Path(__file__).resolve().parent
GOLDEN_SQL = HERE / "golden.sql"
DEFAULT_BIN = REPO / "bin" / "offshoot-bench"

DB = "evals"

# The one task whose stub agent is deterministically flaky: every 3rd trial
# (0-indexed: trial 2, 5, 8, ...) forgets the ROUND() and gets the total
# wrong. With the --k 4 default that's 1 failure in 4 -- pass@1 = 0.75
# (neither 0 nor 1), pass^k = 0 (a single miss sinks the whole run). Every
# other task's stub agent never makes this mistake, so its pass@1 and
# pass^k both land on a clean 1.0 -- the contrast is the point: pass@1
# alone would call this task "mostly fine."
FLAKY_TASK_ID = 4


def load_migrations() -> tuple[str, str, str]:
    """Return (seed_sql, correct_migration, buggy_migration) parsed out of
    golden.sql. The buggy variant is derived from the correct one by
    stripping its ROUND(...) wrapper via regex, never hand-duplicated, so
    it can't silently drift from the migration it's supposed to almost
    match."""
    text = GOLDEN_SQL.read_text()
    seed_sql, marker, after = text.partition("-- === MIGRATION ===")
    if not marker:
        raise RuntimeError(f"{GOLDEN_SQL}: missing the '-- === MIGRATION ===' marker")
    match = re.search(r"UPDATE\s+orders\s+SET\s+total\s*=.*?;", after, re.S | re.I)
    if not match:
        raise RuntimeError(f"{GOLDEN_SQL}: no 'UPDATE orders SET total = ...;' after the marker")
    correct = match.group(0)
    buggy = re.sub(r"ROUND\(\s*(.+?)\s*,\s*2\s*\)", r"\1", correct, count=1, flags=re.S)
    if buggy == correct:
        raise RuntimeError(f"{GOLDEN_SQL}: could not derive a buggy (no-ROUND) migration from {correct!r}")
    return seed_sql, correct, buggy


def resolve_binary() -> Path:
    env = os.environ.get("OFFSHOOT_BIN")
    if env:
        return Path(env)
    if DEFAULT_BIN.exists():
        return DEFAULT_BIN
    found = shutil.which("offshoot")
    if found:
        return Path(found)
    print(f"error: no offshoot binary found (checked $OFFSHOOT_BIN, {DEFAULT_BIN}, PATH)",
          file=sys.stderr)
    print(f"build one with: go build -o {DEFAULT_BIN} ./cmd/offshoot", file=sys.stderr)
    sys.exit(1)


class Daemon:
    """A throwaway daemon on a temp store + unix socket. Same spawn pattern
    as sdk/python/tests/test_client.py's DaemonFixture and scripts/bench-
    isolation.py's Daemon: `offshoot -store S init`, then
    `offshoot -store S serve -socket SOCK`, poll for the socket file."""

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
                # The subprocess is still alive (never reached the readiness
                # check below) but never created its socket -- reap it
                # before raising, or it's leaked as a zombie/orphan for the
                # rest of this process's life (nothing else ever calls
                # .stop() on a Daemon whose __init__ raised).
                if self.proc.poll() is None:
                    self.proc.terminate()
                    try:
                        self.proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
                        self.proc.wait(timeout=10)
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


def apply_sql(path: str, script: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(script)
        conn.commit()
    finally:
        conn.close()


def run_eval(sock: str, k: int, n_tasks: int) -> int:
    seed_sql, correct_migration, buggy_migration = load_migrations()
    wall_start = time.perf_counter()

    with offshoot.connect(sock) as client:
        client.create(DB)

        # Seed once: apply golden.sql's seed section to main, checkpoint it
        # "seed". (We seed main directly, rather than forking a dedicated
        # "evals" seed branch off of it, since create() already gives us a
        # branch -- main -- with nothing on it yet; forking main from
        # itself just to rename it "seed" would be one fork for no benefit.
        # See the README for the tradeoff this implies.)
        main = client.open(DB, "main")
        conn = sqlite3.connect(main.path)
        conn.executescript(seed_sql)
        conn.commit()
        tasks = conn.execute(
            "SELECT id, order_id, description FROM tasks ORDER BY id").fetchall()
        conn.close()
        main.flush(name="seed")
        main.close()

        if n_tasks > len(tasks):
            print(f"warning: --tasks {n_tasks} > {len(tasks)} rows in golden.sql's "
                  f"tasks table; using {len(tasks)}", file=sys.stderr)
        tasks = tasks[:n_tasks]

        # Build the golden reference ONCE: fork "golden" from the seed
        # checkpoint, apply the one correct migration, checkpoint it
        # "expected". Every task's every trial is graded against this same
        # evals@golden@expected target.
        client.fork(DB, "main", "golden", from_checkpoint="seed")
        golden = client.open(DB, "golden")
        try:
            apply_sql(golden.path, correct_migration)
            golden.flush(name="expected")
        finally:
            # golden itself isn't disposable per attempt -- it's destroyed
            # once, below, after every task's trials are graded against it
            # -- but its session's lease must not leak if apply_sql/flush
            # raises above.
            golden.close()

        print(f"{'task':<6}{'description':<62}{'k':>3}  {'pass@1':>7}  {'pass^k':>7}")
        print("-" * 92)

        results = []  # (task_id, pass_at_1, pass_at_k)
        for task_id, order_id, description in tasks:
            outcomes = []
            for trial in range(k):
                branch = f"attempt-{task_id}-{trial}"
                buggy = task_id == FLAKY_TASK_ID and trial % 3 == 2
                client.fork(DB, "main", branch, from_checkpoint="seed",
                            meta={"task": str(task_id), "trial": str(trial)})
                try:
                    attempt = client.open(DB, branch)
                    try:
                        apply_sql(attempt.path, buggy_migration if buggy else correct_migration)
                        attempt.flush()
                    finally:
                        attempt.close()

                    diff = client.diff(f"{DB}@{branch}", f"{DB}@golden@expected")
                    passed = all(t.status == "same" for t in diff.tables)
                    outcomes.append(passed)
                finally:
                    # Destroy the fork even if apply_sql/flush/diff raised
                    # above -- a mid-trial exception must not leak a branch
                    # (or its lease) past this trial.
                    client.destroy(DB, branch)

            pass_at_1 = sum(outcomes) / len(outcomes)
            pass_at_k = all(outcomes)
            results.append((task_id, pass_at_1, pass_at_k))
            print(f"{task_id:<6}{description[:60]:<62}{k:>3}  {pass_at_1:>7.2f}  "
                  f"{'PASS' if pass_at_k else 'FAIL':>7}")

        client.destroy(DB, "golden")

    wall_time = time.perf_counter() - wall_start
    print("-" * 92)
    print(f"{len(tasks)} tasks, k={k}, wall time: {wall_time:.2f}s")

    # A dynamic closing note, built from this run's own numbers rather than
    # a canned string -- so it stays honest under --k/--tasks values other
    # than the defaults, where FLAKY_TASK_ID's row might not even be
    # included or might land on a different pass@1.
    failing = [(t, p1) for t, p1, pk in results if not pk]
    if failing:
        t, p1 = failing[0]
        print()
        print(f"Task {t}'s pass@1 of {p1:.2f} reads as \"mostly fine\" -- pass@1 only asks")
        print("\"what fraction of trials passed?\" pass^k (\"would EVERY one of k independent")
        print("attempts have passed?\") calls the same task a flat failure. That gap is the")
        print("whole reason to measure pass^k, not just pass@1: an agent that's right most")
        print("of the time is still wrong every time you'd actually ship it.")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Runnable pass^k eval loop over offshoot (tau2-bench style): "
                    "seed once, fork per trial, grade with offshoot diff, throw away.")
    ap.add_argument("--k", type=int, default=4, help="trials per task (default: 4)")
    ap.add_argument("--tasks", type=int, default=5, help="number of tasks to run (default: 5)")
    args = ap.parse_args()

    binpath = resolve_binary()
    if not (binpath.exists() and os.access(binpath, os.X_OK)):
        print(f"error: offshoot binary not found or not executable: {binpath}", file=sys.stderr)
        print(f"build it with: go build -o {binpath} ./cmd/offshoot", file=sys.stderr)
        sys.exit(1)

    with tempfile.TemporaryDirectory(prefix="offshoot-eval-pass-k-") as tmp:
        daemon = Daemon(binpath, Path(tmp))
        try:
            code = run_eval(daemon.sock, args.k, args.tasks)
        finally:
            daemon.stop()
    sys.exit(code)


if __name__ == "__main__":
    main()
