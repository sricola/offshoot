# Fixtures for langgraph-checkpoint-offshoot's tests: a real offshoot
# binary + daemon, the same discipline as sdk/python/tests/test_client.py's
# DaemonFixture (OFFSHOOT_BIN env override, else `go build` from this repo).
#
# Full LangGraph availability follows the RequireExec-style stance the repo
# uses for the binary: if `langgraph.graph` (and
# langgraph-checkpoint-sqlite) is not importable, the StateGraph tests SKIP
# locally, but the suite FAILS when OFFSHOOT_REQUIRE_LANGGRAPH (or CI) is set
# — a misconfigured CI env must not silently skip. Importing bare `langgraph`
# is not enough: the checkpoint packages create that namespace even when the
# full framework (and therefore langgraph.graph) is absent.
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

import sys
_HERE = Path(__file__).resolve()
REPO = _HERE.parents[3]
# Import both the package under test and the local offshoot SDK from the
# repo tree, exactly like sdk/python/tests does — no install step needed.
sys.path.insert(0, str(_HERE.parents[1]))          # sdk/python-langgraph
sys.path.insert(0, str(REPO / "sdk" / "python"))   # sdk/python

try:
    import langgraph.graph  # noqa: F401
    import langgraph.checkpoint.sqlite  # noqa: F401
    HAVE_FULL_LANGGRAPH = True
except ImportError:
    HAVE_FULL_LANGGRAPH = False

_REQUIRE = os.environ.get("OFFSHOOT_REQUIRE_LANGGRAPH") or os.environ.get("CI")


def pytest_configure(config: pytest.Config) -> None:
    # Fail LOUD before collection when the strict env demands full LangGraph
    # but it isn't importable; locally, the StateGraph module skips visibly.
    if not HAVE_FULL_LANGGRAPH and _REQUIRE:
        raise pytest.UsageError(
            "full langgraph (including langgraph.graph) / "
            "langgraph-checkpoint-sqlite is not importable but "
            "OFFSHOOT_REQUIRE_LANGGRAPH (or CI) is set — install the test "
            "deps (`pip install -e \"sdk/python-langgraph[test]\"`) "
            "instead of silently skipping this suite.")


def _build_binary(tmp: Path) -> Path:
    binpath = os.environ.get("OFFSHOOT_BIN")
    if binpath:
        return Path(binpath)
    out = tmp / "offshoot"
    subprocess.run(["go", "build", "-o", str(out), "./cmd/offshoot"],
                   cwd=REPO, check=True)
    return out


class Daemon:
    """A daemon on a temp store + socket (mirrors sdk/python's DaemonFixture)."""

    def __init__(self) -> None:
        self.dir = Path(tempfile.mkdtemp(prefix="offshoot-langgraph-"))
        self.bin = _build_binary(self.dir)
        self.store = self.dir / "store"
        subprocess.run([str(self.bin), "-store", str(self.store), "init"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.sock = str(self.dir / "d.sock")
        # stderr=PIPE is only drained on early exit (below) — a pathologically
        # chatty daemon could fill the OS pipe buffer and stall. Bounded risk,
        # accepted deliberately: `offshoot serve` is quiet in steady state, and
        # this mirrors sdk/python/tests/test_client.py's DaemonFixture; fixing
        # it here alone would diverge the two fixtures.
        self.proc = subprocess.Popen(
            [str(self.bin), "-store", str(self.store), "serve", "-socket", self.sock],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 10
        while not os.path.exists(self.sock):
            if self.proc.poll() is not None:
                stderr = self.proc.stderr.read().decode(errors="replace") if self.proc.stderr else ""
                raise RuntimeError(f"offshoot serve exited early:\n{stderr}")
            if time.time() > deadline:
                raise RuntimeError("offshoot serve did not start listening in 10s")
            time.sleep(0.02)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        shutil.rmtree(self.dir, ignore_errors=True)


@pytest.fixture(scope="session")
def daemon() -> Daemon:
    d = Daemon()
    yield d
    d.stop()
