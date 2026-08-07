"""``offshoot.pytest`` — the pytest fixture plugin for seed-once-fork-many
eval/test workloads.

Ships as the ``offshoot-db[pytest]`` extra and registers itself with pytest
via the ``pytest11`` entry point in ``pyproject.toml`` — installing the
extra is enough; nothing needs importing by hand. The base package
(``offshoot-db``, no extra) stays stdlib-only: this module is only ever
imported by pytest itself (which means pytest is already present), but it
still import-guards ``pytest`` below so a stray direct ``import
offshoot.pytest_plugin`` without the extra installed fails with a clear
message instead of a bare ``ModuleNotFoundError`` traceback.

Three fixtures, one helper:

- ``offshoot_daemon`` (session-scoped): locates the ``offshoot`` binary
  (``OFFSHOOT_BIN`` env, else ``PATH``, else ``pytest.skip`` with install
  instructions), starts it on a fresh temp store + temp unix socket, and
  terminates it at session end.
- ``offshoot_db`` (session-scoped): a NAMED-SEED FACTORY,
  ``offshoot_db(name="default", seed=None)``. The first call for a given
  name creates database ``eval-{name}``, runs the seed (a callable taking a
  writable sqlite path, or a SQL string run via ``sqlite3``), and flushes it
  to a checkpoint named ``seed``; later calls for the same name are a pure
  memoization hit. With no ``seed`` argument, the ``offshoot_seed`` ini
  option (a path to a ``.sql`` file) is the zero-code default — the
  "singleton" case (one project, one seed) needs no code at all beyond
  setting that one ini line. Returns a :class:`SeedHandle`.
- ``offshoot_fork`` (function-scoped): a fork-per-test factory,
  ``offshoot_fork(seed_handle=None)`` (default: ``offshoot_db()``'s default
  seed). Forks a fresh, worker-safe-named branch from the seed's checkpoint
  with a TTL (default 1h, ini-overridable via ``offshoot_ttl``), opens a
  session on it, and returns a :class:`ForkedSession`. Teardown closes the
  session and destroys the branch; a destroy failure is a ``UserWarning``,
  never a test failure — the TTL is the backstop for exactly that case.
- ``offshoot_dump(path) -> str``: the way to compare two offshoot-
  materialized SQLite files in a golden-file test. SQLite's on-disk bytes
  are not deterministic across writes with identical logical content (page
  layout, freelist order, vacuum state can all differ) — NEVER byte-compare
  two exported/checked-out ``.db`` files. Compare
  ``offshoot_dump(a) == offshoot_dump(b)`` instead (or diff the two strings
  for a readable failure).

xdist stance: per-worker daemon + store
----------------------------------------
Under ``pytest-xdist``, ``offshoot_daemon`` is session-scoped, but a pytest
"session" is per-**worker-process** under xdist (pytest has no built-in
mechanism to share a fixture's value across worker processes) — so each
worker gets its OWN ``offshoot`` daemon on its OWN temp store, and
therefore its OWN copy of every named seed. This is a deliberate choice,
not an oversight: it needs no cross-process coordination (a lockfile, a
shared daemon address, a "who seeds first" race) and it means one worker's
daemon dying never takes another worker's tests down with it. The cost is
that seed work is **not shared** across workers: with N workers, an
`offshoot_db` seed's cost is paid N times, not once.

Measured (this repo's own smoke test, `sdk/python/tests/test_pytest_plugin.py`
`test_xdist_two_worker_run_passes_and_measures_seed_cost`; a
``CREATE TABLE`` + 200-row ``INSERT`` seed script run as a single
transaction — see `_run_seed`'s note below on why that matters — macOS
arm64, local directory store): one worker's full ``offshoot_db(seed=...)``
call (create the db, open a session, run the seed, flush it to the `seed`
checkpoint, close the session) measures **~80-90ms**. A 2-worker
``pytest -n2`` run has both workers pay that independently and in
parallel — measured ``{'gw0': ~85ms, 'gw1': ~85ms}`` — for **~170ms of
total seed work against ~85-90ms of wall-clock seeding time** (the two
workers' daemons start and seed concurrently, so wall-clock is dominated
by the slower of the two, not their sum). The *ratio* — total seed work
scales linearly at roughly 1x per worker — is the number to plan around,
not the absolute milliseconds: a seed that populates gigabytes of fixture
data, multiplied by a wide ``-n`` fan-out, is a real cost. If your seed is
expensive: keep worker count modest, seed a shared file out-of-band once
and `create --from` it into every worker's store (CLI-only import path —
see docs/status.md's `create --from` row), or split expensive seeds into a
smaller shared subset plus cheap per-worker deltas.

A load-bearing detail behind that ~85ms number: `_run_seed` wraps a
SQL-string seed with no explicit ``BEGIN``/``COMMIT`` in a single
transaction before running it. Without that, `sqlite3.Connection
.executescript` runs every semicolon-separated statement as its OWN
autocommit transaction — 200 unwrapped ``INSERT`` statements measured
**~1.6s** (each one a separate WAL commit the capture engine observes and
encodes), against ~17ms for the same 200 rows in one transaction: nearly
two orders of magnitude, entirely from transaction boundaries, nothing to
do with offshoot itself. A seed callable (the other `seed=` form) is NOT
auto-wrapped — it gets the raw writable path and decides its own
transaction boundaries.

Honest failure modes
---------------------
No ``offshoot`` binary found → the ``offshoot_daemon`` fixture (and
everything depending on it) is **skipped**, with install instructions, not
failed — a machine without the binary shouldn't red the whole suite for
tests that need it. If the daemon dies mid-session (crash, OOM-kill, a bug),
the next ``offshoot_fork``/``offshoot_db`` call that touches it raises
:class:`~offshoot.client.OffshootError` with the daemon's own stderr tail
appended, so the failure names what actually happened instead of a bare
"connection reset."
"""
from __future__ import annotations

import collections
import os
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

try:
    import pytest
except ImportError as e:  # pragma: no cover - exercised only when the
    # [pytest] extra isn't installed; the pytest11 entry point never
    # triggers this import path (pytest is already running by the time it
    # fires), so this only matters for a stray direct import.
    raise ImportError(
        "offshoot.pytest_plugin requires pytest; install the extra: "
        'pip install "offshoot-db[pytest]"'
    ) from e

from .client import Client, OffshootError, Session, connect
from .langgraph import _UNSAFE_RUN, _sanitize

__all__ = [
    "SeedHandle",
    "ForkedSession",
    "offshoot_daemon",
    "offshoot_db",
    "offshoot_fork",
    "offshoot_dump",
]

_WORKER_ENV = "PYTEST_XDIST_WORKER"
_DEFAULT_TTL = "1h"


# --------------------------------------------------------------------------
# offshoot_dump: the golden-file comparison helper (PM Amendment 7 — never
# byte-compare two SQLite files; compare their `.dump` text instead).
# --------------------------------------------------------------------------

def offshoot_dump(path) -> str:
    """Return ``sqlite3 <path> .dump``'s text output.

    THE way to compare two offshoot-materialized SQLite files (e.g. two
    ``Client.export()``/``checkout_at()`` outputs, or a fresh fork's
    checkout against a golden file) in a test: never byte-compare the files
    themselves — SQLite's on-disk representation is not deterministic
    across writes with identical logical content. Compare
    ``offshoot_dump(a) == offshoot_dump(b)`` instead.

    Requires the ``sqlite3`` CLI on PATH (the same one this repo's CI
    installs for `internal/dbfile`'s own tests).
    """
    proc = subprocess.run(
        ["sqlite3", str(path), ".dump"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise OffshootError(
            f"offshoot_dump: `sqlite3 {path} .dump` failed: {proc.stderr.strip()}")
    return proc.stdout


# --------------------------------------------------------------------------
# offshoot_daemon: binary location + a directly-startable/stoppable daemon
# handle. Split from the fixture function itself so "skip when no binary"
# and "daemon start/stop/stderr-tail" are unit-testable without going
# through pytest's fixture machinery.
# --------------------------------------------------------------------------

def _locate_binary(env: dict | None = None) -> Path | None:
    """OFFSHOOT_BIN env var (must exist), else the first `offshoot` on
    PATH, else None."""
    env = os.environ if env is None else env
    explicit = env.get("OFFSHOOT_BIN")
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    found = shutil.which("offshoot", path=env.get("PATH"))
    return Path(found) if found else None


_INSTALL_INSTRUCTIONS = (
    "offshoot binary not found. Set OFFSHOOT_BIN to its path, or put "
    "`offshoot` on PATH. Install: see "
    "https://github.com/offshoot-db/offshoot#install "
    "(from a source checkout: `go build -o /path/to/offshoot "
    "./cmd/offshoot`, then `export OFFSHOOT_BIN=/path/to/offshoot`)."
)


def _drain_stderr(proc: subprocess.Popen, tail: "collections.deque[str]") -> None:
    assert proc.stderr is not None
    try:
        for raw in iter(proc.stderr.readline, b""):
            tail.append(raw.decode(errors="replace").rstrip("\n"))
    finally:
        proc.stderr.close()


class _DaemonHandle:
    """A running `offshoot serve` process on its own temp store + socket."""

    def __init__(self, sock: str, store: Path, proc: subprocess.Popen):
        self.sock = sock
        self.store = store
        self.proc = proc
        self._tail: "collections.deque[str]" = collections.deque(maxlen=200)
        self._drain_thread = threading.Thread(
            target=_drain_stderr, args=(proc, self._tail), daemon=True)
        self._drain_thread.start()

    def stderr_tail(self, n: int = 20) -> str:
        """The last n lines this daemon wrote to stderr, newest last."""
        return "\n".join(list(self._tail)[-n:])

    def stop(self, timeout: float = 10.0) -> None:
        if self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=timeout)


def _start_daemon(binpath: Path, workdir: Path, timeout: float = 10.0) -> _DaemonHandle:
    """Init a store under workdir and start `offshoot serve` on it, waiting
    for its socket to come up. Mirrors sdk/python/tests/test_client.py's
    DaemonFixture, but self-contained (this module ships to users; it must
    not depend on test-only helpers, and it never runs `go build` — it only
    ever locates an already-built binary)."""
    store = workdir / "store"
    init = subprocess.run(
        [str(binpath), "-store", str(store), "init"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if init.returncode != 0:
        raise RuntimeError(
            f"offshoot -store {store} init failed: "
            f"{init.stderr.decode(errors='replace')}")
    sock = str(workdir / "d.sock")
    proc = subprocess.Popen(
        [str(binpath), "-store", str(store), "serve", "-socket", sock],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    handle = _DaemonHandle(sock, store, proc)
    deadline = time.time() + timeout
    while not os.path.exists(sock):
        if proc.poll() is not None:
            raise RuntimeError(
                "offshoot serve exited before it started listening:\n"
                + handle.stderr_tail())
        if time.time() > deadline:
            handle.stop()
            raise RuntimeError(
                f"offshoot serve did not start listening within {timeout}s:\n"
                + handle.stderr_tail())
        time.sleep(0.02)
    return handle


def _worker_id() -> str:
    """The xdist worker id (e.g. "gw0"), or "local" outside xdist."""
    return os.environ.get(_WORKER_ENV, "local")


@pytest.fixture(scope="session")
def offshoot_daemon():
    """Session-scoped `offshoot serve` on a fresh temp store + socket.

    See the module docstring's "xdist stance" section: under xdist this
    fixture is per-WORKER, not shared across workers. Skips (not fails)
    when no `offshoot` binary can be found.
    """
    binpath = _locate_binary()
    if binpath is None:
        pytest.skip(_INSTALL_INSTRUCTIONS)
    workdir = Path(tempfile.mkdtemp(prefix=f"offshoot-pytest-{_worker_id()}-"))
    handle = _start_daemon(binpath, workdir)
    try:
        yield handle
    finally:
        handle.stop()
        shutil.rmtree(workdir, ignore_errors=True)


# --------------------------------------------------------------------------
# offshoot_db: the named-seed factory.
# --------------------------------------------------------------------------

@dataclass
class SeedHandle:
    """What `offshoot_db(...)` returns: the seeded database + checkpoint to
    fork subsequent test branches from."""

    db: str
    checkpoint: str
    name: str = "default"


def _run_seed(session: Session, seed) -> None:
    """Run one seed (a callable(path) or a SQL string) against session's
    writable sqlite path, then flush it to the `seed` checkpoint."""
    if callable(seed):
        seed(session.path)
    else:
        conn = sqlite3.connect(session.path)
        try:
            # A SQL string with N statements and no explicit BEGIN/COMMIT
            # runs as N separate autocommit transactions under
            # `executescript` — each one is a real WAL commit the capture
            # engine has to observe and encode individually. For a
            # realistic seed script (hundreds of INSERTs) that turns a
            # ~20ms flush into ~1.5s+ (measured: 200 unwrapped INSERTs
            # cost ~1.6s vs. ~20ms wrapped in one transaction — nearly
            # 100x). Wrap the script in one transaction unless the caller
            # already opened one, so the common case (a plain multi-
            # statement seed file) is fast by default.
            if not seed.lstrip()[:5].upper().startswith("BEGIN"):
                body = seed.rstrip()
                if not body.endswith(";"):
                    body += ";"
                seed = f"BEGIN;\n{body}\nCOMMIT;"
            conn.executescript(seed)
        finally:
            conn.close()
    session.flush("seed")


class _SeedFactory:
    """Backs the `offshoot_db` fixture: memoized per (worker-process, name)
    — a plain class, not a closure, so seeding/memoization logic is
    directly unit-testable against a real daemon without a nested
    pytest-in-pytest run.
    """

    def __init__(self, client: Client, default_seed_path: str | None):
        self._client = client
        self._default_seed_path = default_seed_path or None
        self._seeded: dict[str, SeedHandle] = {}

    def __call__(self, name: str = "default", seed=None) -> SeedHandle:
        cached = self._seeded.get(name)
        if cached is not None:
            return cached
        effective_seed = seed
        if effective_seed is None:
            if not self._default_seed_path:
                raise OffshootError(
                    f"offshoot_db(name={name!r}): no seed given and no "
                    "`offshoot_seed` ini option set. Either pass "
                    "seed=<callable-or-sql-string>, or set "
                    "`offshoot_seed = path/to/seed.sql` under "
                    "[tool.pytest.ini_options] (pyproject.toml) or "
                    "[pytest] (pytest.ini/setup.cfg).")
            effective_seed = Path(self._default_seed_path).read_text()
        db = f"eval-{name}"
        self._client.create(db)
        session = self._client.open(db, "main")
        try:
            _run_seed(session, effective_seed)
        finally:
            session.close()
        handle = SeedHandle(db=db, checkpoint="seed", name=name)
        self._seeded[name] = handle
        return handle


@pytest.fixture(scope="session")
def offshoot_db(offshoot_daemon, request):
    """Session-scoped named-seed factory: `offshoot_db(name="default",
    seed=None)`. See the module docstring for the full contract."""
    client = connect(offshoot_daemon.sock)
    seed_path = request.config.getini("offshoot_seed") or None
    factory = _SeedFactory(client, seed_path)
    try:
        yield factory
    finally:
        client.close()


# --------------------------------------------------------------------------
# offshoot_fork: the function-scoped fork-per-test factory.
# --------------------------------------------------------------------------

@dataclass
class ForkedSession:
    """What `offshoot_fork(...)` returns: a writable, isolated branch
    forked from a seed checkpoint, with a live session already open on it.
    """

    path: str
    client: Client
    db: str
    branch: str
    _session: Session = field(repr=False)


def _branch_name(worker: str, nodeid: str, n: int) -> str:
    """Worker-safe, `store.ValidateName`-safe branch name for the n-th
    fork a test requests: `t-{worker}-{testname-hash}-{n}`.

    `nodeid` is sanitized via `offshoot.langgraph._sanitize` — the SDK's
    existing deterministic, [a-z0-9-]-only, collision-resistant sanitizer
    (lowercase, unsafe runs collapsed, truncated, hash-suffixed) — so two
    different tests, or two parametrizations of the same test, never
    collide even after truncation. `worker` only needs to be filesystem/
    branch-name safe (xdist worker ids are already `gw0`/`gw1`/...; this
    guards against a custom worker id containing something else), so it's
    lightly cleaned without a hash suffix.
    """
    safe_worker = _UNSAFE_RUN.sub("-", worker.lower()).strip("-") or "w"
    return f"t-{safe_worker}-{_sanitize(nodeid)}-{n}"


class _ForkFactory:
    """Backs the `offshoot_fork` fixture: forks+opens on call, remembers
    every fork it made for teardown. A plain class for the same
    direct-testability reason as `_SeedFactory`.
    """

    def __init__(self, client: Client, seed_factory: _SeedFactory, worker: str,
                 nodeid: str, ttl: str, stderr_tail: Callable[[], str] = lambda: ""):
        self._client = client
        self._seed_factory = seed_factory
        self._worker = worker
        self._nodeid = nodeid
        self._ttl = ttl
        self._stderr_tail = stderr_tail
        self._n = 0
        self.created: list[ForkedSession] = []

    def __call__(self, seed_handle: SeedHandle | None = None) -> ForkedSession:
        if seed_handle is None:
            seed_handle = self._seed_factory()
        branch = _branch_name(self._worker, self._nodeid, self._n)
        self._n += 1
        try:
            self._client.fork(seed_handle.db, "main", branch,
                               from_checkpoint=seed_handle.checkpoint, ttl=self._ttl)
            session = self._client.open(seed_handle.db, branch)
        except OffshootError as e:
            tail = self._stderr_tail()
            if tail:
                raise OffshootError(
                    f"{e}\n--- offshoot_daemon stderr tail ---\n{tail}") from e
            raise
        forked = ForkedSession(path=session.path, client=self._client,
                                db=seed_handle.db, branch=branch, _session=session)
        self.created.append(forked)
        return forked

    def teardown(self) -> None:
        for forked in self.created:
            try:
                forked._session.close()
            except OffshootError as e:
                warnings.warn(
                    f"offshoot_fork: closing the session on "
                    f"{forked.db}@{forked.branch} failed during teardown: {e}",
                    stacklevel=2)
            try:
                self._client.destroy(forked.db, forked.branch, force=True)
            except OffshootError as e:
                warnings.warn(
                    f"offshoot_fork: destroying branch "
                    f"{forked.db}@{forked.branch} failed during teardown "
                    f"(it will leak until its TTL expires): {e}",
                    stacklevel=2)


@pytest.fixture
def offshoot_fork(offshoot_daemon, offshoot_db, request):
    """Function-scoped fork-per-test factory: `offshoot_fork(seed_handle=
    None)`. See the module docstring for the full contract."""
    client = connect(offshoot_daemon.sock)
    ttl = request.config.getini("offshoot_ttl") or _DEFAULT_TTL
    factory = _ForkFactory(client, offshoot_db, _worker_id(), request.node.nodeid, ttl,
                            stderr_tail=offshoot_daemon.stderr_tail)
    try:
        yield factory
    finally:
        factory.teardown()
        client.close()


def pytest_addoption(parser):
    parser.addini(
        "offshoot_seed", default="",
        help="Path to a .sql file run (via sqlite3) as offshoot_db's "
             "zero-code default seed when a call omits seed=.")
    parser.addini(
        "offshoot_ttl", default=_DEFAULT_TTL,
        help="TTL applied to every offshoot_fork branch, as a Go duration "
             f"string (e.g. '1h', '30m'). Default: {_DEFAULT_TTL}.")
