"""Tests for offshoot.pytest_plugin.

Two tiers, per the Milestone 3 Task 4 testing strategy:

1. Fixture LOGIC (naming, TTL, teardown ordering, factory memoization, skip-
   when-no-binary, seed-transaction wrapping, seed fingerprinting, dead-
   daemon connect wrapping) tested directly against a real daemon started
   via `_start_daemon`/`_SeedFactory`/`_ForkFactory` — the same building
   blocks the actual pytest fixtures are thin wrappers around — without
   going through a nested pytest-in-pytest run for every case.
2. A handful of `pytester`-driven SMOKE scenarios that exercise the actual
   fixtures as pytest would load them: the plugin loads via its `pytest11`
   entry point (autoload — no explicit `-p offshoot`), fork-per-test
   isolation actually isolates, an xdist 2-worker run passes (also where
   this repo's Task 4 docstring's seed-cost-per-worker number was
   measured), `offshoot_seed`'s ini path resolves against rootdir from two
   different invocation directories, binary misconfiguration/requirement
   fail loud in the right shape, and teardown warnings survive `-W error`
   with a killed daemon without leaking or skipping cleanup.

Run via `make test-pytest-plugin` (needs the `[pytest]` extra installed —
see pyproject.toml — unlike test_client.py/test_langgraph.py, which run
under plain `unittest` and must stay pytest-free).
"""
import gc
import json
import shutil
import sqlite3
import sys
import tempfile
import textwrap
import warnings
import weakref
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_client import build_binary  # noqa: E402

from offshoot.client import OffshootError  # noqa: E402
from offshoot.pytest_plugin import (  # noqa: E402
    BinaryMisconfigured,
    ForkedSession,
    SeedHandle,
    _connect,
    _ForkFactory,
    _resolve_seed_path,
    _SeedFactory,
    _branch_name,
    _fingerprint_seed,
    _locate_binary,
    _start_daemon,
    _worker_id,
    offshoot_dump,
)

REPO = Path(__file__).resolve().parents[3]


# --------------------------------------------------------------------------
# Tier 1: fixture logic against a directly started daemon.
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bin_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("offshoot-plugin-bin")
    return build_binary(tmp)


@pytest.fixture
def daemon(bin_path):
    # A short tempfile.mkdtemp() dir, NOT pytest's tmp_path: tmp_path nests
    # under the test's own (often long) name, and unix socket paths have a
    # short OS-enforced length cap (sun_path, ~104-108 bytes) that a nested
    # pytest tmp_path can blow past. offshoot_daemon (the real fixture)
    # avoids this the same way — see pytest_plugin.py's _start_daemon.
    workdir = Path(tempfile.mkdtemp(prefix="offshoot-plugin-test-"))
    handle = _start_daemon(bin_path, workdir)
    try:
        yield handle
    finally:
        handle.stop()
        shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture
def client(daemon):
    from offshoot.client import connect
    c = connect(daemon.sock)
    try:
        yield c
    finally:
        c.close()


# --- _locate_binary: skip-when-no-binary vs fail-loud-when-misconfigured ---

def test_locate_binary_prefers_offshoot_bin_env(tmp_path):
    fake = tmp_path / "offshoot"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    found = _locate_binary(env={"OFFSHOOT_BIN": str(fake), "PATH": ""})
    assert found == fake


def test_locate_binary_raises_on_nonexistent_offshoot_bin(tmp_path):
    # OFFSHOOT_BIN set-but-wrong is a configuration mistake: fail loud
    # (raise), never silently fall through to "not found" (skip).
    missing = tmp_path / "nope"
    with pytest.raises(BinaryMisconfigured):
        _locate_binary(env={"OFFSHOOT_BIN": str(missing), "PATH": ""})


def test_locate_binary_rejects_directory_as_offshoot_bin(tmp_path):
    directory = tmp_path / "a-directory"
    directory.mkdir()
    with pytest.raises(BinaryMisconfigured):
        _locate_binary(env={"OFFSHOOT_BIN": str(directory), "PATH": ""})


def test_locate_binary_falls_back_to_path(tmp_path):
    fake = tmp_path / "offshoot"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    found = _locate_binary(env={"PATH": str(tmp_path)})
    assert found == fake


def test_locate_binary_none_when_nothing_found():
    # No OFFSHOOT_BIN at all, nothing on PATH: the ordinary "skip" case,
    # a plain None, not a raise.
    assert _locate_binary(env={"PATH": ""}) is None


# --- _branch_name: worker-safe, sanitized, deterministic, bounded ---

def test_branch_name_is_deterministic():
    a = _branch_name("gw0", "tests/test_x.py::test_one", 0)
    b = _branch_name("gw0", "tests/test_x.py::test_one", 0)
    assert a == b


def test_branch_name_only_uses_safe_characters():
    name = _branch_name("gw0", "tests/test_x.py::test_one[param/with spaces]", 3)
    assert name.startswith("t-gw0-")
    assert name.endswith("-3")
    for c in name:
        assert c.islower() or c.isdigit() or c == "-", name


def test_branch_name_distinguishes_different_tests_and_forks():
    n1 = _branch_name("gw0", "tests/test_x.py::test_one", 0)
    n2 = _branch_name("gw0", "tests/test_x.py::test_two", 0)
    n3 = _branch_name("gw0", "tests/test_x.py::test_one", 1)
    assert len({n1, n2, n3}) == 3


def test_branch_name_stays_well_under_store_max_name_len():
    # internal/store's ValidateName caps names at 128 chars.
    long_nodeid = "tests/" + ("x" * 500) + ".py::test_" + ("y" * 500)
    name = _branch_name("gw0", long_nodeid, 999999)
    assert len(name) < 128


def test_branch_name_bounds_a_pathologically_long_worker_id():
    long_worker = "w" * 500
    name = _branch_name(long_worker, "tests/test_x.py::test_one", 0)
    assert len(name) < 128
    # The worker segment itself is bounded, independent of the nodeid hash.
    worker_segment = name.split("-", 2)[1]
    assert len(worker_segment) <= 20


def test_worker_id_reads_env(monkeypatch):
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    assert _worker_id() == "local"
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    assert _worker_id() == "gw3"


# --- _resolve_seed_path: rootdir-relative, not cwd-relative (unit-level) ---

def test_resolve_seed_path_absolute_passthrough(tmp_path):
    absolute = tmp_path / "seed.sql"
    resolved = _resolve_seed_path(str(absolute), rootpath=Path("/somewhere/unrelated"))
    assert resolved == absolute


def test_resolve_seed_path_relative_resolves_against_rootpath(tmp_path):
    resolved = _resolve_seed_path("fixtures/seed.sql", rootpath=tmp_path)
    assert resolved == tmp_path / "fixtures" / "seed.sql"


# --- _fingerprint_seed / same-name-different-seed mismatch detection ---

def test_fingerprint_seed_is_stable_for_identical_sql():
    assert _fingerprint_seed("CREATE TABLE t (v)") == _fingerprint_seed("CREATE TABLE t (v)")


def test_fingerprint_seed_differs_for_different_sql():
    assert _fingerprint_seed("CREATE TABLE t (v)") != _fingerprint_seed("CREATE TABLE u (v)")


def test_fingerprint_seed_differs_for_different_callables():
    def a(path):
        pass

    def b(path):
        pass

    assert _fingerprint_seed(a) != _fingerprint_seed(b)


def test_fingerprint_seed_stable_for_the_same_callable_object():
    def a(path):
        pass

    assert _fingerprint_seed(a) == _fingerprint_seed(a)


# --- _SeedFactory: memoization, callable vs SQL seed, ini-default fallback ---

def test_seed_factory_memoizes_by_name(client):
    factory = _SeedFactory(client, default_seed_path=None)
    calls = []

    def seed(path):
        calls.append(path)
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (v)")
        conn.commit()
        conn.close()

    h1 = factory("alpha", seed=seed)
    h2 = factory("alpha", seed=seed)  # same name, same seed: memoization hit
    assert h1 is h2
    assert len(calls) == 1


def test_seed_factory_creates_distinct_dbs_per_name(client):
    factory = _SeedFactory(client, default_seed_path=None)
    h1 = factory("one", seed="CREATE TABLE t (v)")
    h2 = factory("two", seed="CREATE TABLE t (v)")
    assert h1.db == "eval-one"
    assert h2.db == "eval-two"
    assert h1.db != h2.db
    assert {h1.db, h2.db}.issubset(set(client.dbs()))


def test_seed_factory_raises_on_mismatched_seed_for_same_name(client):
    # Same name, DIFFERENT seed content on a later call: a clear error, not
    # a silent "keep whatever seeded it the first time".
    factory = _SeedFactory(client, default_seed_path=None)
    factory("shared", seed="CREATE TABLE t (v)")
    with pytest.raises(OffshootError, match="DIFFERENT seed"):
        factory("shared", seed="CREATE TABLE u (v)")


def test_seed_factory_raises_on_mismatched_callable_seed(client):
    def seed_a(path):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (v)")
        conn.commit()
        conn.close()

    def seed_b(path):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE u (v)")
        conn.commit()
        conn.close()

    factory = _SeedFactory(client, default_seed_path=None)
    factory("callables", seed=seed_a)
    with pytest.raises(OffshootError, match="DIFFERENT seed"):
        factory("callables", seed=seed_b)


def test_seed_factory_allows_identical_repeated_seed_for_same_name(client):
    factory = _SeedFactory(client, default_seed_path=None)
    sql = "CREATE TABLE t (v)"
    h1 = factory("repeat", seed=sql)
    h2 = factory("repeat", seed=sql)  # identical content: no raise
    assert h1 is h2


def test_seed_factory_allows_seed_none_on_an_already_cached_name(client):
    factory = _SeedFactory(client, default_seed_path=None)
    h1 = factory("cached", seed="CREATE TABLE t (v)")
    h2 = factory("cached")  # seed=None: no mismatch check, pure memoization
    assert h1 is h2


def test_seed_factory_retains_a_strong_reference_to_a_callable_seed(client):
    # _fingerprint_seed identifies a callable seed by id(), which is only a
    # reliable identity while the object stays alive: CPython is free to
    # reuse a freed object's id() for an unrelated later allocation. If
    # _SeedFactory only remembered the fingerprint string and let the
    # winning callable itself get garbage collected, a later, wholly
    # unrelated callable could coincidentally get allocated at the same
    # id() and be treated as a fingerprint MATCH by the mismatch check in
    # __call__ — silently defeating the "same name, different seed" guard
    # this class exists to enforce. Verified directly via a weakref: the
    # seed callable a factory has fingerprinted must not be collectible for
    # as long as that factory (and its fingerprint) is still alive.
    def seed(path):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (v)")
        conn.commit()
        conn.close()

    factory = _SeedFactory(client, default_seed_path=None)
    factory("ref-retained", seed=seed)
    alive = weakref.ref(seed)
    del seed
    gc.collect()
    assert alive() is not None, (
        "the seed callable was garbage collected while the factory that "
        "fingerprinted it (by id()) is still alive — its id() could now be "
        "reused by an unrelated object, producing a false fingerprint match"
    )


def test_seed_factory_runs_sql_string_seed(client):
    factory = _SeedFactory(client, default_seed_path=None)
    handle = factory("sqlseed", seed="CREATE TABLE t (v); INSERT INTO t VALUES (1), (2);")
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 2
    conn.close()


def test_seed_factory_wraps_unterminated_single_statement_seed(client):
    # Regression: a seed with exactly one statement and no trailing ';'
    # used to produce "BEGIN;\n<stmt>\nCOMMIT;" with no separator between
    # <stmt> and COMMIT, a SQL syntax error.
    factory = _SeedFactory(client, default_seed_path=None)
    handle = factory("no-semicolon", seed="CREATE TABLE t (v)")
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    conn.execute("SELECT * FROM t")  # must not raise: table exists
    conn.close()


def test_seed_factory_wraps_multi_statement_seed_in_one_transaction(client):
    # A multi-statement seed with no explicit BEGIN/COMMIT must run as ONE
    # transaction (one flush-visible commit), not N autocommit transactions
    # — see _run_seed's docstring for why this matters for seed cost.
    factory = _SeedFactory(client, default_seed_path=None)
    sql = "CREATE TABLE t (v);\n" + "\n".join(
        f"INSERT INTO t VALUES ({i});" for i in range(50))
    handle = factory("multi-stmt", seed=sql)
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 50
    conn.close()


def test_seed_factory_respects_seed_with_its_own_begin_commit(client):
    # A seed that already opens its own transaction must not be double-
    # wrapped (which would be a "cannot start a transaction within a
    # transaction" error).
    factory = _SeedFactory(client, default_seed_path=None)
    sql = "BEGIN;\nCREATE TABLE t (v);\nINSERT INTO t VALUES (1);\nCOMMIT;"
    handle = factory("own-begin", seed=sql)
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()


def test_seed_factory_runs_dump_shaped_seed(client, tmp_path):
    # CRITICAL 1 regression: a real `sqlite3 .dump` output — PRAGMA line
    # before BEGIN TRANSACTION — used verbatim as a seed. Naively checking
    # "does this start with BEGIN" misses the leading PRAGMA and
    # double-wraps it in a second, rejected BEGIN.
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (v)")
    conn.execute("INSERT INTO t VALUES ('one')")
    conn.execute("INSERT INTO t VALUES ('two')")
    conn.commit()
    conn.close()
    dump_text = offshoot_dump(src)
    assert dump_text.strip().upper().startswith("PRAGMA")  # sanity: reproduces the shape
    assert "BEGIN TRANSACTION" in dump_text.upper()

    factory = _SeedFactory(client, default_seed_path=None)
    handle = factory("dump-shaped", seed=dump_text)
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    out = sqlite3.connect(checkout)
    assert out.execute("SELECT count(*) FROM t").fetchone()[0] == 2
    out.close()


def test_seed_factory_handles_leading_comment_seed(client):
    seed = ("-- seed data for the widget table\n"
            "CREATE TABLE t (v);\nINSERT INTO t VALUES (1);")
    factory = _SeedFactory(client, default_seed_path=None)
    handle = factory("leading-comment", seed=seed)
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()


def test_seed_factory_handles_trailing_comment_seed(client):
    # CRITICAL 1 regression: the seed's last statement has no trailing ';'
    # before a line comment — `body += ";"` used to glue the semicolon
    # onto the comment text itself, where SQL line comments swallow it.
    seed = "CREATE TABLE t (v);\nINSERT INTO t VALUES (1) -- last row, no semicolon"
    factory = _SeedFactory(client, default_seed_path=None)
    handle = factory("trailing-comment", seed=seed)
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()


def test_seed_factory_runs_callable_seed_with_writable_path(client):
    factory = _SeedFactory(client, default_seed_path=None)

    def seed(path):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (v)")
        conn.execute("INSERT INTO t VALUES ('from-callable')")
        conn.commit()
        conn.close()

    handle = factory("callable-seed", seed=seed)
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT v FROM t").fetchone()[0] == "from-callable"
    conn.close()


def test_seed_factory_stamps_seed_checkpoint(client):
    factory = _SeedFactory(client, default_seed_path=None)
    handle = factory("stamped", seed="CREATE TABLE t (v)")
    assert isinstance(handle, SeedHandle)
    assert handle.checkpoint == "seed"
    info = {b.branch: b for b in client.branches(handle.db)}
    assert "seed" in info["main"].checkpoints


def test_seed_factory_falls_back_to_ini_default_seed_path(client, tmp_path):
    sql_file = tmp_path / "seed.sql"
    sql_file.write_text("CREATE TABLE t (v); INSERT INTO t VALUES ('from-ini-file');")
    factory = _SeedFactory(client, default_seed_path=str(sql_file))
    handle = factory()  # no seed= given -> falls back to the ini-style path
    checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    conn = sqlite3.connect(checkout)
    assert conn.execute("SELECT v FROM t").fetchone()[0] == "from-ini-file"
    conn.close()


def test_seed_factory_raises_clear_error_with_no_seed_and_no_ini_default(client):
    factory = _SeedFactory(client, default_seed_path=None)
    with pytest.raises(OffshootError, match="no seed given"):
        factory()


# --- _connect: dead daemon raises OffshootError, not a bare OSError ---

def test_connect_wraps_dead_daemon_connection_failure(bin_path):
    workdir = Path(tempfile.mkdtemp(prefix="offshoot-plugin-test-"))
    handle = _start_daemon(bin_path, workdir)
    handle.stop()
    try:
        with pytest.raises(OffshootError, match="could not connect"):
            _connect(handle.sock, handle.stderr_tail)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# --- _ForkFactory: TTL applied, teardown ordering, destroy failure warns ---

def test_fork_factory_applies_configured_ttl(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("ttl-test", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_ttl", ttl="2h")
    forked = forks(handle)
    info = {b.branch: b for b in client.branches(handle.db)}
    assert info[forked.branch].ttl.startswith("2h")
    forks.teardown()


def test_fork_factory_yields_isolated_writable_path(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("isolation", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_isolation", ttl="1h")
    forked = forks(handle)
    conn = sqlite3.connect(forked.path)
    conn.execute("INSERT INTO t VALUES ('only-on-fork')")
    conn.commit()
    conn.close()
    # The seed's own checkout is untouched by the fork's write.
    seed_checkout = client.checkout_at(handle.db, "main", handle.checkpoint)
    seed_conn = sqlite3.connect(seed_checkout)
    assert seed_conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    seed_conn.close()
    forks.teardown()


def test_fork_factory_accepts_seed_handle_as_string_name(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    seeds("named", seed="CREATE TABLE t (v); INSERT INTO t VALUES ('x');")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_str_seed", ttl="1h")
    forked = forks("named")  # str, not a SeedHandle
    assert forked.db == "eval-named"
    conn = sqlite3.connect(forked.path)
    assert conn.execute("SELECT v FROM t").fetchone()[0] == "x"
    conn.close()
    forks.teardown()


def test_forked_session_flush_delegates_to_underlying_session(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("flush-test", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_flush_delegate", ttl="1h")
    forked = forks(handle)
    assert isinstance(forked, ForkedSession)
    conn = sqlite3.connect(forked.path)
    conn.execute("INSERT INTO t VALUES ('mid-test')")
    conn.commit()
    conn.close()

    txid = forked.flush(name="checkpoint-a")

    assert txid > 0
    info = {b.branch: b for b in client.branches(forked.db)}
    assert "checkpoint-a" in info[forked.branch].checkpoints
    forks.teardown()


def test_fork_factory_teardown_closes_session_then_destroys_branch(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("teardown-order", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_teardown", ttl="1h")
    forked = forks(handle)
    branch = forked.branch
    assert any(st["db"] == handle.db and st["branch"] == branch for st in client.status())

    forks.teardown()

    # Session closed (no longer in `status`) and branch destroyed.
    assert not any(st["db"] == handle.db and st["branch"] == branch for st in client.status())
    assert branch not in {b.branch for b in client.branches(handle.db)}


def test_fork_factory_multiple_forks_get_distinct_branches(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("multi-fork", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_multi", ttl="1h")
    a = forks(handle)
    b = forks(handle)
    assert a.branch != b.branch
    forks.teardown()


def test_fork_factory_destroy_failure_warns_not_raises(client):
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("destroy-warn", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_warn", ttl="1h")
    forked = forks(handle)
    # Close the session and destroy the branch out from under the factory,
    # so teardown()'s own close+destroy both fail.
    forked._session.close()
    client.destroy(handle.db, forked.branch, force=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        forks.teardown()  # must not raise
    messages = [str(w.message) for w in caught]
    assert any("destroying branch" in m for m in messages)


def test_fork_factory_teardown_continues_past_one_forks_failure(client):
    # CRITICAL 2 regression: a failure tearing down ONE fork must not
    # abort the loop and leak the rest of this test's forks.
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("multi-teardown", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_multi_teardown", ttl="1h")
    broken = forks(handle)
    healthy = forks(handle)
    # Sabotage only the first fork's branch.
    broken._session.close()
    client.destroy(handle.db, broken.branch, force=True)

    # _warn() forces its warnings to always show (that's the whole CRITICAL
    # 2 fix — see test_fork_factory_teardown_warnings_always_show_even_
    # under_error_filter), so record (not ignore) them here to keep this
    # test's own output clean without fighting that design.
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        forks.teardown()

    # The second, healthy fork was still torn down properly despite the
    # first one's failure.
    assert not any(
        st["db"] == handle.db and st["branch"] == healthy.branch for st in client.status())
    assert healthy.branch not in {b.branch for b in client.branches(handle.db)}


def test_fork_factory_teardown_warnings_always_show_even_under_error_filter(client):
    # CRITICAL 2 regression: teardown must not let an ambient
    # `simplefilter("error")` (what `-W error`/`filterwarnings = error`
    # installs) turn its own best-effort notice into a raised exception.
    seeds = _SeedFactory(client, default_seed_path=None)
    handle = seeds("error-filter", seed="CREATE TABLE t (v)")
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_error_filter", ttl="1h")
    forked = forks(handle)
    forked._session.close()
    client.destroy(handle.db, forked.branch, force=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error")  # the hostile ambient filter
        forks.teardown()  # must NOT raise despite the filter above
    messages = [str(w.message) for w in caught]
    assert any("destroying branch" in m for m in messages)


def test_fork_factory_defaults_to_default_seed_when_none_given(client):
    seeds = _SeedFactory(client, default_seed_path=None)

    def default_seed(path):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (v)")
        conn.commit()
        conn.close()

    # Prime the "default" name once via a direct call, mirroring what
    # offshoot_fork's fixture does internally when seed_handle=None.
    seeds("default", seed=default_seed)
    forks = _ForkFactory(client, seeds, worker="gw0", nodeid="test_default", ttl="1h")
    forked = forks()  # no seed_handle -> seeds("default") reused
    assert forked.db == "eval-default"
    forks.teardown()


# --- offshoot_dump: plain function, fixture form, missing-CLI error ---

def test_offshoot_dump_returns_sql_text(tmp_path):
    dbfile = tmp_path / "d.sqlite"
    conn = sqlite3.connect(dbfile)
    conn.execute("CREATE TABLE t (v TEXT)")
    conn.execute("INSERT INTO t VALUES ('hello')")
    conn.commit()
    conn.close()
    dump = offshoot_dump(dbfile)
    assert "CREATE TABLE t" in dump
    assert "hello" in dump


def test_offshoot_dump_is_the_right_comparison_not_bytes(tmp_path):
    # Two files with identical logical content but different on-disk
    # history (a vacuum changes page layout) must dump equal even though
    # their bytes differ — the whole reason offshoot_dump exists.
    a = tmp_path / "a.sqlite"
    b = tmp_path / "b.sqlite"
    for p in (a, b):
        conn = sqlite3.connect(p)
        conn.execute("CREATE TABLE t (v TEXT)")
        conn.execute("INSERT INTO t VALUES ('x')")
        conn.execute("INSERT INTO t VALUES ('y')")
        conn.execute("DELETE FROM t WHERE v = 'y'")
        conn.execute("INSERT INTO t VALUES ('y')")
        conn.commit()
        conn.close()
    conn = sqlite3.connect(a)
    conn.execute("VACUUM")
    conn.close()
    assert a.read_bytes() != b.read_bytes()  # byte-different, as expected
    assert offshoot_dump(a) == offshoot_dump(b)  # logically identical


def test_offshoot_dump_raises_clear_error_when_sqlite3_cli_missing(tmp_path, monkeypatch):
    dbfile = tmp_path / "d.sqlite"
    conn = sqlite3.connect(dbfile)
    conn.execute("CREATE TABLE t (v)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("PATH", str(tmp_path))  # a dir with no `sqlite3` on it
    with pytest.raises(OffshootError, match="sqlite3"):
        offshoot_dump(dbfile)


def test_offshoot_dump_fixture_form_matches_the_plain_function(offshoot_dump, tmp_path):
    # The plugin's own `offshoot_dump` fixture is loaded automatically for
    # THIS test session too (this whole file runs under pytest, with the
    # entry point active) — request it directly, shadowing the module-level
    # import of the same name within this function's scope.
    dbfile = tmp_path / "d.sqlite"
    conn = sqlite3.connect(dbfile)
    conn.execute("CREATE TABLE t (v)")
    conn.commit()
    conn.close()
    assert "CREATE TABLE t" in offshoot_dump(dbfile)


# --------------------------------------------------------------------------
# Tier 2: pytester smoke scenarios.
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def offshoot_bin_str(bin_path):
    return str(bin_path)


def test_plugin_loads_via_entry_point(pytester, offshoot_bin_str, monkeypatch):
    # No "-p offshoot": this proves AUTOLOAD (the pytest11 entry point)
    # actually works, not just that the plugin works once force-loaded.
    monkeypatch.setenv("OFFSHOOT_BIN", offshoot_bin_str)
    pytester.makepyfile(
        """
        def test_fixtures_are_available(offshoot_db, offshoot_fork):
            handle = offshoot_db(seed="CREATE TABLE t (v)")
            forked = offshoot_fork(handle)
            assert forked.path
            assert forked.branch.startswith("t-")
        """
    )
    result = pytester.runpytest_inprocess()
    result.assert_outcomes(passed=1)


def test_fork_per_test_isolation_actually_isolates(pytester, offshoot_bin_str, monkeypatch):
    monkeypatch.setenv("OFFSHOOT_BIN", offshoot_bin_str)
    pytester.makepyfile(
        """
        import sqlite3

        def test_writes_marker_one(offshoot_db, offshoot_fork):
            handle = offshoot_db(seed="CREATE TABLE t (v)")
            forked = offshoot_fork(handle)
            conn = sqlite3.connect(forked.path)
            conn.execute("INSERT INTO t VALUES ('one')")
            conn.commit()
            conn.close()

        def test_never_sees_the_other_tests_write(offshoot_db, offshoot_fork):
            handle = offshoot_db(seed="CREATE TABLE t (v)")
            forked = offshoot_fork(handle)
            conn = sqlite3.connect(forked.path)
            rows = conn.execute("SELECT v FROM t").fetchall()
            conn.close()
            assert rows == []
        """
    )
    result = pytester.runpytest_inprocess()
    result.assert_outcomes(passed=2)


def test_xdist_two_worker_run_passes_and_measures_seed_cost(
    pytester, offshoot_bin_str, monkeypatch, tmp_path_factory
):
    pytest.importorskip("xdist")
    timing_dir = tmp_path_factory.mktemp("offshoot-xdist-timing")
    monkeypatch.setenv("OFFSHOOT_BIN", offshoot_bin_str)
    monkeypatch.setenv("OFFSHOOT_TEST_TIMING_DIR", str(timing_dir))
    pytester.makepyfile(
        """
        import json
        import os
        import sqlite3
        import time
        from pathlib import Path

        import pytest

        SEED_SQL = "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT);\\n" + \\
            "\\n".join(f"INSERT INTO items(val) VALUES ('row-{i}');" for i in range(200))

        @pytest.mark.parametrize("n", range(4))
        def test_seed_once_fork_and_record_timing(n, offshoot_db, offshoot_fork):
            t0 = time.perf_counter()
            handle = offshoot_db(seed=SEED_SQL)
            elapsed = time.perf_counter() - t0
            worker = os.environ.get("PYTEST_XDIST_WORKER", "local")
            timing_dir = Path(os.environ["OFFSHOOT_TEST_TIMING_DIR"])
            path = timing_dir / f"{worker}.json"
            # Only the FIRST call in this worker actually seeds (memoized
            # after); only write once so the recorded number is the real
            # seed cost, not an overwritten cache-hit near-zero.
            if not path.exists():
                path.write_text(json.dumps({"worker": worker, "seed_seconds": elapsed}))

            forked = offshoot_fork(handle)
            conn = sqlite3.connect(forked.path)
            assert conn.execute("SELECT count(*) FROM items").fetchone()[0] == 200
        """
    )
    result = pytester.runpytest_subprocess("-n", "2", timeout=120)
    result.assert_outcomes(passed=4)

    timings = {}
    for f in timing_dir.glob("*.json"):
        data = json.loads(f.read_text())
        timings[data["worker"]] = data["seed_seconds"]
    assert len(timings) >= 1, "expected at least one xdist worker to record a seed timing"
    print(f"\nmeasured per-worker seed cost under -n2: {timings}")


def test_offshoot_seed_ini_resolves_relative_to_rootdir_not_cwd(
    pytester, offshoot_bin_str, monkeypatch
):
    # IMPORTANT 3 regression: offshoot_seed's ini value must resolve
    # against pytest's rootdir, not the process's current working
    # directory — verified from BOTH invocation directions.
    monkeypatch.setenv("OFFSHOOT_BIN", offshoot_bin_str)
    pytester.makeini("[pytest]\noffshoot_seed = seed.sql\n")
    (pytester.path / "seed.sql").write_text(
        "CREATE TABLE t (v);\nINSERT INTO t VALUES ('from-root-relative-seed');\n")
    test_body = textwrap.dedent("""
        import sqlite3

        def test_seed_resolves(offshoot_fork):
            forked = offshoot_fork()
            conn = sqlite3.connect(forked.path)
            row = conn.execute("SELECT v FROM t").fetchone()
            conn.close()
            assert row[0] == "from-root-relative-seed"
        """)
    (pytester.path / "test_from_root.py").write_text(test_body)
    sub = pytester.path / "sub"
    sub.mkdir()
    (sub / "test_from_sub.py").write_text(test_body)

    # Direction 1: cwd == rootdir (the ordinary case).
    result_root = pytester.runpytest_subprocess("test_from_root.py", timeout=60)
    result_root.assert_outcomes(passed=1)

    # Direction 2: cwd == a subdirectory of rootdir. Resolving against
    # Path.cwd() instead of rootdir would look for sub/seed.sql, which
    # doesn't exist, and fail with "no seed given and no offshoot_seed".
    monkeypatch.chdir(sub)
    result_sub = pytester.runpytest_subprocess("test_from_sub.py", timeout=60)
    result_sub.assert_outcomes(passed=1)


def test_offshoot_bin_misconfigured_fails_loud_not_skip(pytester, tmp_path, monkeypatch):
    missing = tmp_path / "does-not-exist"
    monkeypatch.setenv("OFFSHOOT_BIN", str(missing))
    pytester.makepyfile(
        """
        def test_needs_daemon(offshoot_daemon):
            pass
        """
    )
    result = pytester.runpytest_inprocess()
    result.assert_outcomes(errors=1)


def test_offshoot_require_binary_ini_fails_loud_when_missing(pytester, monkeypatch, tmp_path):
    monkeypatch.delenv("OFFSHOOT_BIN", raising=False)
    empty_path_dir = tmp_path / "empty-path"
    empty_path_dir.mkdir()
    monkeypatch.setenv("PATH", str(empty_path_dir))
    pytester.makeini("[pytest]\noffshoot_require_binary = true\n")
    pytester.makepyfile(
        """
        def test_needs_daemon(offshoot_daemon):
            pass
        """
    )
    result = pytester.runpytest_inprocess()
    result.assert_outcomes(errors=1)


def test_offshoot_require_binary_defaults_to_a_skip(pytester, monkeypatch, tmp_path):
    monkeypatch.delenv("OFFSHOOT_BIN", raising=False)
    empty_path_dir = tmp_path / "empty-path"
    empty_path_dir.mkdir()
    monkeypatch.setenv("PATH", str(empty_path_dir))
    pytester.makepyfile(
        """
        def test_needs_daemon(offshoot_daemon):
            pass
        """
    )
    result = pytester.runpytest_inprocess()
    result.assert_outcomes(skipped=1)


def test_teardown_warnings_do_not_escalate_under_dash_w_error(
    pytester, offshoot_bin_str, monkeypatch
):
    # CRITICAL 2, end to end: a killed daemon makes offshoot_fork's
    # teardown (session close + branch destroy) fail for a real fork, under
    # a REAL "-W error" pytest invocation. Before the fix this reproduced
    # as "1 passed, 1 error" (the raised warning aborted the fixture
    # finalizer, and client.close() never ran); after the fix it's cleanly
    # "1 passed" — teardown completes, no error.
    monkeypatch.setenv("OFFSHOOT_BIN", offshoot_bin_str)
    pytester.makepyfile(
        """
        def test_kills_daemon_after_forking(offshoot_daemon, offshoot_db, offshoot_fork):
            handle = offshoot_db(seed="CREATE TABLE t (v)")
            forked = offshoot_fork(handle)
            assert forked.path
            # Kill the daemon out from under this test's own fork so
            # offshoot_fork's teardown (session.close() + branch destroy)
            # is guaranteed to fail.
            offshoot_daemon.proc.kill()
            offshoot_daemon.proc.wait(timeout=10)
        """
    )
    result = pytester.runpytest_subprocess("-W", "error", timeout=60)
    result.assert_outcomes(passed=1)
