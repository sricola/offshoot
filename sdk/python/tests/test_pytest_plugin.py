"""Tests for offshoot.pytest_plugin.

Two tiers, per the Milestone 3 Task 4 testing strategy:

1. Fixture LOGIC (naming, TTL, teardown ordering, factory memoization, skip-
   when-no-binary) tested directly against a real daemon started via
   `_start_daemon`/`_SeedFactory`/`_ForkFactory` — the same building blocks
   the actual pytest fixtures are thin wrappers around — without going
   through a nested pytest-in-pytest run for every case.
2. A handful of `pytester`-driven SMOKE scenarios that exercise the actual
   fixtures as pytest would load them: the plugin loads via its `pytest11`
   entry point, fork-per-test isolation actually isolates, and an xdist
   2-worker run passes (the last one is also where this repo's Task 4
   docstring's seed-cost-per-worker number was measured).

Run via `make test-pytest-plugin` (needs the `[pytest]` extra installed —
see pyproject.toml — unlike test_client.py/test_langgraph.py, which run
under plain `unittest` and must stay pytest-free).
"""
import json
import shutil
import sqlite3
import sys
import tempfile
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_client import build_binary  # noqa: E402

from offshoot.client import OffshootError  # noqa: E402
from offshoot.pytest_plugin import (  # noqa: E402
    _ForkFactory,
    _SeedFactory,
    _branch_name,
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


# --- _locate_binary: skip-when-no-binary logic ---

def test_locate_binary_prefers_offshoot_bin_env(tmp_path):
    fake = tmp_path / "offshoot"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    found = _locate_binary(env={"OFFSHOOT_BIN": str(fake), "PATH": ""})
    assert found == fake


def test_locate_binary_rejects_nonexistent_offshoot_bin(tmp_path):
    missing = tmp_path / "nope"
    # PATH is empty, so falling through to `which` also finds nothing.
    found = _locate_binary(env={"OFFSHOOT_BIN": str(missing), "PATH": ""})
    assert found is None


def test_locate_binary_falls_back_to_path(tmp_path):
    fake = tmp_path / "offshoot"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    found = _locate_binary(env={"PATH": str(tmp_path)})
    assert found == fake


def test_locate_binary_none_when_nothing_found():
    assert _locate_binary(env={"PATH": ""}) is None


# --- _branch_name: worker-safe, sanitized, deterministic ---

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


def test_worker_id_reads_env(monkeypatch):
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    assert _worker_id() == "local"
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    assert _worker_id() == "gw3"


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
    h2 = factory("alpha", seed=seed)  # same name: memoization hit, no reseed
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


# --- offshoot_dump ---

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


# --------------------------------------------------------------------------
# Tier 2: pytester smoke scenarios (2 fix rounds then downgrade timebox per
# PM Amendment 5; these three are the whole smoke tier).
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def offshoot_bin_str(bin_path):
    return str(bin_path)


def test_plugin_loads_via_entry_point(pytester, offshoot_bin_str, monkeypatch):
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
    result = pytester.runpytest_inprocess("-p", "offshoot")
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
    result = pytester.runpytest_inprocess("-p", "offshoot")
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
    result = pytester.runpytest_subprocess("-n", "2", "-p", "offshoot", timeout=120)
    result.assert_outcomes(passed=4)

    timings = {}
    for f in timing_dir.glob("*.json"):
        data = json.loads(f.read_text())
        timings[data["worker"]] = data["seed_seconds"]
    assert len(timings) >= 1, "expected at least one xdist worker to record a seed timing"
    print(f"\nmeasured per-worker seed cost under -n2: {timings}")
