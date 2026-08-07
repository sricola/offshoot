import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offshoot
from datetime import timedelta
from offshoot.client import OffshootError, _ttl_str

REPO = Path(__file__).resolve().parents[3]


class TestTtlStr(unittest.TestCase):
    """Pure-logic tests for _ttl_str; no daemon needed."""

    def test_none_means_no_change(self):
        self.assertEqual(_ttl_str(None), "")

    def test_zero_and_literal_none_clear(self):
        self.assertEqual(_ttl_str(0), "none")
        self.assertEqual(_ttl_str("none"), "none")

    def test_zero_timedelta_clears(self):
        # timedelta(0) == 0 is False in Python (timedelta only compares equal
        # to another timedelta), so this must not fall through to the
        # duration-string branch and render "0s" — that would silently
        # contradict this function's own "0 clears" docstring for anyone
        # passing a timedelta instead of a bare int.
        self.assertEqual(_ttl_str(timedelta(0)), "none")
        self.assertEqual(_ttl_str(timedelta(seconds=0)), "none")

    def test_timedelta_and_duration_string_set(self):
        self.assertEqual(_ttl_str(timedelta(hours=1)), "3600s")
        self.assertEqual(_ttl_str("1h"), "1h")

    def test_nonzero_int_is_rejected(self):
        with self.assertRaises(TypeError):
            _ttl_str(3600)


class TestBranchesWireCompat(unittest.TestCase):
    """Pure-logic tests for Client.branches()'s response parsing; no daemon
    needed — Client.__new__ skips __init__ (which opens a real socket) and
    _call is monkeypatched to return a canned dict, exactly as if it had
    come back over the wire.
    """

    def _client_with_response(self, resp: dict) -> offshoot.Client:
        client = offshoot.Client.__new__(offshoot.Client)
        client._call = lambda op, **fields: resp
        return client

    def test_missing_state_key_defaults_to_empty_string(self):
        # Milestone 4 Task 1 added BranchInfo.state to the wire protocol.
        # An older daemon predating that field sends a "branches" response
        # with no "state" key at all -- Client.branches() must not raise or
        # fabricate a value; Branch.state must default to "" (additive,
        # backward-compatible field; see docs/reference.md's Branch states
        # section).
        client = self._client_with_response({
            "ok": True,
            "branches": [{
                "branch": "main",
                "head_txid": 1,
                # deliberately no "state" key -- pre-Milestone-4 daemon shape.
            }],
        })
        branches = client.branches("app")
        self.assertEqual(len(branches), 1)
        self.assertEqual(branches[0].state, "")

    def test_present_state_key_is_passed_through(self):
        client = self._client_with_response({
            "ok": True,
            "branches": [{"branch": "main", "head_txid": 1, "state": "active"}],
        })
        branches = client.branches("app")
        self.assertEqual(branches[0].state, "active")


def build_binary(tmp: Path) -> Path:
    binpath = os.environ.get("OFFSHOOT_BIN")
    if binpath:
        return Path(binpath)
    out = tmp / "offshoot"
    subprocess.run(["go", "build", "-o", str(out), "./cmd/offshoot"],
                   cwd=REPO, check=True)
    return out


class DaemonFixture:
    """A daemon on a temp store+socket.

    binpath, when given, reuses an already-built binary instead of building
    a fresh one — used by tests that want a second, independently killable
    daemon without paying build_binary's cost again.
    """

    def __init__(self, binpath: Path | None = None):
        self.dir = Path(tempfile.mkdtemp(prefix="offshoot-sdk-"))
        self.bin = binpath or build_binary(self.dir)
        self.store = self.dir / "store"
        # The store must exist before `serve` will open it (ops.Open refuses
        # an uninitialized store — see cmd/offshoot/main.go); mirrors
        # TestSessionHonorsServeSocketOverride in cmd/offshoot/main_test.go.
        subprocess.run([str(self.bin), "-store", str(self.store), "init"],
                        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.sock = str(self.dir / "d.sock")
        self.proc = subprocess.Popen(
            [str(self.bin), "-store", str(self.store), "serve",
             "-socket", self.sock],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 10
        while not os.path.exists(self.sock):
            if time.time() > deadline:
                raise RuntimeError("daemon did not start: " +
                                   self.proc.stderr.peek().decode(errors="replace"))
            if self.proc.poll() is not None:
                raise RuntimeError(self.proc.stderr.read().decode(errors="replace"))
            time.sleep(0.05)

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.proc.stderr.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = DaemonFixture()

    @classmethod
    def tearDownClass(cls):
        cls.d.stop()

    def test_full_lifecycle(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("app")
            s = c.open("app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v TEXT)")
            db.execute("INSERT INTO t VALUES ('one')")
            db.commit()
            txid = s.flush("v1")
            self.assertGreater(txid, 0)
            # Fork from the LIVE session — the row must be there.
            c.fork("app", "main", "try", ttl="1h")
            p = c.checkout("app", "try")
            conn = sqlite3.connect(p)
            rows = conn.execute("SELECT count(*) FROM t").fetchone()[0]
            conn.close()
            self.assertEqual(rows, 1)
            # branches reflects TTL and checkpoints.
            info = {b.branch: b for b in c.branches("app")}
            self.assertIn("try", info)
            self.assertTrue(info["try"].ttl)
            self.assertIn("v1", info["main"].checkpoints)
            # rollback the fork? no session on it — write via at-rest is out of
            # scope; instead prove destroy guard + touch.
            with self.assertRaises(OffshootError):
                c.destroy("app", "main")  # protected by default
            c.touch("app", "try", ttl="none")
            info = {b.branch: b for b in c.branches("app")}
            self.assertEqual(info["try"].ttl, "")
            db.close()
            s.close()
            c.destroy("app", "try")
            self.assertNotIn("try", {b.branch for b in c.branches("app")})

    def test_errors_are_loud(self):
        with offshoot.connect(self.d.sock) as c:
            with self.assertRaises(OffshootError) as cm:
                c.checkout("nope", "main")
            self.assertTrue(str(cm.exception))

    def test_rollback_promote_status(self):
        # Covers Client.rollback/promote/status, which test_full_lifecycle
        # doesn't touch; uses its own db so it can't interfere with that
        # test's assertions even though they share one daemon fixture.
        with offshoot.connect(self.d.sock) as c:
            c.create("rp")
            s = c.open("rp")
            self.assertTrue(
                any(st["db"] == "rp" and st["branch"] == "main" for st in c.status()))

            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v TEXT)")
            db.commit()
            cp1 = s.flush("cp1")
            self.assertGreater(cp1, 0)
            db.execute("INSERT INTO t VALUES ('x')")
            db.commit()
            s.flush("cp2")
            db.close()
            s.close()

            path = c.rollback("rp", "main", "cp1")
            conn = sqlite3.connect(path)
            rows = conn.execute("SELECT count(*) FROM t").fetchone()[0]
            conn.close()
            self.assertEqual(rows, 0)  # rolled back before the insert

            c.fork("rp", "main", "feature")
            txid = c.promote("rp", "feature", "main", force=True)
            self.assertGreater(txid, 0)

    def test_dbs_lists_every_database_sorted(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("dbs-zeta")
            c.create("dbs-alpha")
            names = c.dbs()
            self.assertIn("dbs-zeta", names)
            self.assertIn("dbs-alpha", names)
            self.assertEqual(names, sorted(names))

    def test_fork_meta_and_checkpoints_v2_touched_at(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("meta-app")
            c.fork("meta-app", "main", "with-meta", meta={"eval_run": "42"})
            info = {b.branch: b for b in c.branches("meta-app")}
            self.assertIn("with-meta", info)
            branch = info["with-meta"]
            # touched_at is populated for a freshly forked branch.
            self.assertTrue(branch.touched_at)
            # checkpoints (old field) and checkpoints_v2 (new field) agree on
            # membership: both must list the "fork" checkpoint.
            self.assertIn("fork", branch.checkpoints)
            names_v2 = {cp.name: cp for cp in branch.checkpoints_v2}
            self.assertIn("fork", names_v2)
            self.assertTrue(names_v2["fork"].txid > 0)
            self.assertTrue(names_v2["fork"].created_at)

    def test_fork_without_meta_still_works(self):
        # Old-client-shaped call: no meta kwarg at all.
        with offshoot.connect(self.d.sock) as c:
            c.create("meta-app-2")
            txid = c.fork("meta-app-2", "main", "no-meta")
            self.assertGreater(txid, 0)

    def test_flush_meta_creates_checkpoint_with_meta(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("meta-app-3")
            s = c.open("meta-app-3")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v)")
            db.commit()
            txid = s.flush("v1", meta={"agent": "claude"})
            self.assertGreater(txid, 0)
            info = {b.branch: b for b in c.branches("meta-app-3")}
            names_v2 = {cp.name: cp for cp in info["main"].checkpoints_v2}
            self.assertIn("v1", names_v2)
            self.assertTrue(names_v2["v1"].created_at)
            db.close()
            s.close()

    def test_flush_without_meta_still_works(self):
        # Old-client-shaped call: no meta kwarg at all.
        with offshoot.connect(self.d.sock) as c:
            c.create("meta-app-4")
            s = c.open("meta-app-4")
            txid = s.flush("v1")
            self.assertGreater(txid, 0)
            s.close()

    def test_export_head_and_named_checkpoint(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("export-app")
            s = c.open("export-app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v)")
            db.execute("INSERT INTO t VALUES ('one')")
            db.commit()
            s.flush("v1")
            db.execute("INSERT INTO t VALUES ('two')")
            db.commit()
            s.flush("v2")
            s.close()

            out1 = os.path.join(self.d.dir, "export-v1.db")
            c.export("export-app", "main", out1, checkpoint="v1")
            conn = sqlite3.connect(out1)
            self.assertEqual(conn.execute("SELECT count(*) FROM t").fetchone()[0], 1)
            conn.close()

            out2 = os.path.join(self.d.dir, "export-head.db")
            c.export("export-app", "main", out2)
            conn = sqlite3.connect(out2)
            self.assertEqual(conn.execute("SELECT count(*) FROM t").fetchone()[0], 2)
            conn.close()

    def test_export_refuses_overwrite_then_force(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("export-force-app")
            s = c.open("export-force-app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v)")
            db.commit()
            db.close()
            s.flush("v1")
            s.close()

            out = os.path.join(self.d.dir, "export-force.db")
            c.export("export-force-app", "main", out)
            with self.assertRaises(OffshootError):
                c.export("export-force-app", "main", out)
            c.export("export-force-app", "main", out, force=True)  # must not raise

    def test_export_rejects_relative_path(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("export-relpath-app")
            with self.assertRaises(OffshootError):
                c.export("export-relpath-app", "main", "relative-out.db")

    def test_export_misses_unflushed_session_writes(self):
        # The load-bearing Milestone 3 Task 2 assertion: export reads the
        # store's last DURABLE state, never a live session's unflushed
        # writes.
        with offshoot.connect(self.d.sock) as c:
            c.create("export-unflushed-app")
            s = c.open("export-unflushed-app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v)")
            db.execute("INSERT INTO t VALUES ('durable')")
            db.commit()
            s.flush("v1")

            db.execute("INSERT INTO t VALUES ('unflushed')")
            db.commit()  # committed to the checkout's WAL, never flushed to the store

            out = os.path.join(self.d.dir, "export-unflushed.db")
            c.export("export-unflushed-app", "main", out)
            conn = sqlite3.connect(out)
            rows = conn.execute("SELECT count(*) FROM t").fetchone()[0]
            conn.close()
            self.assertEqual(rows, 1, "export must not include a session's unflushed write")

            s.flush()  # now durable
            out2 = os.path.join(self.d.dir, "export-after-flush.db")
            c.export("export-unflushed-app", "main", out2)
            conn = sqlite3.connect(out2)
            rows = conn.execute("SELECT count(*) FROM t").fetchone()[0]
            conn.close()
            self.assertEqual(rows, 2)
            db.close()
            s.close()

    def test_checkout_at_materializes_separate_readonly_path(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("checkout-at-app")
            s = c.open("checkout-at-app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v)")
            db.execute("INSERT INTO t VALUES ('one')")
            db.commit()
            s.flush("v1")
            db.execute("INSERT INTO t VALUES ('two')")
            db.commit()
            s.flush("v2")
            db.close()

            ro_path = c.checkout_at("checkout-at-app", "main", "v1")
            self.assertNotEqual(ro_path, s.path)
            conn = sqlite3.connect(ro_path)
            self.assertEqual(conn.execute("SELECT count(*) FROM t").fetchone()[0], 1)
            conn.close()
            mode = os.stat(ro_path).st_mode & 0o777
            self.assertEqual(mode, 0o444)

            # Repeat call is a cache hit: same path, no error.
            again = c.checkout_at("checkout-at-app", "main", "v1")
            self.assertEqual(again, ro_path)

            s.close()

    def test_checkout_at_requires_a_checkpoint_name(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("checkout-at-empty-app")
            with self.assertRaises(OffshootError):
                c.checkout_at("checkout-at-empty-app", "main", "")

    def test_checkout_at_rejects_path_traversal_checkpoint(self):
        # The SDK forwards `checkpoint` verbatim as the daemon's `name`
        # field; the server-side ops.Workspace.CheckoutAt fix must reject a
        # crafted value before it ever reaches CheckoutAtPath's
        # filepath.Join, not just an empty one.
        with offshoot.connect(self.d.sock) as c:
            c.create("checkout-at-traversal-app")
            s = c.open("checkout-at-traversal-app")
            db = sqlite3.connect(s.path)
            db.execute("CREATE TABLE t (v)")
            db.execute("INSERT INTO t VALUES ('writable')")
            db.commit()
            db.close()
            s.close()
            for bad in ("../../../etc/passwd", "..", "a/b", "../../checkouts/app/main"):
                with self.assertRaises(OffshootError):
                    c.checkout_at("checkout-at-traversal-app", "main", bad)

    def test_daemon_death_mid_call_raises_offshoot_error(self):
        # A dedicated, short-lived daemon (not the shared class fixture) so
        # killing it doesn't disturb the other tests. Reuses the already-
        # built binary to avoid a second `go build`.
        d = DaemonFixture(binpath=self.d.bin)
        try:
            with offshoot.connect(d.sock) as c:
                c.create("warm")  # prove the connection works before killing it
                d.proc.kill()
                d.proc.wait(timeout=10)
                with self.assertRaises(OffshootError):
                    c.status()
        finally:
            d.stop()
