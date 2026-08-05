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

    def test_timedelta_and_duration_string_set(self):
        self.assertEqual(_ttl_str(timedelta(hours=1)), "3600s")
        self.assertEqual(_ttl_str("1h"), "1h")

    def test_nonzero_int_is_rejected(self):
        with self.assertRaises(TypeError):
            _ttl_str(3600)


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
