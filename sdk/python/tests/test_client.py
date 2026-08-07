import contextlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
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


@contextlib.contextmanager
def fake_subscribe_daemon(lines: list[str], ack: dict | None = None):
    """A minimal one-shot fake daemon: accepts exactly one connection on a
    fresh temp unix socket, discards the `{"op":"subscribe"}` request line
    it's sent, writes `ack` (default `{"ok": true}`) then each of `lines`
    (newline-terminated, in order), then closes.

    Used to drive Client.events()'s decode path end to end — including the
    `dropped_slow_consumer` terminal event — against a scripted server that
    speaks the exact same line-per-event wire shape the real daemon does,
    without needing to force a real daemon's buffer-overflow/slow-consumer
    mechanics deterministically from the SDK side (hard to do reliably; see
    internal/daemon/events_test.go's TestEventBusDropsSlowSubscriberWithTerminalEvent
    for that proof against the real bus instead). Yields the fake socket's
    path.
    """
    ack = {"ok": True} if ack is None else ack
    tmpdir = tempfile.mkdtemp(prefix="offshoot-fake-daemon-")
    sock_path = os.path.join(tmpdir, "d.sock")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(1)

    def serve():
        try:
            conn, _ = srv.accept()
        except OSError:
            return  # srv closed before a client ever connected
        try:
            conn.makefile("rb").readline()  # the subscribe request; discarded
            conn.sendall(json.dumps(ack).encode() + b"\n")
            for line in lines:
                conn.sendall(line.encode() + b"\n")
        finally:
            conn.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    try:
        yield sock_path
    finally:
        srv.close()
        th.join(timeout=5)
        shutil.rmtree(tmpdir, ignore_errors=True)


def fake_events_client(sock_path: str) -> offshoot.Client:
    """A Client wired to sock_path without dialing the real request/response
    handshake __init__ performs (there is no request/response daemon on the
    other end here, only fake_subscribe_daemon's scripted one-shot server) —
    Client.events() only ever reads self._socket_path, so this is enough to
    exercise it standalone. Mirrors TestBranchesWireCompat's
    Client.__new__ technique above.
    """
    client = offshoot.Client.__new__(offshoot.Client)
    client._socket_path = sock_path
    return client


class TestEventsDecodePath(unittest.TestCase):
    """Fake-daemon tests for Client.events()'s decode/contract path — no
    real offshoot daemon needed."""

    def test_dropped_slow_consumer_is_yielded_then_stream_ends(self):
        lines = [
            json.dumps({"v": 1, "ts": "2026-08-07T00:00:00Z", "type": "session_opened",
                        "db": "app", "branch": "main", "detail": {"holder": "h1", "epoch": 1}}),
            json.dumps({"v": 1, "ts": "2026-08-07T00:00:01Z", "type": "dropped_slow_consumer"}),
        ]
        with fake_subscribe_daemon(lines) as sock_path:
            got = list(fake_events_client(sock_path).events())
        # Yielded like any other event, then the generator simply stops
        # (StopIteration via list()'s own loop) -- no exception, per this
        # method's documented contract.
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0].type, "session_opened")
        self.assertEqual(got[0].db, "app")
        self.assertEqual(got[0].branch, "main")
        self.assertEqual(got[0].detail, {"holder": "h1", "epoch": 1})
        self.assertEqual(got[1].type, "dropped_slow_consumer")
        self.assertEqual(got[1].db, "")
        self.assertEqual(got[1].detail, {})

    def test_events_yielded_in_order_with_defaults_for_missing_fields(self):
        lines = [
            json.dumps({"v": 1, "ts": "2026-08-07T00:00:00Z", "type": "reaped"}),
        ]
        with fake_subscribe_daemon(lines) as sock_path:
            got = list(fake_events_client(sock_path).events())
        self.assertEqual(len(got), 1)
        ev = got[0]
        self.assertEqual(ev.v, 1)
        self.assertEqual(ev.type, "reaped")
        self.assertEqual(ev.db, "")
        self.assertEqual(ev.branch, "")
        self.assertEqual(ev.detail, {})

    def test_subscribe_ack_not_ok_raises(self):
        with fake_subscribe_daemon([], ack={"ok": False, "error": "boom"}) as sock_path:
            with self.assertRaises(OffshootError) as cm:
                list(fake_events_client(sock_path).events())
        self.assertIn("boom", str(cm.exception))

    def test_malformed_event_line_raises_offshoot_error(self):
        with fake_subscribe_daemon(["not json"]) as sock_path:
            with self.assertRaises(OffshootError):
                list(fake_events_client(sock_path).events())

    def test_generator_close_before_first_next_is_a_harmless_noop(self):
        # events() is lazy -- calling it alone opens no socket. Closing
        # before ever iterating must not raise (nothing to clean up).
        client = fake_events_client("/nonexistent/path/does/not/matter")
        gen = client.events()
        gen.close()  # must not raise


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

    def _daemon_unix_socket_fd_count(self) -> int | None:
        """Number of the daemon process's currently-open unix-domain-socket
        file descriptors (its listening socket plus one per live
        connection -- ordinary Client connections AND subscribe streams
        alike), via `lsof -p <pid>` filtered to TYPE "unix". None if lsof
        isn't on PATH (the fd-leak assertion below is then skipped -- best
        effort, not load-bearing on every CI runner).

        Deliberately filtered to TYPE "unix" rather than a raw total fd
        count: internal/dbfile's checkout file descriptors are DELIBERATELY
        NEVER closed for the life of the daemon (see docs/status.md's
        Resource behavior table) -- opening a session over the course of
        this test legitimately grows the daemon's REG-file fd count by
        design, which would make a raw total-fd-count comparison spuriously
        fail regardless of whether events()'s dedicated socket itself
        leaked. Counting only "unix" rows isolates exactly the resource
        events()'s dedicated connection actually holds.
        """
        if not shutil.which("lsof"):
            return None
        out = subprocess.run(["lsof", "-p", str(self.d.proc.pid)],
                              capture_output=True, text=True)
        return sum(1 for line in out.stdout.splitlines() if "unix" in line.split())

    def test_events_sees_session_lifecycle_in_order(self):
        # Subscribes via the helper on its own dedicated connection, then
        # drives session_opened/flushed/session_closed on a SEPARATE
        # connection (c2) -- proving events() never shares a socket with
        # ordinary request/response calls (the daemon's "subscribe" op
        # permanently takes a connection out of request/response mode; see
        # events() docstring and docs/reference.md's Eventing section).
        with offshoot.connect(self.d.sock) as c:
            c.create("events-lifecycle-app")
            gen = c.events()
            got: list = []

            def collect(n):
                for _ in range(n):
                    got.append(next(gen))

            th = threading.Thread(target=collect, args=(3,))
            th.start()
            # Let events()'s first next() call actually complete its
            # connect+subscribe+ack round trip (typically sub-millisecond
            # over a local unix socket) before driving transitions on c2 --
            # the bus has no replay, so a transition published before this
            # subscription is registered would be silently missed.
            time.sleep(0.5)

            with offshoot.connect(self.d.sock) as c2:
                s = c2.open("events-lifecycle-app")
                db = sqlite3.connect(s.path)
                db.execute("CREATE TABLE t (v)")
                db.commit()
                db.close()
                deadline = time.time() + 10
                txid = 0
                while time.time() < deadline:
                    txid = s.flush()
                    if txid:
                        break
                    time.sleep(0.1)
                self.assertGreater(txid, 0)
                s.close()

            th.join(timeout=15)
            self.assertFalse(th.is_alive(), "events() never observed all 3 transitions")
            self.assertEqual(len(got), 3)
            self.assertEqual(got[0].type, "session_opened")
            self.assertEqual(got[0].db, "events-lifecycle-app")
            self.assertEqual(got[0].branch, "main")
            self.assertEqual(got[1].type, "flushed")
            self.assertEqual(got[1].db, "events-lifecycle-app")
            self.assertEqual(got[1].detail.get("kind"), "manual")
            self.assertEqual(got[2].type, "session_closed")
            self.assertEqual(got[2].db, "events-lifecycle-app")
            gen.close()

    def test_events_close_mid_stream_closes_the_dedicated_socket(self):
        with offshoot.connect(self.d.sock) as c:
            c.create("events-close-app")

            # Let any startup-transient fd churn settle before sampling a
            # baseline.
            time.sleep(0.2)
            baseline = self._daemon_unix_socket_fd_count()

            gen = c.events()
            got: list = []
            th = threading.Thread(target=lambda: got.append(next(gen)))
            th.start()
            time.sleep(0.3)  # let the subscribe connect+ack land first
            with offshoot.connect(self.d.sock) as c2:
                c2.open("events-close-app")
            th.join(timeout=10)
            self.assertEqual(len(got), 1)
            self.assertEqual(got[0].type, "session_opened")

            # Stop iterating mid-stream -- GeneratorExit propagates through
            # events()'s try/finally, closing the dedicated socket without
            # ever reading it to EOF.
            gen.close()

            if baseline is not None:
                deadline = time.time() + 5
                after = baseline + 1
                while time.time() < deadline:
                    after = self._daemon_unix_socket_fd_count()
                    if after is not None and after <= baseline:
                        break
                    time.sleep(0.1)
                self.assertLessEqual(
                    after, baseline,
                    f"daemon fd count did not return to baseline after events().close() "
                    f"(baseline={baseline}, after={after}) -- possible leaked subscriber fd")

            # The daemon itself must be unaffected: an ordinary op on a
            # fresh connection still works after the dropped subscriber.
            c.branches("events-close-app")

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
