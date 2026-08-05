import shutil
import subprocess
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offshoot
from offshoot.client import OffshootError
from offshoot.langgraph import ThreadForks, _sanitize
from test_client import DaemonFixture, build_binary

REPO = Path(__file__).resolve().parents[3]
AGENT_PY = REPO / "examples" / "langgraph-rewind" / "agent.py"

# Built once and shared across this file's DaemonFixtures to avoid paying
# `go build` three times over.
_SHARED_BIN: Path | None = None
_SHARED_BIN_DIR: Path | None = None


def setUpModule():
    global _SHARED_BIN, _SHARED_BIN_DIR
    _SHARED_BIN_DIR = Path(tempfile.mkdtemp(prefix="offshoot-sdk-langgraph-bin-"))
    _SHARED_BIN = build_binary(_SHARED_BIN_DIR)


def tearDownModule():
    if _SHARED_BIN_DIR is not None:
        shutil.rmtree(_SHARED_BIN_DIR, ignore_errors=True)


class TestThreadLifecycle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = DaemonFixture(binpath=_SHARED_BIN)
        cls.client = offshoot.connect(cls.d.sock)
        cls.client.create("agent")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.d.stop()

    def test_fork_checkpoint_and_rewind(self):
        forks = ThreadForks(self.client, "agent")
        self.addCleanup(forks.close)

        p1 = forks.path("t1")
        self.assertTrue(p1)

        conn = sqlite3.connect(p1)
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)")
        conn.execute("INSERT INTO orders (item) VALUES ('widget')")
        conn.commit()
        conn.close()
        forks.checkpoint("t1", "ckpt-1")

        conn = sqlite3.connect(p1)
        conn.execute("INSERT INTO orders (item) VALUES ('gadget')")
        conn.commit()
        conn.close()
        forks.checkpoint("t1", "ckpt-2")

        # t1 still has both rows.
        conn = sqlite3.connect(p1)
        count_t1 = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
        conn.close()
        self.assertEqual(count_t1, 2)

        # Forking t1 at ckpt-1 into t2 must land in the ckpt-1 world: 1 row.
        p2 = forks.fork_thread("t1", "ckpt-1", "t2")
        conn = sqlite3.connect(p2)
        count_t2 = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
        conn.close()
        self.assertEqual(count_t2, 1)

        # t1 is unaffected by the fork.
        conn = sqlite3.connect(p1)
        count_t1_after = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
        conn.close()
        self.assertEqual(count_t1_after, 2)

        # The offshoot branches are exactly branch_for("t1")/branch_for("t2"),
        # and both carry the default TTL, canonically rendered.
        info = {b.branch: b for b in self.client.branches("agent")}
        self.assertIn(forks.branch_for("t1"), info)
        self.assertIn(forks.branch_for("t2"), info)
        self.assertEqual(info[forks.branch_for("t1")].ttl, "24h0m0s")
        self.assertEqual(info[forks.branch_for("t2")].ttl, "24h0m0s")

    def test_fork_thread_on_unknown_checkpoint_raises_clear_error(self):
        forks = ThreadForks(self.client, "agent")
        self.addCleanup(forks.close)
        forks.path("t3")
        with self.assertRaises(OffshootError) as cm:
            forks.fork_thread("t3", "never-flushed", "t4")
        msg = str(cm.exception)
        self.assertIn("t3", msg)
        self.assertIn("never-flushed", msg)

    def test_fork_thread_from_unknown_thread_raises_clear_error(self):
        forks = ThreadForks(self.client, "agent")
        self.addCleanup(forks.close)
        with self.assertRaises(OffshootError) as cm:
            forks.fork_thread("never-opened", "ckpt-1", "t5")
        self.assertIn("never-opened", str(cm.exception))

    def test_fork_thread_retried_with_same_new_thread_names_the_collision(self):
        # A retried resume that reuses the same new_thread id must NOT be
        # told to re-checkpoint (that can never fix a name collision) — it
        # must be told the target thread's branch already exists.
        forks = ThreadForks(self.client, "agent")
        self.addCleanup(forks.close)
        forks.path("t6")
        forks.checkpoint("t6", "ckpt-1")
        forks.fork_thread("t6", "ckpt-1", "t7")  # first fork succeeds

        with self.assertRaises(OffshootError) as cm:
            forks.fork_thread("t6", "ckpt-1", "t7")  # retried with same new_thread
        msg = str(cm.exception)
        self.assertIn("t7", msg)
        self.assertIn("already has a branch", msg)
        self.assertNotIn("was never recorded", msg)
        self.assertNotIn("ckpt-1", msg)


class TestNameSanitization(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = DaemonFixture(binpath=_SHARED_BIN)
        cls.client = offshoot.connect(cls.d.sock)
        cls.client.create("sanitize")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.d.stop()

    def test_uuid_and_traversal_ids_sanitize_to_names_the_daemon_accepts(self):
        forks = ThreadForks(self.client, "sanitize")
        self.addCleanup(forks.close)

        uuid_id = "550e8400-e29b-41d4-a716-446655440000"
        evil_id = "../../evil"

        b1 = forks.branch_for(uuid_id)
        b2 = forks.branch_for(evil_id)
        self.assertNotEqual(b1, b2)

        # The daemon accepts both: forking (which validates the branch name)
        # succeeds for each.
        forks.path(uuid_id)
        forks.path(evil_id)
        names = {b.branch for b in self.client.branches("sanitize")}
        self.assertIn(b1, names)
        self.assertIn(b2, names)

    def test_sanitization_is_deterministic(self):
        forks = ThreadForks(self.client, "sanitize")
        self.assertEqual(forks.branch_for("abc"), forks.branch_for("abc"))
        self.assertEqual(_sanitize("../../evil"), _sanitize("../../evil"))

    def test_truncation_collisions_still_produce_distinct_names(self):
        # Two ids that share a prefix far longer than the sanitized body's
        # truncation length must NOT collide: the hash suffix (computed over
        # the full, untruncated id) is what prevents it.
        forks = ThreadForks(self.client, "sanitize")
        long_a = "x" * 80 + "AAAA"
        long_b = "x" * 80 + "BBBB"
        self.assertNotEqual(forks.branch_for(long_a), forks.branch_for(long_b))


class TestCloseAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = DaemonFixture(binpath=_SHARED_BIN)
        cls.client = offshoot.connect(cls.d.sock)
        cls.client.create("closeall")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.d.stop()

    def test_close_releases_every_session_so_destroy_succeeds(self):
        forks = ThreadForks(self.client, "closeall")
        forks.path("a")
        forks.path("b")

        # Sanity: destroy refuses a branch with a live session.
        with self.assertRaises(OffshootError):
            self.client.destroy("closeall", forks.branch_for("a"))

        forks.close()

        # No open-session guard error now that every session is closed.
        self.client.destroy("closeall", forks.branch_for("a"))
        self.client.destroy("closeall", forks.branch_for("b"))
        names = {b.branch for b in self.client.branches("closeall")}
        self.assertNotIn(forks.branch_for("a"), names)
        self.assertNotIn(forks.branch_for("b"), names)

    def test_close_one_thread_leaves_the_rest_open(self):
        forks = ThreadForks(self.client, "closeall")
        forks.path("c")
        forks.path("d")
        forks.close("c")

        with self.assertRaises(OffshootError):
            self.client.destroy("closeall", forks.branch_for("d"))

        self.client.destroy("closeall", forks.branch_for("c"))
        forks.close()  # release "d" too
        self.client.destroy("closeall", forks.branch_for("d"))


class TestAgentExampleSmoke(unittest.TestCase):
    """Runs the example end to end against a real daemon, via subprocess,
    exactly as a user following the README would."""

    @classmethod
    def setUpClass(cls):
        cls.d = DaemonFixture(binpath=_SHARED_BIN)

    @classmethod
    def tearDownClass(cls):
        cls.d.stop()

    def test_agent_example_prints_two_diverged_world_states(self):
        proc = subprocess.run(
            [sys.executable, str(AGENT_PY), "--socket", self.d.sock],
            cwd=REPO, capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(
            "original thread, after 3 turns: ['widget', 'gadget', 'gizmo-oops']",
            proc.stdout)
        self.assertIn(
            "rewound thread, after retrying turn 2: ['widget', 'sprocket']",
            proc.stdout)


if __name__ == "__main__":
    unittest.main()
