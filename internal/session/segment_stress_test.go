package session

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

// TestCleanResumeRebaselinesBeforeFirstSegment guards fix 1 of the
// whole-branch review: a session that cleanly resumes into a reused
// Options.Dir must re-establish its checksum baseline (Session.rebaseline)
// even though the capture engine's own startup rebase is skipped on that
// path (see capture.Engine.Resumed's doc comment) — replicaSink.Rebase,
// the only other thing that ever seeds s.checksum/s.flushChecksum, never
// runs there. Left unfixed, the resumed session starts with all of that at
// its zero value while the replica file it's continuing to trust already
// holds real content: the first segment flush declares a bogus
// preApplyChecksum against the chain, and — because the forced-snapshot
// retry that spuriously fails on also folds an incremental checksum
// starting from 0 — every later segment's declared preApplyChecksum stays
// permanently wrong, so reads hard-fail chain verification.
func TestCleanResumeRebaselinesBeforeFirstSegment(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()

	// SnapshotEvery is high enough that, past the mandatory first-ever
	// snapshot, every flush in this test stays a segment — the code path
	// the bug actually corrupts (a snapshot's checksum is self-contained
	// and would mask the bug).
	s1, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", Dir: dir, SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s1.Resumed() {
		t.Fatal("the first Open of a fresh Dir must not report a resume")
	}
	if out, err := exec.Command("sqlite3", s1.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for i := 0; i < 2; i++ {
		if out, err := exec.Command("sqlite3", s1.CheckoutPath(), "INSERT INTO t VALUES ('a');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s1.Flush(""); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen against the SAME Dir: capture's tryResume should now prove
	// continuity (clean marker + empty WAL + matching main-file hash) and
	// skip its startup rebase.
	s2, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", Dir: dir, SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.Resumed() {
		t.Fatal("reopening the same Dir after a clean shutdown must resume, not rebase — this test doesn't exercise the bug's path otherwise")
	}
	for i := 0; i < 2; i++ {
		if out, err := exec.Command("sqlite3", s2.CheckoutPath(), "INSERT INTO t VALUES ('b');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s2.Flush(""); err != nil {
			t.Fatalf("flush after resume %d: %v", i, err)
		}
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	p, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout after resume+flushes: %v", err)
	}
	out, err := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatalf("checkout query: %v", err)
	}
	if string(out) != "4\n" {
		t.Fatalf("rows after resume+flushes = %q, want 4\n", out)
	}
}

// TestEveryFlushIsExactlyMaterializable is the format change's core contract:
// whatever a flush reported durable must materialize to exactly the database
// the agent had at that moment, whether it was stored as a snapshot or a
// segment.
func TestEveryFlushIsExactlyMaterializable(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	for i := 0; i < 10; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			fmt.Sprintf("INSERT INTO t (v) VALUES ('row-%d');", i)).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		name := fmt.Sprintf("cp%d", i)
		if _, err := s.Flush(name); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		// Every checkpoint so far must still materialize to the right row count.
		for j := 0; j <= i; j++ {
			br := fmt.Sprintf("check-%d-%d", i, j)
			if _, err := w.Fork("app", "main", br, fmt.Sprintf("cp%d", j), 0); err != nil {
				t.Fatalf("fork at cp%d: %v", j, err)
			}
			p, err := w.Checkout("app", br)
			if err != nil {
				t.Fatalf("checkout %s: %v", br, err)
			}
			out, err := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
			if err != nil {
				t.Fatalf("%s: %v", br, err)
			}
			if want := fmt.Sprintf("%d\n", j+1); string(out) != want {
				t.Fatalf("cp%d materialized %s rows, want %s", j, out, want)
			}
			// Row counts alone can't catch a content-level divergence that
			// happens to leave the row count unchanged (e.g. a wrong page
			// landing at the right pgno). For the final checkpoint — cp9,
			// the head of the chain covering every kind of flush this test
			// exercised (snapshots and segments alike, SnapshotEvery: 3) —
			// also pin bytes: the materialized fork's on-disk content must
			// checksum identically to the session's own replica immediately
			// after that same flush. ltxio.ChecksumDatabase rather than a
			// raw file hash because the materialized fork and the session's
			// live replica are not guaranteed byte-identical as files (e.g.
			// SQLite header fields such as the change counter, or trailing
			// freelist bytes) even when they represent the exact same
			// database content — ChecksumDatabase is the page-level,
			// header-aware comparison this package's own chain/segment
			// checksums already rely on, so it verifies the property that
			// actually matters (same content) without being defeated by
			// incidental byte-level differences that don't. s.replicaMu
			// guards the replica file exactly as Flush's own encode does
			// (see replicaMu's doc comment); no capture activity is
			// in-flight here (this is the last iteration, no further
			// checkout writes follow), so this read needs no additional
			// synchronization beyond that lock.
			if i == 9 && j == i {
				s.replicaMu.Lock()
				replicaSum, rerr := ltxio.ChecksumDatabase(s.ReplicaPath())
				s.replicaMu.Unlock()
				if rerr != nil {
					t.Fatalf("checksum session replica: %v", rerr)
				}
				forkSum, ferr := ltxio.ChecksumDatabase(p)
				if ferr != nil {
					t.Fatalf("checksum materialized fork: %v", ferr)
				}
				if forkSum != replicaSum {
					t.Fatalf("materialized head checksum %d != session replica checksum %d", forkSum, replicaSum)
				}
			}
		}
	}
}

// TestChainSurvivesSessionRestart proves a chain written by one session is
// readable and extendable by the next.
func TestChainSurvivesSessionRestart(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s1.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for i := 0; i < 3; i++ {
		if out, err := exec.Command("sqlite3", s1.CheckoutPath(), "INSERT INTO t VALUES ('a');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s1.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	out, err := exec.Command("sqlite3", s2.CheckoutPath(), "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "3\n" {
		t.Fatalf("second session sees %q err=%v", out, err)
	}
	if out, err := exec.Command("sqlite3", s2.CheckoutPath(), "INSERT INTO t VALUES ('b');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := s2.Flush(""); err != nil {
		t.Fatalf("flush after restart: %v", err)
	}
	// w.Checkout materializes to the same fixed checkout path s2 already has
	// open; the capture engine holds a continuous read lock on that path (by
	// design — see engine.beginRead's doc comment) until the session closes,
	// so Checkout must wait for that release first, exactly like every other
	// checkout-after-live-session test in this package (e.g.
	// TestFlushedStateIsRecoverableByAnotherWorkspace in flush_test.go).
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err = exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatalf("checkout query: %v", err)
	}
	if string(out) != "4\n" {
		t.Fatalf("rows after restart+flush = %q", out)
	}
}

// TestMissingSegmentIsLoud proves a chain with a deleted member fails closed
// rather than silently materializing an older state.
func TestMissingSegmentIsLoud(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for i := 0; i < 3; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	var victim string
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot {
			victim = k
		}
	}
	if victim == "" {
		t.Skip("no segment was written; nothing to remove")
	}
	if err := w.Store.B.Delete(victim); err != nil {
		t.Fatal(err)
	}
	// s.Close() above just stamped the checkout's .sum sidecar as clean and
	// current (Commit B — sidecar refresh on clean close): a checkout
	// proven clean-and-current skips chain resolution entirely (see
	// ops.Checkout's own fast path), which would make this test's whole
	// premise — that a checkout ACTUALLY consults the chain — silently
	// untrue. Force the real materialization path by removing the checkout
	// (and its now-stale-relative-to-the-deleted-segment sidecar) first,
	// same as a checkout on a fresh machine or after `rm -rf` on the local
	// cache (an intentionally supported operation — see README's
	// resource-behavior section).
	checkoutPath := w.CheckoutPath("app", "main")
	if err := os.Remove(checkoutPath); err != nil {
		t.Fatal(err)
	}
	os.Remove(checkoutPath + ".sum")
	if _, err := w.Checkout("app", "main"); err == nil {
		t.Fatal("a missing chain member must fail loudly, not silently read an older state")
	}
}
