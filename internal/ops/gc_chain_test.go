// Package ops_test tests the ops package from outside it, allowing imports
// of both ops and session (which imports ops) without circular dependencies.
package ops_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/session"
	"github.com/offshoot-db/offshoot/internal/store"
)

// newWS initializes a fresh Workspace for testing. Mirrors the internal
// ops_test helper but accessible from the external test package.
func newWS(t *testing.T) *ops.Workspace {
	t.Helper()
	w, err := ops.Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestReplayStaysBoundedAcrossManyFlushes(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE failed: %v: %s", err, out)
	}
	for i := 0; i < 20; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
			t.Fatalf("INSERT %d failed: %v: %s", i, err, out)
		}
		if _, err := s.Flush(""); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) > 4 {
		t.Fatalf("replay must stay bounded by the snapshot cadence, chain is %d members", len(chain))
	}
	if !chain[0].Snapshot {
		t.Fatal("a chain must start at a snapshot")
	}
}

func TestGCSweepsSegmentsWithTheirLineage(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "doomed", SnapshotEvery: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE failed: %v: %s", err, out)
	}
	for i := 0; i < 3; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").CombinedOutput(); err != nil {
			t.Fatalf("INSERT %d failed: %v: %s", i, err, out)
		}
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	ref, _, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	lineage := ref.Lineage
	keysBefore, err := w.Store.B.List(store.LineagePrefix(lineage))
	if err != nil {
		t.Fatal(err)
	}
	var segs int
	for _, k := range keysBefore {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot {
			segs++
		}
	}
	if segs == 0 {
		t.Fatal("setup produced no segments to sweep")
	}
	s.Close()

	if err := w.Destroy("app", "doomed", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	after, err := w.Store.B.List(store.LineagePrefix(lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("GC must sweep segments with their lineage, %d objects remain: %v", len(after), after)
	}
}

// TestForkFastPathSkipsMultiMemberChains verifies Task 6's fast-path
// precondition: forking a branch whose head is reached via one or more
// segments past its last snapshot (a daemon session's flush cadence, never
// possible from the CLI's own synchronous Checkpoint, which always writes a
// full snapshot) must fall through to the existing slow
// materialize-and-re-encode path unchanged — there is no single source
// object a multi-member chain could hand to CopyObject. This lives in the
// external ops_test package (not internal ops_test.go) because it needs
// session, which imports ops — see this file's package doc comment.
func TestForkFastPathSkipsMultiMemberChains(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("seed: %v: %s", err, out)
	}
	// First flush after Open is always a forced full snapshot (the
	// "settling flush" — see docs/benchmarks.md), regardless of
	// SnapshotEvery. This becomes the branch's only snapshot so far.
	if _, err := s.Flush(""); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}
	// Second flush, still short of the SnapshotEvery=4 cadence: a segment,
	// not a snapshot. The branch's chain is now [snapshot, segment].
	if _, err := s.Flush(""); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) < 2 {
		t.Fatalf("test precondition: want a multi-member chain, got %d member(s): %+v", len(chain), chain)
	}

	before := ops.ForkFastPathHits()
	if _, err := w.Fork("app", "main", "child", "", 0); err != nil {
		t.Fatal(err)
	}
	if got := ops.ForkFastPathHits(); got != before {
		t.Fatalf("fork fast path hits = %d, want %d unchanged (a multi-member chain must take the slow path)", got, before)
	}

	// Functional check: the child still materializes correctly via the slow
	// path, applying both the snapshot and the segment.
	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	got, err := exec.Command("sqlite3", cpath, "SELECT v FROM t ORDER BY v;").Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "1\n2\n" {
		t.Fatalf("child content = %q, want \"1\\n2\\n\"", got)
	}
}
