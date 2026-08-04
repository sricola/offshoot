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
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 20; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
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
	if _, err := w.Fork("app", "main", "doomed", ""); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "doomed", SnapshotEvery: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 3; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	ref, _, _ := w.Store.GetRef("app", "doomed")
	lineage := ref.Lineage
	keysBefore, _ := w.Store.B.List(store.LineagePrefix(lineage))
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
	after, _ := w.Store.B.List(store.LineagePrefix(lineage))
	if len(after) != 0 {
		t.Fatalf("GC must sweep segments with their lineage, %d objects remain: %v", len(after), after)
	}
}
