package ops

import (
	"os/exec"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

func TestDestroyAndGC(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")
	w.Fork("app", "main", "attempt-1", "")
	aref, _, _ := w.Store.GetRef("app", "attempt-1")

	// Protected destroy requires force.
	if err := w.Destroy("app", "main", false); err == nil {
		t.Fatal("destroying protected main without force must fail")
	}
	if err := w.Destroy("app", "attempt-1", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-1"); err == nil {
		t.Fatal("ref must be gone")
	}

	// Phase 1: tombstone the now-unreachable lineage; nothing deleted yet.
	tombstoned, deleted, err := w.GC(time.Hour)
	if err != nil || tombstoned != 1 || deleted != 0 {
		t.Fatalf("gc1: %d %d %v", tombstoned, deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) == 0 {
		t.Fatal("phase 1 must not delete data")
	}
	// Phase 2 with zero grace: swept.
	if _, deleted, err = w.GC(0); err != nil || deleted != 1 {
		t.Fatalf("gc2: deleted=%d err=%v", deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) != 0 {
		t.Fatalf("lineage not swept: %v", keys)
	}
	// Live lineages untouched.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatalf("live branch damaged by GC: %v", err)
	}
}

func TestGCSparesRereferencedLineage(t *testing.T) {
	w := newWS(t)
	w.Create("app")
	// Tombstone main's lineage artificially, then verify phase 2 spares it
	// because it is still referenced.
	ref, _, _ := w.Store.GetRef("app", "main")
	if err := w.tombstone(map[string]string{ref.Lineage: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, deleted, err := w.GC(0); err != nil || deleted != 0 {
		t.Fatalf("gc must spare a referenced lineage: deleted=%d err=%v", deleted, err)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}
}
