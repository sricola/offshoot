// Compact (copy-on-write Task 6) end-to-end tests. External package
// (ops_test) because building a real shared fork with divergence requires
// session's flush cadence, and session imports ops — see gc_chain_test.go's
// package doc comment for the cycle rationale.
package ops_test

import (
	"context"
	"errors"
	"os/exec"
	"testing"

	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
)

// TestCompactMakesBranchSelfContained: after compact, the branch owns ONE
// snapshot in a brand-new lineage, its ref's Base is nil, the new lineage
// has NO base.json, and the checkpoint map was reset to {"compact"}.
func TestCompactMakesBranchSelfContained(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("CREATE TABLE: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("INSERT: %v: %s", err, out)
	}
	// Segment past the settling snapshot: main's chain is multi-member.
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Base == nil {
		t.Fatal("test precondition: fork must share (Base set)")
	}
	oldLineage := ref.Lineage

	// Diverge the child so compact folds base chain + own segment.
	cs, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "child", SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", cs.CheckoutPath(), "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("INSERT: %v: %s", err, out)
	}
	if _, err := cs.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}

	txid, err := w.Compact("app", "child")
	if err != nil {
		t.Fatal(err)
	}

	got, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if got.Base != nil {
		t.Fatalf("compact must clear Base, got %+v", got.Base)
	}
	if got.Lineage == oldLineage {
		t.Fatal("compact must repoint at a NEW lineage")
	}
	if got.HeadTXID != txid || got.Epoch != 1 || got.HeadEpoch != 1 {
		t.Fatalf("compact ref head = (txid %d, epoch %d, headEpoch %d), want (%d, 1, 1)",
			got.HeadTXID, got.Epoch, got.HeadEpoch, txid)
	}
	if len(got.Checkpoints) != 1 {
		t.Fatalf("compact must reset checkpoints to exactly {\"compact\"}, got %v", got.Checkpoints)
	}
	if cp, ok := got.Checkpoints["compact"]; !ok || cp.TXID != txid {
		t.Fatalf("want a \"compact\" checkpoint at txid %d, got %v", txid, got.Checkpoints)
	}
	// No base.json in the new lineage: the branch is self-contained.
	if _, _, err := w.Store.B.Get(store.BaseKey(got.Lineage)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("compact must write no base.json, Get err = %v", err)
	}
	// The branch's chain is exactly its OWN snapshot in its OWN lineage.
	chain, err := w.Store.Chain(got.Lineage, got.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 1 || !chain[0].Snapshot {
		t.Fatalf("compacted chain = %d members (snapshot=%v), want exactly one snapshot",
			len(chain), len(chain) > 0 && chain[0].Snapshot)
	}
	if chain[0].Key != store.SnapshotKey(got.Lineage, 1, txid) {
		t.Fatalf("compacted snapshot key = %s, want %s", chain[0].Key, store.SnapshotKey(got.Lineage, 1, txid))
	}
}

// TestCompactAllowsAncestorReclaim is the point of compact: shared-fork a
// db, compact the child, destroy the PARENT branch, GC — the parent
// lineage's objects (previously pinned by the child's base pointer) must
// be gone, and the compacted child must have been independent of them
// (materialized and read BEFORE the reclaim). The child's abandoned old
// lineage (its base.json) is reclaimed too.
func TestCompactAllowsAncestorReclaim(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("seed: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	mainRef, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	parentLineage := mainRef.Lineage

	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	childRef, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if childRef.Base == nil {
		t.Fatal("test precondition: fork must share (Base set)")
	}
	oldChildLineage := childRef.Lineage

	if _, err := w.Compact("app", "child"); err != nil {
		t.Fatal(err)
	}
	// Materialize the compacted child FIRST: proof it no longer needs the
	// parent before the parent's storage goes away.
	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := exec.Command("sqlite3", cpath, "SELECT v FROM t;").Output(); err != nil || string(got) != "1\n" {
		t.Fatalf("compacted child content = %q (err %v), want \"1\\n\"", got, err)
	}

	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	// Two passes: tombstone, then (grace 0) delete.
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}

	remaining, err := w.Store.B.List(store.LineagePrefix(parentLineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("parent lineage must be reclaimed after compact+destroy+GC, %d objects remain: %v",
			len(remaining), remaining)
	}
	oldChild, err := w.Store.B.List(store.LineagePrefix(oldChildLineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(oldChild) != 0 {
		t.Fatalf("the child's abandoned shared lineage must be reclaimed too, %d objects remain: %v",
			len(oldChild), oldChild)
	}
	// And the compacted child survived the sweep intact.
	got, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.Chain(got.Lineage, got.HeadTXID); err != nil {
		t.Fatalf("compacted child's chain must still resolve after GC: %v", err)
	}
}

// TestCompactPreservesContent: the compacted branch materializes to a
// byte-identical `.dump` as before compact — folding the base chain into
// one snapshot changes storage, never content.
func TestCompactPreservesContent(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (a, b); INSERT INTO t VALUES (1, 'one');").CombinedOutput(); err != nil {
		t.Fatalf("seed: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES (2, 'two');").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	// Diverge the child past the fork point so the compacted state is the
	// child's own head, not just the parent's.
	cs, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "child", SnapshotEvery: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", cs.CheckoutPath(), "INSERT INTO t VALUES (3, 'three');").CombinedOutput(); err != nil {
		t.Fatalf("diverge: %v: %s", err, out)
	}
	if _, err := cs.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}

	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	dumpBefore, err := exec.Command("sqlite3", cpath, ".dump").Output()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.Compact("app", "child"); err != nil {
		t.Fatal(err)
	}
	// Compact refreshed the existing checkout itself (its best-effort
	// tail); the dump at the same path must be byte-identical.
	dumpAfter, err := exec.Command("sqlite3", cpath, ".dump").Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(dumpBefore) != string(dumpAfter) {
		t.Fatalf("compact changed content:\n--- before ---\n%s\n--- after ---\n%s", dumpBefore, dumpAfter)
	}
}
