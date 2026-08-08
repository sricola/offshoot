package ops

import (
	"errors"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// dataKeys lists every object key under data/ (all lineages), sorted, so a
// test can assert an operation left the object store's data plane unchanged.
func dataKeys(t *testing.T, w *Workspace) []string {
	t.Helper()
	keys, err := w.Store.B.List("data/")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	return keys
}

// TestCompactNonSharedBranchIsNoOp pins Compact's Base==nil decision: an
// already self-contained branch returns its head txid with NO error, no ref
// mutation (no "compact" checkpoint, same lineage), and no new lineage
// minted in the object store — least-surprising for a scripted "compact
// everything" loop.
func TestCompactNonSharedBranchIsNoOp(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Base != nil {
		t.Fatal("test precondition: a fresh main must be self-contained (Base nil)")
	}
	before := dataKeys(t, w)

	txid, err := w.Compact("app", "main")
	if err != nil {
		t.Fatalf("compact of a self-contained branch must be a no-op, got %v", err)
	}
	if txid != ref.HeadTXID {
		t.Fatalf("no-op compact returned txid %d, want head %d", txid, ref.HeadTXID)
	}

	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Base != nil {
		t.Fatal("no-op compact must leave Base nil")
	}
	if after.Lineage != ref.Lineage {
		t.Fatalf("no-op compact must not repoint the lineage: %s -> %s", ref.Lineage, after.Lineage)
	}
	if _, ok := after.Checkpoints["compact"]; ok {
		t.Fatal("no-op compact must not add a \"compact\" checkpoint")
	}
	if got := dataKeys(t, w); !reflect.DeepEqual(got, before) {
		t.Fatalf("no-op compact must mint no objects: before %v, after %v", before, got)
	}
}

// TestCompactCASRaceCleansOrphan drives the race the design refuses to
// retry internally: a concurrent flush advances the ref (etag moves)
// between Compact's GetRef and its PutRef. Compact must lose the CAS,
// delete the orphan snapshot it minted in the would-be new lineage, and
// return the retry error — leaving the branch exactly as the winner left
// it. The hook lands the same ref write session flush performs (GetRef,
// advance HeadTXID, PutRef) rather than a full session, which package ops
// cannot import (session imports ops).
func TestCompactCASRaceCleansOrphan(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	childRef, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if childRef.Base == nil {
		t.Fatal("test precondition: a shallow fork must share (Base set)")
	}
	before := dataKeys(t, w)

	compactBeforeCASForTest = func() {
		ref, etag, err := w.Store.GetRef("app", "child")
		if err != nil {
			t.Fatal(err)
		}
		ref.Touch(time.Now())
		if _, err := w.Store.PutRef("app", "child", ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { compactBeforeCASForTest = nil })

	_, err = w.Compact("app", "child")
	if err == nil {
		t.Fatal("compact must lose the CAS when a concurrent ref write lands first")
	}
	if !errors.Is(err, store.ErrCAS) {
		t.Fatalf("want the CAS loss surfaced (store.ErrCAS in the chain), got %v", err)
	}
	if !strings.Contains(err.Error(), "compact lost a race (retry)") {
		t.Fatalf("want the retry-worded error, got %v", err)
	}

	// The orphan snapshot in the would-be new lineage must be gone: the
	// concurrent write touched no data objects, so the data plane must be
	// byte-for-byte the pre-compact set.
	if got := dataKeys(t, w); !reflect.DeepEqual(got, before) {
		t.Fatalf("CAS-losing compact must clean up its orphan snapshot: before %v, after %v", before, got)
	}

	// And the branch is untouched: still shared, still on its old lineage.
	after, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if after.Base == nil || after.Lineage != childRef.Lineage {
		t.Fatalf("CAS-losing compact must not repoint the ref: Base %+v, lineage %s (want %s)",
			after.Base, after.Lineage, childRef.Lineage)
	}
}

// TestPromoteAndRollbackClearStaleBaseMirror pins the Ref.Base invariant
// both repointing ops must uphold: Ref.Base != nil iff
// base.json(Ref.Lineage) exists. Promote and Rollback both repoint a
// branch at a FRESH self-contained lineage (copySnapshotToNewLineage
// writes no base.json), so a formerly-shared branch's Base mirror must
// come out nil — a stale non-nil mirror misreports the branch as shared
// (status, and any mirror-reading op) on a lineage that shares nothing.
func TestPromoteAndRollbackClearStaleBaseMirror(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	// Promote arm: the TARGET is a shared fork.
	if _, err := w.Fork("app", "main", "tgt", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "tgt")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Base == nil {
		t.Fatal("test precondition: a shallow fork must share (Base set)")
	}
	if _, err := w.Promote("app", "main", "tgt", false); err != nil {
		t.Fatal(err)
	}
	got, _, err := w.Store.GetRef("app", "tgt")
	if err != nil {
		t.Fatal(err)
	}
	if got.Base != nil {
		t.Fatalf("promote onto a formerly-shared branch must clear Base, got %+v", got.Base)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(got.Lineage)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("promoted lineage must have no base.json, Get err = %v", err)
	}

	// Rollback arm: a shared fork rolled back to its "fork" checkpoint.
	if _, err := w.Fork("app", "main", "rb", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, _, err = w.Store.GetRef("app", "rb")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Base == nil {
		t.Fatal("test precondition: a shallow fork must share (Base set)")
	}
	if _, err := w.Rollback("app", "rb", "fork"); err != nil {
		t.Fatal(err)
	}
	got, _, err = w.Store.GetRef("app", "rb")
	if err != nil {
		t.Fatal(err)
	}
	if got.Base != nil {
		t.Fatalf("rollback of a shared fork must clear Base, got %+v", got.Base)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(got.Lineage)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rolled-back lineage must have no base.json, Get err = %v", err)
	}
}

// TestCompactNoOpAfterRollbackPreservesCheckpoints is THE regression the
// stale-mirror bug enabled: shared-fork a child, give it named
// checkpoints, Rollback it (which repoints to a self-contained lineage
// and carefully COPIES every kept checkpoint's snapshot across), then
// Compact. Before the fix, Rollback left a stale non-nil Base mirror,
// Compact trusted the mirror, and a "no-op" compact of an
// already-self-contained branch needlessly re-materialized AND wiped the
// rollback-preserved checkpoints down to {"compact"} — silent data loss.
// Now Compact consults the DURABLE base spine: empty spine → true no-op,
// checkpoints untouched. The second half re-injects a stale mirror by
// hand and compacts again, pinning that the durable spine (not the
// mirror) is authoritative for this destructive op even if some future
// code path leaves a stale mirror behind.
func TestCompactNoOpAfterRollbackPreservesCheckpoints(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	forkRef, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if forkRef.Base == nil {
		t.Fatal("test precondition: fork must share (Base set)")
	}
	path, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("seed: %v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "child", "cp1", nil); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "child", "cp2", nil); err != nil {
		t.Fatal(err)
	}

	// Rollback to cp1: repoints to a self-contained lineage and preserves
	// the kept checkpoints ("fork", "cp1") by copying their snapshots in.
	if _, err := w.Rollback("app", "child", "cp1"); err != nil {
		t.Fatal(err)
	}
	rbRef, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"fork", "cp1"} {
		if _, ok := rbRef.Checkpoints[name]; !ok {
			t.Fatalf("test precondition: rollback must keep checkpoint %q, got %v", name, rbRef.Checkpoints)
		}
	}

	// Compact must be a NO-OP (empty durable spine), not a checkpoint wipe.
	txid, err := w.Compact("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if txid != rbRef.HeadTXID {
		t.Fatalf("no-op compact returned txid %d, want head %d", txid, rbRef.HeadTXID)
	}
	got, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if got.Lineage != rbRef.Lineage {
		t.Fatalf("no-op compact must not repoint: %s -> %s", rbRef.Lineage, got.Lineage)
	}
	if _, ok := got.Checkpoints["compact"]; ok {
		t.Fatalf("no-op compact must not add a \"compact\" checkpoint, got %v", got.Checkpoints)
	}
	for _, name := range []string{"fork", "cp1"} {
		if _, ok := got.Checkpoints[name]; !ok {
			t.Fatalf("compact DESTROYED rollback-preserved checkpoint %q: %v", name, got.Checkpoints)
		}
	}

	// Harden-check: even a stale non-nil MIRROR (injected here as a stand-in
	// for any future code path that forgets to clear it) must not trick
	// compact — the durable spine is the authority.
	mainRef, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	stale, etag, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	stale.Base = &store.BasePointer{Lineage: mainRef.Lineage, TXID: 1}
	if _, err := w.Store.PutRef("app", "child", stale, etag); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Compact("app", "child"); err != nil {
		t.Fatal(err)
	}
	got, _, err = w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	if got.Lineage != rbRef.Lineage {
		t.Fatal("a stale Base mirror must not trigger a real compact (durable spine is empty)")
	}
	for _, name := range []string{"fork", "cp1"} {
		if _, ok := got.Checkpoints[name]; !ok {
			t.Fatalf("stale-mirror compact DESTROYED checkpoint %q: %v", name, got.Checkpoints)
		}
	}
}
