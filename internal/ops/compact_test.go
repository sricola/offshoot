package ops

import (
	"errors"
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
