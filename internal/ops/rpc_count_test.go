package ops

import (
	"errors"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// rpcCountBackend wraps a real Backend and counts List calls per prefix and
// Get calls per key, leaving every operation's behavior untouched. It exists
// to pin ops' store-RPC counts: on a remote backend every List/Get is a
// round trip, so a redundant re-resolution or re-enumeration is a real cost
// regression, not noise.
type rpcCountBackend struct {
	store.Backend
	mu    sync.Mutex
	lists map[string]int
	gets  map[string]int
}

func newRPCCountBackend(b store.Backend) *rpcCountBackend {
	return &rpcCountBackend{Backend: b, lists: map[string]int{}, gets: map[string]int{}}
}

func (b *rpcCountBackend) List(prefix string) ([]string, error) {
	b.mu.Lock()
	b.lists[prefix]++
	b.mu.Unlock()
	return b.Backend.List(prefix)
}

func (b *rpcCountBackend) Get(key string) ([]byte, string, error) {
	b.mu.Lock()
	b.gets[key]++
	b.mu.Unlock()
	return b.Backend.Get(key)
}

func (b *rpcCountBackend) listCount(prefix string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lists[prefix]
}

func (b *rpcCountBackend) getCount(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets[key]
}

// A Fork that takes the MATERIALIZE branch must resolve the source chain
// exactly ONCE: the floor decision's resolution is passed through to the
// copy (copySnapshotToNewLineageFromChain), never re-resolved. Before the
// M1 fix, copySnapshotToNewLineage re-resolved the identical (lineage,
// txid), Listing the source prefix a second time — most expensive exactly
// when the floor trips, i.e. when the chain is long. Pinned here as: during
// Fork, the SOURCE lineage's prefix is Listed exactly once. (The one other
// List a materializing fork issues is of the CHILD's fresh prefix, by
// tryFastForkCopy's verification resolve — a different prefix, counted
// separately.)
func TestForkMaterializeResolvesSourceChainOnce(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	src, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Force the MATERIALIZE branch (the floor's branch) regardless of chain
	// depth, without suppressing the in-branch fast copy.
	forkMaterializeForTest = true
	t.Cleanup(func() { forkMaterializeForTest = false })

	cb := newRPCCountBackend(w.Store.B)
	w.Store.B = cb
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if n := cb.listCount(store.LineagePrefix(src.Lineage)); n != 1 {
		t.Fatalf("materializing Fork Listed the source prefix %d times, want exactly 1 (floor decision's resolution must be reused)", n)
	}
}

// A sweeping GC pass enumerates refs exactly TWICE — phase 1's mark and
// phase 2's re-mark — never a third time: the compensating rule's live-head
// set is derived from the re-mark's own ref fetch
// (reachableObjectsAndHeads), not a separate ListRefs + per-ref GetRefs as
// the former standalone liveHeadLineages helper did (perf audit M4).
func TestGCSweepEnumeratesRefsTwiceNotThrice(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// A stray, unreachable object: tombstoned by the first pass, swept by
	// the second (a stone minted in the same run is never swept — sweeps
	// always wait for a later, independent run).
	stray := "data/straylineage/1/snapshot-0000000000000001.ltx"
	if err := w.Store.B.Put(stray, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if tombstoned, _, err := w.GC(0); err != nil || tombstoned != 1 {
		t.Fatalf("first GC pass: tombstoned=%d err=%v, want 1 tombstoned", tombstoned, err)
	}

	cb := newRPCCountBackend(w.Store.B)
	w.Store.B = cb
	_, deleted, err := w.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("second GC pass deleted %d objects, want 1 (the sweep must actually run)", deleted)
	}
	if n := cb.listCount("refs/"); n != 2 {
		t.Fatalf("sweeping GC pass Listed refs/ %d times, want exactly 2 (mark + re-mark; live heads must reuse the re-mark's refs)", n)
	}
	if _, _, err := w.Store.B.Get(stray); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stray object must be swept, Get err = %v", err)
	}
}

// Repeated shared forks pay the EnsureLayoutV2 manifest Get ONCE per
// process, not once per fork (perf audit L4): the layout version is
// monotonic, so store.Store memoizes the ">= v2" observation after the
// first check. Two shared forks: one manifest Get total.
func TestSharedForksGetManifestOnce(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	cb := newRPCCountBackend(w.Store.B)
	w.Store.B = cb
	for _, br := range []string{"fork1", "fork2"} {
		if _, err := w.Fork("app", "main", br, "", 0, nil); err != nil {
			t.Fatal(err)
		}
		ref, _, err := w.Store.GetRef("app", br)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Base == nil {
			t.Fatalf("fork %s of a 1-member chain must share (Base set)", br)
		}
	}
	// "offshoot.json" is store's manifest key (store.InitManifest/
	// EnsureLayoutV2) — pinned here by name since the constant is unexported.
	if n := cb.getCount("offshoot.json"); n != 1 {
		t.Fatalf("two shared forks Got the manifest %d times, want exactly 1 (>= v2 must be memoized)", n)
	}
}
