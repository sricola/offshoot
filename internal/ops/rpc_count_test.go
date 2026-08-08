package ops

import (
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
