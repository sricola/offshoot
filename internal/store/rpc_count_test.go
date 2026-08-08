package store

import (
	"reflect"
	"sync"
	"testing"
)

// listCountBackend wraps a real Backend and counts List calls per prefix,
// leaving every operation's behavior untouched. It exists to pin the store's
// List RPC counts: on a remote backend every List is a round trip, so a
// regression from one List to two per prefix is a real cost, not noise.
type listCountBackend struct {
	Backend
	mu     sync.Mutex
	counts map[string]int
}

func newListCountBackend(b Backend) *listCountBackend {
	return &listCountBackend{Backend: b, counts: map[string]int{}}
}

func (b *listCountBackend) List(prefix string) ([]string, error) {
	b.mu.Lock()
	b.counts[prefix]++
	b.mu.Unlock()
	return b.Backend.List(prefix)
}

func (b *listCountBackend) count(prefix string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.counts[prefix]
}

// A seam-path resolution — a shared child that has diverged past its fork
// point (own segments) but has NOT written its own divergence-floor snapshot,
// the normal working state of an active shared fork — must List the child's
// prefix exactly ONCE. Before the listMembers split, chainFrom Listed it
// twice: once in chainSelf (whose no-own-snapshot sentinel routes to the seam
// path) and again in the seam-range walk over the very same prefix.
func TestChainSeamPathListsChildPrefixOnce(t *testing.T) {
	s := newStore(t)
	putObj(t, s, SnapshotKey("parent", 1, 1))
	putObj(t, s, SegmentKey("parent", 1, 2, 2))
	putObj(t, s, SegmentKey("parent", 1, 3, 3))
	if err := s.WriteLineageBase("child", BasePointer{Lineage: "parent", TXID: 3}); err != nil {
		t.Fatal(err)
	}
	putObj(t, s, SegmentKey("child", 1, 4, 4))
	putObj(t, s, SegmentKey("child", 1, 5, 5))

	cb := newListCountBackend(s.B)
	s.B = cb

	got, err := s.Chain("child", 5)
	if err != nil {
		t.Fatalf("Chain err = %v", err)
	}
	want := []string{
		SnapshotKey("parent", 1, 1),
		SegmentKey("parent", 1, 2, 2),
		SegmentKey("parent", 1, 3, 3),
		SegmentKey("child", 1, 4, 4),
		SegmentKey("child", 1, 5, 5),
	}
	if !reflect.DeepEqual(keysOf(got), want) {
		t.Fatalf("chain keys =\n %v\nwant\n %v", keysOf(got), want)
	}
	if n := cb.count(LineagePrefix("child")); n != 1 {
		t.Fatalf("child prefix Listed %d times, want exactly 1 (seam path must reuse the parsed members)", n)
	}
	if n := cb.count(LineagePrefix("parent")); n != 1 {
		t.Fatalf("parent prefix Listed %d times, want exactly 1", n)
	}
}
