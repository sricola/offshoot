package ops

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// batchDeleteBackend wraps a real Backend with a recording BatchDeleter:
// every DeleteObjects call is captured (so a test can assert HOW the GC
// sweep deleted — one batched call, not N per-key Deletes), per-key Delete
// calls are counted, and failKeys injects the partial-failure shape the
// interface contract specifies (failed keys excluded from deleted, named in
// err). Deletion itself delegates to the wrapped backend so results stay
// real.
type batchDeleteBackend struct {
	store.Backend
	mu         sync.Mutex
	batchCalls [][]string
	deletes    []string
	failKeys   map[string]bool
}

func (b *batchDeleteBackend) Delete(key string) error {
	b.mu.Lock()
	b.deletes = append(b.deletes, key)
	b.mu.Unlock()
	return b.Backend.Delete(key)
}

func (b *batchDeleteBackend) DeleteObjects(keys []string) (deleted []string, err error) {
	b.mu.Lock()
	b.batchCalls = append(b.batchCalls, append([]string(nil), keys...))
	failKeys := b.failKeys
	b.mu.Unlock()
	var failed []string
	for _, k := range keys {
		if failKeys[k] {
			failed = append(failed, k)
			continue
		}
		if derr := b.Backend.Delete(k); derr != nil {
			return deleted, derr
		}
		deleted = append(deleted, k)
	}
	if len(failed) > 0 {
		return deleted, fmt.Errorf("batch delete failed for %s", strings.Join(failed, ", "))
	}
	return deleted, nil
}

// perKeyBackend hides the wrapped backend's BatchDeleter capability (an
// embedded interface's method set is only store.Backend's), counting Delete
// calls — the "plain Backend" a sweep must still handle via its per-key
// fallback.
type perKeyBackend struct {
	store.Backend
	mu      sync.Mutex
	deletes []string
}

func (b *perKeyBackend) Delete(key string) error {
	b.mu.Lock()
	b.deletes = append(b.deletes, key)
	b.mu.Unlock()
	return b.Backend.Delete(key)
}

func strayKey(i int) string {
	return fmt.Sprintf("data/stray%d/1/snapshot-0000000000000001.ltx", i)
}

// A sweeping GC pass on a BatchDeleter backend issues exactly ONE
// DeleteObjects call carrying exactly the eligible key set — not one Delete
// RPC per object (perf audit H2; per-request 1000-key chunking is the
// backend's job, pinned in store's TestS3DeleteObjectsChunksBatches). A
// tombstoned key whose object is already gone is pruned, never handed to
// the delete call.
func TestGCSweepBatchesEligibleKeys(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	var strays []string
	for i := 0; i < 5; i++ {
		strays = append(strays, strayKey(i))
	}
	goner := "data/goner/1/snapshot-0000000000000001.ltx"
	for _, k := range append([]string{goner}, strays...) {
		if err := w.Store.B.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if tombstoned, _, err := w.GC(0); err != nil || tombstoned != 6 {
		t.Fatalf("first GC pass: tombstoned=%d err=%v, want 6", tombstoned, err)
	}
	// goner's object vanishes between passes: its stone must be pruned via
	// the re-list, not swept — so it must NOT appear in the batch call.
	if err := w.Store.B.Delete(goner); err != nil {
		t.Fatal(err)
	}

	bb := &batchDeleteBackend{Backend: w.Store.B}
	w.Store.B = bb
	_, deleted, err := w.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != len(strays) {
		t.Fatalf("deleted = %d, want %d", deleted, len(strays))
	}
	if len(bb.batchCalls) != 1 {
		t.Fatalf("sweep issued %d DeleteObjects calls, want exactly 1", len(bb.batchCalls))
	}
	want := append([]string(nil), strays...)
	sort.Strings(want)
	if !reflect.DeepEqual(bb.batchCalls[0], want) {
		t.Fatalf("batched key set = %v, want exactly the eligible set %v", bb.batchCalls[0], want)
	}
	if len(bb.deletes) != 0 {
		t.Fatalf("sweep issued %d per-key Deletes alongside the batch, want 0: %v", len(bb.deletes), bb.deletes)
	}
	for _, k := range strays {
		if _, _, err := w.Store.B.Get(k); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("stray %s must be swept, Get err = %v", k, err)
		}
	}
	if stones, _, err := w.loadTombstones(); err != nil || len(stones) != 0 {
		t.Fatalf("stones after sweep = %v err=%v, want none", stones, err)
	}
}

// A backend WITHOUT the BatchDeleter capability still sweeps correctly: the
// sweep falls back to one Delete per eligible key with identical results.
func TestGCSweepFallsBackToPerKeyDelete(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	var strays []string
	for i := 0; i < 3; i++ {
		strays = append(strays, strayKey(i))
		if err := w.Store.B.Put(strays[i], []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if tombstoned, _, err := w.GC(0); err != nil || tombstoned != 3 {
		t.Fatalf("first GC pass: tombstoned=%d err=%v, want 3", tombstoned, err)
	}

	pb := &perKeyBackend{Backend: w.Store.B}
	w.Store.B = pb
	if _, ok := store.Backend(pb).(store.BatchDeleter); ok {
		t.Fatal("perKeyBackend must NOT expose BatchDeleter — the test exists to prove the fallback")
	}
	_, deleted, err := w.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != len(strays) {
		t.Fatalf("deleted = %d, want %d", deleted, len(strays))
	}
	got := append([]string(nil), pb.deletes...)
	sort.Strings(got)
	if !reflect.DeepEqual(got, strays) {
		t.Fatalf("per-key fallback deleted %v, want exactly %v", got, strays)
	}
	if stones, _, err := w.loadTombstones(); err != nil || len(stones) != 0 {
		t.Fatalf("stones after sweep = %v err=%v, want none", stones, err)
	}
}

// A partial batch failure prunes ONLY the succeeded keys' tombstones —
// persisted before the pass aborts with the error — while every failed key
// keeps its stone (original timestamp) and its object, so a later pass
// finishes the job.
func TestGCSweepPartialFailureKeepsFailedStones(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	var strays []string
	for i := 0; i < 3; i++ {
		strays = append(strays, strayKey(i))
		if err := w.Store.B.Put(strays[i], []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if tombstoned, _, err := w.GC(0); err != nil || tombstoned != 3 {
		t.Fatalf("first GC pass: tombstoned=%d err=%v, want 3", tombstoned, err)
	}

	stuck := strays[1]
	bb := &batchDeleteBackend{Backend: w.Store.B, failKeys: map[string]bool{stuck: true}}
	w.Store.B = bb
	_, deleted, err := w.GC(0)
	if err == nil || !strings.Contains(err.Error(), stuck) {
		t.Fatalf("partial failure must abort the pass with an error naming %s, got: %v", stuck, err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2 (the succeeded keys only)", deleted)
	}
	if _, _, err := w.Store.B.Get(stuck); err != nil {
		t.Fatalf("failed key's object must survive: %v", err)
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		t.Fatal(err)
	}
	if len(stones) != 1 {
		t.Fatalf("stones after partial failure = %v, want exactly the failed key's", stones)
	}
	if _, ok := stones[stuck]; !ok {
		t.Fatalf("surviving stone = %v, want %s", stones, stuck)
	}

	// The failure clears; the next pass sweeps the leftover.
	bb.mu.Lock()
	bb.failKeys = nil
	bb.mu.Unlock()
	if _, deleted, err := w.GC(0); err != nil || deleted != 1 {
		t.Fatalf("follow-up GC: deleted=%d err=%v, want 1 deleted", deleted, err)
	}
	if _, _, err := w.Store.B.Get(stuck); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("leftover must be swept on the follow-up pass, Get err = %v", err)
	}
	if stones, _, err := w.loadTombstones(); err != nil || len(stones) != 0 {
		t.Fatalf("stones after follow-up = %v err=%v, want none", stones, err)
	}
}
