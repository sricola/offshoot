package ops

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// errMarkInjected is the backend failure the tests below inject into GC's
// mark phase.
var errMarkInjected = errors.New("injected: lineage listing failed")

// markFailBackend wraps a real Backend so that every List of a LINEAGE
// prefix (data/<lineage>/, what store.Chain issues while GC marks) fails
// with errMarkInjected. List("refs/"), GC's own List("data/") and every
// other call pass through untouched, so the only thing broken is the mark.
// With afterDataList set, lineage lists succeed until GC's own
// List("data/") has been seen once — i.e. phase 1's mark completes — and
// fail from then on, so only phase 2's re-mark breaks.
type markFailBackend struct {
	store.Backend
	mu            sync.Mutex
	calls         int
	afterDataList bool
	armed         bool
}

func (b *markFailBackend) List(prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if prefix == "data/" && b.afterDataList {
		b.armed = true
	}
	if strings.HasPrefix(prefix, "data/") && len(prefix) > len("data/") && (!b.afterDataList || b.armed) {
		b.calls++
		return nil, errMarkInjected
	}
	return b.Backend.List(prefix)
}

// seedLiveFork builds a store with real data on app@main plus a fork of it,
// and proves (with one clean GC pass) that every object is reachable.
func seedLiveFork(t *testing.T, w *Workspace) {
	t.Helper()
	testutil.RequireSQLite3(t)
	seedDB(t, w, "app", 20000)
	mustFork(t, w, "app", "main", "child", "")
	if tombstoned, deleted, err := w.GC(0); err != nil || tombstoned != 0 || deleted != 0 {
		t.Fatalf("clean GC on a live store: tombstoned=%d deleted=%d err=%v, want 0 0 nil", tombstoned, deleted, err)
	}
	if keys := dataKeys(t, w); len(keys) < 2 {
		t.Fatalf("precondition: want at least a snapshot and a fork's objects under data/, got %v", keys)
	}
}

// TestGCMarkErrorTombstonesNothing pins docs/limitations.md#Garbage
// collection "an incomplete mark deletes nothing" (gc.go's reachableObjects:
// "Errors fail the whole mark (and with it the GC pass) rather than being
// skipped"): a List failure while resolving a live lineage's chain makes
// GC(0) return that error naming the ref it was marking, write NO
// tombstone (the gc/tombstones object does not even exist afterwards) and
// delete NO object.
//
// Mutation verified against: reachableObjectsAndHeads `continue`-ing past a
// Chain error instead of returning it (gc.go) — the under-approximated mark
// then tombstones every live object and TombstoneBacklog is non-zero.
func TestGCMarkErrorTombstonesNothing(t *testing.T) {
	w := newWS(t)
	seedLiveFork(t, w)
	before := dataKeys(t, w)

	real := w.Store.B
	fb := &markFailBackend{Backend: real}
	w.Store.B = fb
	tombstoned, deleted, err := w.GC(0)
	w.Store.B = real

	if !errors.Is(err, errMarkInjected) {
		t.Fatalf("GC with a failing mark: err = %v, want the injected List error", err)
	}
	if !strings.Contains(err.Error(), "ops: gc mark app@") {
		t.Fatalf("GC error must name the ref whose mark failed, got: %v", err)
	}
	if fb.calls == 0 {
		t.Fatal("the injected failure was never hit: the mark did not List a lineage prefix")
	}
	if tombstoned != 0 || deleted != 0 {
		t.Fatalf("GC with a failing mark reported tombstoned=%d deleted=%d, want 0 0", tombstoned, deleted)
	}
	if n, err := w.TombstoneBacklog(); err != nil || n != 0 {
		t.Fatalf("TombstoneBacklog after a failed mark = %d (err %v), want 0", n, err)
	}
	if _, _, err := w.Store.B.Get(tombstoneKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a failed mark must not create %s: Get err = %v, want ErrNotFound", tombstoneKey, err)
	}
	if after := dataKeys(t, w); !reflect.DeepEqual(after, before) {
		t.Fatalf("a failed mark changed data/: before %v, after %v", before, after)
	}
	if _, err := w.Checkout("app", "child"); err != nil {
		t.Fatalf("live fork damaged by a failed GC pass: %v", err)
	}
}

// TestGCReMarkErrorSweepsNothing is the phase-2 half of the same sentence:
// with a grace-expired stone already on file (a stray object tombstoned by
// an earlier clean pass), a List failure during the sweep's re-mark aborts
// the pass before any delete — the stray survives, its stone is kept (not
// pruned, not swept) and the live lineages are untouched.
//
// Mutation verified against: reachableObjectsAndHeads `continue`-ing past a
// Chain error — the stray is then swept (data/ changes) and the live
// objects are tombstoned.
func TestGCReMarkErrorSweepsNothing(t *testing.T) {
	w := newWS(t)
	seedLiveFork(t, w)
	stray := "data/stray/1/snapshot-0000000000000001.ltx"
	if err := w.Store.B.Put(stray, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if tombstoned, deleted, err := w.GC(0); err != nil || tombstoned != 1 || deleted != 0 {
		t.Fatalf("pass 1: tombstoned=%d deleted=%d err=%v, want 1 0 nil", tombstoned, deleted, err)
	}
	if n, err := w.TombstoneBacklog(); err != nil || n != 1 {
		t.Fatalf("backlog after pass 1 = %d (err %v), want 1", n, err)
	}
	before := dataKeys(t, w)

	real := w.Store.B
	fb := &markFailBackend{Backend: real, afterDataList: true}
	w.Store.B = fb
	tombstoned, deleted, err := w.GC(0)
	w.Store.B = real

	if !errors.Is(err, errMarkInjected) {
		t.Fatalf("GC with a failing re-mark: err = %v, want the injected List error", err)
	}
	if fb.calls == 0 {
		t.Fatal("the injected failure was never hit")
	}
	if !fb.armed {
		t.Fatal("precondition: the failure armed before phase 1 finished, so the re-mark was never reached")
	}
	if tombstoned != 0 || deleted != 0 {
		t.Fatalf("GC with a failing re-mark reported tombstoned=%d deleted=%d, want 0 0", tombstoned, deleted)
	}
	if n, err := w.TombstoneBacklog(); err != nil || n != 1 {
		t.Fatalf("backlog after the failed pass = %d (err %v), want the one pre-existing stone kept", n, err)
	}
	if after := dataKeys(t, w); !reflect.DeepEqual(after, before) {
		t.Fatalf("a failed re-mark changed data/: before %v, after %v", before, after)
	}
	if ok, err := w.Store.ObjectExists(stray); err != nil || !ok {
		t.Fatalf("the grace-expired stray must survive a failed re-mark: exists=%v err=%v", ok, err)
	}
}
