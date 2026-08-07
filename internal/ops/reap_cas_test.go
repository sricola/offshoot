package ops

import (
	"errors"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// These tests pin the two halves of Reap's CAS-claim invariant directly,
// rather than relying on TestConcurrentTouchAndReapHaveExactlyOneWinner to
// hit them by chance: instrumentation showed that test's reaper-wins arm
// never fires in practice (0/200 over repeated runs, launch order flipped,
// GOMAXPROCS=1) because Reap's ListRefs preamble reliably outlasts Touch's
// single GetRef+PutRef — so touch.go:26-28 (refuse a claimed branch) and the
// ErrCAS-loses branch in reapOne's claim write were previously exercised by
// no deterministic test at all.

// TestTouchRefusesWhenReapingClaimed pins touch.go's "too late to touch"
// guard: once a ref is marked Reaping (Reap's CAS claim, landed), Touch must
// refuse rather than silently resurrect a branch mid-destroy.
func TestTouchRefusesWhenReapingClaimed(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "claimed", "", 0, nil); err != nil {
		t.Fatal(err)
	}

	// Land a reap claim directly, exactly as reapOne's own CAS write would.
	ref, etag, err := w.Store.GetRef("app", "claimed")
	if err != nil {
		t.Fatal(err)
	}
	ref.Reaping = true
	if _, err := w.Store.PutRef("app", "claimed", ref, etag); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Touch("app", "claimed", nil, time.Now()); err == nil {
		t.Fatal("Touch must refuse a branch already claimed for reaping (Reaping=true)")
	}
}

// TestReapClaimLosesCASWhenTouchLandsFirst pins reapOne's ErrCAS-loses path:
// if reapOne reads (ref, etag) and a Touch lands before reapOne's own claim
// write, that claim write must fail with ErrCAS rather than clobber the
// Touch's fresh clock and wrongly proceed to destroy a branch that was just
// renewed.
func TestReapClaimLosesCASWhenTouchLandsFirst(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "contested", "", 0, nil); err != nil {
		t.Fatal(err)
	}

	// Capture (ref, etag) exactly as reapOne's own GetRef would.
	ref, staleEtag, err := w.Store.GetRef("app", "contested")
	if err != nil {
		t.Fatal(err)
	}

	// A concurrent Touch lands first, advancing the ref's etag out from
	// under the (ref, etag) pair captured above.
	if _, err := w.Touch("app", "contested", nil, time.Now()); err != nil {
		t.Fatal(err)
	}

	// reapOne's claim write, replayed here with the now-stale etag, must
	// lose the CAS race exactly as it would inside reapOne itself.
	ref.Reaping = true
	if _, err := w.Store.PutRef("app", "contested", ref, staleEtag); !errors.Is(err, store.ErrCAS) {
		t.Fatalf("want ErrCAS when a reap claim races a Touch that already landed, got %v", err)
	}
}

// TestReapOneSkipsErrNotFoundBenignly pins the fix for a branch that
// ListRefs still lists but is gone by the time reapOne reads it (destroyed
// by an operator, or by a sibling reapOne call earlier in the same Reap
// pass, in the window between the two): that must not count as a reap
// failure. Before this fix, Reap folded every non-nil reapOne error
// (including this one) into firstErr, and cmd's `gc` returned before ever
// calling w.GC — so a single vanished-between-list-and-get branch could
// wedge GC forever.
func TestReapOneSkipsErrNotFoundBenignly(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ok, err := w.reapOne("app", "nonexistent", time.Now())
	if ok {
		t.Fatal("reapOne must not report success for a branch that was never there")
	}
	if err != nil {
		t.Fatalf("ErrNotFound must be treated as benign, got %v", err)
	}
}
