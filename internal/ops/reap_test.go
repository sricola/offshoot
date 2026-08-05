package ops

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

func setTTLAt(t *testing.T, w *Workspace, db, branch, ttl, touchedAt string) {
	t.Helper()
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	ref.TTL, ref.TouchedAt = ttl, touchedAt
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		t.Fatal(err)
	}
}

func TestReapDestroysOnlyExpiredUnprotectedUnleased(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	for _, br := range []string{"expired", "fresh", "shielded", "leased", "immortal"} {
		if _, err := w.Fork("app", "main", br, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	setTTLAt(t, w, "app", "expired", "1h", old)
	setTTLAt(t, w, "app", "fresh", "1h", now.Format(time.RFC3339Nano))
	setTTLAt(t, w, "app", "shielded", "1h", old)
	if ref, etag, err := w.Store.GetRef("app", "shielded"); err != nil {
		t.Fatal(err)
	} else {
		ref.Protected = true
		if _, err := w.Store.PutRef("app", "shielded", ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	setTTLAt(t, w, "app", "leased", "1h", old)
	if _, err := w.Store.AcquireLease("app", "leased", "holder-1", time.Hour, now); err != nil {
		t.Fatal(err)
	}
	// "immortal" has no TTL at all; "main" likewise.

	reaped, err := w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 || reaped[0] != "app@expired" {
		t.Fatalf("reaped = %v, want exactly [app@expired]", reaped)
	}
	if _, _, err := w.Store.GetRef("app", "expired"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired ref must be gone, err=%v", err)
	}
	for _, br := range []string{"main", "fresh", "shielded", "leased", "immortal"} {
		if _, _, err := w.Store.GetRef("app", br); err != nil {
			t.Fatalf("%s must survive: %v", br, err)
		}
	}
}

func TestExpiredLeaseDefersToTTLNotBlocksForever(t *testing.T) {
	// Spec: a wedged holder loses the lease first, then TTL applies — an
	// EXPIRED lease does not shield a branch, but the clock includes the
	// lease expiry ("last durable write or lease renewal, whichever is later").
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "wedged", "", 0); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	setTTLAt(t, w, "app", "wedged", "1h", now.Add(-5*time.Hour).Format(time.RFC3339Nano))
	if _, err := w.Store.AcquireLease("app", "wedged", "holder-1", time.Minute, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Lease expired ~3h ago; TTL 1h past that is also gone → reapable.
	reaped, err := w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 || reaped[0] != "app@wedged" {
		t.Fatalf("reaped = %v, want [app@wedged]", reaped)
	}
	// But if the lease expired RECENTLY, TTL counts from that expiry.
	if _, err := w.Fork("app", "main", "recent", "", 0); err != nil {
		t.Fatal(err)
	}
	setTTLAt(t, w, "app", "recent", "1h", now.Add(-5*time.Hour).Format(time.RFC3339Nano))
	if _, err := w.Store.AcquireLease("app", "recent", "holder-2", time.Minute, now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	reaped, err = w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 0 {
		t.Fatalf("lease expired 29m ago + 1h TTL = not yet expired; reaped %v", reaped)
	}
}

func TestConcurrentTouchAndReapHaveExactlyOneWinner(t *testing.T) {
	for i := 0; i < 20; i++ {
		w := newWS(t)
		if err := w.Create("app"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Fork("app", "main", "contested", "", 0); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		setTTLAt(t, w, "app", "contested", "1h", now.Add(-2*time.Hour).Format(time.RFC3339Nano))

		var wg sync.WaitGroup
		var touchErr error
		var reaped []string
		wg.Add(2)
		go func() { defer wg.Done(); _, touchErr = w.Touch("app", "contested", nil, now) }()
		go func() { defer wg.Done(); reaped, _ = w.Reap(now) }()
		wg.Wait()

		_, _, getErr := w.Store.GetRef("app", "contested")
		gone := errors.Is(getErr, store.ErrNotFound)
		switch {
		case len(reaped) == 1 && gone:
			// Reaper won; the touch must NOT have claimed success.
			if touchErr == nil {
				t.Fatalf("iter %d: both touch and reap claimed success", i)
			}
		case len(reaped) == 0 && !gone && touchErr == nil:
			// Toucher won; branch alive with a fresh clock.
		default:
			t.Fatalf("iter %d: incoherent outcome reaped=%v touchErr=%v getErr=%v", i, reaped, touchErr, getErr)
		}
	}
}

// TestReapSelfHealsStaleReapingClaim pins the fix for a crashed reaper: it
// CAS-claimed a branch (Reaping=true) but never reached Destroy, and then
// activity (here, a fresh TouchedAt stamped directly, as ReleaseLease or a
// durable flush would) deferred the deadline back into the future. Before
// the fix, that claim was permanent — Touch refuses forever on Reaping, and
// nothing else ever clears it. A single Reap pass must clear the stale
// claim so Touch succeeds again, while a genuinely-expired Reaping ref
// (deadline still in the past) is left alone to proceed to Destroy exactly
// as before.
func TestReapSelfHealsStaleReapingClaim(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "crashed", "", 0); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	// Simulate a crashed reaper: land the CAS claim directly, exactly as
	// reapOne's own claim write would, on a ref whose deadline had passed.
	ref, etag, err := w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	ref.TTL = "1h"
	ref.TouchedAt = now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	ref.Reaping = true
	if _, err := w.Store.PutRef("app", "crashed", ref, etag); err != nil {
		t.Fatal(err)
	}

	// Activity defers the deadline: a fresh TouchedAt, exactly as
	// ReleaseLease/a durable flush would stamp without ever touching
	// Reaping. The claim is now stale — the branch it names is no longer
	// past its deadline — but Reaping is still set.
	ref, etag, err = w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	ref.TouchedAt = now.Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "crashed", ref, etag); err != nil {
		t.Fatal(err)
	}

	if reaped, err := w.Reap(now); err != nil {
		t.Fatal(err)
	} else if len(reaped) != 0 {
		t.Fatalf("a stale claim must not be destroyed, reaped %v", reaped)
	}

	after, _, err := w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	if after.Reaping {
		t.Fatal("Reap must self-heal a stale Reaping claim once activity has deferred the deadline")
	}

	// Touch must now succeed: the whole point of the fix.
	if _, err := w.Touch("app", "crashed", nil, now); err != nil {
		t.Fatalf("Touch must succeed once the stale claim is cleared, got %v", err)
	}

	// A genuinely-expired Reaping ref (deadline still in the past) must
	// still proceed to Destroy, unaffected by the self-heal path above.
	if _, err := w.Fork("app", "main", "genuinely-expired", "", 0); err != nil {
		t.Fatal(err)
	}
	setTTLAt(t, w, "app", "genuinely-expired", "1h", now.Add(-2*time.Hour).Format(time.RFC3339Nano))
	ref, etag, err = w.Store.GetRef("app", "genuinely-expired")
	if err != nil {
		t.Fatal(err)
	}
	ref.Reaping = true
	if _, err := w.Store.PutRef("app", "genuinely-expired", ref, etag); err != nil {
		t.Fatal(err)
	}
	reaped, err := w.Reap(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 || reaped[0] != "app@genuinely-expired" {
		t.Fatalf("reaped = %v, want exactly [app@genuinely-expired]", reaped)
	}
	if _, _, err := w.Store.GetRef("app", "genuinely-expired"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a genuinely-expired claimed ref must still be destroyed, err=%v", err)
	}
}

func TestReapedLineageIsCollectableByGC(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	lineage := ref.Lineage
	now := time.Now().UTC()
	setTTLAt(t, w, "app", "doomed", "1h", now.Add(-2*time.Hour).Format(time.RFC3339Nano))
	if _, err := w.Reap(now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("reaped lineage must be swept by GC, %d objects remain", len(keys))
	}
}
