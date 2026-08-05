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
