package ops

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

func TestWorkspaceLeaseLifecycle(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	l, err := w.AcquireLease("app", "main", "tester", DefaultLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if l.Holder != "tester" || l.Epoch < 2 {
		t.Fatalf("lease = %+v", l)
	}
	if _, err := w.AcquireLease("app", "main", "other", DefaultLeaseTTL); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	l2, err := w.RenewLease(l, DefaultLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := w.Leases()
	if err != nil || len(infos) != 1 {
		t.Fatalf("infos=%v err=%v", infos, err)
	}
	if infos[0].Holder != "tester" || infos[0].Expired {
		t.Fatalf("info = %+v", infos[0])
	}
	if err := w.ReleaseLease(l2); err != nil {
		t.Fatal(err)
	}
	infos, _ = w.Leases()
	if len(infos) != 0 {
		t.Fatalf("released lease should not be listed: %v", infos)
	}
}

func TestLeasesReportsExpired(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "tester", time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	infos, err := w.Leases()
	if err != nil || len(infos) != 1 {
		t.Fatalf("infos=%v err=%v", infos, err)
	}
	if !infos[0].Expired {
		t.Fatalf("a lease past its expiry must report Expired: %+v", infos[0])
	}
	// An expired lease is reclaimable by anyone.
	if _, err := w.AcquireLease("app", "main", "other", DefaultLeaseTTL); err != nil {
		t.Fatalf("expired lease must be reclaimable: %v", err)
	}
}

func TestLocalHolderIsStable(t *testing.T) {
	if a, b := LocalHolder(), LocalHolder(); a != b || a == "" {
		t.Fatalf("LocalHolder unstable or empty: %q %q", a, b)
	}
}

func TestLeasesTolerateCorruptExpiry(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app1"); err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app2"); err != nil {
		t.Fatal(err)
	}

	// Acquire a healthy lease on app1@main.
	_, err := w.AcquireLease("app1", "main", "holder1", DefaultLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}

	// Create a corrupt ref on app2@main with garbage LeaseExpiry.
	ref2, etag2, err := w.Store.GetRef("app2", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref2.LeaseHolder = "holder2"
	ref2.LeaseExpiry = "not-a-valid-timestamp"
	if _, err := w.Store.PutRef("app2", "main", ref2, etag2); err != nil {
		t.Fatal(err)
	}

	// Capture stderr and call Leases().
	stderr := captureStderr(t, func() {
		// This should not fail; it should tolerate the corrupt expiry.
		infos, err := w.Leases()
		if err != nil {
			t.Fatalf("Leases() should tolerate corrupt expiry, got error: %v", err)
		}

		// Should have exactly 2 leases: one healthy, one corrupt.
		if len(infos) != 2 {
			t.Fatalf("expected 2 leases, got %d: %+v", len(infos), infos)
		}

		// Find and verify the healthy lease (app1@main).
		var healthy, corrupt *LeaseInfo
		for i := range infos {
			if infos[i].DB == "app1" {
				healthy = &infos[i]
			} else if infos[i].DB == "app2" {
				corrupt = &infos[i]
			}
		}

		if healthy == nil {
			t.Fatalf("healthy lease app1@main not found in %+v", infos)
		}
		if corrupt == nil {
			t.Fatalf("corrupt lease app2@main not found in %+v", infos)
		}

		// Healthy lease should not be expired.
		if healthy.Expired {
			t.Fatalf("healthy lease should not be expired: %+v", healthy)
		}
		if healthy.Holder != "holder1" {
			t.Fatalf("healthy lease holder mismatch: %q", healthy.Holder)
		}

		// Corrupt lease should be marked as expired and have zero Expiry.
		if !corrupt.Expired {
			t.Fatalf("corrupt lease must report Expired: %+v", corrupt)
		}
		if !corrupt.Expiry.IsZero() {
			t.Fatalf("corrupt lease must have zero Expiry: %v", corrupt.Expiry)
		}
		if corrupt.Holder != "holder2" {
			t.Fatalf("corrupt lease holder mismatch: %q", corrupt.Holder)
		}
	})

	// Verify warning was printed to stderr.
	if !strings.Contains(stderr, "offshoot: warning:") || !strings.Contains(stderr, "app2@main") {
		t.Fatalf("expected warning about app2@main in stderr, got: %q", stderr)
	}

	// Verify the corrupt lease can be released (proving it's reclaimable).
	// Get the corrupt ref again to capture its current state.
	ref2Updated, _, err := w.Store.GetRef("app2", "main")
	if err != nil {
		t.Fatal(err)
	}
	corruptLease := store.Lease{
		DB:     "app2",
		Branch: "main",
		Holder: ref2Updated.LeaseHolder,
		Epoch:  ref2Updated.Epoch,
	}
	if err := w.ReleaseLease(corruptLease); err != nil {
		t.Fatalf("ReleaseLease on corrupt lease should succeed, got: %v", err)
	}

	// After release, the lease should be gone.
	infos, err := w.Leases()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].DB != "app1" {
		t.Fatalf("after releasing corrupt lease, should only have app1; got %+v", infos)
	}
}

func TestFencedHolderCannotCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	a, err := w.AcquireLease("app", "main", "writer-a", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "writer-b", DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	// A is fenced. Its lease operations must fail loudly rather than silently
	// succeeding against the new epoch.
	if _, err := w.RenewLease(a, DefaultLeaseTTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("fenced renew: want ErrLeaseLost, got %v", err)
	}
	if err := w.ReleaseLease(a); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("fenced release: want ErrLeaseLost, got %v", err)
	}
	// The branch itself is still usable by whoever holds it.
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatalf("branch unusable after fencing: %v", err)
	}
}
