package ops

import (
	"errors"
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
