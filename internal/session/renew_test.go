package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
)

func TestRenewKeepsLeaseAliveBeyondTTL(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main",
		LeaseTTL: 300 * time.Millisecond, RenewEvery: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Well past the original TTL, the lease is still live and ours.
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("session died despite renewal: %v", err)
	}
	infos, err := w.Leases()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Expired {
		t.Fatalf("lease not kept alive: %+v", infos)
	}
	if infos[0].Holder != s.Lease().Holder {
		t.Fatalf("holder = %q, want %q", infos[0].Holder, s.Lease().Holder)
	}
}

func TestRenewalDetectsFencingAndEndsSession(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// RenewEvery is deliberately set well beyond LeaseTTL. If it were shorter
	// (e.g. the "natural"-looking TTL=100ms/RenewEvery=20ms pairing), the
	// renewLoop would renew the lease several times before this test ever
	// gets a chance to steal it, so the lease would never actually lapse and
	// the "thief" AcquireLease below would fail with ErrLeaseHeld instead of
	// succeeding — which is not what this test is trying to exercise. By
	// making the lease expire (100ms) well before the first renewal tick
	// (400ms), the thief can genuinely reclaim the branch while our session
	// is between ticks, and the *next* tick is what discovers ErrLeaseLost.
	// Do not "fix" this back to RenewEvery < LeaseTTL.
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", Holder: "session-a",
		LeaseTTL: 100 * time.Millisecond, RenewEvery: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Wait out the TTL, then steal the branch.
	time.Sleep(150 * time.Millisecond)
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "session to notice it was fenced", func() bool {
		return errors.Is(s.Err(), ErrFenced)
	})
	if _, err := s.Flush(""); !errors.Is(err, ErrFenced) {
		t.Fatalf("a fenced session must refuse to flush, got %v", err)
	}
}
