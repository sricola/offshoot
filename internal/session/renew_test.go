package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

func TestRenewKeepsLeaseAliveBeyondTTL(t *testing.T) {
	testutil.RequireSQLite3(t)
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
	testutil.RequireSQLite3(t)
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
	if _, err := s.Flush("", nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("a fenced session must refuse to flush, got %v", err)
	}
}

// TestCloseDoesNotMarkSessionFenced proves Close joins the renewal goroutine
// before releasing the lease. Without that join, a renewal tick that fires
// in the same instant Close cancels the context can lose the race in two
// visible ways: (1) the straggler's RenewLease call can read the ref AFTER
// Close's own ReleaseLease has already cleared the holder, see a holder
// mismatch, and call fail(ErrFenced) — mislabeling a clean shutdown as
// fencing; or (2) the straggler's RenewLease and Close's ReleaseLease can
// both be mid-flight at once and CAS against each other, so Close itself
// returns a "compare-and-swap conflict" error. A short RenewEvery makes
// ticks frequent enough that one is very likely in flight (or about to fire)
// at the moment Close runs; repeating the open/close cycle many times makes
// the race likely to be hit at least once if the join is missing, even
// though any single iteration might not hit it (empirically, with the join
// removed, this reproduces both symptoms within a few dozen iterations of
// these parameters — see the task-3 report for the pre-fix run this test
// was validated against).
func TestCloseDoesNotMarkSessionFenced(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		s, err := Open(context.Background(), Options{
			WS: w, DB: "app", Branch: "main",
			LeaseTTL: 2 * time.Second, RenewEvery: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("iteration %d: open: %v", i, err)
		}
		time.Sleep(30 * time.Millisecond)
		if err := s.Close(); err != nil {
			t.Fatalf("iteration %d: close: %v", i, err)
		}
		if err := s.Err(); err != nil {
			t.Fatalf("iteration %d: a cleanly-closed session must not record an error, got: %v", i, err)
		}
	}
}

// TestRenewalEndsWhenBranchDestroyed proves that a branch destroyed out from
// under a live session (e.g. `offshoot destroy --force`, which deletes the
// ref RenewLease's own GetRef depends on) ends the session instead of
// spinning the renewal loop forever on a branch that can never come back.
func TestRenewalEndsWhenBranchDestroyed(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "feature", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "feature",
		LeaseTTL: 2 * time.Second, RenewEvery: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Force-destroy the branch this session holds the lease on. This deletes
	// the ref key directly on the backend (simulating an out-of-band
	// destroy), rather than going through ops.Workspace.Destroy
	// itself: Destroy also quiesces and removes the live checkout files,
	// which — with a capture engine still actively polling that same
	// checkout — triggers a second, unrelated race between the engine
	// noticing its checkout vanished (surfacing its own, differently worded,
	// error) and the renewal loop noticing the ref is gone. That collision
	// is a real pre-existing interaction between Destroy and a live capture
	// engine, but it is not what this test exists to exercise — this test is
	// specifically about RenewLease's GetRef seeing store.ErrNotFound (see
	// renew.go), which deleting the ref reproduces directly and
	// deterministically without perturbing the checkout the capture engine
	// depends on.
	//
	// store.Local's Delete does not take the per-key CAS lock that PutIf
	// does, so — rarely, with such a short RenewEvery — a renewal's
	// read-etag-then-write can straddle the delete and resurrect the ref
	// immediately after this call returns. That's a narrow, pre-existing
	// race in the store layer that also isn't what this test is trying to
	// exercise, so tolerate it by deleting again if the ref is still (or
	// again) present.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := w.Store.B.Delete(store.RefKey("app", "feature")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.Store.GetRef("app", "feature"); errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("branch ref kept getting resurrected by a racing renewal; giving up")
		}
	}

	waitFor(t, 3*time.Second, "session to notice its branch is gone", func() bool {
		return s.Err() != nil
	})
	if err := s.Err(); !strings.Contains(err.Error(), "no longer exist") {
		t.Fatalf("want an error mentioning the branch no longer exists, got: %v", err)
	}
}
