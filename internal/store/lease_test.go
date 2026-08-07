package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureStderr redirects os.Stderr for the duration of fn and returns
// everything written to it.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = wr
	fn()
	wr.Close()
	os.Stderr = orig
	out, _ := io.ReadAll(r)
	return string(out)
}

func seedBranch(t *testing.T, s *Store) {
	t.Helper()
	r := Ref{Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1, HeadEpoch: 1}
	r.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	if _, err := s.PutRef("app", "main", r, ""); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireLeaseBumpsEpoch(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if l.Epoch != 2 {
		t.Errorf("epoch = %d, want 2 (bumped on acquisition)", l.Epoch)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.Epoch != 2 || ref.LeaseHolder != "daemon-a" {
		t.Fatalf("ref = %+v", ref)
	}
	// Head still points at the object written under epoch 1.
	if ref.HeadEpoch != 1 {
		t.Errorf("head epoch = %d, want 1 (objects don't move)", ref.HeadEpoch)
	}
}

func TestAcquireRefusesLiveLease(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	if _, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now); err != nil {
		t.Fatal(err)
	}
	_, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(30*time.Second))
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != "daemon-a" || ref.Epoch != 2 {
		t.Fatalf("a refused acquisition must not disturb the ref: %+v", ref)
	}
}

func TestReclaimExpiredLeaseBumpsEpochAgain(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("expired lease must be reclaimable: %v", err)
	}
	if b.Epoch != a.Epoch+1 {
		t.Errorf("reclaim epoch = %d, want %d", b.Epoch, a.Epoch+1)
	}
	// The fenced holder can no longer renew.
	if _, err := s.RenewLease(a, time.Minute, now.Add(2*time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fenced holder renew: want ErrLeaseLost, got %v", err)
	}
}

func TestRenewExtendsOwnLease(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.RenewLease(l, time.Minute, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !l2.Expiry.After(l.Expiry) {
		t.Errorf("renew must extend: %v then %v", l.Expiry, l2.Expiry)
	}
	if l2.Epoch != l.Epoch {
		t.Errorf("renew must NOT bump the epoch: %d then %d", l.Epoch, l2.Epoch)
	}
}

func TestReleaseFreesBranchWithoutBumping(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseLease(l); err != nil {
		t.Fatal(err)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != "" || ref.LeaseExpiry != "" {
		t.Fatalf("release must clear the lease: %+v", ref)
	}
	if ref.Epoch != l.Epoch {
		t.Errorf("clean release must not bump the epoch: %d vs %d", ref.Epoch, l.Epoch)
	}
	// A fresh acquisition after release still bumps.
	l2, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(time.Second))
	if err != nil || l2.Epoch != l.Epoch+1 {
		t.Fatalf("post-release acquire: epoch %d err %v", l2.Epoch, err)
	}
}

func TestReleaseByFencedHolderIsRefused(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, _ := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if _, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseLease(a); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("a fenced holder must not clear the new holder's lease, got %v", err)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != "daemon-b" {
		t.Fatalf("holder = %q, want daemon-b", ref.LeaseHolder)
	}
}

// TestAcquireByCurrentHolderIsIdempotentRenew pins the decision that
// re-acquiring your own live lease is a renew, not a fresh acquisition:
// bumping the epoch here would fence the holder's own in-flight writes.
func TestAcquireByCurrentHolderIsIdempotentRenew(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if l2.Epoch != l.Epoch {
		t.Errorf("re-acquire by current holder must NOT bump the epoch: %d then %d", l.Epoch, l2.Epoch)
	}
	if !l2.Expiry.After(l.Expiry) {
		t.Errorf("re-acquire must extend expiry: %v then %v", l.Expiry, l2.Expiry)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.Epoch != l.Epoch {
		t.Errorf("ref epoch = %d, want unchanged %d", ref.Epoch, l.Epoch)
	}
	if ref.LeaseExpiry != l2.Expiry.Format(time.RFC3339Nano) {
		t.Errorf("ref lease_expiry = %q, want %q", ref.LeaseExpiry, l2.Expiry.Format(time.RFC3339Nano))
	}
}

// TestAcquireOverCorruptExpiryWarnsAndSucceeds pins the fail-open decision
// for a LeaseExpiry that is present but unparseable: it must be reclaimable
// (fail-closed would brick the branch permanently), but the corruption must
// not pass silently.
func TestAcquireOverCorruptExpiryWarnsAndSucceeds(t *testing.T) {
	s := newStore(t)
	r := Ref{Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1, HeadEpoch: 1,
		LeaseHolder: "daemon-a", LeaseExpiry: "not-a-timestamp"}
	r.SetCheckpoint("init", Checkpoint{TXID: 1, Epoch: 1})
	if _, err := s.PutRef("app", "main", r, ""); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	var l Lease
	var err error
	stderr := captureStderr(t, func() {
		l, err = s.AcquireLease("app", "main", "daemon-b", time.Minute, now)
	})
	if err != nil {
		t.Fatalf("acquire over corrupt expiry must succeed (fail-open), got %v", err)
	}
	if l.Epoch != 2 {
		t.Errorf("epoch = %d, want 2 (corrupt expiry treated as reclaimable)", l.Epoch)
	}
	if !strings.Contains(stderr, "app@main") || !strings.Contains(stderr, "not-a-timestamp") {
		t.Errorf("stderr = %q, want a warning naming the branch and the bad value", stderr)
	}
	if !strings.HasPrefix(stderr, "offshoot: warning:") {
		t.Errorf("stderr = %q, want the offshoot warning idiom", stderr)
	}
}

// racingBackend wraps a Backend and, on the first Get for the tracked key,
// runs racer once after fetching the "current" data/etag — deterministically
// reproducing a lost CAS race: the caller (using this backend) reads a ref,
// the racer changes it out from under them, then the caller's own PutIf is
// stale.
type racingBackend struct {
	Backend
	key       string
	racer     func()
	triggered bool
}

func (b *racingBackend) Get(key string) ([]byte, string, error) {
	data, etag, err := b.Backend.Get(key)
	if key == b.key && !b.triggered {
		b.triggered = true
		b.racer()
	}
	return data, etag, err
}

// TestAcquireRaceLossIsErrLeaseHeld pins the decision that a lost CAS race
// inside AcquireLease is translated into an error wrapping both ErrLeaseHeld
// (so callers pattern-matching only that don't miss a genuine concurrent
// loss) and ErrCAS (so the low-level detail is still recoverable).
func TestAcquireRaceLossIsErrLeaseHeld(t *testing.T) {
	base, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	baseStore := &Store{B: base}
	seedBranch(t, baseStore)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	rb := &racingBackend{Backend: base, key: RefKey("app", "main")}
	rb.racer = func() {
		// Simulate a concurrent acquirer winning the race: read-modify-write
		// the ref via PutRef directly, between our own GetRef and PutRef.
		ref, etag, err := baseStore.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		ref.Epoch++
		ref.LeaseHolder = "daemon-racer"
		ref.LeaseExpiry = now.Add(time.Minute).UTC().Format(time.RFC3339Nano)
		if _, err := baseStore.PutRef("app", "main", ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	s := &Store{B: rb}

	_, err = s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	if !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS, got %v", err)
	}
}

func TestConcurrentAcquireHasOneWinner(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	const n = 12
	var wg sync.WaitGroup
	wins := make(chan Lease, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			l, err := s.AcquireLease("app", "main", fmt.Sprintf("holder-%d", idx), time.Minute, now)
			if err == nil {
				wins <- l
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	var won []Lease
	for l := range wins {
		won = append(won, l)
	}
	if len(won) != 1 {
		t.Fatalf("exactly one acquirer must win an unleased branch, got %d", len(won))
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != won[0].Holder || ref.Epoch != won[0].Epoch {
		t.Fatalf("ref %+v disagrees with the winning lease %+v", ref, won[0])
	}
}

func TestEpochNeverDecreasesAcrossReclaims(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	last := uint64(0)
	for i := 0; i < 5; i++ {
		l, err := s.AcquireLease("app", "main", fmt.Sprintf("h%d", i), time.Second, now)
		if err != nil {
			t.Fatal(err)
		}
		if l.Epoch <= last {
			t.Fatalf("epoch went backwards: %d after %d", l.Epoch, last)
		}
		last = l.Epoch
		now = now.Add(2 * time.Second) // let it expire so the next holder reclaims
	}
}

// TestAcquireRefusesReapingClaim pins the fix for the claim->Destroy race:
// a lease acquired while a branch is claimed for reaping (Reaping=true)
// could have that branch deleted out from under it, since Destroy's own
// GetRef can still read the pre-lease ref and DeleteRef is unconditional.
// AcquireLease must refuse outright while the claim stands, and this is
// only safe together with ops.Reap's self-heal of a stale claim (a separate
// fix): once that clears Reaping, AcquireLease must succeed again — a
// permanent claim would otherwise brick the branch's leasability forever.
func TestAcquireRefusesReapingClaim(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	ref, etag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.Reaping = true
	if _, err := s.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	if _, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now); !errors.Is(err, ErrReaping) {
		t.Fatalf("want ErrReaping while a reap claim stands, got %v", err)
	}

	// Once the claim clears (here, simulating ops.Reap's self-heal of a
	// stale claim directly), AcquireLease must succeed again.
	ref, etag, err = s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.Reaping = false
	if _, err := s.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now); err != nil {
		t.Fatalf("AcquireLease must succeed once the reap claim clears, got %v", err)
	}
}

func TestRenewAfterExpiryButBeforeReclaimStillWorks(t *testing.T) {
	// A holder whose lease lapsed but whom nobody has displaced may renew:
	// expiry alone does not fence, only another acquisition does.
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "slow", time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.RenewLease(l, time.Minute, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("uncontested lapsed holder must be able to renew: %v", err)
	}
	if l2.Epoch != l.Epoch {
		t.Errorf("renew bumped the epoch: %d -> %d", l.Epoch, l2.Epoch)
	}
}
