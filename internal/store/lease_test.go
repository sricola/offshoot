package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
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

// TestAcquireLeaseRefReturnsTheWrittenRef: AcquireLeaseRef hands back the
// exact ref its acquire wrote and that write's etag, so a caller can plan
// from the revision its lease is part of and compare-and-swap against it
// without a second GetRef.
func TestAcquireLeaseRefReturnsTheWrittenRef(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, got, etag, err := s.AcquireLeaseRef("app", "main", "checkpoint:h/1/0123abcd", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	stored, storedEtag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, stored) {
		t.Fatalf("returned ref\n %+v\nstored ref\n %+v", got, stored)
	}
	if etag != storedEtag {
		t.Fatalf("returned etag %q, stored etag %q", etag, storedEtag)
	}
	if got.Epoch != 2 || l.Epoch != got.Epoch || got.LeaseHolder != l.Holder {
		t.Fatalf("lease %+v does not match the returned ref %+v", l, got)
	}
	// The etag is the write's own: a compare-and-swap against it lands.
	got.Protected = true
	if _, err := s.PutRef("app", "main", got, etag); err != nil {
		t.Fatalf("PutRef against the returned etag: %v", err)
	}
}

// TestAcquireLeaseRefRefusesALiveLease: AcquireLeaseRef refuses exactly
// what AcquireLease refuses, and returns no ref with the refusal.
func TestAcquireLeaseRefRefusesALiveLease(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	if _, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now); err != nil {
		t.Fatal(err)
	}
	_, ref, etag, err := s.AcquireLeaseRef("app", "main", "checkpoint:h/1/0123abcd", time.Minute, now)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	if !reflect.DeepEqual(ref, Ref{}) || etag != "" {
		t.Fatalf("a refused acquire returned ref %+v etag %q", ref, etag)
	}
}

// TestAcquireLeaseRefIfChecksTheRefItWrites: the check runs on the ref the
// acquire reads, before its own refusals and its write. A refusal is
// returned unwrapped and writes nothing; a pass writes exactly the checked
// ref plus the lease.
func TestAcquireLeaseRefIfChecksTheRefItWrites(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	before, beforeEtag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("refused by the caller")
	var seen Ref
	_, ref, etag, err := s.AcquireLeaseRefIf("app", "main", "checkpoint:h/1/0123abcd", time.Minute, now, func(r Ref) error {
		seen = r
		return refusal
	})
	if err != refusal || !reflect.DeepEqual(ref, Ref{}) || etag != "" {
		t.Fatalf("a refused check: err %v ref %+v etag %q, want the check's own error and nothing else", err, ref, etag)
	}
	if !reflect.DeepEqual(seen, before) {
		t.Fatalf("the check saw %+v, want the stored ref %+v", seen, before)
	}
	if after, afterEtag, err := s.GetRef("app", "main"); err != nil || afterEtag != beforeEtag || !reflect.DeepEqual(after, before) {
		t.Fatalf("a refused check wrote the ref: %+v (%v)", after, err)
	}
	// The check runs before the acquire's own refusals: a claimed branch
	// is the caller's to word.
	before.Deleting = true
	if _, err := s.PutRef("app", "main", before, beforeEtag); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.AcquireLeaseRefIf("app", "main", "checkpoint:h/1/0123abcd", time.Minute, now, func(Ref) error { return refusal }); err != refusal {
		t.Fatalf("check on a claimed branch: %v, want the check's own error", err)
	}
	cur, curEtag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	cur.Deleting = false
	if _, err := s.PutRef("app", "main", cur, curEtag); err != nil {
		t.Fatal(err)
	}
	l, got, _, err := s.AcquireLeaseRefIf("app", "main", "checkpoint:h/1/0123abcd", time.Minute, now, func(r Ref) error {
		seen = r
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := seen
	want.Epoch++
	want.LeaseHolder, want.LeaseExpiry = l.Holder, l.Expiry.Format(time.RFC3339Nano)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("a passed check wrote\n %+v\nwant the checked ref plus the lease\n %+v", got, want)
	}
}

// TestRenewLeaseLeavesAClaimAlone: a renewal of a branch with a destroy
// claim, while the lease has more than half its TTL left, or with a reap
// claim, writes nothing and returns ErrDeleting or ErrReaping, so the
// claim's etag, which Destroy's conditional delete compares against, does
// not move. A lease that is no longer ours is still ErrLeaseLost.
func TestRenewLeaseLeavesAClaimAlone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim func(*Ref)
		want  error
	}{
		{"destroy", func(r *Ref) { r.Deleting = true }, ErrDeleting},
		{"reap", func(r *Ref) { r.Reaping = true }, ErrReaping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t)
			seedBranch(t, s)
			now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
			l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
			if err != nil {
				t.Fatal(err)
			}
			ref, etag, err := s.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			tc.claim(&ref)
			claimEtag, err := s.PutRef("app", "main", ref, etag)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.RenewLease(l, time.Minute, now.Add(time.Second)); !errors.Is(err, tc.want) {
				t.Fatalf("renewal over a %s claim: %v, want %v", tc.name, err, tc.want)
			}
			if _, got, err := s.GetRef("app", "main"); err != nil || got != claimEtag {
				t.Fatalf("a renewal over the claim moved its etag (%v)", err)
			}
			stolen := l
			stolen.Holder = "someone-else"
			if _, err := s.RenewLease(stolen, time.Minute, now.Add(time.Second)); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("renewal of a lease that is not ours under a claim: %v, want ErrLeaseLost", err)
			}
		})
	}
}

// TestRenewLeaseKeepsALiveLeaseUnderADestroyClaim: a destroy claim stops
// renewals only while the lease has more than half its TTL left. Past
// that, a renewal writes over the claim, leaving it set, so the lease
// never lapses under it: a claim a killed destroy stranded stands until
// ops.ClearStaleDeleteClaims finds it 30 s old, by which time a 30 s lease
// that no renewal had extended would always have expired, and the first
// acquirer after the clear would fence the holder. A reap claim still
// stops every renewal: it lands only on a branch whose lease expired a
// whole branch TTL ago.
func TestRenewLeaseKeepsALiveLeaseUnderADestroyClaim(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	ttl := 30 * time.Second
	t0 := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "daemon-a", ttl, t0)
	if err != nil {
		t.Fatal(err)
	}
	ref, etag, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	claimedAt := t0.Add(time.Second).Format(time.RFC3339Nano)
	ref.Deleting, ref.DeletingAt = true, claimedAt
	claimEtag, err := s.PutRef("app", "main", ref, etag)
	if err != nil {
		t.Fatal(err)
	}
	// 20 s of 30 left: the destroy may still be running, so the renewal
	// leaves its etag alone.
	if _, err := s.RenewLease(l, ttl, t0.Add(10*time.Second)); !errors.Is(err, ErrDeleting) {
		t.Fatalf("renewal with 20 s of a 30 s lease left under a destroy claim: %v, want ErrDeleting", err)
	}
	if _, got, err := s.GetRef("app", "main"); err != nil || got != claimEtag {
		t.Fatalf("a renewal with more than half the lease left moved the claim's etag (%v)", err)
	}
	// 10 s left: renewed over the claim, which stays as it was.
	next, err := s.RenewLease(l, ttl, t0.Add(20*time.Second))
	if err != nil {
		t.Fatalf("renewal with 10 s of a 30 s lease left under a destroy claim: %v, want it renewed", err)
	}
	want := t0.Add(50 * time.Second)
	if !next.Expiry.Equal(want) || next.Epoch != l.Epoch || next.Holder != l.Holder {
		t.Fatalf("renewed lease %+v, want %s's epoch %d until %s", next, l.Holder, l.Epoch, want)
	}
	got, _, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Deleting || got.DeletingAt != claimedAt || got.LeaseHolder != l.Holder || got.Epoch != l.Epoch ||
		got.LeaseExpiry != want.Format(time.RFC3339Nano) {
		t.Fatalf("after a renewal over the claim: deleting %v at %q, holder %q epoch %d until %s; want the claim as it was and the lease until %s",
			got.Deleting, got.DeletingAt, got.LeaseHolder, got.Epoch, got.LeaseExpiry, want)
	}
	if !LeaseLive(got, t0.Add(31*time.Second)) {
		t.Fatal("the lease is not live when a stranded claim first becomes clearable")
	}

	// A reap claim stops renewals however little of the lease is left.
	s = newStore(t)
	seedBranch(t, s)
	if l, err = s.AcquireLease("app", "main", "daemon-a", ttl, t0); err != nil {
		t.Fatal(err)
	}
	if ref, etag, err = s.GetRef("app", "main"); err != nil {
		t.Fatal(err)
	}
	ref.Reaping = true
	if claimEtag, err = s.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Duration{20 * time.Second, time.Hour} {
		if _, err := s.RenewLease(l, ttl, t0.Add(at)); !errors.Is(err, ErrReaping) {
			t.Fatalf("renewal %s after the acquire under a reap claim: %v, want ErrReaping", at, err)
		}
	}
	if _, got, err := s.GetRef("app", "main"); err != nil || got != claimEtag {
		t.Fatalf("a renewal moved the reap claim's etag (%v)", err)
	}
}
