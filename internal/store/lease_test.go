package store

import (
	"errors"
	"testing"
	"time"
)

func seedBranch(t *testing.T, s *Store) {
	t.Helper()
	r := Ref{Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1, HeadEpoch: 1}
	r.SetCheckpoint("init", 1, 1)
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
