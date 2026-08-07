package ops

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// TestConcurrentDestroyAndAcquireLeaseHaveExactlyOneWinner is Destroy's own
// claim-guard proof (Milestone 4 Task 6b), the same shape as
// reap_test.go's TestConcurrentTouchAndReapHaveExactlyOneWinner for Reap's
// Reaping claim: racing Destroy against a concurrent AcquireLease on the
// SAME unleased branch must resolve to exactly one winner, and the
// lease-holder must never be silently deleted out from under it — the exact
// GetRef -> lease-check -> unconditional-DeleteRef TOCTOU the M2 review
// documented, now closed for every Destroy call via a CAS-written Deleting
// claim (see Ref.Deleting's doc comment on store.Ref).
//
// Two coherent outcomes:
//   - Destroy's claim lands first: AcquireLease then sees Deleting=true and
//     refuses (store.ErrDeleting); the branch ends up fully gone.
//   - AcquireLease lands first: Destroy's own claim CAS write (using the
//     etag it read before the race) loses (store.ErrCAS); the branch is
//     still there, alive, with the lease intact.
//
// Any other observed combination — both succeeding, both failing, or a
// lease that "won" but the branch is gone anyway — is the race this task
// closes and must never happen.
func TestConcurrentDestroyAndAcquireLeaseHaveExactlyOneWinner(t *testing.T) {
	for i := 0; i < 20; i++ {
		w := newWS(t)
		if err := w.Create("app"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Fork("app", "main", "contested", "", 0, nil); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var destroyErr, leaseErr error
		wg.Add(2)
		go func() { defer wg.Done(); destroyErr = w.Destroy("app", "contested", false) }()
		go func() {
			defer wg.Done()
			_, leaseErr = w.AcquireLease("app", "contested", "holder-a", DefaultLeaseTTL)
		}()
		wg.Wait()

		ref, _, getErr := w.Store.GetRef("app", "contested")
		gone := errors.Is(getErr, store.ErrNotFound)

		switch {
		case destroyErr == nil && gone:
			// Destroy won: the branch must be fully gone, and the lease
			// attempt must NOT have claimed success.
			if leaseErr == nil {
				t.Fatalf("iter %d: destroy won but AcquireLease also claimed success", i)
			}
		case leaseErr == nil && !gone:
			// AcquireLease won: the branch survives, holding the lease it
			// just acquired — and Destroy must have failed, not silently
			// deleted it anyway.
			if destroyErr == nil {
				t.Fatalf("iter %d: lease acquired but destroy also claimed success", i)
			}
			if ref.LeaseHolder != "holder-a" {
				t.Fatalf("iter %d: lease won but branch shows no live holder: %+v", i, ref)
			}
		default:
			t.Fatalf("iter %d: incoherent outcome destroyErr=%v leaseErr=%v gone=%v ref=%+v",
				i, destroyErr, leaseErr, gone, ref)
		}
	}
}

// TestForceDestroyStillClaimGuards proves force=true bypasses the
// protected/live-lease PRE-CHECKS only, never the CAS claim-guard itself
// (Milestone 4 Task 6b's explicit requirement): racing a force Destroy
// against a concurrent AcquireLease on a branch with an ALREADY-EXPIRED
// lease (so the pre-check itself would let an unforced Destroy through too,
// isolating what force specifically changes) must still resolve to exactly
// one winner. If force had skipped the claim entirely — reverting to the
// pre-Task-6b unconditional DeleteRef — AcquireLease could win the lease
// and then still have its branch deleted out from under it moments later;
// that must not happen just because --force was given.
func TestForceDestroyStillClaimGuards(t *testing.T) {
	for i := 0; i < 20; i++ {
		w := newWS(t)
		if err := w.Create("app"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Fork("app", "main", "contested", "", 0, nil); err != nil {
			t.Fatal(err)
		}
		// An already-expired lease: irrelevant to force's own bypass (an
		// unforced Destroy would already sail through this), isolating the
		// claim-guard behavior specifically.
		if _, err := w.AcquireLease("app", "contested", "stale-holder", time.Nanosecond); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var destroyErr, leaseErr error
		wg.Add(2)
		go func() { defer wg.Done(); destroyErr = w.Destroy("app", "contested", true) }()
		go func() {
			defer wg.Done()
			_, leaseErr = w.AcquireLease("app", "contested", "holder-b", DefaultLeaseTTL)
		}()
		wg.Wait()

		ref, _, getErr := w.Store.GetRef("app", "contested")
		gone := errors.Is(getErr, store.ErrNotFound)

		switch {
		case destroyErr == nil && gone:
			if leaseErr == nil {
				t.Fatalf("iter %d: force destroy won but AcquireLease also claimed success", i)
			}
		case leaseErr == nil && !gone:
			if destroyErr == nil {
				t.Fatalf("iter %d: lease acquired but force destroy also claimed success", i)
			}
			if ref.LeaseHolder != "holder-b" {
				t.Fatalf("iter %d: lease won but branch shows no live holder: %+v", i, ref)
			}
		default:
			t.Fatalf("iter %d: incoherent outcome destroyErr=%v leaseErr=%v gone=%v ref=%+v",
				i, destroyErr, leaseErr, gone, ref)
		}
	}
}

// TestDestroySelfHealsStaleDeletingClaim pins the crashed-Destroy self-heal
// (Milestone 4 Task 6b): a Deleting claim landed (as Destroy's own CAS claim
// write would) but the branch was never actually deleted — simulating a
// process killed between the claim and the delete. ClearStaleDeleteClaims
// must clear a claim older than staleDeletingClaimAfter (the branch is
// still there afterward, no delete ever happened — this is a self-heal of
// the CLAIM, not a retry of the delete), and AcquireLease must then succeed
// again. A claim that is NOT yet stale must be left alone.
func TestDestroySelfHealsStaleDeletingClaim(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "crashed", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	// Simulate a crashed Destroy: land the CAS claim directly, exactly as
	// Destroy's own claim write would, stamped older than
	// staleDeletingClaimAfter.
	ref, etag, err := w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	ref.Deleting = true
	ref.DeletingAt = now.Add(-2 * staleDeletingClaimAfter).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "crashed", ref, etag); err != nil {
		t.Fatal(err)
	}

	// AcquireLease must refuse while the claim stands.
	if _, err := w.AcquireLease("app", "crashed", "holder-a", DefaultLeaseTTL); !errors.Is(err, store.ErrDeleting) {
		t.Fatalf("want ErrDeleting while a Deleting claim stands, got %v", err)
	}

	cleared, err := w.ClearStaleDeleteClaims(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared) != 1 || cleared[0] != "app@crashed" {
		t.Fatalf("cleared = %v, want exactly [app@crashed]", cleared)
	}

	after, _, err := w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	if after.Deleting {
		t.Fatal("ClearStaleDeleteClaims must clear a stale Deleting claim")
	}
	// The branch itself must still be there — this heals the CLAIM, it does
	// not retry (or skip) the delete the crashed call never finished.
	if after.Lineage == "" {
		t.Fatal("a stale-claim self-heal must not delete the branch, only clear the claim")
	}

	// AcquireLease must now succeed.
	if _, err := w.AcquireLease("app", "crashed", "holder-a", DefaultLeaseTTL); err != nil {
		t.Fatalf("AcquireLease must succeed once the stale claim is cleared, got %v", err)
	}

	// A FRESH claim (not yet stale) must be left alone.
	if _, err := w.Fork("app", "main", "fresh-claim", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref2, etag2, err := w.Store.GetRef("app", "fresh-claim")
	if err != nil {
		t.Fatal(err)
	}
	ref2.Deleting = true
	ref2.DeletingAt = now.Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "fresh-claim", ref2, etag2); err != nil {
		t.Fatal(err)
	}
	cleared, err = w.ClearStaleDeleteClaims(now)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range cleared {
		if k == "app@fresh-claim" {
			t.Fatal("a fresh (not yet stale) Deleting claim must not be cleared")
		}
	}
	still, _, err := w.Store.GetRef("app", "fresh-claim")
	if err != nil {
		t.Fatal(err)
	}
	if !still.Deleting {
		t.Fatal("a fresh Deleting claim must survive a ClearStaleDeleteClaims pass")
	}
}
