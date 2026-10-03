package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
//
// One outcome needs care: BOTH calls can legitimately succeed, when the
// goroutines happen to serialize with AcquireLease landing its lease before
// Destroy's initial GetRef — force then bypasses the live-lease pre-check by
// design and destroys a leased branch, which is exactly what --force means.
// That is not the bug this test guards against (macOS reorders the two
// goroutines this way ~1% of the time). The bug would be AcquireLease
// succeeding AFTER the Deleting claim landed. So the both-succeed case is
// judged structurally, by the order in which the two ref writes reached the
// backend: lease-then-claim is a sequential force destroy (fine);
// claim-then-lease is the claim guard failing (fatal). The accepted
// both-succeed ordering is therefore exactly "lease:holder-b" before "claim"
// in rec's ref-write log; with that judgment the test ran -count=200 (4000
// races) green on macOS on 2026-10-01, closing the 2026-09-25 flake.
func TestForceDestroyStillClaimGuards(t *testing.T) {
	for i := 0; i < 20; i++ {
		w := newWS(t)
		rec := &refWriteRecorder{Backend: w.Store.B}
		w.Store.B = rec
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
				writes := rec.refWrites(store.RefKey("app", "contested"))
				lease, claim := indexOf(writes, "lease:holder-b"), indexOf(writes, "claim")
				if lease < 0 || claim < 0 || lease > claim {
					t.Fatalf("iter %d: force destroy won but AcquireLease also claimed success, and the lease write did not precede the Deleting claim (ref writes: %v)", i, writes)
				}
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

// refWriteRecorder wraps a backend and records, in order, every successful
// PutIf of a ref key as a short label: "claim" for a Deleting-claim write,
// "lease:<holder>" for a lease write, "other" otherwise. Only the ordering
// of writes to one key is ever asserted on.
type refWriteRecorder struct {
	store.Backend
	mu     sync.Mutex
	writes map[string][]string
}

func (r *refWriteRecorder) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := r.Backend.PutIf(key, data, ifMatch)
	if err != nil || !strings.HasPrefix(key, "refs/") {
		return etag, err
	}
	var ref store.Ref
	label := "other"
	if json.Unmarshal(data, &ref) == nil {
		switch {
		case ref.Deleting:
			label = "claim"
		case ref.LeaseHolder != "":
			label = "lease:" + ref.LeaseHolder
		}
	}
	r.mu.Lock()
	if r.writes == nil {
		r.writes = map[string][]string{}
	}
	r.writes[key] = append(r.writes[key], label)
	r.mu.Unlock()
	return etag, nil
}

// DeleteIf keeps the wrapped backend's conditional-delete capability visible
// through the wrapper (store.DeleteRefIf type-asserts for it).
func (r *refWriteRecorder) DeleteIf(key, ifMatch string) error {
	if cd, ok := r.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return r.Backend.Delete(key)
}

func (r *refWriteRecorder) refWrites(key string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.writes[key]...)
}

func indexOf(xs []string, want string) int {
	for i, x := range xs {
		if x == want {
			return i
		}
	}
	return -1
}

// TestRenewingHolderSurvivesAStrandedDestroyClaim: a `destroy --force`
// killed in its quiesce leaves its claim on a branch a session holds, and
// ClearStaleDeleteClaims clears it only once it is staleDeletingClaimAfter
// (30 s) old, longer than the session's 30 s lease. The session's
// renewals, replayed here on their own clock every TTL/3, each write over
// the claim and leave it set, as on a branch without one, so the lease is
// live for the whole time the claim stands: status says active, an
// unforced destroy is refused, and once the janitor clears the claim
// another writer is still refused and the holder's next renewal goes
// through. Skipping renewals under the claim instead lets the lease lapse
// before the claim is cleared, and the first acquirer after the clear
// fences the session.
func TestRenewingHolderSurvivesAStrandedDestroyClaim(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ttl, every := DefaultLeaseTTL, DefaultLeaseTTL/3
	// The session took the lease 40 s ago; the destroy claimed the branch a
	// second later and was killed.
	start := time.Now().Add(-40 * time.Second)
	l, err := w.Store.AcquireLease("app", "work", "daemon-a", ttl, start)
	if err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "work")
	if err != nil {
		t.Fatal(err)
	}
	ref.Deleting, ref.DeletingAt = true, start.Add(time.Second).UTC().Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "work", ref, etag); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		next, err := w.Store.RenewLease(l, ttl, start.Add(time.Duration(i)*every))
		if err != nil {
			t.Fatalf("renewal %d under the claim: %v, want it renewed", i, err)
		}
		l = next
	}
	if state, err := w.BranchState("app", "work"); err != nil || state != "active" {
		t.Fatalf("branch state under a stranded claim, its holder renewing: %q, %v; want active", state, err)
	}
	if err := w.Destroy("app", "work", false); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("unforced destroy under a stranded claim, its holder renewing: %v, want a live-lease refusal", err)
	}
	cleared, err := w.ClearStaleDeleteClaims(time.Now())
	if err != nil || len(cleared) != 1 || cleared[0] != "app@work" {
		t.Fatalf("ClearStaleDeleteClaims = %v, %v; want [app@work]", cleared, err)
	}
	if _, err := w.Store.AcquireLease("app", "work", "checkpoint:other/1/0123abcd", ttl, time.Now()); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("acquire right after the stale claim was cleared: %v, want ErrLeaseHeld", err)
	}
	if _, err := w.Store.RenewLease(l, ttl, time.Now()); err != nil {
		t.Fatalf("the holder's first renewal after the clear: %v", err)
	}
}

// otherClaimBeforeRefDelete stands in for a second destroy of the same
// branch: ahead of the first delete of refKey it runs claim, which writes
// that destroy's claim over the one on the ref, as an older binary (which
// claims over any claim) or a destroy that found this claim stale does.
// With fail set, every delete of refKey then fails with a store error that
// is not a lost compare-and-swap, as a throttled S3 DeleteObject does;
// otherwise the wrapped backend's delete runs as it is (conditional on
// Local, where it loses its compare-and-swap to that write).
type otherClaimBeforeRefDelete struct {
	store.Backend
	refKey string
	fail   bool
	once   sync.Once
	claim  func()
}

func (b *otherClaimBeforeRefDelete) DeleteIf(key, ifMatch string) error {
	if key == b.refKey {
		b.once.Do(b.claim)
		if b.fail {
			return fmt.Errorf("store: s3 delete %s: 503 SlowDown", key)
		}
	}
	if cd, ok := b.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return b.Backend.Delete(key)
}

// TestFailedDestroyLeavesAnotherDestroysClaim: a destroy that fails after
// its claim unwinds its own claim and nothing else. Here a second
// destroy's claim lands over the first's before the first deletes, and the
// first then fails: on Local its conditional delete loses to that write,
// on S3 the delete itself fails. The second destroy may still be between
// its claim and its delete, so its claim must stand and keep acquires off
// (ErrDeleting). Clearing it would let a session or a checkpoint take the
// branch at a new epoch, and on S3 the second destroy's unconditional
// delete would then remove the branch under that live lease.
func TestFailedDestroyLeavesAnotherDestroysClaim(t *testing.T) {
	for _, tc := range []struct {
		name string
		ws   func(*testing.T) *Workspace
		s3   bool
	}{
		{"local", newWS, false},
		{"s3", newWSOnFakeS3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := tc.ws(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			base := w.Store.B
			plain := &store.Store{B: base}
			othersAt := time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)
			b := &otherClaimBeforeRefDelete{Backend: base, refKey: store.RefKey("app", "work"), fail: tc.s3, claim: func() {
				ref, etag, err := plain.GetRef("app", "work")
				if err != nil {
					t.Error(err)
					return
				}
				if !ref.Deleting {
					t.Error("precondition: the first destroy's claim is not on the ref")
				}
				ref.Deleting, ref.DeletingAt = true, othersAt
				if _, err := plain.PutRef("app", "work", ref, etag); err != nil {
					t.Error(err)
				}
			}}
			w.Store.B = b
			err := w.Destroy("app", "work", true)
			w.Store.B = base
			switch {
			case err == nil:
				t.Fatal("the first destroy succeeded; want it failed after its claim")
			case tc.s3 && !strings.Contains(err.Error(), "503 SlowDown"):
				t.Fatalf("the first destroy: %v, want the failed delete", err)
			case !tc.s3 && !errors.Is(err, store.ErrCAS):
				t.Fatalf("the first destroy: %v, want a lost race (ErrCAS)", err)
			}
			ref, _, err := w.Store.GetRef("app", "work")
			if err != nil {
				t.Fatalf("the failed destroy removed the branch: %v", err)
			}
			if !ref.Deleting || ref.DeletingAt != othersAt {
				t.Fatalf("the failed destroy unwound another destroy's claim: deleting %v at %q, want the claim made at %q", ref.Deleting, ref.DeletingAt, othersAt)
			}
			if _, err := w.Store.AcquireLease("app", "work", "daemon-b", DefaultLeaseTTL, time.Now()); !errors.Is(err, store.ErrDeleting) {
				t.Fatalf("acquire under the other destroy's claim: %v, want ErrDeleting", err)
			}
		})
	}
}

// heldRefDelete holds the first delete of refKey until release is closed,
// closing arrived when that delete gets there. Later deletes are not held
// and do not wait for the first.
type heldRefDelete struct {
	store.Backend
	refKey  string
	held    atomic.Bool
	arrived chan struct{}
	release chan struct{}
}

func (b *heldRefDelete) DeleteIf(key, ifMatch string) error {
	if key == b.refKey && b.held.CompareAndSwap(false, true) {
		close(b.arrived)
		<-b.release
	}
	if cd, ok := b.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return b.Backend.Delete(key)
}

// TestDestroyRefusesABranchAnotherDestroyHasClaimed: while one `destroy
// --force` sits between its claim and its delete, a second one is refused
// (ErrDeleting, retryable) and writes nothing; the first then deletes the
// branch. Claiming over the first's claim instead would leave the branch
// with no claim at all once the second failed and unwound its own, and on
// S3 the first's unconditional delete would then remove a branch an
// acquire had taken in between.
func TestDestroyRefusesABranchAnotherDestroyHasClaimed(t *testing.T) {
	for _, tc := range []struct {
		name string
		ws   func(*testing.T) *Workspace
	}{
		{"local", newWS},
		{"s3", newWSOnFakeS3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := tc.ws(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			base := w.Store.B
			b := &heldRefDelete{Backend: base, refKey: store.RefKey("app", "work"), arrived: make(chan struct{}), release: make(chan struct{})}
			w.Store.B = b
			first := make(chan error, 1)
			go func() { first <- w.Destroy("app", "work", true) }()
			select {
			case <-b.arrived:
			case err := <-first:
				t.Fatalf("the first destroy ended before its delete: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("the first destroy never reached its delete")
			}
			claimed, etag, cerr := w.Store.GetRef("app", "work")
			second := w.Destroy("app", "work", true)
			after, afterEtag, aerr := w.Store.GetRef("app", "work")
			close(b.release)
			ferr := <-first
			w.Store.B = base
			if !errors.Is(second, store.ErrDeleting) || errors.Is(second, store.ErrCAS) {
				t.Fatalf("a second destroy --force under the first's claim: %v, want it refused with ErrDeleting", second)
			}
			if cerr != nil || aerr != nil {
				t.Fatalf("reading the claimed ref: %v, %v", cerr, aerr)
			}
			if afterEtag != etag || after.DeletingAt != claimed.DeletingAt {
				t.Fatalf("the refused destroy wrote the ref: claim at %q (etag %q), want %q (etag %q)", after.DeletingAt, afterEtag, claimed.DeletingAt, etag)
			}
			if ferr != nil {
				t.Fatalf("the first destroy: %v", ferr)
			}
			if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("the branch after the first destroy: %v, want it gone", err)
			}
		})
	}
}

// TestDestroyTakesOverAClaimNoDestroyIsUsing: a claim staleDeletingClaimAfter
// old (its destroy was killed, or failed and could not unwind it), or one
// whose DeletingAt cannot be read (which ClearStaleDeleteClaims never
// clears), does not refuse a destroy: it claims over it and deletes.
func TestDestroyTakesOverAClaimNoDestroyIsUsing(t *testing.T) {
	for _, tc := range []struct {
		name string
		at   string
	}{
		{"stale", time.Now().Add(-2 * staleDeletingClaimAfter).UTC().Format(time.RFC3339Nano)},
		{"unreadable", "not a time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			ref, etag, err := w.Store.GetRef("app", "work")
			if err != nil {
				t.Fatal(err)
			}
			ref.Deleting, ref.DeletingAt = true, tc.at
			if _, err := w.Store.PutRef("app", "work", ref, etag); err != nil {
				t.Fatal(err)
			}
			if err := w.Destroy("app", "work", false); err != nil {
				t.Fatalf("destroy over a %s claim: %v", tc.name, err)
			}
			if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("the branch after the destroy: %v, want it gone", err)
			}
		})
	}
}

// renewalBeforeUnwind fails every delete of refKey with a store error
// that is not a lost compare-and-swap, and runs renew ahead of the first
// write of refKey that clears a destroy claim (the failed destroy's
// unwind), so that write loses its compare-and-swap to the renewal.
type renewalBeforeUnwind struct {
	store.Backend
	refKey string
	once   sync.Once
	renew  func()
}

func (b *renewalBeforeUnwind) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.refKey {
		var r store.Ref
		if json.Unmarshal(data, &r) == nil && !r.Deleting {
			b.once.Do(b.renew)
		}
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

func (b *renewalBeforeUnwind) DeleteIf(key, ifMatch string) error {
	if key == b.refKey {
		return fmt.Errorf("store: s3 delete %s: 503 SlowDown", key)
	}
	return b.Backend.(store.ConditionalDeleter).DeleteIf(key, ifMatch)
}

// TestFailedDestroyUnwindsPastARenewal: a destroy whose delete fails
// unwinds its claim even when the holder's renewal (which writes over the
// claim and leaves it set) wins the unwind's first compare-and-swap; the
// unwind re-reads and clears the claim, which is still its own. The
// renewed lease stays, and the operator's retry is not refused as a
// branch another destroy has claimed.
func TestFailedDestroyUnwindsPastARenewal(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ttl := DefaultLeaseTTL
	l, err := w.Store.AcquireLease("app", "work", "daemon-a", ttl, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	base := w.Store.B
	plain := &store.Store{B: base}
	renewed := false
	b := &renewalBeforeUnwind{Backend: base, refKey: store.RefKey("app", "work"), renew: func() {
		next, err := plain.RenewLease(l, ttl, time.Now().Add(time.Millisecond))
		if err != nil {
			t.Errorf("the holder's renewal over the claim: %v", err)
			return
		}
		l, renewed = next, true
	}}
	w.Store.B = b
	derr := w.Destroy("app", "work", true)
	w.Store.B = base
	if derr == nil || !strings.Contains(derr.Error(), "503 SlowDown") {
		t.Fatalf("destroy: %v, want the failed delete", derr)
	}
	if !renewed {
		t.Fatal("precondition: no renewal landed ahead of the unwind")
	}
	ref, _, err := w.Store.GetRef("app", "work")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Deleting {
		t.Fatalf("the failed destroy left its claim: deleting at %q", ref.DeletingAt)
	}
	exp, perr := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
	if perr != nil || ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch || !exp.Equal(l.Expiry) {
		t.Fatalf("after the unwind: holder %q epoch %d until %s; want the renewed lease", ref.LeaseHolder, ref.Epoch, ref.LeaseExpiry)
	}
	if err := w.Destroy("app", "work", true); err != nil {
		t.Fatalf("the retried destroy --force: %v", err)
	}
}
