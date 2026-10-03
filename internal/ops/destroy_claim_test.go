package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
// old (its destroy was killed, or failed and could not unwind it), one
// whose DeletingAt cannot be read (which ClearStaleDeleteClaims never
// clears), or one stamped further ahead of this host's clock than
// deleteClaimClockSlack (a host whose clock runs ahead, or is set far in
// the future, wrote it) does not refuse a destroy: it claims over it and
// deletes.
func TestDestroyTakesOverAClaimNoDestroyIsUsing(t *testing.T) {
	for _, tc := range []struct {
		name string
		at   string
	}{
		{"stale", time.Now().Add(-2 * staleDeletingClaimAfter).UTC().Format(time.RFC3339Nano)},
		{"unreadable", "not a time"},
		{"future-dated", time.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339Nano)},
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

// claimLandsThenFails is landsThenFails on a destroy's claim writes of
// refKey (the first one lands and reports failure), with the delete kept
// as the wrapped backend has it: conditional on Local, unconditional on S3.
//
// It embeds only store.Backend, so it hides Local's store.SettledWriter,
// and that is what lets it run over Local at all: a real local write that
// reports failure wrote nothing, and landedClaim reports it at once
// (TestDestroyReportsAFailedClaimWriteAtOnceOnALocalStore). Over Local it
// stands for a backend that has a conditional delete but does not settle
// its writes, so the "local-unsettled" variants below drive landedClaim's
// re-read path into deleteClaimedRef's conditional delete; they are not
// what a local store does.
type claimLandsThenFails struct{ *landsThenFails }

func newClaimLandsThenFails(base store.Backend, refKey string, err error) claimLandsThenFails {
	return claimLandsThenFails{&landsThenFails{Backend: base, err: err, match: func(key string, data []byte) bool {
		var r store.Ref
		return key == refKey && json.Unmarshal(data, &r) == nil && r.Deleting
	}}}
}

func (b claimLandsThenFails) DeleteIf(key, ifMatch string) error {
	if cd, ok := b.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return b.Backend.Delete(key)
}

// TestDestroyGoesOnWhenItsClaimLandedButReportedFailure: a destroy's claim
// write that landed but reported failure (the S3 SDK's retry answering 412
// or 409 to its own landed first attempt, which the store reports as a lost
// race, or a timeout that lost the response) leaves this call's own claim
// on the ref, stamped with its DeletingAt. The destroy recognises the claim
// as its own and deletes the branch, past a renewal that lands over the
// claim before it hears back, and the holder's next renewal finds the
// branch gone. Reporting a lost race instead would leave the claim behind,
// and the retry the error asks for would be refused under it as "already
// being destroyed" until it was 30 s old. Only S3 does this; the
// "local-unsettled" variant runs the same path over Local's conditional
// delete through a wrapper (see claimLandsThenFails).
func TestDestroyGoesOnWhenItsClaimLandedButReportedFailure(t *testing.T) {
	lost := fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS)
	timeout := errors.New("store: s3 conditional put refs/app/work: context deadline exceeded")
	for _, bk := range []struct {
		name string
		ws   func(*testing.T) *Workspace
	}{{"local-unsettled", newWS}, {"s3", newWSOnFakeS3}} {
		for _, tc := range []struct {
			name  string
			err   error
			renew bool
		}{
			{"412 after landing", lost, false},
			{"timeout after landing", timeout, false},
			{"412 after landing and a renewal", lost, true},
			{"timeout after landing and a renewal", timeout, true},
		} {
			t.Run(bk.name+"/"+tc.name, func(t *testing.T) {
				w := bk.ws(t)
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
				b := newClaimLandsThenFails(base, store.RefKey("app", "work"), tc.err)
				if s, ok := any(b).(store.SettledWriter); ok && s.WritesSettled() {
					t.Fatal("precondition: claimLandsThenFails settles its writes, so landedClaim would never re-read the ref")
				}
				if tc.renew {
					b.after = func() {
						next, err := plain.RenewLease(l, ttl, time.Now().Add(time.Millisecond))
						if err != nil {
							t.Errorf("the holder's renewal over the landed claim: %v", err)
							return
						}
						l, renewed = next, true
					}
				}
				w.Store.B = b
				derr := w.Destroy("app", "work", true)
				w.Store.B = base
				if n := b.hits.Load(); n != 1 {
					t.Fatalf("precondition: %d claim writes landed and reported failure, want 1", n)
				}
				if tc.renew && !renewed {
					t.Fatal("precondition: no renewal landed over the claim")
				}
				if derr != nil {
					t.Fatalf("destroy --force whose claim landed but reported failure: %v", derr)
				}
				if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("the branch after the destroy: %v, want it gone", err)
				}
				if _, err := plain.RenewLease(l, ttl, time.Now()); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("the holder's next renewal: %v, want ErrNotFound", err)
				}
			})
		}
	}
}

// TestDestroyUnwindsALandedClaimItCannotCarryThrough: a destroy whose claim
// landed but reported failure, and whose ref then moved in a way that
// stops the destroy (any write but a renewal over the claim, or, without
// --force, a renewal that makes the lease live again), fails as it would
// had the same write landed before its delete, and clears its own claim
// first, so the retry its error asks for is not refused as a branch another
// destroy has claimed. As in TestDestroyGoesOnWhenItsClaimLandedButReportedFailure,
// the "local-unsettled" variant is Local behind claimLandsThenFails, not
// a path a local store takes.
func TestDestroyUnwindsALandedClaimItCannotCarryThrough(t *testing.T) {
	for _, bk := range []struct {
		name string
		ws   func(*testing.T) *Workspace
	}{{"local-unsettled", newWS}, {"s3", newWSOnFakeS3}} {
		for _, tc := range []struct {
			name    string
			force   bool
			lapsed  bool
			moveRef func(t *testing.T, s *store.Store, l *store.Lease)
			want    error
			check   func(t *testing.T, ref store.Ref, l store.Lease)
		}{
			{
				name:  "a touch over the claim",
				force: true,
				moveRef: func(t *testing.T, s *store.Store, _ *store.Lease) {
					ref, etag, err := s.GetRef("app", "work")
					if err != nil {
						t.Fatal(err)
					}
					ref.Touch(time.Now().Add(time.Second))
					if _, err := s.PutRef("app", "work", ref, etag); err != nil {
						t.Errorf("the touch over the landed claim: %v", err)
					}
				},
				want: store.ErrCAS,
				check: func(t *testing.T, ref store.Ref, _ store.Lease) {
					if ref.TouchedAt == "" {
						t.Fatal("the touch over the claim was undone")
					}
				},
			},
			{
				name:   "a renewal that makes a lapsed lease live, without --force",
				force:  false,
				lapsed: true,
				moveRef: func(t *testing.T, s *store.Store, l *store.Lease) {
					next, err := s.RenewLease(*l, DefaultLeaseTTL, time.Now())
					if err != nil {
						t.Errorf("the holder's renewal over the landed claim: %v", err)
						return
					}
					*l = next
				},
				want: store.ErrLeaseHeld,
				check: func(t *testing.T, ref store.Ref, l store.Lease) {
					exp, perr := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
					if perr != nil || ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch || !exp.Equal(l.Expiry) {
						t.Fatalf("after the refusal: holder %q epoch %d until %s; want the renewed lease", ref.LeaseHolder, ref.Epoch, ref.LeaseExpiry)
					}
				},
			},
		} {
			t.Run(bk.name+"/"+tc.name, func(t *testing.T) {
				w := bk.ws(t)
				if err := w.Create("app"); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
					t.Fatal(err)
				}
				at := time.Now()
				if tc.lapsed {
					at = at.Add(-2 * DefaultLeaseTTL)
				}
				l, err := w.Store.AcquireLease("app", "work", "daemon-a", DefaultLeaseTTL, at)
				if err != nil {
					t.Fatal(err)
				}
				base := w.Store.B
				plain := &store.Store{B: base}
				b := newClaimLandsThenFails(base, store.RefKey("app", "work"), fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS))
				if s, ok := any(b).(store.SettledWriter); ok && s.WritesSettled() {
					t.Fatal("precondition: claimLandsThenFails settles its writes, so landedClaim would never re-read the ref")
				}
				b.after = func() { tc.moveRef(t, plain, &l) }
				w.Store.B = b
				derr := w.Destroy("app", "work", tc.force)
				w.Store.B = base
				if n := b.hits.Load(); n != 1 {
					t.Fatalf("precondition: %d claim writes landed and reported failure, want 1", n)
				}
				if !errors.Is(derr, tc.want) {
					t.Fatalf("destroy: %v, want %v", derr, tc.want)
				}
				ref, _, err := w.Store.GetRef("app", "work")
				if err != nil {
					t.Fatalf("the branch after the failed destroy: %v", err)
				}
				if ref.Deleting {
					t.Fatalf("the failed destroy left its landed claim: deleting at %q", ref.DeletingAt)
				}
				tc.check(t, ref, l)
				if err := w.Destroy("app", "work", true); err != nil {
					t.Fatalf("the retried destroy --force: %v", err)
				}
			})
		}
	}
}

// TestDeleteClaimsStampedAheadOfTheClock: a destroy claim stamped further
// ahead of this host's clock than deleteClaimClockSlack says nothing about
// whether its destroy is still running, so the janitor clears it as it does
// a stale claim, rather than leave acquires and destroys refused for as
// long as the writer's clock is ahead (for good, for a stamp far in the
// future). A claim stamped only a little ahead, within the slack that
// ordinary skew between hosts needs, is live: a destroy is refused under it
// and the janitor leaves it.
func TestDeleteClaimsStampedAheadOfTheClock(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for br, at := range map[string]time.Time{
		"far-ahead":  now.Add(24 * time.Hour),
		"ahead":      now.Add(deleteClaimClockSlack + 10*time.Second),
		"just-ahead": now.Add(deleteClaimClockSlack / 2),
	} {
		if _, err := w.Fork("app", "main", br, "", 0, nil); err != nil {
			t.Fatal(err)
		}
		ref, etag, err := w.Store.GetRef("app", br)
		if err != nil {
			t.Fatal(err)
		}
		ref.Deleting, ref.DeletingAt = true, at.Format(time.RFC3339Nano)
		if _, err := w.Store.PutRef("app", br, ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Destroy("app", "just-ahead", true); !errors.Is(err, store.ErrDeleting) {
		t.Fatalf("destroy under a claim stamped within the clock slack: %v, want ErrDeleting", err)
	}
	cleared, err := w.ClearStaleDeleteClaims(now)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(cleared)
	if want := []string{"app@ahead", "app@far-ahead"}; strings.Join(cleared, ",") != strings.Join(want, ",") {
		t.Fatalf("cleared = %v, want %v", cleared, want)
	}
	for br, deleting := range map[string]bool{"far-ahead": false, "ahead": false, "just-ahead": true} {
		ref, _, err := w.Store.GetRef("app", br)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Deleting != deleting {
			t.Fatalf("%s after the janitor: deleting %v, want %v", br, ref.Deleting, deleting)
		}
	}
	if _, err := w.AcquireLease("app", "far-ahead", "holder-a", DefaultLeaseTTL); err != nil {
		t.Fatalf("acquire once the far-ahead claim is cleared: %v", err)
	}
}

// TestDestroyDeletesPastAReapingClaimClearedUnderIt: a destroy claims a
// branch the reaper had claimed (Reaping set), and the reaper's unwind of
// its own claim lands before the destroy deletes (an older binary's reaper
// unwinds even under a live destroy claim). That write moves the etag a
// local store's conditional delete compares against, but changes nothing
// the destroy relies on: its own claim still keeps acquires off. So the
// destroy re-reads the ref and deletes the branch, rather than failing as a
// lost race so that neither of them deletes it.
func TestDestroyDeletesPastAReapingClaimClearedUnderIt(t *testing.T) {
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
	ref.Reaping = true
	if _, err := w.Store.PutRef("app", "work", ref, etag); err != nil {
		t.Fatal(err)
	}
	base := w.Store.B
	plain := &store.Store{B: base}
	// otherClaimBeforeRefDelete runs the reaper's unwind ahead of the delete.
	b := &otherClaimBeforeRefDelete{Backend: base, refKey: store.RefKey("app", "work"), claim: func() {
		ref, etag, err := plain.GetRef("app", "work")
		if err != nil {
			t.Error(err)
			return
		}
		if !ref.Deleting || !ref.Reaping {
			t.Errorf("precondition: deleting %v, reaping %v; want the destroy's claim over the reaping claim", ref.Deleting, ref.Reaping)
		}
		ref.Reaping = false
		if _, err := plain.PutRef("app", "work", ref, etag); err != nil {
			t.Error(err)
		}
	}}
	w.Store.B = b
	derr := w.Destroy("app", "work", false)
	w.Store.B = base
	if derr != nil {
		t.Fatalf("destroy past the reaper's unwind: %v", derr)
	}
	if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the branch after the destroy: %v, want it gone", err)
	}
}

// claimLandsLate holds back a destroy's first claim write of refKey and
// reports err for it at once, as a destroy on S3 hears when the SDK's retry
// of a PutObject meets its own first attempt still in flight (a 409, which
// the store reports as a lost compare-and-swap) or when the request times
// out with the write still on its way. The held write lands, conditional
// on the etag it was sent with, right after the landAfter-th read of refKey
// that follows (never, with landAfter 0). With other set, other runs
// before the failure is reported: another writer's write that landed first,
// which a definite 412 answers.
type claimLandsLate struct {
	store.Backend
	refKey    string
	err       error
	landAfter int
	other     func()

	mu      sync.Mutex
	held    func() error
	reads   int
	landed  bool
	landErr error
}

func (b *claimLandsLate) PutIf(key string, data []byte, ifMatch string) (string, error) {
	var r store.Ref
	if key == b.refKey && json.Unmarshal(data, &r) == nil && r.Deleting {
		b.mu.Lock()
		first := b.held == nil
		if first {
			b.held = func() error {
				_, err := b.Backend.PutIf(key, data, ifMatch)
				return err
			}
		}
		b.mu.Unlock()
		if first {
			if b.other != nil {
				b.other()
			}
			return "", b.err
		}
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

func (b *claimLandsLate) Get(key string) ([]byte, string, error) {
	data, etag, err := b.Backend.Get(key)
	if key == b.refKey {
		b.mu.Lock()
		if b.held != nil && !b.landed {
			b.reads++
			if b.reads == b.landAfter {
				b.landErr, b.landed = b.held(), true
			}
		}
		b.mu.Unlock()
	}
	return data, etag, err
}

func (b *claimLandsLate) DeleteIf(key, ifMatch string) error {
	if cd, ok := b.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return b.Backend.Delete(key)
}

// WritesSettled keeps the wrapped backend's answer visible through the
// wrapper (ops.landedClaim type-asserts for it): true on Local, whose
// failed writes never land, and false on S3.
func (b *claimLandsLate) WritesSettled() bool {
	s, ok := b.Backend.(store.SettledWriter)
	return ok && s.WritesSettled()
}

// TestDestroyWaitsForAClaimWriteStillInFlight: on S3, a destroy's claim
// write that reported failure without a verdict (a 409 against its own
// first attempt still in flight, or a timeout) can land after the
// destroy's first re-read of the ref. While the ref still reads as it did
// before the claim write, the destroy re-reads it a few more times over a
// short window, and once its claim shows up it goes on and deletes the
// branch. Reporting a lost race at the first re-read instead would leave
// the claim to land with no destroy running, and the retry the error asks
// for would be refused as "already being destroyed" for 30 s. A claim that
// never shows up fails the destroy with an error that says it may still
// land. A claim write refused because another write landed first (a 412:
// the ref has moved) is a definite loss, reported at the first re-read. A
// local store's failed writes never land
// (TestDestroyReportsAFailedClaimWriteAtOnceOnALocalStore).
func TestDestroyWaitsForAClaimWriteStillInFlight(t *testing.T) {
	defer func(d time.Duration) { claimSettleEvery = d }(claimSettleEvery)
	claimSettleEvery = 10 * time.Millisecond
	conflict := fmt.Errorf("%w: refs/app/work: ConditionalRequestConflict", store.ErrCAS)
	timeout := errors.New("store: s3 conditional put refs/app/work: context deadline exceeded")
	for _, tc := range []struct {
		name      string
		err       error
		landAfter int
		other     bool
	}{
		{"409, lands after the first re-read", conflict, 1, false},
		{"timeout, lands after the first re-read", timeout, 1, false},
		{"409, lands before the last re-read", conflict, claimSettleReads, false},
		{"409, never lands", conflict, 0, false},
		{"timeout, never lands", timeout, 0, false},
		{"412 after another write landed", fmt.Errorf("%w: refs/app/work", store.ErrCAS), 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWSOnFakeS3(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			base := w.Store.B
			plain := &store.Store{B: base}
			b := &claimLandsLate{Backend: base, refKey: store.RefKey("app", "work"), err: tc.err, landAfter: tc.landAfter}
			if tc.other {
				b.other = func() {
					if _, err := plain.AcquireLease("app", "work", "daemon-b", DefaultLeaseTTL, time.Now()); err != nil {
						t.Errorf("the other write: %v", err)
					}
				}
			}
			w.Store.B = b
			derr := w.Destroy("app", "work", false)
			w.Store.B = base
			if b.landErr != nil {
				t.Fatalf("precondition: the held claim write did not land: %v", b.landErr)
			}
			ref, _, gerr := w.Store.GetRef("app", "work")
			switch {
			case tc.landAfter > 0:
				if !b.landed {
					t.Fatalf("precondition: the held claim write never landed (%d reads)", b.reads)
				}
				if derr != nil {
					t.Fatalf("destroy whose claim landed after its first re-read: %v", derr)
				}
				if !errors.Is(gerr, store.ErrNotFound) {
					t.Fatalf("the branch after the destroy: %v, want it gone", gerr)
				}
			case tc.other:
				if !errors.Is(derr, store.ErrCAS) || strings.Contains(derr.Error(), "may still land") {
					t.Fatalf("destroy that lost its claim write to another write: %v, want a lost race", derr)
				}
				if b.reads != 1 {
					t.Fatalf("destroy re-read the ref %d times after a definite loss, want 1", b.reads)
				}
				if gerr != nil || ref.Deleting || ref.LeaseHolder != "daemon-b" {
					t.Fatalf("the branch after the lost race: deleting %v, holder %q, %v; want the other write's lease and no claim", ref.Deleting, ref.LeaseHolder, gerr)
				}
			default:
				if derr == nil || !strings.Contains(derr.Error(), "may still land") {
					t.Fatalf("destroy whose claim write never landed: %v, want an error saying it may still land", derr)
				}
				if errors.Is(tc.err, store.ErrCAS) != errors.Is(derr, store.ErrCAS) {
					t.Fatalf("destroy whose claim write never landed: %v, want it to wrap %v", derr, tc.err)
				}
				if b.reads != 1+claimSettleReads {
					t.Fatalf("destroy re-read the ref %d times, want %d", b.reads, 1+claimSettleReads)
				}
				if gerr != nil || ref.Deleting {
					t.Fatalf("the branch after the failed destroy: deleting %v, %v", ref.Deleting, gerr)
				}
			}
		})
	}
}

// TestDestroyReportsAFailedClaimWriteAtOnceOnALocalStore: a local store
// settles every write before it returns (store.SettledWriter), so a claim
// write that reported failure wrote nothing and never will. The destroy
// reports the failure at once, without re-reading the ref or saying the
// claim may still land, even when the ref reads as it did before the claim
// write: a lock timeout, or a compare-and-swap lost to a write that was
// then undone to the same bytes. Its retry goes through. On S3 the same
// failure can be a write still on its way
// (TestDestroyWaitsForAClaimWriteStillInFlight).
func TestDestroyReportsAFailedClaimWriteAtOnceOnALocalStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		// undone has another write land on the ref, and be undone, before
		// the claim write fails.
		undone bool
	}{
		{"lock timeout", errors.New("store: lock timeout on refs/app/work.lock (if no offshoot process is running, delete this file)"), false},
		{"compare-and-swap lost to a write since undone", fmt.Errorf("%w: etag mismatch", store.ErrCAS), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			base := w.Store.B
			plain := &store.Store{B: base}
			_, sent, err := plain.GetRef("app", "work")
			if err != nil {
				t.Fatal(err)
			}
			b := &claimLandsLate{Backend: base, refKey: store.RefKey("app", "work"), err: tc.err}
			if tc.undone {
				b.other = func() {
					for _, protected := range []bool{true, false} {
						ref, etag, err := plain.GetRef("app", "work")
						if err != nil {
							t.Error(err)
							return
						}
						ref.Protected = protected
						if _, err := plain.PutRef("app", "work", ref, etag); err != nil {
							t.Errorf("the other write: %v", err)
							return
						}
					}
				}
			}
			w.Store.B = b
			derr := w.Destroy("app", "work", false)
			w.Store.B = base
			if b.reads != 0 {
				t.Fatalf("destroy re-read the ref %d times after a local claim write failed, want none", b.reads)
			}
			if derr == nil || strings.Contains(derr.Error(), "may still land") {
				t.Fatalf("destroy whose local claim write failed: %v, want the failure without a warning that the claim may still land", derr)
			}
			if errors.Is(tc.err, store.ErrCAS) != errors.Is(derr, store.ErrCAS) {
				t.Fatalf("destroy whose local claim write failed: %v, want it to wrap %v", derr, tc.err)
			}
			ref, etag, err := w.Store.GetRef("app", "work")
			if err != nil || ref.Deleting || etag != sent {
				t.Fatalf("the branch after the failed destroy: deleting %v, at its etag before the claim %v, %v", ref.Deleting, etag == sent, err)
			}
			if err := w.Destroy("app", "work", false); err != nil {
				t.Fatalf("the retried destroy: %v", err)
			}
			if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("the branch after the retried destroy: %v, want it gone", err)
			}
		})
	}
}

// slowClaim holds up a destroy's claim write of refKey for hold before
// passing it on, as response timeouts and SDK retries hold up a PutObject
// on S3, and hides the wrapped backend's conditional delete, as S3 has
// none: a destroy's delete of refKey is a plain Delete. It embeds only
// store.Backend, so it hides the wrapped backend's store.SettledWriter
// too, as S3 does not implement it: over Local (or a claimLandsLate over
// Local, which forwards Local's answer) a claim write that reports failure
// is settled by landedClaim's re-reads, as on S3, never reported at once.
// beforeDelete, when set, runs as that delete is sent, before it lands:
// what other hosts do while it is on its way. Run under synctest, so holds
// take no real time.
type slowClaim struct {
	store.Backend
	refKey       string
	hold         time.Duration
	beforeDelete func()
	deletes      int
}

func (b *slowClaim) PutIf(key string, data []byte, ifMatch string) (string, error) {
	var r store.Ref
	if key == b.refKey && json.Unmarshal(data, &r) == nil && r.Deleting {
		time.Sleep(b.hold)
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

func (b *slowClaim) Delete(key string) error {
	if key == b.refKey {
		b.deletes++
		if b.beforeDelete != nil {
			b.beforeDelete()
		}
	}
	return b.Backend.Delete(key)
}

// TestDestroyDeletesOnlyWhileItsClaimStands: on S3, where the delete is
// unconditional, a destroy's claim is all that keeps an acquire from
// taking the branch before the delete lands, and the claim stands only
// until it is staleDeletingClaimAfter old, when the janitor on any host
// clears it as abandoned. A destroy held up until its claim is that old,
// or within deleteClaimMargin of it, by the time it would send its delete
// (its claim write held up by response timeouts and SDK retries, whether
// the write then succeeds or reports a timeout and lands during the
// destroy's settle reads) deletes nothing: it clears its own claim and
// fails, retryable, and the retry goes through. Sending the delete would
// let the janitor clear the claim and an acquire take the branch at a new
// epoch while it is on its way, and it would then remove the branch under
// that lease. A destroy that reaches its delete with more than the margin
// left deletes the branch, with acquires refused throughout.
func TestDestroyDeletesOnlyWhileItsClaimStands(t *testing.T) {
	timeout := errors.New("store: s3 conditional put refs/app/work: context deadline exceeded")
	inTime := staleDeletingClaimAfter - deleteClaimMargin
	for _, tc := range []struct {
		name string
		// hold is how long the claim write takes; with lands set, it
		// then reports a timeout and lands after the destroy's second
		// re-read of the ref (landedClaim).
		hold  time.Duration
		lands bool
		// holdDelete is how long the delete takes to land once sent.
		holdDelete time.Duration
		deletes    bool
	}{
		{"claim lands after a timeout, already stale", 61 * time.Second, true, 0, false},
		{"claim write succeeds stale", staleDeletingClaimAfter + time.Second, false, 0, false},
		{"claim write succeeds inside the margin, delete lands after the claim is stale", inTime + time.Second, false, deleteClaimMargin, false},
		{"claim write succeeds in time", inTime - time.Second, false, 0, true},
		{"claim lands after a timeout, in time", inTime - 3*time.Second, true, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			synctest.Test(t, func(t *testing.T) {
				base := w.Store.B
				plain := &store.Store{B: base}
				janitor := &Workspace{Store: plain, Root: w.Root}
				refKey := store.RefKey("app", "work")
				var late *claimLandsLate
				b := &slowClaim{Backend: base, refKey: refKey, hold: tc.hold}
				if tc.lands {
					late = &claimLandsLate{Backend: base, refKey: refKey, err: timeout, landAfter: 2}
					b.Backend = late
				}
				var cleared []string
				var acqErr error
				b.beforeDelete = func() {
					time.Sleep(tc.holdDelete)
					var err error
					if cleared, err = janitor.ClearStaleDeleteClaims(time.Now()); err != nil {
						t.Errorf("the janitor while the delete is on its way: %v", err)
					}
					_, acqErr = plain.AcquireLease("app", "work", "holder-b", DefaultLeaseTTL, time.Now())
				}
				w.Store.B = b
				derr := w.Destroy("app", "work", false)
				w.Store.B = base
				if late != nil && (!late.landed || late.landErr != nil) {
					t.Fatalf("precondition: the held claim write landed %v (%v)", late.landed, late.landErr)
				}
				ref, _, gerr := w.Store.GetRef("app", "work")
				if !tc.deletes {
					if derr == nil {
						t.Fatalf("destroy whose claim is too old to delete under: deleted the branch (janitor cleared %v, holder-b acquire: %v)", cleared, acqErr)
					}
					if !errors.Is(derr, store.ErrCAS) {
						t.Fatalf("destroy whose claim is too old to delete under: %v, want a retryable lost race", derr)
					}
					if b.deletes != 0 {
						t.Fatalf("destroy sent %d deletes under a claim too old to delete under, want none", b.deletes)
					}
					if gerr != nil || ref.Deleting {
						t.Fatalf("the branch after the destroy gave up: deleting %v, %v; want it there without the claim", ref.Deleting, gerr)
					}
					b.hold, b.beforeDelete = 0, nil
					w.Store.B = b
					derr = w.Destroy("app", "work", false)
					w.Store.B = base
					if derr != nil {
						t.Fatalf("the retried destroy: %v", derr)
					}
					if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
						t.Fatalf("the branch after the retried destroy: %v, want it gone", err)
					}
					return
				}
				if derr != nil {
					t.Fatalf("destroy that reached its delete in time: %v", derr)
				}
				if b.deletes != 1 {
					t.Fatalf("destroy sent %d deletes, want 1", b.deletes)
				}
				if len(cleared) != 0 || !errors.Is(acqErr, store.ErrDeleting) {
					t.Fatalf("while the delete was on its way: janitor cleared %v, holder-b acquire %v; want nothing cleared and the acquire refused", cleared, acqErr)
				}
				if !errors.Is(gerr, store.ErrNotFound) {
					t.Fatalf("the branch after the destroy: %v, want it gone", gerr)
				}
			})
		})
	}
}

// TestDestroyChecksItsClaimsAgeAfterTheQuiesce: the checkout quiesce runs
// between a destroy's claim and its delete, and a quiesce held up (a busy
// checkout's busy timeout, a slow WAL checkpoint, a suspended process)
// ages the claim as a slow claim write does. One that ends with the claim
// within deleteClaimMargin of going stale sends no delete: the destroy
// clears its own claim and fails, retryable, and the retry goes through.
// Checking the claim's age before the quiesce instead would send S3's
// unconditional delete under a claim the janitor may clear, and an
// acquire take the branch, before it lands. A quiesce that ends with more
// than the margin left deletes the branch.
func TestDestroyChecksItsClaimsAgeAfterTheQuiesce(t *testing.T) {
	inTime := staleDeletingClaimAfter - deleteClaimMargin
	for _, tc := range []struct {
		name    string
		hold    time.Duration
		deletes bool
	}{
		{"quiesce ends inside the margin", inTime + time.Second, false},
		{"quiesce ends in time", inTime - time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			path := w.CheckoutPath("app", "work")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			defer func(q func(string) error) { destroyQuiesce = q }(destroyQuiesce)
			synctest.Test(t, func(t *testing.T) {
				base := w.Store.B
				// slowClaim with no hold: the claim write goes straight
				// through, and the delete is S3's unconditional one.
				b := &slowClaim{Backend: base, refKey: store.RefKey("app", "work")}
				quiesced := 0
				destroyQuiesce = func(string) error {
					quiesced++
					time.Sleep(tc.hold)
					return nil
				}
				w.Store.B = b
				derr := w.Destroy("app", "work", false)
				w.Store.B = base
				if quiesced != 1 {
					t.Fatalf("precondition: the destroy quiesced the checkout %d times, want 1", quiesced)
				}
				ref, _, gerr := w.Store.GetRef("app", "work")
				if tc.deletes {
					if derr != nil || b.deletes != 1 || !errors.Is(gerr, store.ErrNotFound) {
						t.Fatalf("destroy whose quiesce ended in time: %v, %d deletes, branch %v; want it deleted", derr, b.deletes, gerr)
					}
					return
				}
				if !errors.Is(derr, store.ErrCAS) {
					t.Fatalf("destroy whose quiesce ended inside the margin: %v, want a retryable lost race", derr)
				}
				if b.deletes != 0 {
					t.Fatalf("destroy sent %d deletes after a quiesce that ended inside the margin, want none", b.deletes)
				}
				if gerr != nil || ref.Deleting {
					t.Fatalf("the branch after the destroy gave up: deleting %v, %v; want it there without the claim", ref.Deleting, gerr)
				}
				destroyQuiesce = func(string) error { return nil }
				w.Store.B = b
				derr = w.Destroy("app", "work", false)
				w.Store.B = base
				if derr != nil || b.deletes != 1 {
					t.Fatalf("the retried destroy: %v, %d deletes", derr, b.deletes)
				}
				if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("the branch after the retried destroy: %v, want it gone", err)
				}
			})
		})
	}
}
