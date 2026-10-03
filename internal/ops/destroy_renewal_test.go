package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// renewalBeforeDelete holds Destroy's first delete of refKey until a lease
// renewal has written the ref over the destroy's claim: the holder's
// renewer ticking while the destroy quiesces the checkout. It counts each
// successful write of refKey that carries both a destroy claim and a lease
// holder (the claim itself, then each renewal over it), and the held
// delete waits for one more than it saw when it arrived. waited is closed
// once the first delete has been let through, so a caller can tell a
// destroy that reached its delete from one that lost its claim write.
type renewalBeforeDelete struct {
	store.Backend
	refKey  string
	once    sync.Once
	claimed atomic.Int32
	waited  chan struct{}
}

func (b *renewalBeforeDelete) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err == nil && key == b.refKey {
		var r store.Ref
		if json.Unmarshal(data, &r) == nil && r.Deleting && r.LeaseHolder != "" {
			b.claimed.Add(1)
		}
	}
	return etag, err
}

// DeleteIf keeps the wrapped backend's delete as it is: conditional on
// Local (through casGate's DeleteIf), unconditional on S3.
func (b *renewalBeforeDelete) DeleteIf(key, ifMatch string) error {
	if key == b.refKey {
		b.once.Do(func() {
			defer close(b.waited)
			n := b.claimed.Load()
			deadline := time.Now().Add(10 * time.Second)
			for b.claimed.Load() <= n && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	return b.Backend.(store.ConditionalDeleter).DeleteIf(key, ifMatch)
}

func (b *renewalBeforeDelete) reachedDelete() bool {
	select {
	case <-b.waited:
		return true
	default:
		return false
	}
}

// TestDestroyForceAbortsACheckpointWhoseRenewalLandsInItsWindow:
// `destroy --force` during an at-rest checkpoint, with one of the
// checkpoint's lease renewals landing between the destroy's claim and its
// delete, as it does whenever the destroy's quiesce spans a renewal tick.
// The renewal writes over the claim and leaves it set; the destroy still
// deletes the branch, on a local store (whose conditional delete then
// re-reads the ref and deletes again) and on S3 (whose delete is
// unconditional), and the checkpoint's next renewal finds the branch gone,
// so the checkpoint fails without committing. Only a renewal that lands
// before the claim, winning its compare-and-swap, makes the destroy retry.
func TestDestroyForceAbortsACheckpointWhoseRenewalLandsInItsWindow(t *testing.T) {
	for _, tc := range []struct {
		name string
		ws   func(*testing.T) *Workspace
	}{
		{"local", newWS},
		{"s3", newWSOnFakeS3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := tc.ws(t)
			seedDB(t, w, "app", 1<<16)
			mustFork(t, w, "app", "main", "work", "seed")
			mustSQL(t, mustCheckout(t, w, "app", "work"), "INSERT INTO t (v) VALUES (randomblob(100));")
			before := refOf(t, w, "app", "work")
			key := privateSnapshotKey(before)
			terminal := renewTerminal(t)
			g := gateRefCAS(w, "app", "work")
			g.holdObject(key)
			rb := &renewalBeforeDelete{Backend: g, refKey: store.RefKey("app", "work"), waited: make(chan struct{})}
			w.Store.B = rb
			done := make(chan error, 1)
			go func() {
				// A 2 s lease renewed every 2/3 s, the default ttl/3 ratio.
				_, err := w.CheckpointWith("app", "work", "a", nil, CheckpointOptions{Snapshot: true, LeaseTTL: 2 * time.Second})
				done <- err
			}()
			release := heldUpload(t, g, done)
			var derr error
			for i := 0; i < 20; i++ {
				derr = w.Destroy("app", "work", true)
				if derr == nil || rb.reachedDelete() || !errors.Is(derr, store.ErrCAS) {
					break
				}
			}
			if !rb.reachedDelete() {
				t.Fatalf("the destroy never reached its delete: %v", derr)
			}
			if n := rb.claimed.Load(); n < 2 {
				t.Fatalf("no renewal landed over the claim before the delete (%d claimed writes)", n)
			}
			if derr != nil {
				release()
				t.Fatalf("destroy --force with a renewal between its claim and its delete: %v (the checkpoint then returned %v)", derr, <-done)
			}
			if err := awaitRenewTerminal(t, terminal, "the destroyed branch"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("the checkpoint renewer's terminal error: %v, want ErrNotFound", err)
			}
			release()
			if err := <-done; !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("checkpoint of a branch destroyed while it ran: %v, want it failed on the missing branch", err)
			}
			if storeHas(w, key) {
				t.Fatal("the aborted checkpoint's object survived")
			}
			if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("the destroyed branch came back: %v", err)
			}
		})
	}
}

// writeBeforeRefDelete runs before ahead of each of the first n deletes of
// refKey, a write landing between Destroy's claim and each delete attempt,
// and counts every delete of refKey. The backend's delete is kept as it
// is: conditional on Local, unconditional on S3.
type writeBeforeRefDelete struct {
	store.Backend
	refKey  string
	n       atomic.Int32
	deletes atomic.Int32
	before  func()
}

func (b *writeBeforeRefDelete) DeleteIf(key, ifMatch string) error {
	if key == b.refKey {
		b.deletes.Add(1)
		if b.n.Add(-1) >= 0 {
			b.before()
		}
	}
	if cd, ok := b.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return b.Backend.Delete(key)
}

// TestDestroyDeletesPastARenewalOverItsClaim: a write lands between
// Destroy's claim and its delete. A renewal by the holder leaves the claim
// set and moves only the lease expiry, and a forced destroy still deletes
// the branch, on Local by re-reading the ref and deleting again (renewals
// racing each retry are ridden out up to destroyDeleteAttempts), on S3 by
// its unconditional delete; the holder's next renewal then finds the
// branch gone, which ends a session or a checkpoint. An unforced destroy
// that went ahead on a lapsed lease does not delete one its holder renewed
// in that window on Local, where it can see it: it is refused as a live
// lease. Any other write fails the destroy as a lost race (retryable), as
// does a renewal before every attempt; each refusal leaves the branch and
// unwinds its own claim, and only that: another destroy's claim written
// over it (an older binary's, or one that found this claim stale) stays,
// since that destroy may still be between its claim and its delete.
func TestDestroyDeletesPastARenewalOverItsClaim(t *testing.T) {
	type outcome int
	const (
		deleted   outcome = iota
		lostRace          // ErrCAS, branch kept, claim unwound
		leaseHeld         // ErrLeaseHeld, branch and lease kept, claim unwound
	)
	othersAt := time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)
	for _, tc := range []struct {
		name    string
		s3      bool
		force   bool
		lapsed  bool             // the lease lapsed before the destroy read it
		n       int32            // writes, one before each of the first n deletes
		write   func(*store.Ref) // the write; nil is the holder's RenewLease
		want    outcome
		deletes int32 // deletes Destroy sends, when it matters
		// The write is another destroy's claim (DeletingAt othersAt),
		// which the refused destroy must leave on the ref.
		othersClaim bool
	}{
		{name: "forced/local", force: true, n: 1, want: deleted, deletes: 2},
		{name: "forced/s3", s3: true, force: true, n: 1, want: deleted, deletes: 1},
		{name: "renewals racing each retry/local", force: true, n: destroyDeleteAttempts - 1, want: deleted, deletes: destroyDeleteAttempts},
		{name: "a renewal before every attempt/local", force: true, n: 100, want: lostRace, deletes: destroyDeleteAttempts},
		{name: "unforced, the lapsed holder renews/local", lapsed: true, n: 1, want: leaseHeld, deletes: 1},
		{name: "unforced, the lapsed holder renews/s3", s3: true, lapsed: true, n: 1, want: deleted, deletes: 1},
		{name: "touch/local", force: true, n: 1, write: func(r *store.Ref) { r.Touch(time.Now().Add(time.Second)) }, want: lostRace},
		{name: "flush/local", force: true, n: 1, write: func(r *store.Ref) { r.HeadTXID++ }, want: lostRace},
		{name: "release/local", force: true, n: 1, write: func(r *store.Ref) { r.LeaseHolder, r.LeaseExpiry = "", "" }, want: lostRace},
		{name: "another destroy's claim/local", force: true, n: 1, write: func(r *store.Ref) {
			r.DeletingAt = othersAt
		}, want: lostRace, othersClaim: true},
		{name: "a renewal and a touch/local", force: true, n: 1, write: func(r *store.Ref) {
			r.LeaseExpiry = time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
			r.Touch(time.Now().Add(time.Second))
		}, want: lostRace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var w *Workspace
			if tc.s3 {
				w = newWSOnFakeS3(t)
			} else {
				w = newWS(t)
			}
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			ttl := DefaultLeaseTTL
			at := time.Now()
			if tc.lapsed {
				at = at.Add(-time.Hour)
			}
			l, err := w.Store.AcquireLease("app", "work", "daemon-a", ttl, at)
			if err != nil {
				t.Fatal(err)
			}
			base := w.Store.B
			plain := &store.Store{B: base}
			isRenewal := tc.write == nil
			i := 0
			b := &writeBeforeRefDelete{Backend: base, refKey: store.RefKey("app", "work"), before: func() {
				i++
				if isRenewal {
					// A millisecond apart, so no two renewals write the same
					// bytes.
					next, err := plain.RenewLease(l, ttl, time.Now().Add(time.Duration(i)*time.Millisecond))
					if err != nil {
						t.Errorf("the holder's renewal %d over the claim: %v", i, err)
						return
					}
					l = next
					return
				}
				ref, etag, err := plain.GetRef("app", "work")
				if err != nil {
					t.Error(err)
					return
				}
				tc.write(&ref)
				if _, err := plain.PutRef("app", "work", ref, etag); err != nil {
					t.Error(err)
				}
			}}
			b.n.Store(tc.n)
			w.Store.B = b
			derr := w.Destroy("app", "work", tc.force)
			w.Store.B = base
			if tc.deletes != 0 && b.deletes.Load() != tc.deletes {
				t.Errorf("destroy sent %d deletes of the ref, want %d", b.deletes.Load(), tc.deletes)
			}
			ref, _, gerr := w.Store.GetRef("app", "work")
			switch tc.want {
			case deleted:
				if derr != nil || !errors.Is(gerr, store.ErrNotFound) {
					t.Fatalf("destroy: %v (ref read: %v), want the branch deleted", derr, gerr)
				}
				_, err := w.Store.RenewLease(l, ttl, time.Now())
				if !errors.Is(err, store.ErrNotFound) || !renewErrTerminal(err) {
					t.Fatalf("the holder's next renewal: %v, want a terminal ErrNotFound", err)
				}
				return
			case lostRace:
				if !errors.Is(derr, store.ErrCAS) || errors.Is(derr, store.ErrLeaseHeld) {
					t.Fatalf("destroy: %v, want a retryable lost race (ErrCAS)", derr)
				}
			case leaseHeld:
				if !errors.Is(derr, store.ErrLeaseHeld) {
					t.Fatalf("destroy: %v, want a live-lease refusal (ErrLeaseHeld)", derr)
				}
			}
			if gerr != nil {
				t.Fatalf("the refused destroy removed the branch: %v", gerr)
			}
			if tc.othersClaim {
				if !ref.Deleting || ref.DeletingAt != othersAt {
					t.Fatalf("the refused destroy unwound another destroy's claim: deleting %v at %q, want the claim made at %q", ref.Deleting, ref.DeletingAt, othersAt)
				}
				if _, err := w.Store.AcquireLease("app", "work", "daemon-b", ttl, time.Now()); !errors.Is(err, store.ErrDeleting) {
					t.Fatalf("acquire under the other destroy's claim: %v, want ErrDeleting", err)
				}
				return
			}
			if ref.Deleting {
				t.Fatalf("the refused destroy left its claim: deleting at %q", ref.DeletingAt)
			}
			if isRenewal {
				if ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch || !store.LeaseLive(ref, time.Now()) {
					t.Fatalf("after the refused destroy: holder %q epoch %d until %s; want %q's renewed lease at epoch %d", ref.LeaseHolder, ref.Epoch, ref.LeaseExpiry, l.Holder, l.Epoch)
				}
				if _, err := w.Store.RenewLease(l, ttl, time.Now()); err != nil {
					t.Fatalf("the holder's next renewal: %v", err)
				}
			}
		})
	}
}

// failRefPuts fails every put of key while fail is set, with a store error
// that is not a lost compare-and-swap (an S3 503, a timeout), writing
// nothing.
type failRefPuts struct {
	store.Backend
	key  string
	fail bool
}

func (b *failRefPuts) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.key && b.fail {
		return "", fmt.Errorf("store: s3 conditional put %s: 503 SlowDown", key)
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// TestStrandedClaimCostsNoRenewalSlack: a destroy killed after its claim
// (or one whose claim write landed though the store reported it failed)
// leaves the claim until the janitor finds it 30 s old. The holder's
// renewals, replayed here on its own clock every TTL/3, write over it like
// any other renewal, so the lease keeps the two missed renewals of slack
// it has without a claim: with one renewal failing and the next late, or
// with two failing, the lease is still live when the claim is cleared.
// The branch reads active, an unforced destroy is refused, another
// acquirer after the clear is refused, and the holder renews.
func TestStrandedClaimCostsNoRenewalSlack(t *testing.T) {
	ttl, every := DefaultLeaseTTL, DefaultLeaseTTL/3
	for _, tc := range []struct {
		name string
		ago  time.Duration // since the acquire, now
		fail []bool        // per renewal tick since the acquire: fails with a store error
	}{
		// The third tick, due 1.2 s ago, has not run yet.
		{"one failed and the next late", 3*every + 1200*time.Millisecond, []bool{false, true}},
		// The fourth tick is not due yet.
		{"two failed", 3*every + 1500*time.Millisecond, []bool{false, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "work", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			start := time.Now().Add(-tc.ago)
			l, err := w.Store.AcquireLease("app", "work", "daemon-a", ttl, start)
			if err != nil {
				t.Fatal(err)
			}
			ref, etag, err := w.Store.GetRef("app", "work")
			if err != nil {
				t.Fatal(err)
			}
			// The destroy claimed the branch a second after the acquire.
			ref.Deleting, ref.DeletingAt = true, start.Add(time.Second).UTC().Format(time.RFC3339Nano)
			if _, err := w.Store.PutRef("app", "work", ref, etag); err != nil {
				t.Fatal(err)
			}
			base := w.Store.B
			b := &failRefPuts{Backend: base, key: store.RefKey("app", "work")}
			w.Store.B = b
			for i, fail := range tc.fail {
				b.fail = fail
				next, err := w.Store.RenewLease(l, ttl, start.Add(time.Duration(i+1)*every))
				if fail {
					continue
				}
				if err != nil {
					w.Store.B = base
					t.Fatalf("renewal %d under the claim: %v, want it renewed", i+1, err)
				}
				l = next
			}
			w.Store.B = base
			if state, err := w.BranchState("app", "work"); err != nil || state != "active" {
				t.Fatalf("branch state under a stranded claim: %q, %v; want active", state, err)
			}
			if err := w.Destroy("app", "work", false); !errors.Is(err, store.ErrLeaseHeld) {
				t.Fatalf("unforced destroy under a stranded claim: %v, want a live-lease refusal", err)
			}
			cleared, err := w.ClearStaleDeleteClaims(time.Now())
			if err != nil || len(cleared) != 1 || cleared[0] != "app@work" {
				t.Fatalf("ClearStaleDeleteClaims = %v, %v; want [app@work]", cleared, err)
			}
			if _, err := w.Store.AcquireLease("app", "work", "checkpoint:other/1/0123abcd", ttl, time.Now()); !errors.Is(err, store.ErrLeaseHeld) {
				t.Fatalf("acquire right after the stale claim was cleared: %v, want ErrLeaseHeld", err)
			}
			if _, err := w.Store.RenewLease(l, ttl, time.Now()); err != nil {
				t.Fatalf("the holder's renewal after the clear: %v", err)
			}
		})
	}
}
