package ops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// checkpointHolderPrefix marks a lease held by an at-rest checkpoint, so a
// refusal can say another checkpoint is in progress rather than send the
// user looking for a session to close.
const checkpointHolderPrefix = "checkpoint:"

// newCheckpointHolder is the lease holder for one at-rest checkpoint call:
// "checkpoint:<host>/<pid>/<8 hex>" (see newHolder in lease.go).
func newCheckpointHolder() string {
	return newHolder("checkpoint")
}

// IsCheckpointHolder reports whether a lease holder is an at-rest
// checkpoint's (see newCheckpointHolder). Exported for the session
// package's settle classification; isCheckpointHolder is kept as an
// unexported alias so this package's own call sites need no churn.
func IsCheckpointHolder(holder string) bool {
	return strings.HasPrefix(holder, checkpointHolderPrefix)
}

// isCheckpointHolder is IsCheckpointHolder, unexported for this package's
// own call sites.
func isCheckpointHolder(holder string) bool {
	return IsCheckpointHolder(holder)
}

// leaseTTL is LeaseTTL, or DefaultLeaseTTL when it is not set.
func (o CheckpointOptions) leaseTTL() time.Duration {
	if o.LeaseTTL > 0 {
		return o.LeaseTTL
	}
	return DefaultLeaseTTL
}

// renewEvery is RenewEvery, or a third of ttl when it is not set: the
// interval a daemon session renews at, which leaves the lease two missed
// renewals of slack.
func (o CheckpointOptions) renewEvery(ttl time.Duration) time.Duration {
	if o.RenewEvery > 0 {
		return o.RenewEvery
	}
	if every := ttl / 3; every > 0 {
		return every
	}
	return time.Millisecond
}

// ErrDetachedCheckout is what a checkpoint refused for a detached checkout
// unwraps to: the branch was repointed (rollback, promote, compact) after
// the checkout was materialized, so checkpointing it would revert that
// repoint. Its message is CLI-shaped; MCP's offshoot_checkpoint, which has
// no force, turns it into the tool calls an agent has.
var ErrDetachedCheckout = errors.New("ops: checkout is detached")

// detachedError is checkpointPreconditions' refusal of a detached
// checkout: its own message, unwrapping to ErrDetachedCheckout.
type detachedError struct{ msg string }

func (e *detachedError) Error() string { return e.msg }
func (e *detachedError) Unwrap() error { return ErrDetachedCheckout }

// checkpointPreconditions are the checks CheckpointWith runs on the ref
// its lease acquire reads, before that acquire writes
// (store.AcquireLeaseRefIf), so the ref the lease lands on is the ref that
// passed them: the name is new on the branch, a checkout exists, and the
// checkout is not detached (unless opts.Force).
func checkpointPreconditions(db, branch, name, path string, ref store.Ref, opts CheckpointOptions) error {
	if _, exists := ref.Checkpoints[name]; exists {
		return fmt.Errorf("ops: checkpoint %q already exists on %s@%s", name, db, branch)
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("ops: no checkout for %s@%s (run 'offshoot checkout %s@%s' first)", db, branch, db, branch)
	}
	// A detached checkout embodies a lineage the branch no longer points
	// at: a promote, rollback or compact repointed the ref but could not
	// refresh this file (it was busy, or the process died in between).
	// Checkpointing it would snapshot the OLD content onto the NEW lineage
	// and silently undo that repoint, so refuse and name the ways out.
	if rec, ok := readSidecar(path); ok && rec.Lineage != ref.Lineage && !opts.Force {
		return &detachedError{fmt.Sprintf("ops: checkout of %s@%s is detached: the branch was repointed (now at txid %d) after this checkout was materialized, so checkpointing it would revert that repoint; run 'offshoot checkout %s@%s' to refresh it (discarding its local edits), 'offshoot export' to keep them as a file, or pass --force to checkpoint it anyway",
			db, branch, ref.HeadTXID, db, branch)}
	}
	return nil
}

// checkpointAcquireAttempts bounds acquireCheckpointLease: the acquire and
// one retry.
const checkpointAcquireAttempts = 2

// acquireCheckpointLease takes db@branch's lease for holder, a holder unique
// to this call (newCheckpointHolder), and returns it with the ref it is part
// of. check is CheckpointWith's refusals (a live lease, a taken name, a
// missing or detached checkout); it runs on the ref each acquire reads,
// before that acquire writes (store.AcquireLeaseRefIf), so a refusal writes
// nothing, and a writer that moves the ref after the check fails the
// acquire's compare-and-swap. A failed acquire write is settled by
// re-reading the ref (SettleAcquire, which session.Open shares):
//
//   - our own holder means the acquire landed though it reported failure:
//     the S3 SDK's retry answering 412 to its own landed first attempt
//     (which the store reports as a lost acquisition race), or a timeout
//     that lost the response. Nothing else writes this holder, so the
//     lease, under the epoch the landed write minted, is ours. It is
//     adopted from the re-read ref as it stands once check passes on that
//     ref, with no second acquire: a write that just landed and reported
//     failure can do so again;
//   - a live lease held by anyone else, or a branch mid-destroy or
//     mid-reap, is refused with refuseIfHeld's own message, so a second
//     checkpoint reads the same refusal whichever check caught it;
//   - no live lease, with the epoch and lineage the failed acquire read,
//     means the compare-and-swap lost to a write that took no lease
//     (touch, protect, a TTL change), so the retry is a fresh acquire;
//   - no live lease under a newer epoch means another writer took the
//     lease and let it go in between: a checkpoint that ran start to
//     finish, or a session that flushed and closed (a new lineage means a
//     repoint landed). That is the concurrent writer the lease exists to
//     turn away, so it is refused as a lease-held error, the refusal this
//     call would have met a moment earlier, rather than retried into a
//     second checkpoint.
//
// A refusal by check of a ref that carries our own holder (one being
// adopted, or a retry's read finding an earlier acquire that landed late)
// releases that lease, so a checkpoint that never started does not leave
// the branch refused to every writer for a whole TTL.
func (w *Workspace) acquireCheckpointLease(db, branch, holder string, ttl time.Duration, check func(store.Ref) error) (store.Lease, store.Ref, error) {
	// read is the ref the latest acquire read, and refused what check
	// returned on it; readOK is false when that read itself failed.
	var read store.Ref
	var readOK bool
	var refused error
	checked := func(r store.Ref) error {
		read, readOK = r, true
		refused = check(r)
		return refused
	}
	var err error
	for attempt := 1; ; attempt++ {
		readOK, refused = false, nil
		lease, ref, _, aerr := w.Store.AcquireLeaseRefIf(db, branch, holder, ttl, time.Now(), checked)
		if aerr == nil {
			return lease, ref, nil
		}
		if refused != nil {
			if read.LeaseHolder == holder {
				w.releaseCheckpointLease(leaseOn(db, branch, holder, read))
			}
			return store.Lease{}, store.Ref{}, refused
		}
		if !readOK && attempt == 1 {
			// No branch, or a store that cannot be read: nothing was
			// written, and the error is the read's own.
			return store.Lease{}, store.Ref{}, aerr
		}
		err = aerr
		if !readOK {
			break
		}
		adoptedLease, cur, adopted, serr := w.SettleAcquire(db, branch, holder, aerr)
		if serr != nil {
			break
		}
		if adopted {
			if cerr := check(cur); cerr != nil {
				w.releaseCheckpointLease(adoptedLease)
				return store.Lease{}, store.Ref{}, cerr
			}
			return adoptedLease, cur, nil
		}
		if rerr := refuseIfHeld(db, branch, cur, "checkpoint", false); rerr != nil {
			return store.Lease{}, store.Ref{}, rerr
		}
		if errors.Is(err, store.ErrLeaseHeld) && (cur.Epoch != read.Epoch || cur.Lineage != read.Lineage) {
			return store.Lease{}, store.Ref{}, &leaseHeldError{msg: fmt.Sprintf("ops: %s@%s was written by another checkpoint, session or repoint while this checkpoint took its lease (the head is now txid %d); retry", db, branch, cur.HeadTXID)}
		}
		if attempt < checkpointAcquireAttempts && errors.Is(err, store.ErrLeaseHeld) {
			continue
		}
		break
	}
	return store.Lease{}, store.Ref{}, fmt.Errorf("ops: checkpoint %s@%s: %w", db, branch, err)
}

// SettleAcquire settles an acquire of db@branch's lease for holder that
// returned acquireErr, by re-reading the ref. A failed acquire write can
// still have landed: the S3 SDK's retry answering 412 to its own first
// attempt that landed (reported as a lost acquisition race), or a timeout
// that lost the response. holder must be unique to this acquire
// (NewSessionHolder, newCheckpointHolder), since nothing else writes it:
//
//   - the ref names holder: the acquire landed, and the lease, under the
//     epoch and expiry the ref records, is the caller's (adopted is true);
//   - the ref names any other holder, or none: the acquire did not land,
//     and the caller decides what that ref means for it (adopted is
//     false, err nil);
//   - the re-read failed: whether the acquire landed is unknown, and err
//     says why. A lease that did land lapses at its expiry.
//
// It compares the holder exactly; a prefix never decides ownership.
func (w *Workspace) SettleAcquire(db, branch, holder string, acquireErr error) (lease store.Lease, ref store.Ref, adopted bool, err error) {
	ref, _, err = w.Store.GetRef(db, branch)
	if err != nil {
		return store.Lease{}, store.Ref{}, false, fmt.Errorf("ops: %s@%s: re-reading the ref after a failed lease acquire (%v): %w", db, branch, acquireErr, err)
	}
	if ref.LeaseHolder == holder {
		return leaseOn(db, branch, holder, ref), ref, true, nil
	}
	return store.Lease{}, ref, false, nil
}

// leaseOn is holder's lease on db@branch as ref records it.
func leaseOn(db, branch, holder string, ref store.Ref) store.Lease {
	expiry, _ := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
	return store.Lease{DB: db, Branch: branch, Holder: holder, Epoch: ref.Epoch, Expiry: expiry}
}

// releaseCheckpointLease is the last step of a checkpoint that failed after
// its acquire: a best-effort release, so the next writer need not wait out
// the TTL. ReleaseLease clears the lease only while it is still ours, so a
// lease someone else took (ErrLeaseLost) or a branch that is gone
// (ErrNotFound) is left alone silently; a release lost to a concurrent
// metadata write is retried. A branch mid-destroy or mid-reap is left alone
// too: it is on its way out, and a release written over the claim would
// change the etag Destroy's conditional delete compares against and fail
// that destroy. Anything else is logged with the expiry the ref carries
// (renewals have moved it past the one l was acquired with), and the lease
// then expires at that time and the next acquirer reclaims it with a
// higher epoch.
func (w *Workspace) releaseCheckpointLease(l store.Lease) {
	var err error
	for i := 0; i < 3; i++ {
		if ref, _, gerr := w.Store.GetRef(l.DB, l.Branch); gerr == nil {
			if ref.Deleting || ref.Reaping {
				return
			}
			if ref.LeaseHolder == l.Holder && ref.Epoch == l.Epoch {
				if expiry, perr := time.Parse(time.RFC3339Nano, ref.LeaseExpiry); perr == nil {
					l.Expiry = expiry
				}
			}
		}
		if err = w.Store.ReleaseLease(l); err == nil || !errors.Is(err, store.ErrCAS) {
			break
		}
	}
	if err != nil && !errors.Is(err, store.ErrLeaseLost) && !errors.Is(err, store.ErrNotFound) {
		fmt.Fprintf(os.Stderr, "offshoot: could not release the checkpoint lease on %s@%s (it expires at %s): %v\n",
			l.DB, l.Branch, l.Expiry.Format(time.RFC3339), err)
	}
}

// errCheckpointRenewStopped is the cancel cause stop records when the
// checkpoint itself ends the renewals, so a terminal renewal error stays
// distinguishable from a normal stop.
var errCheckpointRenewStopped = errors.New("ops: checkpoint lease renewals stopped")

// checkpointRenewer keeps an at-rest checkpoint's lease alive while it
// quiesces, encodes and uploads, the way a session's renewLoop does
// (internal/session/renew.go): a renewal that finds the lease gone
// (ErrLeaseLost) or the branch destroyed (ErrNotFound) is terminal and
// cancels ctx with that error as its cause; any other error, a store error
// or a compare-and-swap lost to another write of the ref, is retried on
// the next tick. A renewal under a `destroy --force` claim writes over it
// and leaves it set (store.RenewLease); the destroy's delete gets past
// that (deleteClaimedRef), and the next renewal finds the branch gone and
// ends the checkpoint.
type checkpointRenewer struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// checkpointRenewTerminalForTest, when non-nil, runs in a checkpoint's
// renewer with the terminal renewal error it has just cancelled the
// checkpoint with. A test that holds the checkpoint (its upload, or the
// after-quiesce hook) waits on it: the held checkpoint reads nothing, so
// only the renewer can see a lost lease or a destroyed branch, and the test
// must know the checkpoint's next lease check will see it before it lets
// the checkpoint go on. It runs after the cancel for that reason: run
// before it, a checkpoint released at once could stop the renewals first
// and never hear of the loss. Test-only; process-global, restore via
// t.Cleanup (as checkpointAfterQuiesceForTest).
var checkpointRenewTerminalForTest func(error)

// startCheckpointRenewer renews l every every, each renewal extending it
// by ttl, until stop is called or a renewal is terminal.
func (w *Workspace) startCheckpointRenewer(l store.Lease, ttl, every time.Duration) *checkpointRenewer {
	ctx, cancel := context.WithCancelCause(context.Background())
	r := &checkpointRenewer{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			next, err := w.Store.RenewLease(l, ttl, time.Now())
			switch {
			case err == nil:
				l = next
			case renewErrTerminal(err):
				cancel(err)
				if checkpointRenewTerminalForTest != nil {
					checkpointRenewTerminalForTest(err)
				}
				return
			}
		}
	}()
	return r
}

// renewErrTerminal reports whether a renewal error ends the checkpoint.
func renewErrTerminal(err error) bool {
	return errors.Is(err, store.ErrLeaseLost) || errors.Is(err, store.ErrNotFound)
}

// lost is the terminal renewal error, or nil while the lease is held.
func (r *checkpointRenewer) lost() error {
	if c := context.Cause(r.ctx); c != nil && c != errCheckpointRenewStopped {
		return c
	}
	return nil
}

// stop ends the renewals and waits for the goroutine to exit, so no
// renewal is in flight when the caller writes the ref (Session.Close joins
// its renewLoop before ReleaseLease for the same reason). It returns the
// terminal renewal error, if one ended the renewals first. Safe to call
// more than once.
func (r *checkpointRenewer) stop() error {
	r.cancel(errCheckpointRenewStopped)
	<-r.done
	return r.lost()
}

// checkpointCommit is what the head write of an at-rest checkpoint needs:
// the checkpoint, the lease it holds, and the lineage and txid it encoded.
type checkpointCommit struct {
	db, branch, name string
	meta             map[string]string
	kind             string
	lease            store.Lease
	lineage          string
	txid             uint64
}

// premise reports whether cur is still the ref this checkpoint planned
// from, give or take lease bookkeeping and metadata: ours, and no destroy
// or reap claim. Destroy's claim leaves the lease fields alone, so only the
// claim itself shows that a `destroy --force` is underway; a head written
// over it would change the etag Destroy's conditional delete compares
// against (failing that destroy), or, on S3, be deleted a moment after the
// checkpoint reported it committed.
func (c checkpointCommit) premise(cur store.Ref) bool {
	return c.ours(cur) && !cur.Deleting && !cur.Reaping
}

// ours reports whether cur carries our holder and epoch, the lineage we
// encoded against and the head one txid below ours: the premise, with any
// destroy or reap claim on top left out.
func (c checkpointCommit) ours(cur store.Ref) bool {
	return cur.LeaseHolder == c.lease.Holder && cur.Epoch == c.lease.Epoch &&
		cur.Lineage == c.lineage && cur.HeadTXID == c.txid-1
}

// headUnderOurEpoch reports whether cur's head is at or past our txid, on
// our lineage, under the epoch only our lease minted. No offshoot writer
// of this version puts a head there, but an older binary's `checkpoint
// --force` plans under whatever epoch the ref carries and so computes our
// very key, overwrites our object with its own and makes it the head: an
// object that head may name is not ours to delete.
func (c checkpointCommit) headUnderOurEpoch(cur store.Ref) bool {
	return cur.Lineage == c.lineage && cur.HeadEpoch == c.lease.Epoch && cur.HeadTXID >= c.txid
}

// advance is cur with the head moved to this checkpoint's object, the
// checkpoint recorded, the activity clock stamped and the lease released:
// the one ref write that commits an at-rest checkpoint. HeadEpoch moves to
// our epoch in the same write as HeadTXID, so every identity check that
// compares against HeadEpoch (sidecar, segmentShadow, BranchStateAt) sees
// a consistent pair.
func (c checkpointCommit) advance(cur store.Ref) store.Ref {
	next := cur
	next.HeadTXID, next.HeadEpoch = c.txid, c.lease.Epoch
	next.Checkpoints = maps.Clone(cur.Checkpoints)
	next.SetCheckpoint(c.name, store.Checkpoint{TXID: c.txid, Epoch: c.lease.Epoch, CreatedAt: nowStamp(), Meta: c.meta, Kind: c.kind})
	next.LeaseHolder, next.LeaseExpiry = "", ""
	next.Touch(time.Now())
	return next
}

// lostTo is the error for a checkpoint whose premise failed: its lease was
// taken (a reclaim after expiry), cleared (a forced repoint, `lease
// release`), or the branch moved under it, all of which wrap
// store.ErrLeaseLost; or the branch is being destroyed or reaped, which
// wrap store.ErrDeleting and store.ErrReaping. A branch that moved while
// our holder and epoch are still on it moved under our live lease, which
// only a writer that ignores the lease can do; that is said, rather than
// reported as a lease lost to our own holder.
func (c checkpointCommit) lostTo(cur store.Ref) error {
	switch {
	case cur.Deleting:
		return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w", c.name, c.db, c.branch, store.ErrDeleting)
	case cur.Reaping:
		return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w", c.name, c.db, c.branch, store.ErrReaping)
	case cur.LeaseHolder == c.lease.Holder && cur.Epoch == c.lease.Epoch:
		return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w: the branch moved under this checkpoint's own lease (it is now at lineage %s, head txid %d), which only a writer that ignores the lease can do, such as an older offshoot binary's `checkpoint --force`; upgrade every binary that writes this store, then retry",
			c.name, c.db, c.branch, store.ErrLeaseLost, cur.Lineage, cur.HeadTXID)
	}
	return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w (the branch is now at lineage %s, head txid %d, lease held by %q at epoch %d); retry",
		c.name, c.db, c.branch, store.ErrLeaseLost, cur.Lineage, cur.HeadTXID, cur.LeaseHolder, cur.Epoch)
}

// claimedAfterWrite is the error for a checkpoint whose head write was sent
// and failed, and whose branch then took a destroy or reap claim with
// nothing else moved. The claim fails the premise, but unlike every other
// change that fails it, it can be undone to the very bytes it was put on: a
// destroy whose quiesce or conditional delete fails unwinds its claim, and
// so do the janitor and the reaper for a stale one. The ref's etag is then
// the one the write was sent against, and the store may still apply that
// write, so the object is kept and the checkpoint may have committed.
func (c checkpointCommit) claimedAfterWrite(cur store.Ref, writeErr error) error {
	claim, verb := store.ErrDeleting, "destroyed"
	if !cur.Deleting {
		claim, verb = store.ErrReaping, "reaped"
	}
	return fmt.Errorf("ops: checkpoint %q on %s@%s may have committed: its head write failed (%w), and the branch is now being %s (%w); if that is abandoned, the ref goes back to exactly what the write was sent against and the store may still apply it, so its object is kept",
		c.name, c.db, c.branch, writeErr, verb, claim)
}

// unconfirmed is the error for a checkpoint that could not read the ref to
// commit; a destroyed branch is named as such.
func (c checkpointCommit) unconfirmed(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("ops: checkpoint %q: %s@%s was destroyed while the checkpoint ran: %w", c.name, c.db, c.branch, err)
	}
	return fmt.Errorf("ops: checkpoint %q on %s@%s: reading the ref to commit: %w", c.name, c.db, c.branch, err)
}

// renewLost is the error for a checkpoint whose renewals found its lease
// gone or its branch destroyed before its head write; it wraps the
// renewal's error, so both stay testable with errors.Is.
func (c checkpointCommit) renewLost(err error) error {
	return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: its lease ended while it ran: %w", c.name, c.db, c.branch, err)
}

// mayHaveCommitted is the error for a checkpoint whose head write was sent
// and failed, when the ref cannot settle whether that write landed or
// still will: why says what is unknown, and err, the failure behind it, is
// wrapped. The object is kept in every such case (see commitCheckpoint).
func (c checkpointCommit) mayHaveCommitted(why string, err error) error {
	return fmt.Errorf("ops: checkpoint %q on %s@%s may have committed: %s: %w", c.name, c.db, c.branch, why, err)
}

// checkpointCommitAttempts bounds the head write. With the premise intact,
// a lost compare-and-swap can only be a writer that left our lease, lineage
// and head alone (touch, protect, a TTL change); each lands once, so three
// attempts ride out a burst of them.
const checkpointCommitAttempts = 3

// landed reports whether cur records this checkpoint: our name at our txid
// and private epoch, on our lineage. Only our own head write can have put
// that entry there, so it is how a write whose response was lost (the S3
// SDK's retry answering 412 to its own first attempt, a timeout) is
// recognised as committed.
func (c checkpointCommit) landed(cur store.Ref) bool {
	cp, ok := cur.Checkpoints[c.name]
	return ok && cur.Lineage == c.lineage && cp.TXID == c.txid && cp.Epoch == c.lease.Epoch
}

// unreferenced reports whether cur proves that no head write naming our
// object ever landed: a lineage's head only advances, so the same lineage
// with its head still below txid means none did.
func (c checkpointCommit) unreferenced(cur store.Ref) bool {
	return cur.Lineage == c.lineage && cur.HeadTXID < c.txid
}

// commitCheckpoint is the head write: it re-reads the ref, checks the
// premise, and advances the head in one write that also records the
// checkpoint and releases the lease, re-reading and reapplying up to
// checkpointCommitAttempts times when a write fails with the premise still
// intact. On error, deletable reports whether our object provably is
// unreferenced and will stay so; otherwise it is left to GC, which keeps an
// object above the head at the ref's own epoch and reclaims it once the
// next acquire bumps the epoch, if no head write ever named it.
//
// Before any head write is sent, nothing of ours can name the object, so a
// failed premise deletes it, unless the head has moved to our txid under
// our own epoch (headUnderOurEpoch), which only an older binary writing
// under our lease can do and which may name our key.
//
// Once a head write has been sent, the object is deleted only when the ref
// proves that no write of ours landed and none still can: the same lineage
// with its head below our txid (none landed, since a lineage's head only
// advances), changed in a way that cannot be undone (our lease taken or
// cleared, the epoch bumped), so the etag every write was sent against is
// gone for good. A store can apply a write after the client has given up
// on it: a timeout or a 5xx gets no verdict at all, and on S3 a 409
// conflict, which the store reports as a lost compare-and-swap like a 412,
// can be the SDK's retry colliding with its own first attempt still in
// flight. So the object is kept, and the error says so, when
//
//   - only a destroy or reap claim fails the premise (claimedAfterWrite):
//     an abandoned claim is undone to the very bytes the write was sent
//     against;
//   - the branch moved to another lineage (a repoint), or the ref could
//     not be read: a write that landed first may be what a fork now reads
//     through;
//   - every attempt failed with the premise intact: the ref may still
//     carry the etag an unsettled write was sent against.
//
// session flush never deletes after a write without a verdict, for the
// same reason.
func (w *Workspace) commitCheckpoint(c checkpointCommit) (committed store.Ref, deletable bool, err error) {
	attempts := 0
	var lastErr, unsettled error
	for {
		cur, etag, gerr := w.Store.GetRef(c.db, c.branch)
		if gerr != nil {
			if attempts == 0 || errors.Is(gerr, store.ErrNotFound) {
				return store.Ref{}, attempts == 0, c.unconfirmed(gerr)
			}
			return store.Ref{}, false, c.mayHaveCommitted(fmt.Sprintf("its head write failed (%v), and the ref could not be re-read to tell whether it landed, so its object is kept", lastErr), gerr)
		}
		if attempts > 0 && c.landed(cur) {
			return cur, false, nil
		}
		if !c.premise(cur) {
			switch {
			case attempts == 0 && c.headUnderOurEpoch(cur):
				return store.Ref{}, false, fmt.Errorf("%w; its object is kept, since that head, under this checkpoint's own epoch, may name it", c.lostTo(cur))
			case attempts == 0:
				return store.Ref{}, true, c.lostTo(cur)
			case c.ours(cur):
				return store.Ref{}, false, c.claimedAfterWrite(cur, lastErr)
			case c.unreferenced(cur):
				return store.Ref{}, true, c.lostTo(cur)
			}
			return store.Ref{}, false, c.mayHaveCommitted(fmt.Sprintf("its head write failed, and the branch moved before the checkpoint could re-read it (it is now at lineage %s, head txid %d), so whether that write landed first cannot be told; its object is kept, since a fork may read through it", cur.Lineage, cur.HeadTXID), lastErr)
		}
		if attempts == checkpointCommitAttempts {
			if unsettled != nil {
				return store.Ref{}, false, c.mayHaveCommitted("a head write failed without a verdict from the store, which may still apply it, so its object is kept (a retry under the same name is refused as already existing if it did)", unsettled)
			}
			// The premise holds, so the head is still txid-1 on our
			// lineage, but the ref may still carry the etag a refused
			// write was sent against, and on S3 a refusal (a 409) does not
			// rule out an earlier attempt of that write still landing.
			return store.Ref{}, false, fmt.Errorf("ops: checkpoint %q on %s@%s: the head write lost %d compare-and-swaps to concurrent ref writes (retry; its object is left for GC, since on S3 a conflict answer does not rule out an earlier attempt of the write still landing, in which case a retry under the same name is refused as already existing): %w",
				c.name, c.db, c.branch, attempts, lastErr)
		}
		next := c.advance(cur)
		attempts++
		_, perr := w.Store.PutRef(c.db, c.branch, next, etag)
		if perr == nil {
			return next, false, nil
		}
		lastErr = perr
		if !errors.Is(perr, store.ErrCAS) {
			unsettled = perr
		}
	}
}

// putCheckpointObject is the create-only put of data at key, a key under an
// epoch only this checkpoint's lease minted; it returns the etag the
// post-commit check compares against. An object already there is ours when
// its bytes are ours: the S3 SDK retries a PutObject whose response was
// lost, and the retry's If-None-Match then fails against our own first
// attempt. An object with other bytes is corruption, since nothing else
// writes this key, so it is an error and is left in place for inspection.
// A put refused as a taken key with nothing readable there (on S3, a 409
// against an earlier attempt still in flight, or a read-back that failed)
// is a retryable failure, and whatever lands later is left to GC. Any
// other put error may still have landed the object, which nothing names,
// so it is deleted.
func (w *Workspace) putCheckpointObject(key string, data []byte) (string, error) {
	etag, err := w.Store.B.PutIf(key, data, "")
	if err == nil {
		return etag, nil
	}
	if !errors.Is(err, store.ErrCAS) {
		w.bestEffortDelete(key)
		return "", fmt.Errorf("ops: upload checkpoint object %s: %w", key, err)
	}
	got, gotEtag, gerr := w.Store.B.Get(key)
	switch {
	case gerr == nil && bytes.Equal(got, data):
		return gotEtag, nil
	case gerr == nil:
		return "", fmt.Errorf("ops: checkpoint object %s already exists under this checkpoint's own epoch, which nothing else writes; refusing to overwrite it (store corruption?): %w", key, err)
	case errors.Is(gerr, store.ErrNotFound):
		return "", fmt.Errorf("ops: upload checkpoint object %s: the create-only put reported the key taken, but nothing is there (on S3, a conflict with an earlier attempt of the same put still in flight); retry: %w", key, err)
	}
	return "", fmt.Errorf("ops: upload checkpoint object %s: the create-only put reported the key taken (%w), and reading it back failed: %w; retry", key, err, gerr)
}
