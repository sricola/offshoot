package ops

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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
// "checkpoint:<host>/<pid>/<8 hex>". It keeps LocalHolder's <host>/<pid>,
// which `lease list` and status print, and adds a per-call nonce:
// AcquireLease treats the same holder on a live lease as a self-renew with
// no epoch bump, so two checkpoints in one process sharing a holder would
// share an epoch, and with it an object key, and race again.
func newCheckpointHolder() string {
	var nonce [4]byte
	_, _ = rand.Read(nonce[:]) // crypto/rand.Read does not return an error since Go 1.24
	return checkpointHolderPrefix + LocalHolder() + "/" + hex.EncodeToString(nonce[:])
}

// isCheckpointHolder reports whether a lease holder is an at-rest
// checkpoint's (see newCheckpointHolder).
func isCheckpointHolder(holder string) bool {
	return strings.HasPrefix(holder, checkpointHolderPrefix)
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

// checkpointPreconditions are the checks CheckpointWith runs on the ref it
// first reads and again on the ref its lease acquire returns, since a
// session can flush and close, or a repoint land, in between: the name is
// new on the branch, a checkout exists, and the checkout is not detached
// (unless opts.Force).
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
		return fmt.Errorf("ops: checkout of %s@%s is detached: the branch was repointed (now at txid %d) after this checkout was materialized, so checkpointing it would revert that repoint; run 'offshoot checkout %s@%s' to refresh it (discarding its local edits), 'offshoot export' to keep them as a file, or pass --force to checkpoint it anyway",
			db, branch, ref.HeadTXID, db, branch)
	}
	return nil
}

// checkpointAcquireAttempts bounds acquireCheckpointLease: the acquire and
// one retry.
const checkpointAcquireAttempts = 2

// acquireCheckpointLease takes db@branch's lease for holder, a holder unique
// to this call (newCheckpointHolder), and returns it with the ref the
// acquire wrote. first is the ref CheckpointWith checked before the
// acquire. A failed acquire is settled by re-reading the ref:
//
//   - a live lease held by anyone else, or a branch mid-destroy or
//     mid-reap, is refused with refuseIfHeld's own message, so a second
//     checkpoint reads the same refusal whichever check caught it;
//   - our own holder means the acquire landed though it reported failure:
//     the S3 SDK's retry answering 412 to its own landed first attempt
//     (which the store reports as a lost acquisition race), or a timeout
//     that lost the response. Nothing else writes this holder, so the
//     lease is ours, and the retry adopts it as an idempotent self-renew
//     under the epoch the landed write minted;
//   - no live lease, with the epoch and lineage first had, means the
//     compare-and-swap lost to a write that took no lease (touch, protect,
//     a TTL change), so the retry is a fresh acquire;
//   - no live lease under a newer epoch means another writer took the
//     lease and let it go in between: a checkpoint that ran start to
//     finish, or a session that flushed and closed (a new lineage means a
//     repoint landed). That is the concurrent writer the lease exists to
//     turn away, so it is refused as a lease-held error, the refusal this
//     call would have met a moment earlier, rather than retried into a
//     second checkpoint.
//
// A lease of ours still on the ref when the attempts run out is released,
// so a checkpoint that never started does not leave the branch refused to
// every writer for a whole TTL.
func (w *Workspace) acquireCheckpointLease(db, branch, holder string, ttl time.Duration, first store.Ref) (store.Lease, store.Ref, error) {
	var err error
	for attempt := 1; ; attempt++ {
		var lease store.Lease
		var ref store.Ref
		if lease, ref, _, err = w.Store.AcquireLeaseRef(db, branch, holder, ttl, time.Now()); err == nil {
			return lease, ref, nil
		}
		cur, _, gerr := w.Store.GetRef(db, branch)
		if gerr != nil {
			break
		}
		ours := cur.LeaseHolder == holder
		if !ours {
			if rerr := refuseIfHeld(db, branch, cur, "checkpoint", false); rerr != nil {
				return store.Lease{}, store.Ref{}, rerr
			}
			if errors.Is(err, store.ErrLeaseHeld) && (cur.Epoch != first.Epoch || cur.Lineage != first.Lineage) {
				return store.Lease{}, store.Ref{}, &leaseHeldError{fmt.Sprintf("ops: %s@%s was written by another checkpoint, session or repoint while this checkpoint took its lease (the head is now txid %d); retry", db, branch, cur.HeadTXID)}
			}
		}
		if attempt < checkpointAcquireAttempts && (ours || errors.Is(err, store.ErrLeaseHeld)) {
			continue
		}
		if ours {
			expiry, _ := time.Parse(time.RFC3339Nano, cur.LeaseExpiry)
			w.releaseCheckpointLease(store.Lease{DB: db, Branch: branch, Holder: holder, Epoch: cur.Epoch, Expiry: expiry})
		}
		break
	}
	return store.Lease{}, store.Ref{}, fmt.Errorf("ops: checkpoint %s@%s: %w", db, branch, err)
}

// releaseCheckpointLease is the last step of a checkpoint that failed after
// its acquire: a best-effort release, so the next writer need not wait out
// the TTL. ReleaseLease clears the lease only while it is still ours, so a
// lease someone else took (ErrLeaseLost) or a branch that is gone
// (ErrNotFound) is left alone silently; a release lost to a concurrent
// metadata write is retried. A branch mid-destroy or mid-reap is left alone
// too: it is on its way out, and a release written over the claim would
// change the etag Destroy's conditional delete compares against and fail
// that destroy. Anything else is logged, and the lease then expires after
// its TTL and the next acquirer reclaims it with a higher epoch.
func (w *Workspace) releaseCheckpointLease(l store.Lease) {
	var err error
	for i := 0; i < 3; i++ {
		if ref, _, gerr := w.Store.GetRef(l.DB, l.Branch); gerr == nil && (ref.Deleting || ref.Reaping) {
			return
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
// cancels ctx with that error as its cause; any other error, including
// RenewLease's unretried ErrCAS against a concurrent touch, is retried on
// the next tick, since the lease outlives two missed renewals.
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
// from, give or take lease bookkeeping and metadata: our holder and epoch,
// the lineage we encoded against, the head one txid below ours, and no
// destroy or reap claim. Destroy's claim leaves the lease fields alone, so
// only the claim itself shows that a `destroy --force` is underway; a head
// written over it would change the etag Destroy's conditional delete
// compares against (failing that destroy), or, on S3, be deleted a moment
// after the checkpoint reported it committed.
func (c checkpointCommit) premise(cur store.Ref) bool {
	return cur.LeaseHolder == c.lease.Holder && cur.Epoch == c.lease.Epoch &&
		cur.Lineage == c.lineage && cur.HeadTXID == c.txid-1 &&
		!cur.Deleting && !cur.Reaping
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
// wrap store.ErrDeleting and store.ErrReaping.
func (c checkpointCommit) lostTo(cur store.Ref) error {
	switch {
	case cur.Deleting:
		return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w", c.name, c.db, c.branch, store.ErrDeleting)
	case cur.Reaping:
		return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w", c.name, c.db, c.branch, store.ErrReaping)
	}
	return fmt.Errorf("ops: checkpoint %q on %s@%s did not commit: %w (the branch is now at lineage %s, head txid %d, lease held by %q at epoch %d); retry",
		c.name, c.db, c.branch, store.ErrLeaseLost, cur.Lineage, cur.HeadTXID, cur.LeaseHolder, cur.Epoch)
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
// unreferenced and will stay so: no head write was sent, or the ref shows
// none landed and none still can. Otherwise the object is left to GC and
// the error says the checkpoint may have committed: after a write was
// sent, a ref that moved to another lineage (a repoint) or could not be
// read proves nothing, and a write that landed may be what a fork now
// reads through.
//
// A head write that failed with anything but a lost compare-and-swap (a
// timeout, a 5xx the SDK gave up on) got no verdict from the store, which
// can still apply it for as long as the ref keeps the etag it was sent
// against; session flush never deletes after such an error for the same
// reason. A ref that has changed since settles it, because the etag moved
// on. Giving up with the premise intact does not, since the ref may still
// carry that etag, so the object is kept then too. GC keeps an object
// above the head at the ref's own epoch, and reclaims it once the next
// acquire bumps the epoch if no head write ever named it.
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
			if attempts == 0 || c.unreferenced(cur) {
				return store.Ref{}, true, c.lostTo(cur)
			}
			return store.Ref{}, false, c.mayHaveCommitted(fmt.Sprintf("its head write failed, and the branch moved before the checkpoint could re-read it (it is now at lineage %s, head txid %d), so whether that write landed first cannot be told; its object is kept, since a fork may read through it", cur.Lineage, cur.HeadTXID), lastErr)
		}
		if attempts == checkpointCommitAttempts {
			if unsettled != nil {
				return store.Ref{}, false, c.mayHaveCommitted("a head write failed without a verdict from the store, which may still apply it, so its object is kept (a retry under the same name is refused as already existing if it did)", unsettled)
			}
			// The premise holds, so the head is still txid-1 on our
			// lineage, and the store refused every write outright: nothing
			// names our object, and nothing still can.
			return store.Ref{}, true, fmt.Errorf("ops: checkpoint %q on %s@%s: the head write lost %d compare-and-swaps to concurrent ref writes (retry): %w",
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
// Any other error may still have landed the object, which nothing names,
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
	if got, gotEtag, gerr := w.Store.B.Get(key); gerr == nil && bytes.Equal(got, data) {
		return gotEtag, nil
	}
	return "", fmt.Errorf("ops: checkpoint object %s already exists under this checkpoint's own epoch, which nothing else writes; refusing to overwrite it (store corruption?): %w", key, err)
}
