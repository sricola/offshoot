package ops

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// Reap destroys every branch whose TTL has expired. Per the spec: a live
// lease always defers expiry; protected branches are never reaped; branches
// without a TTL live until destroyed. The claim is CAS'd so a concurrent
// touch and reap have exactly one winner.
func (w *Workspace) Reap(now time.Time) ([]string, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	var reaped []string
	var firstErr error
	dbs := make([]string, 0, len(refs))
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		branches := append([]string(nil), refs[db]...)
		sort.Strings(branches)
		for _, branch := range branches {
			ok, err := w.reapOne(db, branch, now)
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("reap %s@%s: %w", db, branch, err)
			}
			if ok {
				reaped = append(reaped, db+"@"+branch)
			}
		}
	}
	return reaped, firstErr
}

// reapOne reaps db@branch iff it is unprotected and its TTL has expired.
// Expiry itself is computed by ReapDeadline (shared with Status): TTL counted
// from the later of the activity clock and the lease expiry. That already
// folds in "a live lease defers expiry" as a corollary — a live LeaseExpiry
// is by definition still in the future, so it can only push the deadline
// later than now, never make a branch reapable — so there is no separate
// live-lease check here.
func (w *Workspace) reapOne(db, branch string, now time.Time) (bool, error) {
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Listed by ListRefs but gone by the time we read it: destroyed
			// (by an operator, or by a sibling reapOne call in this same
			// Reap pass — see Rollback/Destroy) in the window between the
			// two. Not this call's problem to report; the branch is already
			// exactly as reaped as it needs to be.
			return false, nil
		}
		return false, err
	}
	if ref.TTL == "" || ref.Protected {
		return false, nil
	}
	deadline, ok := ReapDeadline(ref)
	if !ok {
		if ref.Reaping {
			// A previous claim's activity clock (or TTL) no longer computes a
			// deadline at all — e.g. Touch's guard was bypassed by a direct
			// ref edit, or the TTL was cleared out from under a stale claim.
			// Same self-heal as the now.Before(deadline) branch below: see
			// its comment for why this can't be left set.
			return w.clearStaleReapingClaim(db, branch, ref, etag)
		}
		if _, derr := time.ParseDuration(ref.TTL); derr != nil {
			fmt.Fprintf(os.Stderr, "offshoot: warning: %s@%s has unparseable ttl %q; not reaping\n", db, branch, ref.TTL)
		}
		// No/unparseable activity clock: refuse to reap (a TTL with no
		// clock is a config bug, not consent to delete).
		return false, nil
	}
	if now.Before(deadline) {
		if ref.Reaping {
			// A reaper claimed this ref (set Reaping=true) but never reached
			// Destroy — most likely it crashed between the claim write and
			// the destroy call, since a normal run unwinds the claim itself
			// on failure (below) and clears it on success (the ref is just
			// gone). Left alone, that stale claim is permanent: Touch refuses
			// forever (touch.go's Reaping guard), and both ReleaseLease and a
			// durable flush stamp a fresh TouchedAt without ever clearing
			// Reaping, so ordinary activity defers the deadline right back
			// past `now` on every subsequent cycle without ever un-sticking
			// the branch. Since activity has in fact deferred the deadline
			// (we're in the now.Before(deadline) arm), this claim is stale,
			// not active: CAS-clear it so the branch is touchable again.
			// Genuinely-expired claims (deadline already passed) skip this
			// and fall through to the claim-and-destroy path below as today.
			return w.clearStaleReapingClaim(db, branch, ref, etag)
		}
		return false, nil
	}

	// CAS claim: mark the ref as reaping. A concurrent Touch either landed
	// first (our PutRef fails on ErrCAS -> re-evaluate next cycle) or will
	// fail loudly on seeing Reaping (see Touch).
	ref.Reaping = true
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		if errors.Is(err, store.ErrCAS) {
			return false, nil // lost to a concurrent writer; not an error
		}
		return false, err
	}
	// force=false: if a lease was acquired in the window after our claim,
	// Destroy refuses and we unwind the claim rather than kill a live
	// writer. Protected also wins here even though force could bypass it —
	// Reap deliberately never passes force.
	if err := w.Destroy(db, branch, false); err != nil {
		if ref2, etag2, gerr := w.Store.GetRef(db, branch); gerr == nil && ref2.Reaping {
			ref2.Reaping = false
			_, _ = w.Store.PutRef(db, branch, ref2, etag2) // best effort; next cycle retries
		}
		return false, err
	}
	return true, nil
}

// clearStaleReapingClaim CAS-clears ref's Reaping flag: ref is currently
// claimed but does not compute a deadline in the past, so the claim cannot
// be an in-progress reap (that would require the deadline to have already
// passed to have set Reaping in the first place) — it is a leftover from a
// reaper that claimed the branch and then never finished (crash, panic, a
// killed process) before activity moved the deadline back into the future.
// A lost CAS race (etag stale — someone else already cleared or reclaimed
// this ref) is benign: the next Reap cycle just re-evaluates from scratch.
func (w *Workspace) clearStaleReapingClaim(db, branch string, ref store.Ref, etag string) (bool, error) {
	ref.Reaping = false
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		if errors.Is(err, store.ErrCAS) {
			return false, nil
		}
		return false, err
	}
	return false, nil
}
