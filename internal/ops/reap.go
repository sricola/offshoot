package ops

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
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
// Expiry itself is computed by ttlDeadline (shared with Status): TTL counted
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
	deadline, ok := ttlDeadline(ref)
	if !ok {
		if _, derr := time.ParseDuration(ref.TTL); derr != nil {
			fmt.Fprintf(os.Stderr, "offshoot: warning: %s@%s has unparseable ttl %q; not reaping\n", db, branch, ref.TTL)
		}
		// No/unparseable activity clock: refuse to reap (a TTL with no
		// clock is a config bug, not consent to delete).
		return false, nil
	}
	if now.Before(deadline) {
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
