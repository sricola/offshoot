package store

import (
	"errors"
	"fmt"
	"os"
	"time"
)

var (
	// ErrLeaseHeld reports that another holder owns an unexpired lease.
	ErrLeaseHeld = errors.New("store: branch lease is held")
	// ErrLeaseLost reports that the caller no longer holds the lease it
	// claimed: someone reclaimed the branch, the caller's epoch is dead, and
	// anything it writes now lands in an unreferenced prefix.
	ErrLeaseLost = errors.New("store: branch lease lost")
	// ErrReaping reports that db@branch has an active reap claim
	// (Reaping=true). A lease acquired in the window between Reap's claim
	// and its Destroy call would have its branch deleted out from under it
	// (Destroy's own GetRef can still read the pre-claim ref, and DeleteRef
	// is unconditional), so AcquireLease refuses outright rather than race
	// it. The claim is transient: it clears when Reap's Destroy call
	// unwinds it (failure) or the branch is gone (success), or — for a
	// claim stranded by a crashed reaper — the next Reap cycle's self-heal
	// (see ops.reapOne). Retrying shortly is always the right move.
	ErrReaping = errors.New("store: branch is being reaped")
	// ErrDeleting reports that db@branch has an active Destroy claim
	// (Ref.Deleting=true) — the generalization of ErrReaping's TOCTOU fix
	// (Milestone 4 Task 6b) to every Destroy call, not just Reap's: a lease
	// acquired in the window between Destroy's GetRef and its delete would
	// otherwise have its branch deleted out from under it, so AcquireLease
	// refuses outright here too. Transient exactly like ErrReaping: it
	// clears when Destroy's own claim unwind runs (a failure after the
	// claim landed), the branch is gone (success — GetRef itself then
	// returns ErrNotFound instead of this), or — for a claim stranded by a
	// crashed Destroy — ops.ClearStaleDeleteClaims's age-based self-heal
	// (see Ref.Deleting's doc comment). Retrying shortly is always the
	// right move, same as ErrReaping.
	ErrDeleting = errors.New("store: branch is being deleted")
)

// Lease is a claim on a branch, valid until Expiry unless renewed.
type Lease struct {
	DB, Branch string
	Holder     string
	Epoch      uint64
	Expiry     time.Time
}

// parseExpiry parses a LeaseExpiry string. The second return distinguishes
// "empty" (no lease held; the zero value is fine to treat as expired) from
// "corrupt" (a non-empty value that doesn't parse) — callers that care about
// the difference check s == "" themselves, since parseExpiry's bool alone
// collapses both to false.
func parseExpiry(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// LeaseLive reports whether ref carries a lease that is still live at now:
// a holder is recorded AND its expiry parses AND that expiry is still in
// the future. Exported so callers outside this package that need the exact
// same liveness verdict AcquireLease itself uses — ops.BranchStateAt's
// "active" branch state, in particular — never independently reimplement
// (and risk drifting from) this check. A LeaseExpiry that fails to parse is
// treated as not live here, the same fail-open-to-reclaimable stance
// AcquireLease takes for corrupt expiries, just without that call's own
// stderr warning (a read-only liveness check has no "reclaim" action to
// warn about).
func LeaseLive(ref Ref, now time.Time) bool {
	exp, ok := parseExpiry(ref.LeaseExpiry)
	return ok && ref.LeaseHolder != "" && now.Before(exp)
}

// AcquireLease claims db@branch for holder until now+ttl.
//
// A ref with an active reap claim (Reaping=true) or an active Destroy claim
// (Deleting=true, Milestone 4 Task 6b) refuses outright — see ErrReaping/
// ErrDeleting — before any of the lease logic below even runs.
//
// A fresh acquisition, or a reclaim of an expired (or corrupt, see below)
// lease, bumps the epoch so any previous holder's subsequent writes are
// fenced into a dead prefix. But if the caller already holds a live lease
// (same holder, not yet expired), AcquireLease is instead an idempotent
// renew: it extends the expiry and returns the SAME epoch, exactly like
// RenewLease. Bumping in that case would fence the holder's own in-flight
// writes, which is never what a re-acquiring holder wants; a caller that
// genuinely wants a fresh epoch must ReleaseLease then AcquireLease again.
//
// A LeaseExpiry that is present but fails to parse is corruption, not an
// available lease — but it is still treated as fail-open (reclaimable) here
// rather than fail-closed, because fail-closed would brick the branch
// permanently with no recovery path. The reclaim is logged to stderr so the
// corruption doesn't pass silently.
//
// If PutRef loses a concurrent-acquire race (ErrCAS), that is reported as
// an error wrapping BOTH ErrLeaseHeld and ErrCAS: a caller that only checks
// ErrLeaseHeld still sees "someone else holds it," while a caller that
// wants the low-level detail can still find ErrCAS via errors.Is.
func (s *Store) AcquireLease(db, branch, holder string, ttl time.Duration, now time.Time) (Lease, error) {
	if holder == "" {
		return Lease{}, errors.New("store: lease holder must be named")
	}
	ref, etag, err := s.GetRef(db, branch)
	if err != nil {
		return Lease{}, err
	}
	if ref.Reaping {
		return Lease{}, fmt.Errorf("%w: %s@%s; retry shortly", ErrReaping, db, branch)
	}
	if ref.Deleting {
		return Lease{}, fmt.Errorf("%w: %s@%s; retry shortly", ErrDeleting, db, branch)
	}
	_, parseOK := parseExpiry(ref.LeaseExpiry)
	if ref.LeaseExpiry != "" && !parseOK {
		fmt.Fprintf(os.Stderr,
			"offshoot: warning: %s@%s has a corrupt lease_expiry %q; treating as expired and reclaiming\n",
			db, branch, ref.LeaseExpiry)
	}
	live := LeaseLive(ref, now)
	if live && ref.LeaseHolder != holder {
		return Lease{}, fmt.Errorf("%w by %q until %s",
			ErrLeaseHeld, ref.LeaseHolder, ref.LeaseExpiry)
	}

	expiry := now.Add(ttl).UTC()
	if !live {
		// Fresh acquisition, or reclaim of a dead/corrupt lease: bump the
		// epoch to fence out whatever the previous holder might still write.
		ref.Epoch++
	}
	// live && same holder falls through here without bumping: an idempotent
	// self-renew (see doc comment above).
	ref.LeaseHolder = holder
	ref.LeaseExpiry = expiry.Format(time.RFC3339Nano)
	if _, err := s.PutRef(db, branch, ref, etag); err != nil {
		if errors.Is(err, ErrCAS) {
			return Lease{}, fmt.Errorf("%w: lost an acquisition race on %s@%s: %w",
				ErrLeaseHeld, db, branch, err)
		}
		return Lease{}, fmt.Errorf("store: acquire lease on %s@%s: %w", db, branch, err)
	}
	return Lease{DB: db, Branch: branch, Holder: holder, Epoch: ref.Epoch, Expiry: expiry}, nil
}

// RenewLease extends the caller's own lease without touching the epoch.
func (s *Store) RenewLease(l Lease, ttl time.Duration, now time.Time) (Lease, error) {
	ref, etag, err := s.GetRef(l.DB, l.Branch)
	if err != nil {
		return Lease{}, err
	}
	if ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch {
		return Lease{}, fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
			ErrLeaseLost, l.DB, l.Branch, ref.LeaseHolder, ref.Epoch)
	}
	expiry := now.Add(ttl).UTC()
	ref.LeaseExpiry = expiry.Format(time.RFC3339Nano)
	if _, err := s.PutRef(l.DB, l.Branch, ref, etag); err != nil {
		return Lease{}, fmt.Errorf("store: renew lease on %s@%s: %w", l.DB, l.Branch, err)
	}
	l.Expiry = expiry
	return l, nil
}

// ReleaseLease clears the caller's lease. The epoch is left alone: a clean
// release means the holder's own objects stay reachable.
func (s *Store) ReleaseLease(l Lease) error {
	ref, etag, err := s.GetRef(l.DB, l.Branch)
	if err != nil {
		return err
	}
	if ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch {
		return fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
			ErrLeaseLost, l.DB, l.Branch, ref.LeaseHolder, ref.Epoch)
	}
	ref.LeaseHolder = ""
	ref.LeaseExpiry = ""
	// A lease that was just live counts as activity: stamping the clock here
	// means a branch isn't instantly eligible for reaping the moment its
	// session closes.
	ref.Touch(time.Now())
	if _, err := s.PutRef(l.DB, l.Branch, ref, etag); err != nil {
		return fmt.Errorf("store: release lease on %s@%s: %w", l.DB, l.Branch, err)
	}
	return nil
}
