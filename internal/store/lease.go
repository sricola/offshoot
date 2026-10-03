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

// AcquireLease claims db@branch for holder until now+ttl. It is
// AcquireLeaseRef without the ref and etag; see AcquireLeaseRef for the
// rules.
func (s *Store) AcquireLease(db, branch, holder string, ttl time.Duration, now time.Time) (Lease, error) {
	l, _, _, err := s.AcquireLeaseRef(db, branch, holder, ttl, now)
	return l, err
}

// AcquireLeaseRef claims db@branch for holder until now+ttl, and returns
// with the lease the exact ref its acquire wrote and that write's etag. A
// caller that plans from the ref (ops.CheckpointWith) then builds on the
// revision its lease is part of, rather than on a second GetRef that a
// concurrent writer could already have moved.
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
func (s *Store) AcquireLeaseRef(db, branch, holder string, ttl time.Duration, now time.Time) (Lease, Ref, string, error) {
	return s.AcquireLeaseRefIf(db, branch, holder, ttl, now, nil)
}

// AcquireLeaseRefIf is AcquireLeaseRef with a caller's check run on the ref
// it reads, before any of its own refusals and before its write: an error
// from check is returned as is, and nothing is written. The ref the acquire
// then writes is the one check passed plus the lease, so a caller that
// refuses on the ref's content (ops.CheckpointWith's live-lease, name and
// detached-checkout checks) needs no read of its own before the acquire,
// and no second check after it: a writer that moves the ref in between
// fails the acquire's compare-and-swap instead. A nil check is
// AcquireLeaseRef.
func (s *Store) AcquireLeaseRefIf(db, branch, holder string, ttl time.Duration, now time.Time, check func(Ref) error) (Lease, Ref, string, error) {
	if holder == "" {
		return Lease{}, Ref{}, "", errors.New("store: lease holder must be named")
	}
	ref, etag, err := s.GetRef(db, branch)
	if err != nil {
		return Lease{}, Ref{}, "", err
	}
	if check != nil {
		if err := check(ref); err != nil {
			return Lease{}, Ref{}, "", err
		}
	}
	if ref.Reaping {
		return Lease{}, Ref{}, "", fmt.Errorf("%w: %s@%s; retry shortly", ErrReaping, db, branch)
	}
	if ref.Deleting {
		return Lease{}, Ref{}, "", fmt.Errorf("%w: %s@%s; retry shortly", ErrDeleting, db, branch)
	}
	_, parseOK := parseExpiry(ref.LeaseExpiry)
	if ref.LeaseExpiry != "" && !parseOK {
		fmt.Fprintf(os.Stderr,
			"offshoot: warning: %s@%s has a corrupt lease_expiry %q; treating as expired and reclaiming\n",
			db, branch, ref.LeaseExpiry)
	}
	live := LeaseLive(ref, now)
	if live && ref.LeaseHolder != holder {
		return Lease{}, Ref{}, "", fmt.Errorf("%w by %q until %s",
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
	written, err := s.PutRef(db, branch, ref, etag)
	if err != nil {
		if errors.Is(err, ErrCAS) {
			return Lease{}, Ref{}, "", fmt.Errorf("%w: lost an acquisition race on %s@%s: %w",
				ErrLeaseHeld, db, branch, err)
		}
		return Lease{}, Ref{}, "", fmt.Errorf("store: acquire lease on %s@%s: %w", db, branch, err)
	}
	// PutRef normalizes a copy before encoding it; apply the same here so
	// the returned ref is what a GetRef now decodes.
	ref.Schema = RefSchema
	if ref.HeadEpoch == 0 {
		ref.HeadEpoch = ref.Epoch
	}
	return Lease{DB: db, Branch: branch, Holder: holder, Epoch: ref.Epoch, Expiry: expiry}, ref, written, nil
}

// renewLeaseAttempts bounds RenewLease's read-modify-write of the ref. A
// renewal that loses its compare-and-swap to another write (the holder's
// own flush, a touch, a TTL change, the janitor clearing a claim) re-reads
// the ref and tries again while the lease is still the caller's; the
// bound only ends a run of losses, after which the renewer tries on its
// next tick.
const renewLeaseAttempts = 4

// RenewLease extends the caller's own lease without touching the epoch.
//
// A write that loses its compare-and-swap is retried on a re-read of the
// ref, up to renewLeaseAttempts writes in all, with every check below run
// again on the ref it re-reads: a lease that is no longer the caller's is
// ErrLeaseLost, and a claim is treated as below. A re-read that already
// carries the expiry this call wrote means an earlier write landed though
// it reported the loss (an S3 SDK retry answering 412 to its own first
// attempt), and the renewal is reported without another write.
//
// A branch with a destroy claim (Deleting) is not renewed while the lease
// has more than half of ttl left: RenewLease writes nothing and returns
// ErrDeleting, which a renewer retries on its next tick like any transient
// error. A renewal written over the claim would move the etag Destroy's
// conditional delete compares against, failing that destroy (it unwinds
// its claim and the holder carries on). A destroy that is still running
// holds its claim for its quiesce, a few seconds at most, and a renewer at
// the default cadence (ttl/3) does not normally write over the claim until
// ttl/3 or more after it landed.
//
// Once half of ttl or less is left, the renewal writes over the claim and
// leaves it set, because a claim that a killed or crashed destroy stranded
// stands until ops.ClearStaleDeleteClaims finds it 30 s old, by which time
// a default 30 s lease that no renewal had extended would always have
// expired: an unforced destroy would find no live lease while the claim
// stood, and the first acquirer after the clear would reclaim the branch
// and fence the holder. The skip still costs one renewal of slack. At the
// default cadence only every other renewal under a destroy claim writes,
// with ttl/3 left, so a renewal that fails with a store error (a lost
// compare-and-swap is retried, above) leaves the next with nothing to
// spare, and two in a row let the lease lapse; without a claim, two leave
// nothing to spare and three let it lapse.
//
// A branch with a reap claim (Reaping) is never renewed (ErrReaping): Reap
// claims only a branch whose lease expired a whole branch TTL ago, so no
// live lease can lapse under that claim.
func (s *Store) RenewLease(l Lease, ttl time.Duration, now time.Time) (Lease, error) {
	expiry := now.Add(ttl).UTC()
	stamp := expiry.Format(time.RFC3339Nano)
	for attempt := 1; ; attempt++ {
		ref, etag, err := s.GetRef(l.DB, l.Branch)
		if err != nil {
			return Lease{}, err
		}
		if ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch {
			return Lease{}, fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
				ErrLeaseLost, l.DB, l.Branch, ref.LeaseHolder, ref.Epoch)
		}
		if attempt > 1 && ref.LeaseExpiry == stamp {
			l.Expiry = expiry
			return l, nil
		}
		if ref.Reaping {
			return Lease{}, fmt.Errorf("%w: %s@%s; not renewing over the claim", ErrReaping, l.DB, l.Branch)
		}
		if ref.Deleting {
			if exp, ok := parseExpiry(ref.LeaseExpiry); ok && exp.Sub(now) > ttl/2 {
				return Lease{}, fmt.Errorf("%w: %s@%s; not renewing over the claim while more than half the lease is left", ErrDeleting, l.DB, l.Branch)
			}
		}
		ref.LeaseExpiry = stamp
		_, err = s.PutRef(l.DB, l.Branch, ref, etag)
		if err == nil {
			l.Expiry = expiry
			return l, nil
		}
		if !errors.Is(err, ErrCAS) || attempt == renewLeaseAttempts {
			return Lease{}, fmt.Errorf("store: renew lease on %s@%s: %w", l.DB, l.Branch, err)
		}
	}
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
