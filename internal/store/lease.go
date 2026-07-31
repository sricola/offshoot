package store

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrLeaseHeld reports that another holder owns an unexpired lease.
	ErrLeaseHeld = errors.New("store: branch lease is held")
	// ErrLeaseLost reports that the caller no longer holds the lease it
	// claimed: someone reclaimed the branch, the caller's epoch is dead, and
	// anything it writes now lands in an unreferenced prefix.
	ErrLeaseLost = errors.New("store: branch lease lost")
)

// Lease is a claim on a branch, valid until Expiry unless renewed.
type Lease struct {
	DB, Branch string
	Holder     string
	Epoch      uint64
	Expiry     time.Time
}

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

// AcquireLease claims db@branch for holder until now+ttl, bumping the epoch
// so any previous holder's subsequent writes are fenced into a dead prefix.
func (s *Store) AcquireLease(db, branch, holder string, ttl time.Duration, now time.Time) (Lease, error) {
	if holder == "" {
		return Lease{}, errors.New("store: lease holder must be named")
	}
	ref, etag, err := s.GetRef(db, branch)
	if err != nil {
		return Lease{}, err
	}
	if exp, ok := parseExpiry(ref.LeaseExpiry); ok && ref.LeaseHolder != "" &&
		ref.LeaseHolder != holder && now.Before(exp) {
		return Lease{}, fmt.Errorf("%w by %q until %s",
			ErrLeaseHeld, ref.LeaseHolder, ref.LeaseExpiry)
	}
	expiry := now.Add(ttl).UTC()
	ref.Epoch++
	ref.LeaseHolder = holder
	ref.LeaseExpiry = expiry.Format(time.RFC3339Nano)
	if _, err := s.PutRef(db, branch, ref, etag); err != nil {
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
	if _, err := s.PutRef(l.DB, l.Branch, ref, etag); err != nil {
		return fmt.Errorf("store: release lease on %s@%s: %w", l.DB, l.Branch, err)
	}
	return nil
}
