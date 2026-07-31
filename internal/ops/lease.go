package ops

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// DefaultLeaseTTL is how long an acquired lease stays valid without renewal.
const DefaultLeaseTTL = 30 * time.Second

// LocalHolder returns "<hostname>/<pid>", the conventional holder identity.
func LocalHolder() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

// AcquireLease claims db@branch for this process. holder identifies the
// claimant in diagnostics and in the ref; pass ops.LocalHolder() for the
// conventional "<hostname>/<pid>" form.
func (w *Workspace) AcquireLease(db, branch, holder string, ttl time.Duration) (store.Lease, error) {
	return w.Store.AcquireLease(db, branch, holder, ttl, time.Now())
}

func (w *Workspace) RenewLease(l store.Lease, ttl time.Duration) (store.Lease, error) {
	return w.Store.RenewLease(l, ttl, time.Now())
}

func (w *Workspace) ReleaseLease(l store.Lease) error { return w.Store.ReleaseLease(l) }

// LeaseInfo describes a branch's current lease for display.
type LeaseInfo struct {
	DB, Branch, Holder string
	Epoch              uint64
	Expiry             time.Time
	Expired            bool
}

// Leases lists every branch carrying a lease record, sorted by db then branch.
// If a LeaseExpiry value is present but unparseable (corrupt), it is included
// in the output with Expired: true and Expiry set to zero. This mirrors the
// store layer's fail-open policy (see store.AcquireLease): treating corrupt
// expiries as reclaimable prevents a single corrupt ref from bricking the
// branch permanently with no recovery path. A warning is emitted to stderr
// for any corrupt expiry encountered.
func (w *Workspace) Leases() ([]LeaseInfo, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	var dbs []string
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	now := time.Now()
	var out []LeaseInfo
	for _, db := range dbs {
		for _, br := range refs[db] {
			ref, _, err := w.Store.GetRef(db, br)
			if err != nil {
				// A concurrent Destroy (or other ref removal/corruption)
				// shouldn't take down the whole listing: skip this branch and
				// keep going, warning so the gap doesn't pass silently.
				fmt.Fprintf(os.Stderr, "offshoot: warning: skipping %s@%s: %v\n", db, br, err)
				continue
			}
			if ref.LeaseHolder == "" {
				continue
			}
			exp, err := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
			if err != nil {
				// Corrupt expiry: treat as expired and reclaimable, matching the
				// store layer's fail-open policy. Emit a warning so corruption
				// doesn't pass silently.
				fmt.Fprintf(os.Stderr,
					"offshoot: warning: %s@%s has a corrupt lease_expiry %q\n",
					db, br, ref.LeaseExpiry)
				exp = time.Time{}
			}
			out = append(out, LeaseInfo{
				DB: db, Branch: br, Holder: ref.LeaseHolder, Epoch: ref.Epoch,
				Expiry: exp, Expired: !now.Before(exp),
			})
		}
	}
	return out, nil
}
