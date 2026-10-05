package ops

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// DefaultLeaseTTL is how long an acquired lease stays valid without renewal.
const DefaultLeaseTTL = 30 * time.Second

// LocalHolder returns "<hostname>/<pid>", the conventional holder identity.
// Bare (unprefixed by newHolder) is for a caller with no finer-grained
// holder of its own — "offshoot lease acquire" and tests — where sharing
// this string across every such caller on the same host is accepted:
// AcquireLease's same-holder self-renew makes two of them collide rather
// than fence. A daemon session uses NewSessionHolder, and an at-rest
// checkpoint newCheckpointHolder, each a per-call holder built on this
// with newHolder, precisely so two of those never share an epoch.
func LocalHolder() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

// newHolder returns a per-call lease holder for kind ("session" or
// "checkpoint"): "<kind>:<host>/<pid>/<8 hex>". It keeps LocalHolder's
// <host>/<pid>, which `lease list` and status print, and adds a per-call
// nonce: AcquireLease treats the same holder on a live lease as a
// self-renew with no epoch bump, so two callers of the same kind in one
// process sharing a holder would share an epoch, and with it an object
// key, and race again.
func newHolder(kind string) string {
	var nonce [4]byte
	_, _ = rand.Read(nonce[:]) // crypto/rand.Read does not return an error since Go 1.24
	return kind + ":" + LocalHolder() + "/" + hex.EncodeToString(nonce[:])
}

// sessionHolderPrefix marks a lease held by an open daemon session, so a
// refusal can say which kind of holder it is without ever comparing a
// holder to LocalHolder() or anything else by equality.
const sessionHolderPrefix = "session:"

// NewSessionHolder is the lease holder for one daemon session:
// "session:<host>/<pid>/<8 hex>" (see newHolder). Each session.Open call
// gets its own, so two sessions in one process — or two daemons that
// happen to share a hostname and pid, as in containers with one hostname
// and PID 1 — never collide into the same holder and silently share an
// epoch.
func NewSessionHolder() string {
	return newHolder("session")
}

// IsSessionHolder reports whether a lease holder is a daemon session's
// (see NewSessionHolder).
func IsSessionHolder(holder string) bool {
	return strings.HasPrefix(holder, sessionHolderPrefix)
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

// leaseHeldError is an at-rest refusal of a branch under a live lease. It
// keeps its own message and unwraps to store.ErrLeaseHeld, so a caller
// tests errors.Is(err, store.ErrLeaseHeld) whichever verb refused.
//
// noForce is the message for a caller that cannot pass --force (MCP's
// offshoot_rollback; promote and destroy through an MCP server without
// -allow-force; the daemon's rollback and compact ops, so both SDKs), and
// "" when msg offers no --force; WithoutForceAdvice picks it. checkpoint
// reports that the holder is an at-rest checkpoint's, whose lease clears
// on its own within seconds (CheckpointInProgress).
type leaseHeldError struct {
	msg, noForce string
	checkpoint   bool
}

func (e *leaseHeldError) Error() string { return e.msg }
func (e *leaseHeldError) Unwrap() error { return store.ErrLeaseHeld }

// WithoutForceAdvice is err as a caller that cannot pass --force reports
// it: a live-lease refusal (store.ErrLeaseHeld) loses the --force advice it
// gave the CLI, so it names only the steps that caller has, such as waiting
// for a checkpoint in progress to finish. Any other error is returned
// unchanged.
func WithoutForceAdvice(err error) error {
	var lhe *leaseHeldError
	if !errors.As(err, &lhe) || lhe.noForce == "" {
		return err
	}
	msg := lhe.noForce
	if err != error(lhe) {
		msg = strings.Replace(err.Error(), lhe.msg, lhe.noForce, 1)
	}
	return &leaseHeldError{msg: msg, checkpoint: lhe.checkpoint}
}

// CheckpointInProgress reports whether err is a live-lease refusal whose
// holder is an at-rest checkpoint (holder "checkpoint:<host>/<pid>/<nonce>").
// That lease clears on its own when the checkpoint ends, within seconds,
// or 30 s after its process died, so the next step is to retry, not to
// reach for --force or a human.
func CheckpointInProgress(err error) bool {
	var lhe *leaseHeldError
	return errors.As(err, &lhe) && lhe.checkpoint
}
