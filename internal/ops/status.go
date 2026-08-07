package ops

import (
	"errors"
	"os"
	"sort"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// BranchStatus is one branch's row in Status()'s output.
type BranchStatus struct {
	DB, Branch  string
	HeadTXID    uint64
	Checkpoints []string
	Protected   bool
	Parent      string
	CheckedOut  bool
	// TTL is the branch's TTL verbatim from the ref ("" if none).
	TTL string
	// TTLRemaining is how long until the branch is eligible for reaping,
	// measured from the later of TouchedAt and LeaseExpiry ("" if TTL is
	// unset; "expired" once past the deadline).
	TTLRemaining string
	// State is this branch's computed state — see BranchStateAt's doc
	// comment for the full taxonomy and precedence. Always one of active,
	// dirty, detached, or idle here: Status() has no session map to consult
	// (it is the CLI/at-rest entry point), so pending/error — the two
	// session-derived states only a daemon can see — never appear in this
	// field. A daemon's own "branches" op (internal/daemon/server.go's
	// BranchInfo.State) layers those two on top of this same computation.
	State string
}

// ReapDeadline computes when ref's TTL expires, measured from the later of
// its activity clock (TouchedAt) and its lease expiry — either kind of
// activity defers reaping. ok is false when ref has no TTL, its TTL is
// non-positive (a negative or zero duration string is not a real TTL — fail
// closed rather than compute a deadline already in the past), or its TTL or
// timestamps fail to parse.
func ReapDeadline(ref store.Ref) (time.Time, bool) {
	if ref.TTL == "" {
		return time.Time{}, false
	}
	d, err := time.ParseDuration(ref.TTL)
	if err != nil || d <= 0 {
		return time.Time{}, false
	}
	base, ok := parseRefTime(ref.TouchedAt)
	if !ok {
		return time.Time{}, false
	}
	if lease, ok := parseRefTime(ref.LeaseExpiry); ok && lease.After(base) {
		base = lease
	}
	return base.Add(d), true
}

// FormatTTLRemaining renders ref's time-to-reap as of now, for display:
// "" if ref has no (real) TTL, "expired" once past ReapDeadline, otherwise
// the remaining time.Duration's String(). Shared by Status and the daemon's
// "branches" op so the two report identical numbers computed the same way,
// rather than each formatting ReapDeadline's result independently.
func FormatTTLRemaining(ref store.Ref, now time.Time) string {
	deadline, ok := ReapDeadline(ref)
	if !ok {
		return ""
	}
	if now.After(deadline) {
		return "expired"
	}
	return deadline.Sub(now).String()
}

// parseRefTime parses an RFC3339Nano ref timestamp field (TouchedAt or
// LeaseExpiry), which is empty when unset.
func parseRefTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// BranchStateAt computes db@branch's state from an already-fetched ref and
// its fixed checkout path, evaluated at now — the pure computation behind
// both BranchState (below) and Status(), and reused as-is by the daemon's
// opBranches (internal/daemon/server.go's branchState) so every caller
// derives the identical verdict from the identical inputs, the same pattern
// FormatTTLRemaining already established for TTL rendering.
//
// The split this function embodies is deliberate and documented here once:
// everything BranchStateAt can determine — active, dirty, detached, idle —
// comes from durable truth alone (the ref plus the checkout's .sum
// sidecar), which is exactly what ops has to work with in CLI/at-rest mode,
// with no daemon and no in-memory session map. A daemon knows more: which
// db@branch keys have a session slot reserved (mid-Open) or a live session
// whose Err() has gone non-nil. Those two states — pending and error — are
// NOT computable here, and BranchStateAt never returns them; the daemon
// layers them on top of this function's verdict itself (see
// internal/daemon/server.go's branchState), rather than this package
// growing a session-awareness it fundamentally cannot have (ops has no
// daemon dependency, and must not gain one just to report state).
//
// Full precedence, most to least specific (the daemon-only states are
// listed for completeness; BranchStateAt itself only ever returns the last
// four):
//
//   - "error" (daemon only): a session is open for this branch and its
//     Err() is non-nil — lease loss, a capture failure, any terminal
//     session failure.
//   - "pending" (daemon only): this daemon has reserved a session slot for
//     the branch and is still inside its (slow, unlocked) session.Open —
//     no live session exists yet, but the branch is spoken for.
//   - "active": ref.LeaseHolder carries a live lease (store.LeaseLive) —
//     someone, in or out of a daemon, holds this branch right now. This is
//     also what makes an OPEN, HEALTHY daemon session read as "active"
//     here with no daemon-side casing needed at all: the session's own
//     lease renewal is what keeps this true.
//   - "dirty": no live lease; a checkout exists whose sidecar identity
//     (lineage+epoch+txid) matches the ref but whose content hash does not
//     (checkoutState's "modified" verdict) — un-checkpointed local edits.
//     Determining this REQUIRES a quiesce (WAL checkpoint) before hashing,
//     the same step every other identity/hash comparison in this package
//     already takes (checkoutState's callers — CheckoutProven,
//     warnIfUncheckpointed — always quiesce first): a committed write can
//     sit in the checkout's WAL, invisible to a bare hash of the main
//     file, until something folds it in. A checkout that's currently busy
//     (a live connection, reader or writer, blocking that checkpoint right
//     now) is itself treated as "dirty" rather than "idle" — someone is
//     actively using an unleased checkout, which is exactly the kind of
//     activity this state exists to surface, even though the exact byte
//     diff can't be observed at that instant. See errQuiesceBusy.
//   - "detached": no live lease; a checkout exists whose sidecar-recorded
//     LINEAGE does not match the ref's CURRENT lineage — a checkout
//     orphaned by a rollback/promote that repointed the branch at a new
//     lineage without (or before) refreshing this checkout (Rollback/
//     Promote's own checkout refresh is best-effort: a busy checkout at
//     the moment of repoint leaves exactly this behind — see Promote's own
//     doc comment). Deliberately narrower than checkoutState's "stale"
//     verdict, which fires on ANY identity mismatch: a sidecar whose
//     lineage still matches but whose epoch/txid lags (the branch advanced
//     via checkpoint/flush since this checkout was last refreshed, still
//     the SAME lineage) is not orphaned, just behind — that's "idle"
//     below, not "detached". "dirty" and "detached" are structurally
//     mutually exclusive on any single evaluation: a lineage mismatch
//     returns "detached" immediately, before the identity/hash comparison
//     that could ever produce "dirty" is even reached.
//   - "idle": none of the above. Also where checkoutState's "stale"
//     verdict lands when the lineage still matches (needs a
//     re-materialize, isn't orphaned), and where a missing/corrupt/absent
//     sidecar or no checkout at all lands too (no evidence to report
//     anything more specific). The design spec's original taxonomy had no
//     idle state at all — it assumed a daemon was always present to layer
//     active/pending/error over every branch. Plan-2's CLI/at-rest mode
//     has no daemon, so a branch with nothing going on still needs a name;
//     idle is added deliberately here, not carried over from the spec —
//     see docs/status.md's branch-state-taxonomy row and docs/reference.md
//     for this addition documented for operators.
//
// Known blind spot: a checkout with NO readable .sum sidecar at all (never
// stamped, or corrupt/legacy) always reads "idle" here, even if its
// content has in fact diverged from the ref — there's no sidecar to detect
// that against. This mirrors checkoutState's own "unknown" verdict for the
// identical input and is a deliberate "no evidence, stay silent" stance,
// not an oversight; a checkout materialized entirely outside this
// package's own Checkout/Checkpoint/Rollback/Promote (which always stamp
// one) is the only way to hit it.
//
// Cost: unlike active/detached/idle's checks (a ref field compare, an
// os.Stat, a sidecar read), reaching "dirty" for a checkout whose sidecar
// identity matches the ref requires quiescing the checkout (a real
// wal_checkpoint(TRUNCATE), up to quiesce's 3-second busy timeout) and
// hashing its full content (crypto/sha256 over the whole file). This runs
// PER BRANCH, on every Status()/opBranches call, for every branch that is
// (a) checked out, (b) not leased, and (c) not already detached — i.e.
// exactly the branches where "is it dirty" is still an open question. A
// store with many large, checked-out, unleased branches will feel this on
// every `offshoot status` / "branches" call; there is no cheap
// short-circuit here (file size/mtime cannot substitute for a real hash —
// mtime is exactly what this codebase's own `.last-used` touch-file
// convention elsewhere works around, not something to trust for content
// identity).
func BranchStateAt(ref store.Ref, checkoutPath string, now time.Time) string {
	if store.LeaseLive(ref, now) {
		return "active"
	}
	if _, err := os.Stat(checkoutPath); err != nil {
		return "idle" // no checkout materialized: nothing further to report
	}
	rec, ok := readSidecar(checkoutPath)
	if !ok {
		return "idle" // no sidecar, or corrupt/legacy: no provenance to act on (known blind spot, see doc comment)
	}
	if rec.Lineage != ref.Lineage {
		return "detached"
	}
	if rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID {
		return "idle" // same lineage, stale identity: needs re-materialize, not orphaned
	}
	// Fold any WAL-resident committed content into the main file before
	// hashing it — see the "dirty" bullet above for why skipping this can
	// silently misreport dirty as idle.
	if err := quiesce(checkoutPath); err != nil {
		if errors.Is(err, errQuiesceBusy) {
			return "dirty" // see errQuiesceBusy and the "dirty" bullet above
		}
		return "idle" // can't quiesce for another reason: no evidence to act on
	}
	got, err := fileSum(checkoutPath)
	if err != nil {
		return "idle"
	}
	if got != rec.Hash {
		return "dirty"
	}
	return "idle"
}

// BranchState computes db@branch's current state from a fresh GetRef — see
// BranchStateAt's doc comment for the full taxonomy, the ops/daemon split,
// and precedence. Thin convenience wrapper for callers (CLI/at-rest) that
// don't already have a ref and checkout path in hand; Status() and the
// daemon's opBranches both already do, and call BranchStateAt directly to
// avoid this function's extra GetRef.
func (w *Workspace) BranchState(db, branch string) (string, error) {
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	return BranchStateAt(ref, w.CheckoutPath(db, branch), time.Now()), nil
}

func (w *Workspace) Status() ([]BranchStatus, error) {
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
	var out []BranchStatus
	for _, db := range dbs {
		for _, br := range refs[db] {
			r, _, err := w.Store.GetRef(db, br)
			if err != nil {
				return nil, err
			}
			var cps []string
			for name := range r.Checkpoints {
				cps = append(cps, name)
			}
			sort.Strings(cps)
			path := w.CheckoutPath(db, br)
			_, coErr := os.Stat(path)
			out = append(out, BranchStatus{
				DB: db, Branch: br, HeadTXID: r.HeadTXID, Checkpoints: cps,
				Protected: r.Protected, Parent: r.Parent, CheckedOut: coErr == nil,
				TTL: r.TTL, TTLRemaining: FormatTTLRemaining(r, now),
				State: BranchStateAt(r, path, now),
			})
		}
	}
	return out, nil
}
