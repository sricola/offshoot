package ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

const tombstoneKey = "gc/tombstones"

// Destroy deletes db@branch. Per the fault matrix, a protected branch or one
// under an active lease requires --force (an expired, or unparseable — fail
// open, matching the store layer's own reclaim policy for corrupt
// expiries — lease does not block; it's already reclaimable by anyone).
//
// Milestone 4 Task 6b — claim-guarded delete: between an original M2
// version's GetRef and its (unconditional) DeleteRef sat a TOCTOU window in
// which a concurrent AcquireLease could land, and then have its brand-new
// lease's branch deleted out from under it. Destroy now CAS-writes a
// Deleting claim (Ref.Deleting/DeletingAt, the generalization of Reap's own
// Reaping-flag claim to every Destroy call — see Ref.Deleting's doc comment
// for why this is a sibling field, not a unification) before it does
// anything irreversible; AcquireLease refuses outright once it sees the
// claim (store.ErrDeleting), so a lease can no longer land in that window.
// force bypasses the protected/live-lease checks above — an operator's
// explicit override of THOSE — but never the claim safety itself: a forced
// Destroy still claims before it deletes, and a lease claimed a moment
// before force lands still wins the CAS race on the ref (this call's own
// claim write then fails with ErrCAS, reported as a retryable race loss,
// same as an unforced call). A claim write that reports failure but
// landed is this call's own claim all the same, and Destroy goes on under
// it (landedClaim).
//
// The lease holder's renewals do not stop under the claim
// (store.RenewLease), so one can land between the claim and the delete;
// see deleteClaimedRef for how the delete gets past it.
//
// One destroy never claims over another's live claim (liveDeleteClaim):
// it is refused with store.ErrDeleting, retryable, and writes nothing,
// force or not. And a destroy that fails after its claim unwinds its own
// claim only (unwindDeletingClaim). Either way no other destroy clears or
// replaces a claim while it is live, and a reap pass writes nothing to the
// ref under one (reapOne): were a claim cleared under its destroy, an
// acquire could take the branch at a new epoch, and on S3 that destroy's
// unconditional delete would remove the branch under the fresh lease. A
// claim is live for staleDeletingClaimAfter (30 s) from its stamp
// (liveDeleteClaim), not for as long as its destroy may still delete: once
// it is that old, ClearStaleDeleteClaims clears it and another destroy
// takes it over.
// So a destroy sends its delete only while its own stamp says the claim
// has more than deleteClaimMargin (10 s) of that left; one held up past
// that (its claim write held up by response timeouts and SDK retries,
// whether it then succeeded or landed after reporting failure) unwinds its
// claim and fails, retryable, deleting nothing. On a local store the delete
// is also conditional on the claim's etag, so a destroy whose delete lands
// later than that fails instead. On S3 it is not: a destroy whose delete
// request is itself held up until the claim is stale, or whose process is
// suspended that long between that check and its delete, can delete a
// branch an acquire took after the janitor cleared its claim
// (docs/limitations.md).
func (w *Workspace) Destroy(db, branch string, force bool) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	if err := store.ValidateName(branch); err != nil {
		return err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return err
	}
	if ref.Protected && !force {
		return fmt.Errorf("ops: %s@%s is protected; use --force", db, branch)
	}
	if ref.LeaseHolder != "" && !force {
		exp, perr := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
		if perr == nil && time.Now().Before(exp) {
			return destroyLeaseRefusal(db, branch, ref)
		}
	}
	if liveDeleteClaim(ref, time.Now()) {
		return fmt.Errorf("ops: %s@%s is already being destroyed (claimed at %s); retry once that destroy ends, or once its claim is %s old if it was interrupted: %w",
			db, branch, ref.DeletingAt, staleDeletingClaimAfter, store.ErrDeleting)
	}

	// CAS claim: mark the ref as being deleted. A concurrent AcquireLease
	// either landed first (our PutRef below fails on ErrCAS — report as a
	// lost race, retryable) or will fail loudly on seeing Deleting (see
	// store.AcquireLease/ErrDeleting).
	ref.Deleting = true
	ref.DeletingAt = time.Now().UTC().Format(time.RFC3339Nano)
	claimEtag, err := w.Store.PutRef(db, branch, ref, etag)
	if err != nil {
		if claimEtag, err = w.landedClaim(db, branch, ref, etag, force, err); err != nil {
			return err
		}
	}

	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			// This call knows right now it isn't going to finish — unwind
			// the claim eagerly rather than leave it for
			// ClearStaleDeleteClaims's age-based self-heal to eventually
			// catch.
			w.unwindDeletingClaim(db, branch, ref.DeletingAt)
			return fmt.Errorf("ops: checkout in use; close connections before destroy: %w", err)
		}
	}
	// The claim's stamp is this call's own, so its age needs no read of
	// the ref. A claim too near staleDeletingClaimAfter old may be cleared
	// by the janitor on any host, and the branch taken at a new epoch,
	// before the delete lands; on S3 that delete would then remove the
	// branch under the new lease.
	if !liveDeleteClaim(ref, time.Now().Add(deleteClaimMargin)) {
		w.unwindDeletingClaim(db, branch, ref.DeletingAt)
		return fmt.Errorf("ops: destroy of %s@%s was held up too long to delete under its claim (made at %s; another host may clear a claim %s old and take the branch), so it deleted nothing (retry): %w",
			db, branch, ref.DeletingAt, staleDeletingClaimAfter, store.ErrCAS)
	}
	// DeleteRefIf is a true CAS delete on the local backend (belt-and-
	// suspenders on top of the claim above) and an unconditional delete on
	// S3 (which cannot condition a DeleteObject call at all) — either way,
	// the Deleting claim already landed above is what actually serializes
	// this against a concurrent AcquireLease/Destroy on every backend. See
	// store.DeleteRefIf's doc comment.
	if err := w.deleteClaimedRef(db, branch, ref, claimEtag, force); err != nil {
		w.unwindDeletingClaim(db, branch, ref.DeletingAt)
		return err
	}
	// Best-effort checkout removal: the ref is already gone, so a failed
	// remove cannot corrupt anything (a re-created branch's Checkout
	// overwrites), but nothing else ever cleans checkouts/ — log so the
	// stale files aren't a silent disk leak.
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".sum", shadowPath(path), shadowPath(path) + ".tmp"} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "offshoot: destroy %s@%s: remove checkout file %s: %v\n",
				db, branch, p, err)
		}
	}
	return nil
}

// landedClaim settles Destroy's claim write, of claimed against the ref
// read at sentEtag, that reported failure (werr) but may have landed: the
// S3 SDK's retry of a PutObject whose first attempt landed gets a 412 (or
// a 409) back from that attempt, which the store reports as a lost
// compare-and-swap, and a timeout can lose the response to a write that
// landed. Reported as it stands, such a failure would leave this call's
// claim on the ref, and the retry the error asks for would be refused
// under it (liveDeleteClaim) until it was staleDeletingClaimAfter old. So
// landedClaim re-reads the ref. One that carries this call's claim (its
// DeletingAt, stamped per call) is that write, landed. With nothing but
// the lease expiry moved since (onlyRenewed), and, without force, no lease
// live again, Destroy goes on from the etag read, as deleteClaimedRef does
// past a renewal over the claim, and still deletes only if the claim is
// young enough by then (Destroy): a write that took long enough to time out
// lands a claim that is already stale. Otherwise Destroy fails as
// deleteClaimedRef would have failed it over the same write, and unwinds
// its claim first. A ref without this call's claim (the write did not
// land, or another write has since taken the claim off), or one that
// cannot be read, reports the write's failure.
//
// A ref still at sentEtag has had nothing land on it, so the failure gave
// no verdict: a 409 against a write still in flight, perhaps this call's
// own first attempt, or a timeout with the write still on its way, which
// can land after the re-read. landedClaim re-reads such a ref
// claimSettleReads more times, claimSettleEvery apart, and goes on as
// above once anything lands; a definite loss (a 412: another write landed
// first) has moved the ref and is reported at once. A ref still at
// sentEtag after that reports the write's failure, saying the claim may
// still land and a retry be refused under it for up to
// staleDeletingClaimAfter. (A ref that another write moved and an unwind
// put back to the same bytes also reads at sentEtag; that costs the wait
// and the warning, never a go-ahead, which takes this call's claim.)
func (w *Workspace) landedClaim(db, branch string, claimed store.Ref, sentEtag string, force bool, werr error) (string, error) {
	failed := werr
	if errors.Is(werr, store.ErrCAS) {
		failed = fmt.Errorf("ops: destroy lost a race on %s@%s (retry): %w", db, branch, werr)
	}
	cur, etag, err := w.Store.GetRef(db, branch)
	for n := 0; err == nil && etag == sentEtag && n < claimSettleReads; n++ {
		time.Sleep(claimSettleEvery)
		cur, etag, err = w.Store.GetRef(db, branch)
	}
	if err == nil && etag == sentEtag {
		return "", fmt.Errorf("%w; the claim write got no verdict and may still land, and a retry would then be refused as already being destroyed until the claim is %s old", failed, staleDeletingClaimAfter)
	}
	if err != nil || !cur.Deleting || cur.DeletingAt != claimed.DeletingAt {
		return "", failed
	}
	if !onlyRenewed(claimed, cur) {
		w.unwindDeletingClaim(db, branch, claimed.DeletingAt)
		return "", fmt.Errorf("ops: destroy lost a race on %s@%s to another write of its ref (retry): %w", db, branch, store.ErrCAS)
	}
	if !force && store.LeaseLive(cur, time.Now()) {
		w.unwindDeletingClaim(db, branch, claimed.DeletingAt)
		return "", destroyLeaseRefusal(db, branch, cur)
	}
	return etag, nil
}

// claimSettleReads and claimSettleEvery bound landedClaim's wait for a
// claim write that got no verdict: about 2 s in all, well past the moment
// a write still in flight lands in all but a stalled request, and short
// enough for an operator's destroy to wait out. claimSettleEvery is a var
// only so tests can shrink it.
const claimSettleReads = 3

var claimSettleEvery = 700 * time.Millisecond

// destroyLeaseRefusal is Destroy's refusal, without --force, of a branch
// whose lease ref shows live.
func destroyLeaseRefusal(db, branch string, ref store.Ref) error {
	held := fmt.Sprintf("ops: %s@%s has a live lease held by %q until %s", db, branch, ref.LeaseHolder, ref.LeaseExpiry)
	if isCheckpointHolder(ref.LeaseHolder) {
		// It ends on its own within seconds: say so first, so an agent or
		// script retries instead of fetching a human for --force, which
		// would make the checkpoint fail. "; use --force" stays the
		// message's tail, as callers that rewrite it expect.
		refusal := held + " (another checkpoint is in progress); a destroy now would make that checkpoint fail without committing — " + checkpointRetryAdvice
		return &leaseHeldError{msg: refusal + "; use --force", noForce: refusal, checkpoint: true}
	}
	return &leaseHeldError{msg: held + "; use --force", noForce: held}
}

// destroyDeleteAttempts bounds deleteClaimedRef's conditional deletes. A
// holder renews once per renewal interval (10 s at the default TTL), so a
// second attempt wins unless renewals come faster than one re-read and
// delete of the ref; the bound only ends such a run.
const destroyDeleteAttempts = 4

// deleteClaimedRef is Destroy's delete of the ref its claim wrote: claimed,
// at etag. The lease holder's renewals go on under the claim
// (store.RenewLease writes over it and leaves it set), and one that lands
// before the delete moves the etag a local store's conditional delete
// compares against. So a delete that loses its compare-and-swap re-reads
// the ref, and while the ref is still exactly this call's claim with only
// the lease expiry moved (onlyRenewed), deletes again against the etag it
// read, up to destroyDeleteAttempts deletes in all. The claim holds every
// acquire off throughout, and the holder's next renewal finds the branch
// gone (ErrNotFound), which ends a session or a checkpoint.
//
// Without force, Destroy went ahead only because the lease had lapsed; if
// its holder has renewed it since, the destroy is refused as a live lease,
// as it would have been had the renewal landed before its read. Any other
// change to the ref (a flush, a touch, a release, another destroy's claim
// over this one, the janitor clearing this one), or a renewal before every
// attempt, fails the destroy as a lost race (retryable, ErrCAS). On S3 the
// delete is unconditional and never loses a compare-and-swap, so none of
// this runs there: a renewal between the claim and the delete is not seen,
// and the branch is deleted, forced or not.
func (w *Workspace) deleteClaimedRef(db, branch string, claimed store.Ref, etag string, force bool) error {
	for attempt := 1; ; attempt++ {
		err := w.Store.DeleteRefIf(db, branch, etag)
		if err == nil || !errors.Is(err, store.ErrCAS) {
			return err
		}
		lost := fmt.Errorf("ops: destroy lost a race on %s@%s to another write of its ref (retry): %w", db, branch, err)
		if attempt == destroyDeleteAttempts {
			return lost
		}
		cur, curEtag, gerr := w.Store.GetRef(db, branch)
		if gerr != nil || !onlyRenewed(claimed, cur) {
			return lost
		}
		if !force && store.LeaseLive(cur, time.Now()) {
			return destroyLeaseRefusal(db, branch, cur)
		}
		etag = curEtag
	}
}

// onlyRenewed reports whether cur is claimed, the ref Destroy's claim
// wrote, with nothing but its lease expiry moved: what a renewal by the
// holder writes over the claim. The same DeletingAt is part of that, so
// another destroy's claim is not this one's. A reaping claim that was set
// under this claim and has since been cleared (a reaper unwinding its own
// claim after its destroy was refused) counts as no change either: the
// destroy's own claim still keeps acquires off. Both refs are compared as
// PutRef encodes them; both come through GetRef's decode, so the
// encodings agree on everything a renewal leaves alone.
func onlyRenewed(claimed, cur store.Ref) bool {
	cur.LeaseExpiry = claimed.LeaseExpiry
	if claimed.Reaping {
		cur.Reaping = true
	}
	a, aerr := json.Marshal(claimed)
	b, berr := json.Marshal(cur)
	return aerr == nil && berr == nil && bytes.Equal(a, b)
}

// unwindDeletingClaim best-effort clears the Deleting claim this Destroy
// call landed, stamped claimedAt, when the call cannot carry it through to
// a delete (a checkout-quiesce failure, a failed or lost delete). It
// clears that claim and no other: a claim with another DeletingAt belongs
// to a destroy that wrote over this one (an older binary, which claims
// over any claim, or one that found this claim stale) and may still be
// between its claim and its delete, so clearing it would open the window
// the claim closes (see Destroy). The holder's renewals write over the
// claim and leave it set, so a write that loses its compare-and-swap
// re-reads and tries again while the claim is still this call's, up to
// destroyDeleteAttempts writes. A claim the unwind could not clear blocks
// acquires and other destroys until it is staleDeletingClaimAfter old,
// when ClearStaleDeleteClaims clears it and a destroy takes it over.
func (w *Workspace) unwindDeletingClaim(db, branch, claimedAt string) {
	for attempt := 1; attempt <= destroyDeleteAttempts; attempt++ {
		ref, etag, err := w.Store.GetRef(db, branch)
		if err != nil || !ref.Deleting || ref.DeletingAt != claimedAt {
			return
		}
		ref.Deleting = false
		ref.DeletingAt = ""
		if _, err := w.Store.PutRef(db, branch, ref, etag); !errors.Is(err, store.ErrCAS) {
			return
		}
	}
}

// liveDeleteClaim reports whether ref carries a destroy's claim made less
// than staleDeletingClaimAfter before now, so that its destroy may still be
// between its claim and its delete. A claim whose DeletingAt cannot be
// read is not live: ClearStaleDeleteClaims never clears one, and only a
// destroy taking it over gets the branch out from under it. Nor is one
// stamped more than deleteClaimClockSlack after now (deleteClaimFresh).
func liveDeleteClaim(ref store.Ref, now time.Time) bool {
	if !ref.Deleting {
		return false
	}
	at, err := time.Parse(time.RFC3339Nano, ref.DeletingAt)
	return err == nil && deleteClaimFresh(at, now)
}

// deleteClaimFresh reports whether a destroy claim stamped at is too recent,
// at now, to have been abandoned: less than staleDeletingClaimAfter old,
// and stamped no more than deleteClaimClockSlack in the future. A stamp
// further ahead than that came from a clock that runs ahead of this one (or
// was set far in the future), and its distance from now says nothing about
// whether its destroy is still running; counted as fresh, it would refuse
// acquires and destroys, and keep the janitor off it, for as long as that
// clock is ahead (for good, for a stamp far in the future). Treating it as
// stale assumes, as the 30 s bound already does, that hosts sharing a store
// agree on the time to well within the slack.
func deleteClaimFresh(at, now time.Time) bool {
	age := now.Sub(at)
	return age < staleDeletingClaimAfter && age >= -deleteClaimClockSlack
}

// deleteClaimClockSlack is how far ahead of this host's clock a destroy
// claim's DeletingAt may be and still count as fresh: room for the
// ordinary clock skew between hosts sharing a store (see deleteClaimFresh).
const deleteClaimClockSlack = time.Minute

// deleteClaimMargin is how much of its staleDeletingClaimAfter a destroy's
// own claim must still have left when the destroy sends its delete: room
// for that delete to land, and for a janitor whose clock runs a little
// ahead of this one, before any host may count the claim abandoned and
// clear it (see Destroy).
const deleteClaimMargin = 10 * time.Second

// staleDeletingClaimAfter bounds how long a Destroy claim (Ref.Deleting) can
// sit unresolved before ClearStaleDeleteClaims treats it as abandoned by a
// crashed Destroy call rather than one still legitimately in flight. Destroy
// between its claim write and its terminal DeleteRefIf/unwind is a handful
// of local filesystem removes plus at most one quiesce call (bounded by its
// own internal busy-timeout) — order of milliseconds in the healthy case, so
// this is deliberately generous, matching the local backend's own
// lock-staleness window (Local.lock: 30s) rather than trying to tune a
// tighter bound. On S3, where the delete is unconditional, it also bounds
// how long a destroy has to reach its delete safely: a destroy sends its
// delete only with more than deleteClaimMargin of this left (see Destroy).
const staleDeletingClaimAfter = 30 * time.Second

// ClearStaleDeleteClaims self-heals a Deleting claim (Milestone 4 Task 6b)
// stranded by a Destroy call that crashed (or was killed) after CAS-writing
// its claim but before it could either finish the delete or unwind the
// claim on failure — the "crashed reaper" scenario clearStaleReapingClaim
// already handles for Reap's own claim, generalized here as a sibling
// (Task 6b's timeboxed fallback: this does NOT touch Reaping's own CAS
// mechanics — see Ref.Deleting's doc comment).
//
// Unlike Reap's self-heal (driven by a TTL/activity deadline recomputing
// into the future), a Deleting claim has no deadline concept to recompute:
// it self-heals purely by age (staleDeletingClaimAfter), and a claim
// stamped more than deleteClaimClockSlack in the future counts as stale
// too (deleteClaimFresh). A ref with an unparseable DeletingAt is left
// alone (conservative, matching Reap's own stance on an unparseable
// TTL/expiry elsewhere in this package) rather than guessed at. Called by
// the janitor on every tick, alongside Reap/GC.
func (w *Workspace) ClearStaleDeleteClaims(now time.Time) (cleared []string, err error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
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
			ref, etag, gerr := w.Store.GetRef(db, branch)
			if gerr != nil {
				if errors.Is(gerr, store.ErrNotFound) {
					continue // destroyed since ListRefs (by this claim's own Destroy finishing, most likely)
				}
				if firstErr == nil {
					firstErr = fmt.Errorf("clear stale delete claim %s@%s: %w", db, branch, gerr)
				}
				continue
			}
			if !ref.Deleting {
				continue
			}
			claimedAt, perr := time.Parse(time.RFC3339Nano, ref.DeletingAt)
			if perr != nil || deleteClaimFresh(claimedAt, now) {
				continue
			}
			ref.Deleting = false
			ref.DeletingAt = ""
			if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
				if errors.Is(err, store.ErrCAS) {
					continue // lost to a concurrent writer; benign, matches clearStaleReapingClaim
				}
				if firstErr == nil {
					firstErr = fmt.Errorf("clear stale delete claim %s@%s: %w", db, branch, err)
				}
				continue
			}
			cleared = append(cleared, db+"@"+branch)
		}
	}
	return cleared, firstErr
}

func (w *Workspace) loadTombstones() (map[string]string, string, error) {
	data, etag, err := w.Store.B.Get(tombstoneKey)
	if errors.Is(err, store.ErrNotFound) {
		return map[string]string{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, "", fmt.Errorf("ops: corrupt tombstone list: %w", err)
	}
	return m, etag, nil
}

// TombstoneBacklog reports how many OBJECTS are currently tombstoned and
// awaiting GC's grace period before deletion — offshoot_gc_backlog's data
// source. A plain len() of loadTombstones' map: cheap (one store.Get), no
// separate bookkeeping to keep in sync with GC's own phase-1/phase-2 logic.
func (w *Workspace) TombstoneBacklog() (int, error) {
	m, _, err := w.loadTombstones()
	if err != nil {
		return 0, err
	}
	return len(m), nil
}

func (w *Workspace) tombstone(m map[string]string) error {
	cur, etag, err := w.loadTombstones()
	if err != nil {
		return err
	}
	for k, v := range m {
		cur[k] = v
	}
	data, _ := json.Marshal(cur)
	if _, err := w.Store.B.PutIf(tombstoneKey, data, etag); err != nil {
		return fmt.Errorf("ops: gc tombstone update lost a race (retry): %w", err)
	}
	return nil
}

// markCache is a read-through store.Backend wrapper memoizing List and Get
// results for the duration of ONE reachability mark pass — the per-pass cache
// the GC redesign requires for scale: store.Chain Lists a lineage's prefix
// (and Gets its base.json) every time it resolves into that lineage, so
// without memoization a shared ancestor would be Listed once per descendant
// per txid instead of once per pass. Wrapping the backend (rather than
// caching resolved member sets in GC itself) keeps the mark on the REAL
// Chain/keepHighestEpoch code path — never a reimplementation.
//
// A Get's ErrNotFound is memoized too (lineageBase probes base.json and
// treats absence as "no base"); any other error is NOT cached, so a
// transient backend failure never sticks for the pass. Write methods
// delegate untouched — the mark path never calls them. Not safe for
// concurrent use; each mark pass builds its own.
type markCache struct {
	b     store.Backend
	lists map[string][]string
	gets  map[string]markCacheGet
}

type markCacheGet struct {
	data     []byte
	etag     string
	notFound bool
}

func newMarkCache(b store.Backend) *markCache {
	return &markCache{b: b, lists: map[string][]string{}, gets: map[string]markCacheGet{}}
}

func (c *markCache) Get(key string) ([]byte, string, error) {
	if g, ok := c.gets[key]; ok {
		if g.notFound {
			return nil, "", store.ErrNotFound
		}
		return g.data, g.etag, nil
	}
	data, etag, err := c.b.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		c.gets[key] = markCacheGet{notFound: true}
		return nil, "", err
	}
	if err != nil {
		return nil, "", err
	}
	c.gets[key] = markCacheGet{data: data, etag: etag}
	return data, etag, nil
}

func (c *markCache) List(prefix string) ([]string, error) {
	if keys, ok := c.lists[prefix]; ok {
		return keys, nil
	}
	keys, err := c.b.List(prefix)
	if err != nil {
		return nil, err
	}
	c.lists[prefix] = keys
	return keys, nil
}

func (c *markCache) Put(key string, data []byte) error { return c.b.Put(key, data) }
func (c *markCache) PutIf(key string, data []byte, ifMatch string) (string, error) {
	return c.b.PutIf(key, data, ifMatch)
}
func (c *markCache) Delete(key string) error          { return c.b.Delete(key) }
func (c *markCache) CopyObject(dst, src string) error { return c.b.CopyObject(dst, src) }

// reachableObjects computes GC's live object set: the union, over every live
// ref, of
//
//   - the ref's resolved chain members at its HEAD and at EVERY checkpoint
//     (store.Chain, the real base-following resolver — target <= Base.TXID
//     resolves transitively into ancestor lineages, so an ancestor kept
//     alive only by a descendant's base pointer is marked here), and
//   - the base.json object of every lineage in the ref's base SPINE
//     ({r.Lineage} ∪ BaseSpine(r.Lineage)) that actually exists.
//
// The checkpoint union is essential, not optional: marking only the head
// chain would sweep the older snapshot an old checkpoint anchors on and
// break Fork(at=cp)/Rollback(to=cp). And base.json marking must follow the
// SPINE, not the member-contributing lineages: a pass-through lineage
// (forked but never diverged) contributes zero members yet its base.json IS
// read during resolution — sweeping it would break every descendant.
// BaseSpine's walk already learns exactly which spine lineages have a
// base.json (see its invariant note), so existence needs no extra probes:
// spine[i]'s base.json exists iff the walk continued past it.
//
// Errors fail the whole mark (and with it the GC pass) rather than being
// skipped: an incomplete mark under-approximates reachability, and sweeping
// against an under-approximation deletes live data. A ref that disappears
// between ListRefs and GetRef (a completed Destroy) is the one benign case
// and is skipped.
//
// Everything Chain/BaseSpine reads goes through one markCache (see its doc
// comment), so a shared ancestor's prefix is Listed once per pass no matter
// how many descendants resolve into it; Chain results are additionally
// memoized per (lineage, txid) so shared fork points re-walk nothing.
func (w *Workspace) reachableObjects() (map[string]bool, error) {
	reachable, _, err := w.reachableObjectsAndHeads()
	return reachable, err
}

// liveHead is the per-lineage input to GC's compensating rule: the live
// ref's HeadTXID and current writer-generation Epoch (Ref.Epoch, NOT
// HeadEpoch — see reachableObjectsAndHeads' doc comment on why the current
// generation, not the head object's generation, is what a retry writes
// under).
type liveHead struct {
	TXID, Epoch uint64
}

// reachableObjectsAndHeads is reachableObjects' full form: alongside the
// reachable set it returns, for every lineage some live ref names as its OWN
// (head) lineage, a liveHead{TXID, Epoch} — the input to GC's compensating
// rule (see the phase-2 sweep): an object ABOVE a live ref's head in that
// ref's own lineage, and at an epoch a CURRENT writer could still make live,
// may be an orphan a session flush is entitled to re-create at the same key
// (session/flush.go's ambiguous-ref-write retry overwrites the SAME
// (lineage, epoch, txid) key), or an at-rest checkpoint's object between
// its upload and its head write, under the epoch its lease acquire just
// minted, so the sweep must never delete there.
//
// Both fields take the MINIMUM across every ref naming the lineage, and both
// for the same reason: erring toward OVER-protection under a stale or
// disagreeing read.
//   - Smallest HeadTXID is the txid a retrying writer would build up from,
//     and a smaller head protects MORE keys above it.
//   - Smallest Epoch is the generation the compensating rule's ">="
//     check compares against (see phase 2 below), and a smaller Epoch makes
//     that check true — protecting — for MORE keys.
//
// The rule uses Ref.Epoch (bumped by AcquireLease on every fresh
// acquisition or reclaim — the lineage's CURRENT writer generation), not
// Ref.HeadEpoch (merely the generation the head object happened to be
// written under, which lags Epoch whenever a lease was acquired or renewed
// since the head was written). A retry by the current holder writes under
// its live lease's epoch, i.e. Ref.Epoch — so that is the bound the rule
// must compare against, not HeadEpoch.
//
// Deriving both from the SAME ListRefs+GetRef fetch as the mark (rather than
// a separate re-enumeration, as a standalone liveHeadLineages helper once
// did) both saves 1 ListRefs + R GetRef RPCs per sweeping pass and
// guarantees the compensating rule sees exactly the refs the re-mark saw —
// one consistency window, not two.
func (w *Workspace) reachableObjectsAndHeads() (map[string]bool, map[string]liveHead, error) {
	ms := &store.Store{B: newMarkCache(w.Store.B)}
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, nil, err
	}
	reachable := map[string]bool{}
	heads := map[string]liveHead{}
	spines := map[string][]string{}
	chainsDone := map[string]bool{} // "lineage@txid" -> already marked
	dbs := make([]string, 0, len(refs))
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		for _, branch := range refs[db] {
			r, _, err := w.Store.GetRef(db, branch)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					continue // destroyed since ListRefs
				}
				return nil, nil, fmt.Errorf("ops: gc mark %s@%s: %w", db, branch, err)
			}
			if h, ok := heads[r.Lineage]; !ok {
				heads[r.Lineage] = liveHead{TXID: r.HeadTXID, Epoch: r.Epoch}
			} else {
				if r.HeadTXID < h.TXID {
					h.TXID = r.HeadTXID
				}
				if r.Epoch < h.Epoch {
					h.Epoch = r.Epoch
				}
				heads[r.Lineage] = h
			}
			spine, ok := spines[r.Lineage]
			if !ok {
				spine, err = ms.BaseSpine(r.Lineage)
				if err != nil {
					return nil, nil, fmt.Errorf("ops: gc mark %s@%s: %w", db, branch, err)
				}
				spines[r.Lineage] = spine
			}
			// Mark the base.json objects that exist along the spine: the
			// ref's own iff it has a base at all (spine non-empty), and each
			// ancestor's iff the walk continued past it.
			if len(spine) > 0 {
				reachable[store.BaseKey(r.Lineage)] = true
			}
			for i := 0; i+1 < len(spine); i++ {
				reachable[store.BaseKey(spine[i])] = true
			}
			txids := map[uint64]bool{r.HeadTXID: true}
			for _, cp := range r.Checkpoints {
				txids[cp.TXID] = true
			}
			for t := range txids {
				ck := fmt.Sprintf("%s@%d", r.Lineage, t)
				if chainsDone[ck] {
					continue
				}
				chainsDone[ck] = true
				members, err := ms.Chain(r.Lineage, t)
				if err != nil {
					return nil, nil, fmt.Errorf("ops: gc mark %s@%s at txid %d: %w", db, branch, t, err)
				}
				for _, m := range members {
					reachable[m.Key] = true
				}
			}
		}
	}
	return reachable, heads, nil
}

// lineageOfDataKey extracts the lineage component of a data/{lineage}/...
// object key; ok is false for anything shaped differently.
func lineageOfDataKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, "data/")
	if !ok {
		return "", false
	}
	lineage, _, ok := strings.Cut(rest, "/")
	if !ok || lineage == "" {
		return "", false
	}
	return lineage, true
}

// GC is the object-granular reachability collector: phase 1 tombstones every
// object under data/ that no live ref can reach (see reachableObjects — the
// transitive base closure of every ref's head and checkpoints), phase 2
// sweeps tombstoned objects whose grace has passed and that are STILL
// unreachable under a fresh re-mark. It returns how many OBJECTS (not
// lineages — reachability is finer than a lineage: a destroyed parent's
// below-fork objects stay live through a descendant's base pointer while its
// above-fork range is reclaimed) were newly tombstoned and deleted.
//
// grace must exceed the maximum plausible fork duration: an in-flight fork
// reads its source's chain before its own ref lands, and the tombstone→grace
// →re-mark→delete two-phase (plus the mint-this-run skip below) is what
// keeps that window safe.
func (w *Workspace) GC(grace time.Duration) (tombstoned, deleted int, err error) {
	reachable, err := w.reachableObjects()
	if err != nil {
		return 0, 0, err
	}
	// List AFTER marking: an object that lands in the gap (a fork's base.json,
	// a flush's segment) may be listed as unreachable and tombstoned, but the
	// mint-this-run skip plus phase 2's re-mark on a later run rescue it —
	// the reverse order could miss marking an object it then never re-lists.
	all, err := w.Store.B.List("data/")
	if err != nil {
		return 0, 0, err
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		return 0, 0, err
	}

	// Phase 1: tombstone unreachable objects not already marked.
	newStones := map[string]string{}
	for _, key := range all {
		if reachable[key] {
			continue
		}
		if _, marked := stones[key]; !marked {
			newStones[key] = time.Now().UTC().Format(time.RFC3339Nano)
			tombstoned++
		}
	}
	if len(newStones) > 0 {
		if err := w.tombstone(newStones); err != nil {
			return tombstoned, 0, err
		}
		// Reload stones after tombstone to get the newly marked entries
		stones, _, err = w.loadTombstones()
		if err != nil {
			return tombstoned, 0, err
		}
	}

	// Everything phase 2 removes from `stones` — swept keys, stones whose
	// object is already gone, rescued (re-referenced) stones, legacy
	// lineage-keyed stones — is persisted at the end as a prune of exactly
	// those keys against the store's CURRENT list, by CAS (pruneTombstones).
	// loadedKeys is the reference point for "removed by this pass".
	loadedKeys := make([]string, 0, len(stones))
	for k := range stones {
		loadedKeys = append(loadedKeys, k)
	}
	removedByThisPass := func() []string {
		var removed []string
		for _, k := range loadedKeys {
			if _, still := stones[k]; !still {
				removed = append(removed, k)
			}
		}
		return removed
	}

	// Phase 2: sweep stones older than grace that are STILL unreachable
	// under a re-mark (recomputed lazily, once, when the first grace-eligible
	// stone is found — a fork or flush could have re-referenced an object
	// since phase 1's mark, and since a previous run's). The re-list of
	// data/ alongside it prunes stones whose object is already gone —
	// including any lineage-keyed stone left by the pre-object-granular
	// format, which names no real object key and so re-lists to nothing.
	cutoff := time.Now().Add(-grace)
	var reMark, existing map[string]bool
	var liveHeads map[string]liveHead
	var toDelete []string
	for key, markedAt := range stones {
		if _, mintedThisRun := newStones[key]; mintedThisRun {
			// A stone minted in phase 1 above is timestamped `time.Now()`,
			// which trivially satisfies a grace=0 cutoff computed moments
			// later in this same call. Without this skip, one gc(0) call
			// could tombstone AND sweep a newly unreachable object in a
			// single run — e.g. one a fork is mid-flight reading, in the
			// narrow window between the fork's read and its own ref landing.
			// Sweeps must always wait for a later, independent GC run.
			continue
		}
		ts, perr := time.Parse(time.RFC3339Nano, markedAt)
		if perr != nil || !ts.Before(cutoff) {
			continue
		}
		if reMark == nil {
			// The re-mark's own ref fetch doubles as the compensating rule's
			// live-head input (see reachableObjectsAndHeads): one ListRefs +
			// R GetRefs instead of two, and liveHeads reflects exactly the
			// refs this re-mark resolved — one consistency window.
			if reMark, liveHeads, err = w.reachableObjectsAndHeads(); err != nil {
				return tombstoned, deleted, err
			}
			relisted, lerr := w.Store.B.List("data/")
			if lerr != nil {
				return tombstoned, deleted, lerr
			}
			existing = make(map[string]bool, len(relisted))
			for _, k := range relisted {
				existing[k] = true
			}
		}
		if reMark[key] {
			// Re-referenced during grace (a fork or flush landed and its ref
			// now reaches this object): PRUNE the stone rather than keeping
			// it. Keeping it with its original timestamp would let a later,
			// legitimate death of this object (its branch destroyed) be
			// swept against the STALE stone with effectively zero grace —
			// the exact fork-in-flight window the grace period exists to
			// protect. Pruned, the object gets a FRESH tombstone and a full
			// grace period from phase 1 of a later run if it ever becomes
			// unreachable again.
			delete(stones, key)
			continue
		}
		if !existing[key] {
			delete(stones, key) // already gone (or a legacy lineage-keyed stone): prune
			continue
		}
		// Compensating rule: NEVER delete a member object above a live ref's
		// head in that ref's OWN lineage, UNLESS the object is provably
		// fenced — written under an epoch strictly older than the lineage's
		// CURRENT writer generation, and so can never win the ref CAS that
		// would make it live. A session flush that hit an ambiguous
		// ref-write failure retries by re-Putting the SAME (lineage, epoch,
		// txid) key (see session/flush.go's orphan-overwrite path) —
		// HeadTXID only advances on a successful ref write, so its retry
		// txid is always head+1 and the sweep deleting that key in the
		// window between the retry's Put and its ref CAS would leave a live
		// ref pointing at nothing. Such an orphan is unreachable by
		// definition (nothing references beyond head), so reachability alone
		// cannot protect it; this rule does, narrowly: only member-parseable
		// keys (base.json never carries a txid), only in a lineage a live ref
		// names as its head lineage, only above that head, only when the
		// object's epoch could still become the live ref's epoch.
		//
		// Epoch-blind protection leaks: an orphan written by a writer that
		// was later FENCED (AcquireLease bumped Epoch out from under it —
		// see lease.go) sits at an epoch strictly below the lineage's
		// current Epoch, and can never win a future ref CAS: the fenced
		// writer's own flush would refuse outright (session/flush.go's
		// ref.Epoch != lease.Epoch check, ErrFenced) before ever reaching
		// its terminal PutRef, and even a writer that raced past that check
		// somehow still loses the CAS itself — the epoch bump goes through
		// AcquireLease, which writes a fresh etag (lease.go's PutRef), so
		// the fenced writer's PutRef (built from its stale GetRef's etag)
		// fails unconditionally, not just probabilistically. (An at-rest
		// checkpoint is the same shape: it holds the lease, writes under the
		// epoch its acquire minted, and its head write re-checks holder and
		// epoch before its CAS.)
		// keepHighestEpoch is NOT the mechanism here — it has no knowledge
		// of Ref.Epoch at all; it only picks the higher-epoch member between
		// two objects covering the IDENTICAL txid range, which is chain
		// RESOLUTION's tie-break, not what stops a fenced writer's CAS.
		// So a fenced orphan can never become live — yet an epoch-blind rule
		// protects it forever while the branch lives. Requiring
		// m.Epoch >= refEpoch closes that leak while preserving the
		// original guarantee: a retry by the CURRENT holder always writes
		// under Ref.Epoch (its live lease's epoch), so its retry key always
		// satisfies m.Epoch == refEpoch >= refEpoch and stays protected.
		//
		// Direction of safety: liveHeads' Epoch is the MINIMUM Epoch read
		// across every ref naming the lineage (see reachableObjectsAndHeads),
		// so a stale or racing read only ever makes refEpoch SMALLER, which
		// only makes ">=" true for MORE keys — errs toward protecting, never
		// toward sweeping something that might still become live. A key at
		// an epoch NEWER than the (possibly stale) refEpoch we read — e.g. a
		// new acquirer bumped Epoch after our read — still satisfies ">="
		// and stays protected. Only a key at an epoch STRICTLY OLDER than
		// what we read is reclaimed, and "strictly older than even our
		// lower-bound read" is provably fenced under any interleaving.
		if m, ok := store.ParseMemberKey(key); ok {
			if lineage, lok := lineageOfDataKey(key); lok {
				if h, live := liveHeads[lineage]; live && m.MaxTXID > h.TXID && m.Epoch >= h.Epoch {
					continue
				}
			}
		}
		toDelete = append(toDelete, key)
	}
	// Delete every eligible key in one batched call (sweepDelete: the
	// backend's BatchDeleter capability when it has one — S3's DeleteObjects,
	// 1000 keys per RPC instead of one Delete RPC each, perf audit H2 — else
	// a per-key loop with identical results). Gates and eligible set are
	// exactly the per-key loop's; only the RPC shape changed. Sorted so the
	// delete order (and, on a partial failure, which keys got done first) is
	// deterministic instead of map-iteration order.
	if len(toDelete) > 0 {
		sort.Strings(toDelete)
		succeeded, derr := w.sweepDelete(toDelete)
		for _, key := range succeeded {
			delete(stones, key)
		}
		deleted += len(succeeded)
		if derr != nil {
			// Abort the pass on a delete error, but don't lose the progress
			// made: the succeeded keys' stones are pruned and persisted, the
			// failed keys keep their (original-timestamp) stones for a later
			// pass. The same clobber-safety argument as the Put below applies.
			if perr := w.pruneTombstones(removedByThisPass()); perr != nil {
				return tombstoned, deleted, errors.Join(derr, perr)
			}
			return tombstoned, deleted, derr
		}
	}
	if gcBeforePruneForTest != nil {
		gcBeforePruneForTest() // test-only: a concurrent pass writes between our load and our prune
	}
	// Persist this pass's removals by CAS against the CURRENT list. An
	// unconditional write of our in-memory `stones` would clobber whatever
	// a concurrent GC or janitor pass wrote since we loaded it: phase-1
	// additions (merely delaying their sweep) but also prunes, resurrecting
	// a stone with its old timestamp for a key the other pass already
	// deleted — and a fork in flight could recreate that key under a stale,
	// already expired stone. Pruning exactly our removals by CAS loses
	// neither.
	if err := w.pruneTombstones(removedByThisPass()); err != nil {
		return tombstoned, deleted, err
	}
	return tombstoned, deleted, nil
}

// gcBeforePruneForTest, when non-nil, runs between GC's sweep and its final
// tombstone prune: the window a concurrent GC or janitor pass races. Nil in
// production; test-only, process-global, restore via t.Cleanup.
var gcBeforePruneForTest func()

// pruneTombstones removes keys from the stored tombstone list by
// compare-and-swap: reload the list with its etag, drop exactly keys, write
// it back conditionally, and on a conflict reload and apply the same prune
// again. A bounded retry: every loser re-reads the winner's list, so the
// only way to keep losing is a storm of concurrent GC passes, which the
// janitor's single-flight schedule does not produce.
func (w *Workspace) pruneTombstones(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		cur, etag, err := w.loadTombstones()
		if err != nil {
			return err
		}
		changed := false
		for _, k := range keys {
			if _, ok := cur[k]; ok {
				delete(cur, k)
				changed = true
			}
		}
		if !changed {
			return nil // another pass already pruned them
		}
		data, _ := json.Marshal(cur)
		_, err = w.Store.B.PutIf(tombstoneKey, data, etag)
		if err == nil {
			return nil
		}
		if !errors.Is(err, store.ErrCAS) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("ops: gc tombstone prune lost %d races: %w", 8, lastErr)
}

// sweepDelete is GC phase 2's delete step: remove keys via the backend's
// optional BatchDeleter capability (store.BatchDeleter — S3 batches 1000
// keys per DeleteObjects RPC) when present, else per-key Delete. Both paths
// return the keys ACTUALLY deleted so the caller prunes exactly those
// tombstones; on an error the returned prefix of keys is what succeeded
// before the failure (BatchDeleter's own contract; the fallback loop stops
// at the first error, exactly what the old inline per-key sweep did).
func (w *Workspace) sweepDelete(keys []string) (deleted []string, err error) {
	if bd, ok := w.Store.B.(store.BatchDeleter); ok {
		return bd.DeleteObjects(keys)
	}
	for _, key := range keys {
		if err := w.Store.B.Delete(key); err != nil {
			return deleted, err
		}
		deleted = append(deleted, key)
	}
	return deleted, nil
}
