package ops

import (
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
// same as an unforced call).
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
			return fmt.Errorf("ops: %s@%s has a live lease held by %q until %s; use --force",
				db, branch, ref.LeaseHolder, ref.LeaseExpiry)
		}
	}

	// CAS claim: mark the ref as being deleted. A concurrent AcquireLease
	// either landed first (our PutRef below fails on ErrCAS — report as a
	// lost race, retryable) or will fail loudly on seeing Deleting (see
	// store.AcquireLease/ErrDeleting).
	ref.Deleting = true
	ref.DeletingAt = time.Now().UTC().Format(time.RFC3339Nano)
	claimEtag, err := w.Store.PutRef(db, branch, ref, etag)
	if err != nil {
		if errors.Is(err, store.ErrCAS) {
			return fmt.Errorf("ops: destroy lost a race on %s@%s (retry): %w", db, branch, err)
		}
		return err
	}

	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			// This call knows right now it isn't going to finish — unwind
			// the claim eagerly rather than leave it for
			// ClearStaleDeleteClaims's age-based self-heal to eventually
			// catch.
			w.unwindDeletingClaim(db, branch)
			return fmt.Errorf("ops: checkout in use; close connections before destroy: %w", err)
		}
	}
	// DeleteRefIf is a true CAS delete on the local backend (belt-and-
	// suspenders on top of the claim above) and an unconditional delete on
	// S3 (which cannot condition a DeleteObject call at all) — either way,
	// the Deleting claim already landed above is what actually serializes
	// this against a concurrent AcquireLease/Destroy on every backend. See
	// store.DeleteRefIf's doc comment.
	if err := w.Store.DeleteRefIf(db, branch, claimEtag); err != nil {
		w.unwindDeletingClaim(db, branch)
		return err
	}
	// Best-effort checkout removal: the ref is already gone, so a failed
	// remove cannot corrupt anything (a re-created branch's Checkout
	// overwrites), but nothing else ever cleans checkouts/ — log so the
	// stale files aren't a silent disk leak.
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".sum"} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "offshoot: destroy %s@%s: remove checkout file %s: %v\n",
				db, branch, p, err)
		}
	}
	return nil
}

// unwindDeletingClaim best-effort clears a Deleting claim this Destroy call
// itself just landed but could not carry through to a successful delete (a
// checkout-quiesce failure, a DeleteRefIf error). A lost CAS race here
// (someone else already cleared or reclaimed this ref in the meantime) is
// benign — the same idempotent-unwind shape as reapOne's own Reaping unwind
// on a failed Destroy call.
func (w *Workspace) unwindDeletingClaim(db, branch string) {
	if ref, etag, err := w.Store.GetRef(db, branch); err == nil && ref.Deleting {
		ref.Deleting = false
		ref.DeletingAt = ""
		_, _ = w.Store.PutRef(db, branch, ref, etag) // best effort; ClearStaleDeleteClaims retries later regardless
	}
}

// staleDeletingClaimAfter bounds how long a Destroy claim (Ref.Deleting) can
// sit unresolved before ClearStaleDeleteClaims treats it as abandoned by a
// crashed Destroy call rather than one still legitimately in flight. Destroy
// between its claim write and its terminal DeleteRefIf/unwind is a handful
// of local filesystem removes plus at most one quiesce call (bounded by its
// own internal busy-timeout) — order of milliseconds in the healthy case, so
// this is deliberately generous, matching the local backend's own
// lock-staleness window (Local.lock: 30s) rather than trying to tune a
// tighter bound.
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
// it self-heals purely by age (staleDeletingClaimAfter). A ref with an
// unparseable DeletingAt is left alone (conservative, matching Reap's own
// stance on an unparseable TTL/expiry elsewhere in this package) rather than
// guessed at. Called by the janitor on every tick, alongside Reap/GC.
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
			if perr != nil || now.Sub(claimedAt) < staleDeletingClaimAfter {
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

// reachableObjectsAndHeads is reachableObjects' full form: alongside the
// reachable set it returns, for every lineage some live ref names as its OWN
// (head) lineage, the smallest HeadTXID among the refs naming it — the input
// to GC's compensating rule (see the phase-2 sweep): an object ABOVE a live
// ref's head in that ref's own lineage may be an orphan a session flush is
// entitled to re-create at the same key (session/flush.go's ambiguous-
// ref-write retry overwrites the SAME (lineage, epoch, txid) key), so the
// sweep must never delete there. Smallest head because that is the txid a
// retrying writer would build up from. Deriving heads from the SAME
// ListRefs+GetRef fetch as the mark (rather than a separate re-enumeration,
// as a standalone liveHeadLineages helper once did) both saves 1 ListRefs +
// R GetRef RPCs per sweeping pass and guarantees the compensating rule sees
// exactly the refs the re-mark saw — one consistency window, not two.
func (w *Workspace) reachableObjectsAndHeads() (map[string]bool, map[string]uint64, error) {
	ms := &store.Store{B: newMarkCache(w.Store.B)}
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, nil, err
	}
	reachable := map[string]bool{}
	heads := map[string]uint64{}
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
			if h, ok := heads[r.Lineage]; !ok || r.HeadTXID < h {
				heads[r.Lineage] = r.HeadTXID
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

	// Phase 2: sweep stones older than grace that are STILL unreachable
	// under a re-mark (recomputed lazily, once, when the first grace-eligible
	// stone is found — a fork or flush could have re-referenced an object
	// since phase 1's mark, and since a previous run's). The re-list of
	// data/ alongside it prunes stones whose object is already gone —
	// including any lineage-keyed stone left by the pre-object-granular
	// format, which names no real object key and so re-lists to nothing.
	cutoff := time.Now().Add(-grace)
	var reMark, existing map[string]bool
	var liveHeads map[string]uint64
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
		// head in that ref's OWN lineage. A session flush that hit an
		// ambiguous ref-write failure retries by re-Putting the SAME
		// (lineage, epoch, txid) key (see session/flush.go's orphan-overwrite
		// path) — HeadTXID only advances on a successful ref write, so its
		// retry txid is always head+1 and the sweep deleting that key in the
		// window between the retry's Put and its ref CAS would leave a live
		// ref pointing at nothing. Such an orphan is unreachable by
		// definition (nothing references beyond head), so reachability alone
		// cannot protect it; this rule does, narrowly: only member-parseable
		// keys (base.json never carries a txid), only in a lineage a live ref
		// names as its head lineage, only above that head. The stone is KEPT:
		// if the writer's retry lands, the object becomes reachable and the
		// stone is rescued/pruned above; if the branch dies instead, the
		// lineage drops out of liveHeads and a later run sweeps it.
		if m, ok := store.ParseMemberKey(key); ok {
			if lineage, lok := lineageOfDataKey(key); lok {
				if head, live := liveHeads[lineage]; live && m.MaxTXID > head {
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
			data, _ := json.Marshal(stones)
			if perr := w.Store.B.Put(tombstoneKey, data); perr != nil {
				return tombstoned, deleted, errors.Join(derr, perr)
			}
			return tombstoned, deleted, derr
		}
	}
	// Persist the pruned stone list. This is an unconditional Put, not a
	// CAS: it can clobber phase-1 additions from a concurrent GC run that
	// landed after we loaded `stones` above. That's safe in only one
	// direction — the clobbered object isn't lost, it just isn't tombstoned
	// yet; the next GC run's phase 1 re-derives it from reachableObjects and
	// the data/ listing and re-marks it with a fresh timestamp, at worst
	// delaying its eventual sweep by one grace period. It can never cause a
	// live or still-within-grace object to be deleted early.
	data, _ := json.Marshal(stones)
	if err := w.Store.B.Put(tombstoneKey, data); err != nil {
		return tombstoned, deleted, err
	}
	return tombstoned, deleted, nil
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
