package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

// ErrFenced reports that the session's lease was lost: its epoch is dead, so
// it must not write. Fencing is terminal for a session.
var ErrFenced = errors.New("session: fenced — lease lost")

// Flush uploads the replica's current state as a snapshot under the session's
// lease epoch and advances the branch head. name is optional: when non-empty
// the flushed state is also recorded as a named checkpoint; Flush checks
// up front that no checkpoint of that name already exists on the branch and
// fails without touching anything if it does — a courtesy beyond the minimal
// requirement (PutRef's CAS alone would eventually catch a stale ref) that
// lets a name collision fail fast with a clear message instead of surfacing
// as an opaque ref-CAS retry.
//
// Flush never touches the agent's checkout directly: it asks the capture
// engine to catch up to whatever is currently committed there (see
// capture.Engine.DrainNow) and then encodes the replica, which the capture
// engine keeps at a transaction boundary.
//
// The whole call runs under flushMu, so concurrent Flush calls on the same
// Session are serialized rather than racing to compute and write the same
// txid. See flushMu's doc comment for the lock-ordering invariant this
// implies.
//
// meta (nil = none) is only meaningful when name != "": it is stored on the
// resulting named checkpoint's store.Checkpoint.Meta, capped by
// ops.ValidateMeta exactly like ops.Workspace.Checkpoint's own meta param
// (this is the daemon's live-session equivalent of that at-rest call — see
// the daemon "flush" op's doc comment). A non-empty meta with name == "" is
// rejected: there is no checkpoint for it to attach to, and silently
// dropping it would surprise a caller who set both.
func (s *Session) Flush(name string, meta map[string]string) (uint64, error) {
	return s.flush(name, meta, false)
}

// flush is Flush's actual implementation, plus the auto/manual distinction
// flushLoop needs for its own "flushed"/"flush-failed" transition logging
// (see the deferred call below) — a distinction Flush's own public signature
// has no room for. flushLoop calls this directly (same package, so no
// exported surface changes) with meta always nil, since its unnamed
// auto-flushes never create a checkpoint. Flush itself is just auto=false.
//
// Named returns (txid, err) exist for exactly one reason: the deferred log
// call below needs to see this call's actual outcome regardless of which of
// the many return statements in the body below produced it, without
// touching every one of them individually. Every "return N, someErr"
// statement in the body works unchanged under named returns — Go assigns
// through the names before running deferred funcs either way.
func (s *Session) flush(name string, meta map[string]string, auto bool) (txid uint64, err error) {
	// flushKind — not "kind", which the body below already uses for the
	// object kind ("snapshot" vs "segment") — is this call's auto/manual
	// tag for the deferred transition log below.
	flushKind := "manual"
	if auto {
		flushKind = "auto"
	}
	// start times the WHOLE call (every path below: catch-up drain, encode,
	// upload, ref CAS) — the same span the "flushed"/"flush-failed"
	// transition below reports outcome for — so "duration_seconds" in that
	// kv is this attempt's full observed latency, not just its encode or
	// upload sub-step. Read only by the deferred transition log immediately
	// below; nothing else in this function depends on it.
	start := time.Now()
	defer func() {
		durationSeconds := time.Since(start).Seconds()
		if err != nil {
			if errors.Is(err, ErrClosed) {
				// A Flush racing Close's own shutdown is not an operational
				// failure worth logging — see ErrClosed's doc comment; the
				// "closed" transition (Session.Close) already covers this
				// moment.
				return
			}
			s.logTransition("flush-failed", "kind", flushKind, "error", err.Error(), "duration_seconds", durationSeconds)
			return
		}
		s.logTransition("flushed", "kind", flushKind, "txid", txid, "duration_seconds", durationSeconds)
	}()

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return 0, ErrClosed
	}
	if err := s.Err(); err != nil {
		return 0, err
	}
	if name != "" {
		if err := store.ValidateName(name); err != nil {
			return 0, err
		}
	} else if len(meta) > 0 {
		return 0, fmt.Errorf("session: meta given with no checkpoint name; meta attaches to a named checkpoint")
	}
	if err := ops.ValidateMeta(meta); err != nil {
		return 0, err
	}

	// Catch the replica up to whatever is already committed in the checkout
	// before encoding it. Without this, the capture engine's own poll
	// interval (default 10ms) can leave the replica lagging a write the
	// caller already committed to the checkout: Flush would then encode a
	// stale replica, advance the branch head to reference a snapshot that
	// silently omits that write, and still report success. See
	// capture.Engine.DrainNow's doc comment.
	// This timeout must comfortably exceed DrainNow's own worst case, not
	// just approximate it: a single pollOnce triggered by DrainNow can, under
	// write contention, spend up to ~25s inside the capture engine alone —
	// drainUntil's own drainSafetyDeadline (15s) backstopping the drain-to-
	// target phase itself, plus up to ~10s more if a takeover follows:
	// checkpoint() retries busy for up to 5s, and a takeover that completes
	// its checkpoint but then needs beginReadRetry to reacquire the read
	// lock retries for up to another 5s (see internal/capture/engine.go's
	// drainSafetyDeadline, checkpoint, and beginReadRetry). A tighter
	// flush-side timeout could then spuriously fire under exactly the
	// contention DrainNow exists to ride out. 30s leaves a real (if not
	// huge) margin above that ~25s budget; if the engine's own retry or
	// drain-safety budgets ever grow, this constant should be revisited
	// rather than left silently coupled to them.
	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = s.captured.DrainNow(dctx)
	cancel()
	if err != nil {
		return 0, fmt.Errorf("session: catch up before flush: %w", err)
	}

	lease := s.Lease()
	st := s.ws.Store

	ref, etag, err := st.GetRef(s.db, s.branch)
	if err != nil {
		return 0, err
	}
	if ref.LeaseHolder != lease.Holder || ref.Epoch != lease.Epoch {
		err := fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
			ErrFenced, s.db, s.branch, ref.LeaseHolder, ref.Epoch)
		s.fail(err)
		return 0, err
	}
	if name != "" {
		if _, exists := ref.Checkpoints[name]; exists {
			return 0, fmt.Errorf("session: checkpoint %q already exists on %s@%s",
				name, s.db, s.branch)
		}
	}

	// Divergence-floor seeding (copy-on-write Task 4 — see divergenceSeeded's
	// doc comment on Session for the full argument): on this session's first
	// flush, initialize flushesSinceSnapshot from the branch's DURABLE
	// divergence — the trailing run of segment members in the resolved chain
	// at head — so the snapshot cadence continues across sessions instead of
	// restarting at zero with every process. Only the settling-flush
	// suppression makes this observable (every other first flush writes a
	// full snapshot and resets the counter regardless), so the probe is
	// skipped whenever this first flush is already bound for the snapshot
	// path: a brand-new lineage (HeadTXID 0 → txid 1), forceSnapshot still
	// set (no suppression fired), or pageSize 0 (recordApply never ran).
	// That gating is also the cost story: read-only sessions never flush and
	// never pay the List; a non-suppressed writer's first flush snapshots
	// without paying it; only a suppressed writer's first flush costs one
	// store List — on the write path, alongside the GetRef/upload/PutRef it
	// is already paying. Peeking forceSnapshot/pageSize takes replicaMu
	// briefly (flushMu → replicaMu is this package's documented lock order);
	// a rebase racing in AFTER the peek can only turn this flush INTO a
	// snapshot, wasting the probe's List, never the reverse.
	if !s.divergenceSeeded {
		s.divergenceSeeded = true
		s.replicaMu.Lock()
		suppressed := !s.forceSnapshot && s.pageSize != 0
		s.replicaMu.Unlock()
		if suppressed && ref.HeadTXID > 0 {
			if chain, cerr := st.Chain(ref.Lineage, ref.HeadTXID); cerr == nil {
				trailing := 0
				for i := len(chain) - 1; i >= 0 && !chain[i].Snapshot; i-- {
					trailing++
				}
				s.flushesSinceSnapshot = trailing
			} else {
				// Fail toward the bounded outcome: with no durable count to
				// trust, treat the cadence as due so this flush writes a full
				// snapshot (always safe — it depends on nothing pending) and
				// resets the counter, rather than propagating a probe-only
				// error or silently under-counting and letting the chain grow
				// past the floor.
				s.flushesSinceSnapshot = s.snapshotEvery - 1
			}
		}
	}

	txid = ref.HeadTXID + 1
	var buf bytes.Buffer

	// replicaMu excludes the capture engine's Rebase/Apply for the duration
	// of this section, so the replica file — and the pages/checksum/commit
	// state recordApply and rebaseline maintain alongside it (see
	// replicaMu's doc comment) — are frozen at a single transaction boundary
	// while this flush decides what to write and encodes it.
	s.replicaMu.Lock()
	if FlushEncodeHook != nil {
		FlushEncodeHook() // test hook; nil (a no-op) in production
	}

	// The fraction check below only matters once the database is big enough
	// that "segment vs. snapshot" is actually a meaningful size trade-off —
	// skip it under minPagesForFractionCheck so a handful of changed pages
	// in a small database (where nearly every write touches a large share
	// of a tiny page count) doesn't defeat the SnapshotEvery cadence by
	// forcing a snapshot on every flush.
	pageFrac := 0.0
	if s.commit >= minPagesForFractionCheck {
		pageFrac = float64(s.pages.len()) / float64(s.commit)
	}
	// Snapshot instead of segment when: this is TXID 1 — the very first
	// entry a lineage can have (a segment cannot start a chain there:
	// EncodeSegment itself refuses MinTXID 1, and there is no preceding
	// member for it to declare a preApplyChecksum against); the cadence says
	// it's time; forceSnapshot demands it (rebaseline set it, or an earlier
	// flush attempt already drained this pending state without confirming
	// success — see below); or the pending pages are already a large enough
	// fraction of the database that a segment buys little over just
	// re-snapshotting.
	//
	// This snapshot check is reached by forceSnapshot alone on a session's
	// first-ever Flush call in the common case, but — since the
	// settling-flush suppression, M2 follow-up, see rebaseline's doc
	// comment — no longer unconditionally. A session whose startup rebase
	// proved the checkout was already clean-and-unchanged against the
	// durable head (cleanAtOpen && headPostApplyChecksum matched) has
	// forceSnapshot cleared by that same rebaseline call: for THAT session,
	// forceSnapshot is false, flushesSinceSnapshot is still 0, and txid is
	// NOT 1 (cleanAtOpen requires an existing durable head to have been
	// clean against, so the branch already has history) going into its
	// first real Flush — whichever the session's own first actual write
	// eventually triggers. None of the conditions in the list above force a
	// snapshot there, so that flush takes the SEGMENT path, continuing
	// directly from the branch's pre-session head with no intervening
	// settle-snapshot at all. This is a real, exercised shape, not a
	// theoretical corner — see TestCleanAtOpenSessionsFirstRealFlushIsASegment
	// — and it is exactly why headPostApplyChecksum has to be the TRUE
	// durable head's checksum (fetched from the store) rather than anything
	// weaker: a segment declared here has preApplyChecksum = s.flushChecksum,
	// which is only correct if the checkout genuinely never diverged from
	// that durable head during the suppressed rebase.
	//
	// On every OTHER path — a brand-new lineage (ops.Create's own txid==1),
	// any checkout that was not clean-at-open, or a resumed session where
	// the suppression's proof didn't hold — forceSnapshot IS still
	// unconditionally true going into the first Flush, exactly as before
	// this suppression existed: on the ordinary path (a branch that already
	// has history — the common non-suppressed case) the capture engine's
	// mandatory startup Rebase runs before Open returns and calls
	// replicaSink.Rebase, which sets forceSnapshot true (and the
	// suppression did not clear it, not being eligible); on a resumed
	// session, Open's own direct rebaseline() call does the same. Either
	// way, txid==1 or forceSnapshot alone already forces a snapshot there,
	// so the cadence/fraction checks above are redundant with it, not the
	// deciding factor. Kept listed anyway (rather than collapsed into "just
	// forceSnapshot") because each remains independently sufficient
	// reasoning on its own — txid==1 in particular is a hard
	// EncodeSegment-level invariant, true regardless of any session's own
	// forceSnapshot bookkeeping.
	//
	// s.pageSize == 0 belongs in this same list, not handled separately
	// below: it means recordApply has never run (a Flush racing session
	// startup, before the capture engine's first Apply — pageSize is only
	// ever set there, never by rebaseline), so there is no valid
	// pageSizeAtEncode for EncodeSegment to use. EncodeSnapshot has no such
	// dependency — it derives the page size itself from the replica file's
	// own SQLite header — so forcing a snapshot here sidesteps the problem
	// entirely instead of merely detecting and erroring on it.
	snapshot := txid == 1 ||
		s.flushesSinceSnapshot >= s.snapshotEvery-1 ||
		s.forceSnapshot ||
		pageFrac >= largeSegmentFraction ||
		s.pageSize == 0

	// Either branch below consumes the pending pageSet: a snapshot makes it
	// stale (it's now fully reflected in the fresh full state) and a segment
	// consumes it as its payload. Capture the checksum this attempt is
	// keyed to, and the rebase generation as of right now, in the same
	// breath: genAtEncode lets the post-upload bookkeeping below detect a
	// rebase that raced this flush's upload/PutRef (which run without
	// replicaMu held) and defer to rebaseline's own, more current baseline
	// instead of silently overwriting it with these now-stale values.
	pages := s.pages.drain()
	checksumAtEncode := s.checksum
	pageSizeAtEncode, commitAtEncode := s.pageSize, s.commit
	genAtEncode := s.rebaseGen
	// appliedGenAtEncode is the idle short-circuit's counterpart to
	// genAtEncode: the value appliedGen held at exactly this drain point, so
	// a confirmed success below can record "everything through here is now
	// durable" without racing a transaction applied later, concurrently,
	// while this attempt's upload/PutRef run without replicaMu held — see
	// appliedGen's doc comment on Session.
	appliedGenAtEncode := s.appliedGen
	// Pessimistically assume this attempt will not pan out: pageSet has
	// already been drained above, so if anything below fails, a retried
	// Flush must not trust a segment rebuilt from what (little, or nothing)
	// is left in pageSet — force it to fall back to a full snapshot, which
	// needs no continuity with pageSet at all. Cleared only once this
	// attempt is confirmed to have succeeded, below.
	s.forceSnapshot = true

	if snapshot {
		// The returned checksum is discarded: Session already tracks its
		// own running checksum incrementally (s.checksum, maintained by
		// recordApply/rebaseline) and checksumAtEncode below is captured
		// from exactly that — it necessarily already equals whatever
		// EncodeSnapshot independently (re-)computes over this same replica
		// file, so there is nothing new to learn from asking again here.
		_, err = ltxio.EncodeSnapshot(s.replica.Path(), txid, &buf)
	} else {
		// pageSizeAtEncode is guaranteed non-zero here: snapshot's own
		// condition above already covers s.pageSize == 0, so this branch is
		// only ever reached once recordApply has run at least once.
		err = ltxio.EncodeSegment(pageSizeAtEncode, commitAtEncode, txid, txid,
			s.flushChecksum, checksumAtEncode, pages, &buf)
	}
	s.replicaMu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("session: encode replica: %w", err)
	}

	if FlushUploadHook != nil {
		FlushUploadHook() // test hook; nil (a no-op) in production
	}

	var objKey, kind string
	if snapshot {
		objKey, kind = store.SnapshotKey(ref.Lineage, lease.Epoch, txid), "snapshot"
	} else {
		objKey, kind = store.SegmentKey(ref.Lineage, lease.Epoch, txid, txid), "segment"
	}
	if _, err := st.B.PutIf(objKey, buf.Bytes(), ""); err != nil {
		if !errors.Is(err, store.ErrCAS) {
			return 0, fmt.Errorf("session: upload %s: %w", kind, err)
		}
		// The only thing that can already occupy this key is an orphan from a
		// crashed prior attempt: HeadTXID only advances on a successful ref
		// write, so nothing references txid beyond head yet.
		if err := st.B.Put(objKey, buf.Bytes()); err != nil {
			return 0, fmt.Errorf("session: overwrite orphaned %s: %w", kind, err)
		}
	}

	ref.HeadTXID, ref.HeadEpoch = txid, lease.Epoch
	if name != "" {
		ref.SetCheckpoint(name, store.Checkpoint{
			TXID: txid, Epoch: lease.Epoch,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
			Meta:      meta,
		})
	}
	ref.Touch(time.Now())
	if _, err := st.PutRef(s.db, s.branch, ref, etag); err != nil {
		// Decide whether to clean up the uploaded object after a failed ref
		// update. This mirrors ops.Checkpoint's identical decision exactly
		// (see its comment for the full reasoning) — flushMu serializes
		// concurrent Flush calls on THIS Session, but a rival can still win
		// the ref CAS out from under us: another holder that reclaimed the
		// lease between our GetRef above and this PutRef, or a crashed prior
		// Flush attempt that already occupies objKey.
		//
		// On ErrCAS (lost the CAS race): PutIf's serialization means the
		// winner's ref, if any, is already visible. Re-read it and delete the
		// object ONLY when it's confirmed unreferenced — the current ref is
		// on a different lineage, or its HeadTXID hasn't reached txid. If the
		// current ref DOES already reference (lineage, txid), some other
		// actor already committed exactly what we tried to commit, and
		// deleting would rip a live object out from under it.
		//
		// On any non-CAS error (lock timeout, I/O error, network blip): the
		// write may have actually landed server-side while the client only
		// saw an error — this is the ambiguous case a failure here MUST NOT
		// guess through. Deleting unconditionally, as this code used to,
		// would risk leaving a live ref pointing at objKey with no object
		// behind it: silent, unrecoverable data loss in the one primitive
		// whose job is preventing exactly that. Leave the object alone; at
		// worst it's a harmless orphan that a future flush's overwrite path
		// (above) reclaims, if and when it computes this same txid again.
		// GC does NOT help here: it only tombstones and sweeps whole
		// lineages once they become entirely unreachable (see
		// ops.Workspace.GC) — it has no per-object sweep for an orphaned
		// object sitting inside a still-live lineage. Reclaiming those is
		// future work.
		if errors.Is(err, store.ErrCAS) {
			if cur, _, gerr := st.GetRef(s.db, s.branch); gerr == nil &&
				(cur.Lineage != ref.Lineage || cur.HeadTXID < txid) {
				st.B.Delete(objKey)
			}
			return 0, fmt.Errorf("session: flush lost a race (retry): %w", err)
		}
		return 0, fmt.Errorf("session: advance ref: %w", err)
	}

	// Confirmed success: fold this attempt's checksum baseline back in, and
	// only now clear forceSnapshot — unless a rebase raced this flush's
	// upload/PutRef (see genAtEncode above), in which case rebaseline's own
	// baseline is already more current than what this attempt captured, and
	// must not be overwritten with these stale values.
	s.replicaMu.Lock()
	if s.rebaseGen == genAtEncode {
		s.flushChecksum = checksumAtEncode
		s.forceSnapshot = false
		// flushedRebaseGen MUST be gated identically to forceSnapshot/
		// flushChecksum above, not advanced unconditionally: a rebase that
		// raced this upload has already folded content into the replica
		// (via checkpoint(TRUNCATE), before ever calling Apply — see
		// appliedGen/flushedRebaseGen's doc comment on Session) that this
		// attempt's uploaded bytes do NOT contain, because they were encoded
		// before the race. Advancing flushedRebaseGen past that race would
		// mark those un-uploaded, rebase-folded bytes durable — and since
		// forceSnapshot is deliberately left true by this same race (the gate
		// above didn't run), nothing would ever call Flush again to actually
		// ship them: flushLoop's idle short-circuit would then skip every
		// future tick forever, believing there was nothing pending, and the
		// rebase-folded write would sit unflushed indefinitely. Leaving
		// flushedRebaseGen at its pre-race value instead keeps
		// `rebaseGen != flushedRebaseGen` true, so flushLoop's very next tick
		// sees it pending and retries.
		s.flushedRebaseGen = genAtEncode
	}
	// flushedGen, by contrast, IS unconditional: appliedGen only ever
	// advances from recordApply (a real Apply call), never from a rebase,
	// and the payload this attempt uploaded was drained from pages at
	// exactly appliedGenAtEncode — so "everything Applied through here is
	// now durable" holds regardless of whether a rebase also raced this
	// upload. Any transaction Applied DURING the upload/PutRef window
	// already bumped appliedGen past appliedGenAtEncode by the time we
	// regain replicaMu here, so it correctly remains visible as
	// still-pending afterward (via the appliedGen != flushedGen half of
	// flushLoop's pending check) regardless of what this assignment does.
	s.flushedGen = appliedGenAtEncode
	s.replicaMu.Unlock()

	if snapshot {
		s.flushesSinceSnapshot = 0
	} else {
		s.flushesSinceSnapshot++
	}

	s.mu.Lock()
	s.durable = txid
	// Stamp the single ref-write site's success — covers both the snapshot
	// and segment paths above, and both a manual and an automatic
	// (flushLoop) caller: LastFlushErr clears on ANY successful flush, per
	// the Options.FlushEvery contract.
	s.lastFlushAt = time.Now()
	s.lastFlushTXID = txid
	s.lastFlushOK = true
	s.lastFlushErr = nil
	s.mu.Unlock()
	return txid, nil
}

// largeSegmentFraction bounds when Flush prefers a full snapshot over an
// incremental segment even off the SnapshotEvery cadence: once the pending
// pages cover at least this fraction of the database, a segment carries
// little size advantage over just re-snapshotting, while a snapshot resets
// the chain (bounding how many members a future read must materialize
// through) and needs no preApplyChecksum continuity with anything.
const largeSegmentFraction = 0.5

// minPagesForFractionCheck floors largeSegmentFraction's applicability: below
// this many total pages, the database is small enough that a segment vs. a
// snapshot is a negligible size difference either way, so the fraction check
// is skipped entirely and the SnapshotEvery cadence alone decides.
const minPagesForFractionCheck = 64

// FlushEncodeHook, when non-nil, is invoked by Flush immediately before it
// calls ltxio.EncodeSnapshot or ltxio.EncodeSegment, while still holding both
// flushMu and replicaMu. It exists purely for tests exercising Close/Flush
// concurrency (holding a Flush paused mid-encode so a concurrent Close can be
// observed waiting on flushMu); nil (the default) is a no-op and imposes no
// cost in production.
var FlushEncodeHook func()

// FlushUploadHook, when non-nil, is invoked by Flush immediately after it
// releases replicaMu following a successful encode — i.e. exactly inside the
// window where Flush's own upload (PutIf/PutRef) runs without replicaMu
// held, and a concurrent Rebase (a real one via the capture engine, or a
// test driving replicaSink.Rebase directly) can race in and change rebaseGen
// out from under this attempt. It exists purely for tests exercising that
// race (see TestFlushLoopRetriesAfterRebaseDuringUpload); nil (the default)
// is a no-op and imposes no cost in production.
var FlushUploadHook func()

// DurableTXID is the txid the store is durable through for this session, or
// 0 before the first flush.
func (s *Session) DurableTXID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable
}
