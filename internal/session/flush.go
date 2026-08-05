package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
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
func (s *Session) Flush(name string) (uint64, error) {
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
	err := s.captured.DrainNow(dctx)
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

	txid := ref.HeadTXID + 1
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
	// In practice this snapshot check is never reached by forceSnapshot alone
	// on a session's first-ever Flush call — it's already covered above,
	// unconditionally: every Open() leaves forceSnapshot true by the time
	// any Flush can run. On the ordinary path (a branch that already has
	// history — the common case, since ops.Create already wrote TXID 1
	// before any session ever exists) the capture engine's mandatory startup
	// Rebase runs before Open returns and calls replicaSink.Rebase, which
	// sets it. On a session that instead resumed cleanly from a reused
	// Options.Dir — where the engine deliberately skips that startup Rebase,
	// see capture.Engine.Resumed's doc comment — Open calls rebaseline()
	// itself for exactly this reason (see Open's comment). Either way,
	// forceSnapshot is true and the pending pageSet is empty going into this
	// session's first Flush, so txid==1 and the cadence/fraction checks
	// above are redundant with it there, not the deciding factor. Kept
	// listed anyway (rather than collapsed into "just forceSnapshot") because
	// each remains independently sufficient reasoning on its own — txid==1
	// in particular is a hard EncodeSegment-level invariant, true regardless
	// of whether this session's own forceSnapshot bookkeeping is ever
	// involved at all.
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
	// Pessimistically assume this attempt will not pan out: pageSet has
	// already been drained above, so if anything below fails, a retried
	// Flush must not trust a segment rebuilt from what (little, or nothing)
	// is left in pageSet — force it to fall back to a full snapshot, which
	// needs no continuity with pageSet at all. Cleared only once this
	// attempt is confirmed to have succeeded, below.
	s.forceSnapshot = true

	if snapshot {
		err = ltxio.EncodeSnapshot(s.replica.Path(), txid, &buf)
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
		ref.SetCheckpoint(name, txid, lease.Epoch)
	}
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
	}
	s.replicaMu.Unlock()

	if snapshot {
		s.flushesSinceSnapshot = 0
	} else {
		s.flushesSinceSnapshot++
	}

	s.mu.Lock()
	s.durable = txid
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

// DurableTXID is the txid the store is durable through for this session, or
// 0 before the first flush.
func (s *Session) DurableTXID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable
}
