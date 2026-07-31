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
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	// of the read, so the replica file is frozen at a single transaction
	// boundary while it is encoded — see replicaSink's doc comment.
	s.replicaMu.Lock()
	err = ltxio.EncodeSnapshot(s.replica.Path(), txid, &buf)
	s.replicaMu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("session: encode replica: %w", err)
	}

	snapKey := store.SnapshotKey(ref.Lineage, lease.Epoch, txid)
	if _, err := st.B.PutIf(snapKey, buf.Bytes(), ""); err != nil {
		if !errors.Is(err, store.ErrCAS) {
			return 0, fmt.Errorf("session: upload snapshot: %w", err)
		}
		// The only thing that can already occupy this key is an orphan from a
		// crashed prior attempt: HeadTXID only advances on a successful ref
		// write, so nothing references txid beyond head yet.
		if err := st.B.Put(snapKey, buf.Bytes()); err != nil {
			return 0, fmt.Errorf("session: overwrite orphaned snapshot: %w", err)
		}
	}

	ref.HeadTXID, ref.HeadEpoch = txid, lease.Epoch
	if name != "" {
		ref.SetCheckpoint(name, txid, lease.Epoch)
	}
	if _, err := st.PutRef(s.db, s.branch, ref, etag); err != nil {
		// Decide whether to clean up the uploaded snapshot after a failed ref
		// update. This mirrors ops.Checkpoint's identical decision exactly
		// (see its comment for the full reasoning) — flushMu serializes
		// concurrent Flush calls on THIS Session, but a rival can still win
		// the ref CAS out from under us: another holder that reclaimed the
		// lease between our GetRef above and this PutRef, or a crashed prior
		// Flush attempt that already occupies snapKey.
		//
		// On ErrCAS (lost the CAS race): PutIf's serialization means the
		// winner's ref, if any, is already visible. Re-read it and delete the
		// snapshot ONLY when it's confirmed unreferenced — the current ref is
		// on a different lineage, or its HeadTXID hasn't reached txid. If the
		// current ref DOES already reference (lineage, txid), some other
		// actor already committed exactly what we tried to commit, and
		// deleting would rip a live snapshot out from under it.
		//
		// On any non-CAS error (lock timeout, I/O error, network blip): the
		// write may have actually landed server-side while the client only
		// saw an error — this is the ambiguous case a failure here MUST NOT
		// guess through. Deleting unconditionally, as this code used to,
		// would risk leaving a live ref pointing at SnapshotKey with no
		// object behind it: silent, unrecoverable data loss in the one
		// primitive whose job is preventing exactly that. Leave the object
		// alone; at worst it's a harmless orphan a future flush's overwrite
		// path (above) or GC reclaims.
		if errors.Is(err, store.ErrCAS) {
			if cur, _, gerr := st.GetRef(s.db, s.branch); gerr == nil &&
				(cur.Lineage != ref.Lineage || cur.HeadTXID < txid) {
				st.B.Delete(snapKey)
			}
			return 0, fmt.Errorf("session: flush lost a race (retry): %w", err)
		}
		return 0, fmt.Errorf("session: advance ref: %w", err)
	}

	s.mu.Lock()
	s.durable = txid
	s.mu.Unlock()
	return txid, nil
}

// DurableTXID is the txid the store is durable through for this session, or
// 0 before the first flush.
func (s *Session) DurableTXID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durable
}
