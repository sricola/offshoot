package session

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

// ErrFenced reports that the session's lease was lost: its epoch is dead, so
// it must not write. Fencing is terminal for a session.
var ErrFenced = errors.New("session: fenced — lease lost")

// Flush uploads the replica's current state as a snapshot under the session's
// lease epoch and advances the branch head. name is optional: when non-empty
// the flushed state is also recorded as a named checkpoint. Returns the txid
// the branch is now durable through.
//
// Flush never touches the agent's checkout: it encodes the replica, which the
// capture engine keeps at a transaction boundary.
func (s *Session) Flush(name string) (uint64, error) {
	if err := s.Err(); err != nil {
		return 0, err
	}
	if name != "" {
		if err := store.ValidateName(name); err != nil {
			return 0, err
		}
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
		if delErr := st.B.Delete(snapKey); delErr != nil {
			return 0, fmt.Errorf("session: ref update failed (%v) and cleanup failed: %w", err, delErr)
		}
		if errors.Is(err, store.ErrCAS) {
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
