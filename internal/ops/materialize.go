package ops

import (
	"bytes"
	"fmt"
	"io"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

// materializeChainAt writes the state of ref's lineage at cp into dst by
// resolving the object chain — the newest snapshot with MaxTXID <= cp.TXID,
// followed by every contiguous segment after it up to cp.TXID — and applying
// it in order. It replaces the single-snapshot read path: a lineage that has
// only ever been snapshotted resolves to a one-member chain (just the
// snapshot), so behavior for existing stores (Plan 2..6) is unchanged.
//
// cp.Epoch is not used to select objects: Store.Chain lists a lineage's
// entire object prefix regardless of which epoch wrote each member (an epoch
// bump never moves objects), so cp.TXID alone determines the chain.
//
// Returns the materialized content's checksum (see ltxio.MaterializeChain)
// as a byproduct of the verification MaterializeChain already does — a
// caller that also needs to fingerprint what it just wrote (e.g. a checkout
// .sum sidecar stamp) gets it for free, no second full-file scan.
func (w *Workspace) materializeChainAt(ref store.Ref, cp store.Checkpoint, dst string) (uint64, error) {
	members, err := w.Store.Chain(ref.Lineage, cp.TXID)
	if err != nil {
		return 0, fmt.Errorf("ops: resolving chain for lineage %s to txid %d (target %s): %w",
			ref.Lineage, cp.TXID, dst, err)
	}
	return w.materializeMembersAt(ref.Lineage, members, dst)
}

// materializeMembersAt applies an already-resolved chain (see store.Chain)
// into dst: fetches the snapshot and every segment object in order, then
// hands them to ltxio.MaterializeChain. lineage is used only for error
// messages (the members themselves already carry everything needed to fetch
// and apply them). See materializeChainAt's doc comment for the returned
// checksum.
//
// Split out from materializeChainAt so a caller that has already resolved
// the chain for another reason can reuse that resolution instead of issuing
// a second store.Chain call. Task 6's fast-path fork attempt
// (ops.Workspace.tryFastForkCopy) is exactly that case: it must resolve the
// source chain to even decide whether the fast-path precondition (a single
// snapshot member) holds, and when it doesn't, or the backend doesn't
// support CopyObject, the slow path takes over immediately after — re-
// resolving from scratch would mean a second store.Chain call (an extra
// List RPC on a remote backend like S3) for the exact same lineage and
// txid. See copySnapshotToNewLineage/copySnapshotIntoLineageFromChain —
// neither of those two callers needs the checksum (they copy chain content
// into a brand-new lineage's own snapshot object, never touching a local
// checkout's .sum sidecar), so both simply discard it.
func (w *Workspace) materializeMembersAt(lineage string, members []store.ChainMember, dst string) (uint64, error) {
	snapData, _, err := w.Store.B.Get(members[0].Key)
	if err != nil {
		return 0, fmt.Errorf("ops: fetching snapshot %s for lineage %s (target %s): %w",
			members[0].Key, lineage, dst, err)
	}
	segs := make([]io.Reader, 0, len(members)-1)
	for _, m := range members[1:] {
		segData, _, err := w.Store.B.Get(m.Key)
		if err != nil {
			return 0, fmt.Errorf("ops: fetching segment %s for lineage %s (target %s): %w",
				m.Key, lineage, dst, err)
		}
		segs = append(segs, bytes.NewReader(segData))
	}
	_, checksum, err := ltxio.MaterializeChain(bytes.NewReader(snapData), segs, dst)
	if err != nil {
		return 0, fmt.Errorf("ops: materializing chain for lineage %s (target %s): %w",
			lineage, dst, err)
	}
	return checksum, nil
}
