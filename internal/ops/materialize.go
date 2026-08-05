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
func (w *Workspace) materializeChainAt(ref store.Ref, cp store.Checkpoint, dst string) error {
	members, err := w.Store.Chain(ref.Lineage, cp.TXID)
	if err != nil {
		return fmt.Errorf("ops: resolving chain for lineage %s to txid %d (target %s): %w",
			ref.Lineage, cp.TXID, dst, err)
	}
	snapData, _, err := w.Store.B.Get(members[0].Key)
	if err != nil {
		return fmt.Errorf("ops: fetching snapshot %s for lineage %s (target %s): %w",
			members[0].Key, ref.Lineage, dst, err)
	}
	segs := make([]io.Reader, 0, len(members)-1)
	for _, m := range members[1:] {
		segData, _, err := w.Store.B.Get(m.Key)
		if err != nil {
			return fmt.Errorf("ops: fetching segment %s for lineage %s (target %s): %w",
				m.Key, ref.Lineage, dst, err)
		}
		segs = append(segs, bytes.NewReader(segData))
	}
	if _, err := ltxio.MaterializeChain(bytes.NewReader(snapData), segs, dst); err != nil {
		return fmt.Errorf("ops: materializing chain for lineage %s (target %s): %w",
			ref.Lineage, dst, err)
	}
	return nil
}
