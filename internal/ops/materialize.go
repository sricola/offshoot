package ops

import (
	"bytes"
	"fmt"
	"io"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
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
	if rg, ok := w.Store.B.(store.ReaderGetter); ok {
		return materializeMembersAtStreaming(rg, lineage, members, dst)
	}
	return materializeMembersAtBuffered(w.Store.B, lineage, members, dst)
}

// materializeMembersAtBuffered is the pre-streaming fallback: every member's
// full bytes are fetched (one Get RPC each) before MaterializeChain runs, so
// peak memory is the snapshot plus every segment resident at once. Used when
// the backend does not implement store.ReaderGetter.
func materializeMembersAtBuffered(b store.Backend, lineage string, members []store.ChainMember, dst string) (uint64, error) {
	snapData, _, err := b.Get(members[0].Key)
	if err != nil {
		return 0, fmt.Errorf("ops: fetching snapshot %s for lineage %s (target %s): %w",
			members[0].Key, lineage, dst, err)
	}
	segs := make([]io.Reader, 0, len(members)-1)
	for _, m := range members[1:] {
		segData, _, err := b.Get(m.Key)
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

// materializeMembersAtStreaming applies the chain with at most ONE member's
// bytes resident at a time: every member (the snapshot and each segment)
// gets a lazyReader that opens its GetReader stream on first Read and closes
// it as soon as it observes EOF. This is safe because MaterializeChain
// consumes its inputs strictly sequentially — the snapshot reader fully,
// then each segment reader fully, in order, never touching an earlier or
// later reader once it has moved on (see MaterializeChain's doc comment) —
// so only the member currently being decoded ever has an open stream.
//
// Every lazyReader is deferred-closed on return, success or error: one that
// reached EOF during MaterializeChain already closed itself (Close is
// idempotent, so the defer is a no-op there); one that was opened but
// MaterializeChain errored out mid-read gets closed here; one that
// MaterializeChain never reached at all (an earlier member failed first)
// was never opened, so closing it is also a no-op. No FD/stream leak on any
// path.
func materializeMembersAtStreaming(rg store.ReaderGetter, lineage string, members []store.ChainMember, dst string) (uint64, error) {
	readers := make([]*lazyReader, len(members))
	for i, m := range members {
		label := "segment"
		if i == 0 {
			label = "snapshot"
		}
		readers[i] = newLazyReader(rg, m.Key, label)
	}
	defer func() {
		for _, r := range readers {
			_ = r.close()
		}
	}()

	segs := make([]io.Reader, len(readers)-1)
	for i, r := range readers[1:] {
		segs[i] = r
	}
	_, checksum, err := ltxio.MaterializeChain(readers[0], segs, dst)
	if err != nil {
		return 0, fmt.Errorf("ops: materializing chain for lineage %s (target %s): %w",
			lineage, dst, err)
	}
	return checksum, nil
}

// lazyReader is an io.Reader over a single store object that defers opening
// the underlying stream (via store.ReaderGetter.GetReader) until its first
// Read call, and closes that stream as soon as it observes EOF. Paired with
// materializeMembersAtStreaming's sequential-consumption guarantee, this
// keeps at most one member's stream open at a time when applying a
// materialization chain. close is idempotent (guarded by closed) so it is
// safe to call again from a deferred cleanup after a reader has already
// self-closed on EOF, and safe to call on a reader that was never opened at
// all (r stays nil; close is then a pure no-op).
type lazyReader struct {
	rg    store.ReaderGetter
	key   string
	label string // "snapshot" or "segment", for error messages

	r      io.ReadCloser
	closed bool
}

func newLazyReader(rg store.ReaderGetter, key, label string) *lazyReader {
	return &lazyReader{rg: rg, key: key, label: label}
}

func (l *lazyReader) Read(p []byte) (int, error) {
	if l.closed {
		return 0, io.EOF
	}
	if l.r == nil {
		r, _, err := l.rg.GetReader(l.key)
		if err != nil {
			return 0, fmt.Errorf("ops: opening %s %s: %w", l.label, l.key, err)
		}
		l.r = r
	}
	n, err := l.r.Read(p)
	if err == io.EOF {
		if cerr := l.close(); cerr != nil {
			// The stream delivered EOF but failed to Close cleanly: surface
			// the close failure instead of masking it as a clean end of
			// input, so a caller (MaterializeChain) doesn't mistake it for
			// a successfully fully-read member.
			return n, fmt.Errorf("ops: closing %s %s: %w", l.label, l.key, cerr)
		}
	}
	return n, err
}

func (l *lazyReader) close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	if l.r == nil {
		return nil
	}
	return l.r.Close()
}
