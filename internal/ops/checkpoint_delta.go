package ops

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// largeSegmentFraction and minPagesForFractionCheck are copied from
// internal/session/flush.go (ops cannot import session), where their
// rationale lives: once the changed pages cover at least half the
// database, a segment saves little over a snapshot, which also resets the
// chain; below 64 pages the fraction is skipped and the chain bound alone
// decides. An at-rest checkpoint applies the same rule so both writers
// shape a lineage's chain the same way.
const (
	largeSegmentFraction     = 0.5
	minPagesForFractionCheck = 64
)

// errShadowDiverged reports a shadow whose content is not the state its
// sidecar records (its rolling checksum differs from the sidecar's
// post-apply checksum). A segment diffed against it would declare a
// pre-apply checksum its pages do not produce, so the caller writes a
// snapshot instead.
var errShadowDiverged = errors.New("ops: shadow does not match the recorded checkpoint state")

// errPageSizeChanged reports a checkout whose page size differs from its
// shadow's (a VACUUM after PRAGMA page_size): a segment cannot express that.
var errPageSizeChanged = errors.New("ops: page size changed since the last checkpoint")

// segmentDelta is what an at-rest checkpoint needs to encode a segment.
type segmentDelta struct {
	pages     []ltxio.Page
	commit    uint32
	pageSize  uint32
	pre, post uint64
}

// planSegment decides whether the checkpoint about to be written at
// ref.HeadTXID+1 from the quiesced checkout at path can be a segment, and
// returns its content when it can. Every precondition that fails, and
// every error along the way, answers "no": the caller then writes a
// snapshot exactly as it always has, which depends on nothing here.
//
// The rule, all of which must hold:
//   - the caller did not ask for a snapshot, and txid > 1 (a segment
//     cannot start a chain);
//   - the sidecar records a shadow (sumRecord.Shadow), the shadow file
//     exists, and the sidecar's identity is the ref's head, so the shadow
//     is the head's content and its PostApplyChecksum the head's checksum;
//   - the head's resolved chain is shorter than the snapshot bound
//     (SnapshotEvery, or ForkShareMaxDepth when unset), so the new member
//     keeps it within the bound; the bound-th checkpoint is a snapshot;
//   - diffPages succeeds: page size unchanged and the shadow verified
//     against the recorded checksum;
//   - the changed fraction is below largeSegmentFraction (skipped when the
//     checkout is under minPagesForFractionCheck pages, as session.Flush
//     skips it by the replica's current size).
func (w *Workspace) planSegment(path string, ref store.Ref, opts CheckpointOptions) (segmentDelta, bool) {
	if opts.Snapshot || ref.HeadTXID+1 <= 1 {
		return segmentDelta{}, false
	}
	rec, ok := readSidecar(path)
	if !ok || !rec.Shadow || rec.PostApplyChecksum == 0 ||
		rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID {
		return segmentDelta{}, false
	}
	shadow := shadowPath(path)
	if _, err := os.Stat(shadow); err != nil {
		return segmentDelta{}, false
	}
	bound := w.SnapshotEvery
	if bound <= 0 {
		bound = ForkShareMaxDepth
	}
	members, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil || len(members) >= bound {
		return segmentDelta{}, false
	}
	pages, commit, pageSize, changedFrac, post, err := diffPages(path, shadow, rec.PostApplyChecksum)
	if err != nil {
		return segmentDelta{}, false
	}
	if commit >= minPagesForFractionCheck && changedFrac >= largeSegmentFraction {
		return segmentDelta{}, false
	}
	return segmentDelta{pages: pages, commit: commit, pageSize: pageSize, pre: rec.PostApplyChecksum, post: post}, true
}

// diffPages compares the quiesced database at cur with shadow, the state
// whose LTX rolling checksum is pre, page by page, and returns the pages a
// segment must carry to turn shadow into cur: every page of cur that
// differs from shadow's, plus every page past shadow's end (growth). Pages
// of shadow past cur's end (a shrink) are not carried; commit, cur's size
// in pages, tells the reader to truncate them.
//
// post is cur's rolling checksum, derived from pre with
// ltxio.UpdateChecksum exactly as ltxio's applySegments folds a segment in
// while materializing: a carried page within shadow swaps its old
// contribution for its new one, a carried page past shadow's end adds its
// new one, and a shadow page past commit removes its old one. The lock
// page is skipped throughout, as every checksum in ltxio skips it. The
// shadow's own checksum is computed on the same pass and must equal pre
// (errShadowDiverged otherwise), so post is never derived from a shadow
// that is not the recorded state.
//
// changedFrac is len(pages) / max(pages in cur, pages in shadow). Both
// files are read with plain opens: the caller has quiesced cur, and the
// at-rest checkpoint holds no SQLite connection on it (see Checkpoint's
// doc comment on the lock hazard).
func diffPages(cur, shadow string, pre uint64) (pages []ltxio.Page, commit uint32, pageSize uint32, changedFrac float64, post uint64, err error) {
	cf, err := os.Open(cur)
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	defer cf.Close()
	sf, err := os.Open(shadow)
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	defer sf.Close()
	pageSize, commit, err = ltxio.ReadDBHeader(io.NewSectionReader(cf, 0, 100))
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	shadowSize, shadowCommit, err := ltxio.ReadDBHeader(io.NewSectionReader(sf, 0, 100))
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	if shadowSize != pageSize {
		return nil, 0, 0, 0, 0, errPageSizeChanged
	}

	lock := ltxio.LockPgno(pageSize)
	cr := bufio.NewReaderSize(cf, 1<<20)
	sr := bufio.NewReaderSize(sf, 1<<20)
	curBuf := make([]byte, pageSize)
	oldBuf := make([]byte, pageSize)
	shadowSum := uint64(0)
	post = pre
	for pgno := uint32(1); pgno <= max(commit, shadowCommit); pgno++ {
		var curPage, oldPage []byte
		if pgno <= commit {
			if _, err := io.ReadFull(cr, curBuf); err != nil {
				return nil, 0, 0, 0, 0, fmt.Errorf("ops: read page %d of %s: %w", pgno, cur, err)
			}
			curPage = curBuf
		}
		if pgno <= shadowCommit {
			if _, err := io.ReadFull(sr, oldBuf); err != nil {
				return nil, 0, 0, 0, 0, fmt.Errorf("ops: read page %d of %s: %w", pgno, shadow, err)
			}
			oldPage = oldBuf
		}
		if pgno == lock {
			continue
		}
		if oldPage != nil {
			shadowSum = ltxio.UpdateChecksum(shadowSum, pgno, nil, oldPage)
		}
		switch {
		case curPage == nil: // past cur's end: dropped by the shrink
			post = ltxio.UpdateChecksum(post, pgno, oldPage, nil)
		case oldPage == nil || !bytes.Equal(curPage, oldPage):
			pages = append(pages, ltxio.Page{Pgno: pgno, Data: bytes.Clone(curPage)})
			post = ltxio.UpdateChecksum(post, pgno, oldPage, curPage)
		}
	}
	if shadowSum != pre {
		return nil, 0, 0, 0, 0, errShadowDiverged
	}
	return pages, commit, pageSize, float64(len(pages)) / float64(max(commit, shadowCommit)), post, nil
}
