package ltxio

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"
)

// Page is one database page destined for a segment.
type Page struct {
	Pgno uint32
	Data []byte // exactly PageSize bytes
}

// EncodeSegment writes an LTX segment carrying only pages, covering
// transactions [minTXID, maxTXID]. commit is the database size in pages after
// maxTXID — the value SQLite's own commit frame records — so a reader can
// truncate correctly when the database shrank. Pages must be sorted by Pgno
// and contain no duplicates; EncodeSegment returns an error otherwise rather
// than writing a segment a reader would misinterpret.
//
// Unlike a snapshot (MinTXID 1), a segment is applied on top of an earlier
// state, so the ltx format requires it to carry the rolling checksum of that
// earlier state (github.com/superfly/ltx Header.Validate rejects a non-
// snapshot header with a zero PreApplyChecksum). preApplyChecksum is that
// value — the checksum of the database as it stood after maxTXID-1 (i.e.
// after the previous member of the chain). postApplyChecksum is the
// resulting checksum after this segment's pages are applied; the ltx
// trailer requires it too (Trailer.Validate). Both use the same rolling
// checksum ChecksumDatabase computes, so a caller building a segment from a
// diff between two on-disk database states typically obtains them via
// ChecksumDatabase(before) and ChecksumDatabase(after).
func EncodeSegment(pageSize, commit uint32, minTXID, maxTXID uint64, preApplyChecksum, postApplyChecksum uint64, pages []Page, w io.Writer) error {
	if minTXID == 0 || maxTXID < minTXID {
		return fmt.Errorf("ltxio: bad segment range [%d,%d]", minTXID, maxTXID)
	}
	if minTXID == 1 {
		return fmt.Errorf("ltxio: a segment cannot start at TXID 1 (that is a snapshot)")
	}
	for i, p := range pages {
		if len(p.Data) != int(pageSize) {
			return fmt.Errorf("ltxio: page %d is %d bytes, want %d", p.Pgno, len(p.Data), pageSize)
		}
		if i > 0 && p.Pgno <= pages[i-1].Pgno {
			return fmt.Errorf("ltxio: pages must be sorted and unique (pgno %d after %d)",
				p.Pgno, pages[i-1].Pgno)
		}
	}
	if preApplyChecksum == 0 {
		return fmt.Errorf("ltxio: preApplyChecksum is required for a segment")
	}
	if postApplyChecksum == 0 {
		return fmt.Errorf("ltxio: postApplyChecksum is required for a segment")
	}

	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return fmt.Errorf("ltxio: new encoder: %w", err)
	}
	hdr := ltx.Header{
		Version:          ltx.Version,
		PageSize:         pageSize,
		Commit:           commit,
		MinTXID:          ltx.TXID(minTXID),
		MaxTXID:          ltx.TXID(maxTXID),
		Timestamp:        time.Now().UnixMilli(),
		PreApplyChecksum: ltx.Checksum(preApplyChecksum),
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return fmt.Errorf("ltxio: encode header: %w", err)
	}

	lockPgno := hdr.LockPgno()
	for _, p := range pages {
		if p.Pgno == lockPgno {
			// The lock page carries no real data and the encoder rejects it.
			continue
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: p.Pgno}, p.Data); err != nil {
			return fmt.Errorf("ltxio: encode page %d: %w", p.Pgno, err)
		}
	}
	enc.SetPostApplyChecksum(ltx.Checksum(postApplyChecksum))
	return enc.Close()
}

// ChecksumDatabase returns the LTX rolling checksum of a quiesced SQLite
// database's current on-disk state — the same value EncodeSnapshot embeds as
// a snapshot's post-apply checksum, and the value EncodeSegment's
// preApplyChecksum/postApplyChecksum parameters expect. dbPath must have no
// pending WAL (checkpoint(TRUNCATE) first).
func ChecksumDatabase(dbPath string) (uint64, error) {
	if fi, err := os.Stat(dbPath + "-wal"); err == nil && fi.Size() > 0 {
		return 0, fmt.Errorf("ltxio: %s has a non-empty WAL; checkpoint(TRUNCATE) first", dbPath)
	}
	f, err := os.Open(dbPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	hdr := make([]byte, dbHeaderSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return 0, fmt.Errorf("ltxio: read db header: %w", err)
	}
	pageSize := uint32(binary.BigEndian.Uint16(hdr[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	nPages := binary.BigEndian.Uint32(hdr[28:32])

	chksum, err := checksumPages(f, pageSize, nPages)
	if err != nil {
		return 0, err
	}
	return uint64(chksum), nil
}

// checksumPages computes the LTX rolling checksum over pages [1, nPages] read
// from src, skipping the lock page — the same convention EncodeSnapshot uses
// when it computes a snapshot's post-apply checksum. src must have at least
// nPages*pageSize bytes available.
func checksumPages(src io.ReaderAt, pageSize, nPages uint32) (ltx.Checksum, error) {
	lockPgno := ltx.LockPgno(pageSize)
	buf := make([]byte, pageSize)
	chksum := ltx.ChecksumFlag
	for pgno := uint32(1); pgno <= nPages; pgno++ {
		if pgno == lockPgno {
			continue
		}
		if _, err := src.ReadAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
			return 0, fmt.Errorf("ltxio: read page %d: %w", pgno, err)
		}
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(pgno, buf))
	}
	return chksum, nil
}

// MaterializeChain writes the database formed by a full snapshot followed by
// zero or more segments applied in order into dbPath. Every member's checksum
// is verified; the destination is written atomically (temp + rename) so a
// failure anywhere leaves no partial file. Segments must be contiguous in
// TXID: each segment's MinTXID must be exactly the previous member's MaxTXID+1,
// and a gap is an error, never a silent skip. Each segment's PreApplyChecksum
// is also verified against the chain's running state — a stronger guarantee
// than TXID contiguity alone, since it is tied to actual page content rather
// than a caller-declared number — and after a segment's pages are applied,
// the resulting database is independently re-checksummed and compared against
// the segment's declared post-apply checksum before it becomes the new
// running state. Returns the resulting MaxTXID.
func MaterializeChain(snapshot io.Reader, segments []io.Reader, dbPath string) (uint64, error) {
	dir := filepath.Dir(dbPath)
	tmp, err := os.CreateTemp(dir, filepath.Base(dbPath)+".tmp-*")
	if err != nil {
		return 0, fmt.Errorf("ltxio: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	hdr, trailer, err := decodeSnapshot(snapshot, tmp)
	if err != nil {
		return 0, err
	}

	pageSize := hdr.PageSize
	prevMaxTXID := uint64(hdr.MaxTXID)
	runningChecksum := trailer.PostApplyChecksum

	for i, segR := range segments {
		dec := ltx.NewDecoder(segR)
		if err := dec.DecodeHeader(); err != nil {
			return 0, fmt.Errorf("ltxio: decode segment %d header: %w", i, err)
		}
		shdr := dec.Header()
		if shdr.IsSnapshot() {
			return 0, fmt.Errorf("ltxio: segment %d is a snapshot (MinTXID=1), expected an incremental segment", i)
		}
		if shdr.PageSize != pageSize {
			return 0, fmt.Errorf("ltxio: segment %d page size %d does not match chain page size %d", i, shdr.PageSize, pageSize)
		}
		if uint64(shdr.MinTXID) != prevMaxTXID+1 {
			return 0, fmt.Errorf("ltxio: segment %d has a TXID gap: chain is at %d, segment starts at %d", i, prevMaxTXID, shdr.MinTXID)
		}
		if shdr.PreApplyChecksum != runningChecksum {
			return 0, fmt.Errorf("ltxio: segment %d pre-apply checksum %s does not match chain state %s", i, shdr.PreApplyChecksum, runningChecksum)
		}

		var pageHeader ltx.PageHeader
		buf := make([]byte, pageSize)
		for {
			if err := dec.DecodePage(&pageHeader, buf); err == io.EOF {
				break
			} else if err != nil {
				return 0, fmt.Errorf("ltxio: decode segment %d page: %w", i, err)
			}
			if _, err := tmp.WriteAt(buf, int64(pageHeader.Pgno-1)*int64(pageSize)); err != nil {
				return 0, fmt.Errorf("ltxio: write page %d: %w", pageHeader.Pgno, err)
			}
		}
		// Close verifies the segment's own whole-file CRC64 checksum
		// (catches a mid-file bit flip) and populates Trailer().
		if err := dec.Close(); err != nil {
			return 0, fmt.Errorf("ltxio: close segment %d: %w", i, err)
		}

		if err := tmp.Truncate(int64(shdr.Commit) * int64(pageSize)); err != nil {
			return 0, fmt.Errorf("ltxio: truncate to commit size: %w", err)
		}

		actual, err := checksumPages(tmp, pageSize, shdr.Commit)
		if err != nil {
			return 0, fmt.Errorf("ltxio: recompute checksum after segment %d: %w", i, err)
		}
		declared := dec.Trailer().PostApplyChecksum
		if actual != declared {
			return 0, fmt.Errorf("ltxio: segment %d post-apply checksum mismatch: computed %s, declared %s", i, actual, declared)
		}

		runningChecksum = declared
		prevMaxTXID = uint64(shdr.MaxTXID)
	}

	if err := finalizeDestination(tmp, tmpPath, dbPath); err != nil {
		return 0, err
	}
	ok = true
	return prevMaxTXID, nil
}
