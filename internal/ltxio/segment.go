package ltxio

import (
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
//
// This is a full O(database size) scan, appropriate for a one-off checksum —
// e.g. bootstrapping a chain from a database that arrived by some other
// means. A caller that needs to keep a checksum current across many small
// changes (one flush at a time, one segment at a time) should not call this
// again after every change: that reduces to the O(N × database size) cost
// this package exists to avoid. Maintain it incrementally instead with
// ChecksumPage and UpdateChecksum.
//
// Like EncodeSnapshot (and the ltx decoder's own snapshot checksum
// verification), this deliberately SKIPS the lock page — unlike the ltx
// library's ltx.ChecksumReader, which includes it. The two conventions
// produce different checksums for the same database; don't mix them.
//
// Caller contract (POSIX lock hazard): this reads dbPath with an ordinary
// os.Open/Close, which is safe ONLY because it is called on files no SQLite
// connection in this process has open — a quiesced checkout (see ops.quiesce)
// or a freshly materialized temp file. POSIX advisory locks are keyed by
// (process, inode), so closing this descriptor would drop every lock this
// process holds on dbPath; against a live capture engine that silently
// unlocks it and loses every subsequent write. See internal/dbfile. Do not
// call this on a database another goroutine may have open; route raw reads
// of live databases through dbfile instead.
func ChecksumDatabase(dbPath string) (uint64, error) {
	if fi, err := os.Stat(dbPath + "-wal"); err == nil && fi.Size() > 0 {
		return 0, fmt.Errorf("ltxio: %s has a non-empty WAL; checkpoint(TRUNCATE) first", dbPath)
	}
	f, err := os.Open(dbPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	pageSize, nPages, err := readDBHeader(f)
	if err != nil {
		return 0, err
	}

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
//
// This is the O(database size) primitive backing ChecksumDatabase. Code that
// updates a small number of pages at a time (MaterializeChain, and any
// caller maintaining a checksum across repeated flushes) should use
// UpdateChecksum instead of calling this on every change.
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

// ChecksumPage returns the LTX per-page checksum that the rolling database
// checksum (ChecksumDatabase, UpdateChecksum) folds together: it combines
// pgno with data's bytes. data must be exactly the database's page size.
// This is a thin wrapper over github.com/superfly/ltx so callers of this
// package never need to import ltx directly.
func ChecksumPage(pgno uint32, data []byte) uint64 {
	return uint64(ltx.ChecksumPage(pgno, data))
}

// LockPgno returns the page number SQLite reserves for its lock byte at the
// given page size — the one page ChecksumDatabase, EncodeSnapshot,
// EncodeSegment, and MaterializeChain all skip when folding page content
// into the rolling checksum (it never holds real page data; see
// UpdateChecksum's doc comment). This is a thin wrapper over
// github.com/superfly/ltx so callers maintaining their own incremental
// checksum outside this package (e.g. session.Session, updating on every
// captured WAL frame) can apply the same skip without importing ltx
// directly.
func LockPgno(pageSize uint32) uint32 {
	return ltx.LockPgno(pageSize)
}

// UpdateChecksum returns the rolling database checksum after page pgno
// changes from oldData to newData, given running — the checksum before the
// change. This is the O(1) counterpart to ChecksumDatabase's O(database
// size) full scan: the rolling checksum is an XOR-fold of independent
// per-page checksums (ChecksumPage), and XOR is self-cancelling, so
// replacing one page's contribution only requires removing the old one and
// adding the new one — the rest of the fold is untouched.
//
// Pass nil (or a zero-length slice) for oldData when pgno did not
// previously exist in the database (a newly created page growing the file):
// there is no old contribution to remove. Pass nil for newData when pgno no
// longer exists (a page dropped by truncating the database smaller): there
// is no new contribution to add. Passing nil for both is a no-op returning
// running unchanged. Never pass the lock page's number here (see
// ltx.LockPgno) — like ChecksumDatabase and EncodeSnapshot, the lock page
// contributes nothing to the checksum, and folding it in produces a
// checksum nothing else will agree with.
func UpdateChecksum(running uint64, pgno uint32, oldData, newData []byte) uint64 {
	c := running
	if len(oldData) > 0 {
		c ^= ChecksumPage(pgno, oldData)
	}
	if len(newData) > 0 {
		c ^= ChecksumPage(pgno, newData)
	}
	return uint64(ltx.ChecksumFlag) | c
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
// the resulting checksum is compared against the segment's declared
// post-apply checksum before it becomes the new running state.
//
// The running checksum is maintained incrementally (UpdateChecksum), not by
// re-scanning the whole database after every segment: for each page a
// segment writes, the page's old contribution is XOR'd out (its prior bytes
// are read before being overwritten) and its new contribution XOR'd in.
// Pages a shrinking commit drops, and pages a growing commit adds beyond
// what the segment explicitly wrote, are folded in the same way. This makes
// replaying a chain of N segments O(total bytes changed) rather than O(N ×
// database size), while verifying exactly the same guarantee as a full
// re-checksum — the incremental and full-scan checksums are provably the
// same rolling XOR-fold, just computed by different paths (see
// TestMaterializeChainIncrementalChecksumMatchesFullRescan). Returns the
// resulting MaxTXID.
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
	prevCommit := hdr.Commit
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

		lockPgno := shdr.LockPgno()
		running := uint64(runningChecksum)
		touched := make(map[uint32]bool)

		var pageHeader ltx.PageHeader
		buf := make([]byte, pageSize)
		oldBuf := make([]byte, pageSize)
		for {
			if err := dec.DecodePage(&pageHeader, buf); err == io.EOF {
				break
			} else if err != nil {
				return 0, fmt.Errorf("ltxio: decode segment %d page: %w", i, err)
			}
			pgno := pageHeader.Pgno
			touched[pgno] = true

			// Read the page's prior contents (if any) before they're
			// overwritten, so its old checksum contribution can be XOR'd
			// out below.
			var oldData []byte
			if pgno <= prevCommit && pgno != lockPgno {
				if _, err := tmp.ReadAt(oldBuf, int64(pgno-1)*int64(pageSize)); err != nil {
					return 0, fmt.Errorf("ltxio: read old page %d: %w", pgno, err)
				}
				oldData = oldBuf
			}

			if _, err := tmp.WriteAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
				return 0, fmt.Errorf("ltxio: write page %d: %w", pgno, err)
			}

			// A page this segment writes may still end up truncated away
			// below (Commit shrinking past it); only fold in its new
			// contribution if it survives into the final database.
			var newData []byte
			if pgno <= shdr.Commit && pgno != lockPgno {
				newData = buf
			}

			running = UpdateChecksum(running, pgno, oldData, newData)
		}
		// Close verifies the segment's own whole-file CRC64 checksum
		// (catches a mid-file bit flip) and populates Trailer().
		if err := dec.Close(); err != nil {
			return 0, fmt.Errorf("ltxio: close segment %d: %w", i, err)
		}

		// A shrinking commit drops trailing pages the segment had no reason
		// to write (there is no new content for a page that is going away).
		// Their old contribution is still baked into `running`, so it must
		// be removed explicitly here, reading their bytes before Truncate
		// destroys them.
		if shdr.Commit < prevCommit {
			for pgno := shdr.Commit + 1; pgno <= prevCommit; pgno++ {
				if pgno == lockPgno || touched[pgno] {
					continue
				}
				if _, err := tmp.ReadAt(oldBuf, int64(pgno-1)*int64(pageSize)); err != nil {
					return 0, fmt.Errorf("ltxio: read dropped page %d: %w", pgno, err)
				}
				running = UpdateChecksum(running, pgno, oldBuf, nil)
			}
		}

		if err := tmp.Truncate(int64(shdr.Commit) * int64(pageSize)); err != nil {
			return 0, fmt.Errorf("ltxio: truncate to commit size: %w", err)
		}

		// A growing commit can, in principle, extend past what the segment
		// explicitly wrote (Truncate zero-extends the file); fold in any
		// such page so the running checksum matches what a full re-scan of
		// the resulting file would compute. Every real segment writes every
		// page it introduces, so in practice this loop never executes.
		if shdr.Commit > prevCommit {
			for pgno := prevCommit + 1; pgno <= shdr.Commit; pgno++ {
				if pgno == lockPgno || touched[pgno] {
					continue
				}
				if _, err := tmp.ReadAt(oldBuf, int64(pgno-1)*int64(pageSize)); err != nil {
					return 0, fmt.Errorf("ltxio: read new page %d: %w", pgno, err)
				}
				running = UpdateChecksum(running, pgno, nil, oldBuf)
			}
		}

		declared := dec.Trailer().PostApplyChecksum
		if actual := ltx.Checksum(running); actual != declared {
			return 0, fmt.Errorf("ltxio: segment %d post-apply checksum mismatch: computed %s, declared %s", i, actual, declared)
		}

		runningChecksum = declared
		prevMaxTXID = uint64(shdr.MaxTXID)
		prevCommit = shdr.Commit
	}

	if err := finalizeDestination(tmp, tmpPath, dbPath); err != nil {
		return 0, err
	}
	ok = true
	return prevMaxTXID, nil
}
