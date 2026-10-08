package ltxio

import (
	"bytes"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"

	"github.com/sricola/offshoot/internal/reflink"
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

// TrailerPostApplyChecksum decodes an already-fetched snapshot or segment
// LTX object (data, in memory) far enough to validate and read its trailer,
// and returns the PostApplyChecksum it declares — by construction, always
// exactly the checksum of the FULL database content that results from
// applying it as the last member of a chain (see EncodeSegment's doc
// comment on preApplyChecksum/postApplyChecksum, and EncodeSnapshot, which
// is what makes this true for a snapshot too: MinTXID 1 has no prior state,
// so its postApplyChecksum already covers the whole database on its own).
// That invariant holds regardless of chain length or member type, so this
// needs to decode ONLY the single object handed to it, never the chain
// behind it.
//
// Page content is decoded and immediately discarded — never written
// anywhere, not even to a temp file — because only the trailer's declared
// value is needed here, not the materialized bytes. Close() still runs and
// still verifies the object's own whole-file CRC64 integrity (catching a
// truncated or bit-flipped download) before the trailer is trusted; this
// does NOT re-derive the checksum from the page content the way
// MaterializeChain does when it needs to catch a WRITER's bug (a declared
// value that doesn't match what the pages actually produce) — a caller
// needing "what does this object's trailer say the checksum is" doesn't
// need that stronger, more expensive verification.
//
// No production caller remains: an earlier version of the settling-flush
// suppression (internal/session) used this indirectly, via a now-deleted
// ops.Workspace.HeadPostApplyChecksum, to fetch and decode the branch
// head's chain member on every session Open — which meant downloading the
// FULL head object whenever it happened to be a snapshot, defeating the
// suppression's own purpose for exactly the read-only-reopen case it
// targeted. That was replaced with reading the checksum out of the
// checkout's local .sum sidecar instead (see ops.CheckoutResult.
// PostApplyChecksum), which needs no store read and therefore no decoding
// at all. Kept as a tested primitive (see
// TestTrailerPostApplyChecksumMatchesEncodedValue) for decoding a trailer
// without materializing content, in case a future caller needs exactly
// that.
//
// Works uniformly for both object shapes: a segment's pages are sparse and
// order-independent from a decode-and-discard perspective (unlike
// MaterializeChain's segment path, which must apply them to specific
// absolute offsets to reconstruct real content — see its own doc comment),
// so a plain header-then-drain-pages-then-close loop, with no destination
// writer at all, is sufficient and correct for either a snapshot's
// contiguous full page run or a segment's sparse changed-page set.
func TrailerPostApplyChecksum(data []byte) (uint64, error) {
	dec := ltx.NewDecoder(newFrameGuard(bytes.NewReader(data)))
	if err := dec.DecodeHeader(); err != nil {
		return 0, fmt.Errorf("ltxio: decode header: %w", err)
	}
	pageSize := dec.Header().PageSize
	var pageHeader ltx.PageHeader
	buf := make([]byte, pageSize)
	for {
		if err := dec.DecodePage(&pageHeader, buf); err == io.EOF {
			break
		} else if err != nil {
			return 0, fmt.Errorf("ltxio: decode page: %w", err)
		}
	}
	if err := dec.Close(); err != nil {
		return 0, fmt.Errorf("ltxio: close: %w", err)
	}
	return uint64(dec.Trailer().PostApplyChecksum), nil
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

// streamBlockSize is the number of bytes StreamChecksum reads at a time. It
// must stay a positive multiple of 65536 (and so, trivially, at least 100
// bytes): every valid SQLite page size, a power of two from 512 to 65536 as
// checkDBHeader enforces, then divides it evenly, so a page never straddles
// two blocks, and the first block always holds the whole 100-byte file
// header. Tests lower it, while preserving that property, to cross block
// boundaries cheaply.
var streamBlockSize = 1 << 20

// ErrNotWholeDatabase is wrapped by every StreamChecksum error that is a
// verdict on the bytes themselves, rather than a failure to read or tee
// them: a header that is too short for readDBHeader's 100-byte minimum, a
// header whose magic or page size fails readDBHeader's check, or a file that
// ends before its header's nPages*pageSize bytes. By the time it is
// returned, the tee has already received the whole file.
var ErrNotWholeDatabase = errors.New("ltxio: not a whole database")

// dbHeaderMagic is the fixed byte string every valid SQLite database file
// begins with, including the trailing NUL. Checking it is the cheapest real
// test for "this is not a database at all"; readDBHeader applies it
// (checkDBHeader) for every reader of a database header.
var dbHeaderMagic = []byte("SQLite format 3\x00")

// StreamChecksum reads a SQLite database from r to EOF, in blocks of
// streamBlockSize bytes, and returns its LTX rolling checksum, the same
// value ChecksumDatabase computes for the file (pages 1..nPages, the lock
// page skipped, ltx.ChecksumFlag set). Every byte it reads is also written
// to tee, in order, so one read of the file serves both this checksum and
// whatever digest tee computes; the two run concurrently, each block's tee
// write on its own goroutine while its pages are folded in two halves on
// two more (foldPages; the rolling checksum is an XOR fold, so the halves
// recombine in any order), which brings the pass down to the slower of the
// tee's digest and half the page checksum. A panic in tee.Write or in a
// fold therefore ends the process rather than unwinding to the caller; the
// hash writers this package is used with do not panic.
//
// The header check a block must pass before any page of it is folded is:
// at least 100 bytes (readDBHeader's own minimum), the 16-byte
// "SQLite format 3\x00" magic, and a page size that is a power of two in
// [512, 65536] (checkDBHeader). Failing any part of that, or a file
// that ends before the header's nPages*pageSize bytes, is a verdict on the
// bytes: StreamChecksum keeps draining r into tee regardless, and returns
// the verdict wrapped in ErrNotWholeDatabase only once r is exhausted, so a
// caller hashing the tee always ends up holding the hash of the whole file.
// A read error from r or a write error from tee, by contrast, returns at
// once, unwrapped, since there is no point draining towards a tee that is
// itself broken. Bytes past nPages*pageSize are written to tee and not
// folded, as ChecksumDatabase ignores them too.
func StreamChecksum(r io.Reader, tee io.Writer) (uint64, error) {
	buf := make([]byte, streamBlockSize)
	h, h2 := ltx.NewHasher(), ltx.NewHasher()
	chksum := ltx.ChecksumFlag
	var (
		pageSize, nPages uint32
		lockPgno         uint32
		headerErr        error
		headerRead       bool
		folded           int64 // bytes folded into chksum so far
		seen             int64 // bytes read so far
	)
	for {
		n, rerr := io.ReadFull(r, buf)
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			return 0, rerr
		}
		block := buf[:n]
		// The header is parsed and validated from the first block read,
		// even an empty one (an empty file never satisfies readDBHeader's
		// 100-byte minimum, so this is where an empty file's error comes
		// from; without checking here regardless of n, a reader that
		// returns n=0 on its very first call would never have its header
		// checked at all).
		if !headerRead {
			headerRead = true
			pageSize, nPages, headerErr = readDBHeader(bytes.NewReader(block))
			if headerErr != nil {
				// readDBHeader fails on an in-memory block for a short
				// block, or for a header checkDBHeader refuses. Its message
				// carries its own "ltxio: " prefix, which would repeat the
				// one ErrNotWholeDatabase gives the wrapped verdict, so the
				// short case is restated and the check's own un-prefixed
				// error (the one readDBHeader wrapped) is used for the rest.
				if inner := errors.Unwrap(headerErr); inner != nil && !errors.Is(headerErr, io.ErrUnexpectedEOF) && !errors.Is(headerErr, io.EOF) {
					headerErr = inner
				} else {
					headerErr = fmt.Errorf("header: %d bytes, shorter than the %d-byte header", n, dbHeaderSize)
				}
			}
			if headerErr == nil {
				lockPgno = ltx.LockPgno(pageSize)
			}
		}
		if n > 0 {
			done := make(chan error, 1)
			go func() {
				wn, err := tee.Write(block)
				if err == nil && wn != len(block) {
					err = io.ErrShortWrite
				}
				done <- err
			}()
			if headerErr == nil {
				// The pages of this block that the header accounts for:
				// whole pages within the block, up to nPages overall.
				limit := int64(nPages) * int64(pageSize)
				k := int64(n) / int64(pageSize)
				if remaining := (limit - seen) / int64(pageSize); remaining < k {
					k = remaining
				}
				if k > 0 {
					firstPgno := uint32(seen/int64(pageSize)) + 1
					// The rolling checksum is an XOR fold of per-page
					// checksums (see UpdateChecksum), so the block's pages
					// fold in two halves on two hashers, concurrently with
					// each other and with the tee write, and the halves XOR
					// together; the order never matters. CRC-64 is the
					// slower digest on this path, so splitting it brings the
					// pass to within a few milliseconds per 64 MiB of the
					// tee's own speed (docs/benchmarks.md has the numbers).
					mid := k / 2
					var upper uint64
					upperDone := make(chan struct{})
					go func() {
						upper = foldPages(h2, block, pageSize, firstPgno+uint32(mid), lockPgno, mid, k)
						close(upperDone)
					}()
					lower := foldPages(h, block, pageSize, firstPgno, lockPgno, 0, mid)
					<-upperDone
					chksum = ltx.ChecksumFlag | (chksum ^ ltx.Checksum(lower) ^ ltx.Checksum(upper))
					folded += k * int64(pageSize)
				}
			}
			seen += int64(n)
			if err := <-done; err != nil {
				return 0, err
			}
		}
		if rerr != nil { // io.EOF or io.ErrUnexpectedEOF: r is exhausted
			break
		}
	}
	if headerErr != nil {
		return 0, fmt.Errorf("%w: %w", ErrNotWholeDatabase, headerErr)
	}
	if want := int64(nPages) * int64(pageSize); folded < want {
		return 0, fmt.Errorf("%w: truncated: %d bytes, header says %d pages of %d", ErrNotWholeDatabase, seen, nPages, pageSize)
	}
	return uint64(chksum), nil
}

// foldPages XORs together the per-page checksums (ltx.ChecksumPageWithHasher
// on h) of block's pages with indexes [from, to), where index i holds page
// number firstPgno+i-from at byte offset i*pageSize, skipping lockPgno. Each
// page checksum carries ltx.ChecksumFlag, so the result's top bit is set
// after an odd number of pages and clear after an even number; the caller
// re-ORs the flag after combining, so that bit does not matter.
// StreamChecksum calls it for the two halves of a block on two hashers at
// once.
func foldPages(h hash.Hash64, block []byte, pageSize uint32, firstPgno, lockPgno uint32, from, to int64) uint64 {
	var acc uint64
	for i := from; i < to; i++ {
		pgno := firstPgno + uint32(i-from)
		if pgno == lockPgno {
			continue
		}
		off := i * int64(pageSize)
		acc ^= uint64(ltx.ChecksumPageWithHasher(h, pgno, block[off:off+int64(pageSize)]))
	}
	return acc
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
// resulting MaxTXID and the resulting content's checksum (the same value
// ltxio.ChecksumDatabase would compute over dbPath afterward) — the second
// is a byproduct of verification this function already does at every step
// (applySegments's running checksum), so a caller that also needs to fingerprint the
// materialized content (e.g. ops' checkout-sidecar stamping) doesn't have
// to pay a second full-file pass to get it.
func MaterializeChain(snapshot io.Reader, segments []io.Reader, dbPath string) (txid uint64, checksum uint64, err error) {
	dir := filepath.Dir(dbPath)
	tmp, err := os.CreateTemp(dir, filepath.Base(dbPath)+".tmp-*")
	if err != nil {
		return 0, 0, fmt.Errorf("ltxio: create temp file: %w", err)
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
		return 0, 0, err
	}

	txid, checksum, err = applySegments(tmp, hdr.PageSize, uint64(hdr.MaxTXID), hdr.Commit, trailer.PostApplyChecksum, segments)
	if err != nil {
		return 0, 0, err
	}

	if err := finalizeDestination(tmp, tmpPath, dbPath); err != nil {
		return 0, 0, err
	}
	ok = true
	return txid, checksum, nil
}

// applySegments is the segment-apply loop MaterializeChain and ApplySegments
// share: it applies segments in order onto f, whose current content is the
// database state (pageSize, prevCommit pages, rolling checksum
// runningChecksum) as of txid prevMaxTXID, verifying TXID contiguity, each
// segment's pre-apply checksum against the running state, and each
// segment's post-apply checksum after its pages land. prevMaxTXID == 0
// means "unknown" (ApplySegments starts from a plain file, which does not
// record its txid): the first segment's MinTXID is then taken as given, and
// the pre-apply checksum check — tied to actual page content, not a
// declared number — is what anchors it. Returns the final MaxTXID and
// checksum; f is left open and unsynced for the caller to finalize.
func applySegments(f *os.File, pageSize uint32, prevMaxTXID uint64, prevCommit uint32, runningChecksum ltx.Checksum, segments []io.Reader) (txid uint64, checksum uint64, err error) {
	for i, segR := range segments {
		dec := ltx.NewDecoder(newFrameGuard(segR))
		if err := dec.DecodeHeader(); err != nil {
			return 0, 0, fmt.Errorf("ltxio: decode segment %d header: %w", i, err)
		}
		shdr := dec.Header()
		if shdr.IsSnapshot() {
			return 0, 0, fmt.Errorf("ltxio: segment %d is a snapshot (MinTXID=1), expected an incremental segment", i)
		}
		if shdr.PageSize != pageSize {
			return 0, 0, fmt.Errorf("ltxio: segment %d page size %d does not match chain page size %d", i, shdr.PageSize, pageSize)
		}
		if i == 0 && prevMaxTXID == 0 {
			prevMaxTXID = uint64(shdr.MinTXID) - 1
		}
		if uint64(shdr.MinTXID) != prevMaxTXID+1 {
			return 0, 0, fmt.Errorf("ltxio: segment %d has a TXID gap: chain is at %d, segment starts at %d", i, prevMaxTXID, shdr.MinTXID)
		}
		if shdr.PreApplyChecksum != runningChecksum {
			return 0, 0, fmt.Errorf("ltxio: segment %d pre-apply checksum %s does not match chain state %s", i, shdr.PreApplyChecksum, runningChecksum)
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
				return 0, 0, fmt.Errorf("ltxio: decode segment %d page: %w", i, err)
			}
			pgno := pageHeader.Pgno
			touched[pgno] = true

			// Read the page's prior contents (if any) before they're
			// overwritten, so its old checksum contribution can be XOR'd
			// out below.
			var oldData []byte
			if pgno <= prevCommit && pgno != lockPgno {
				if _, err := f.ReadAt(oldBuf, int64(pgno-1)*int64(pageSize)); err != nil {
					return 0, 0, fmt.Errorf("ltxio: read old page %d: %w", pgno, err)
				}
				oldData = oldBuf
			}

			if _, err := f.WriteAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
				return 0, 0, fmt.Errorf("ltxio: write page %d: %w", pgno, err)
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
			return 0, 0, fmt.Errorf("ltxio: close segment %d: %w", i, err)
		}

		// A segment can grow the database only by pages it carries: SQLite
		// writes every page it adds to the database (pagerWalFrames logs
		// every dirty page up to the commit size), except the lock page,
		// which never holds data and is never carried. A segment that
		// declares a larger commit than its pages cover is malformed, and
		// accepting it used to mean folding every missing, zero-extended
		// page into the checksum one at a time — a CRC-valid 120-byte
		// segment declaring 2^30 pages ran for hours (found by
		// FuzzApplySegments). session.recordApply enforces the same rule on
		// the write side, so no segment this repo writes can trip it.
		if err := checkGrowth(shdr.Commit, prevCommit, lockPgno, touched); err != nil {
			return 0, 0, fmt.Errorf("ltxio: segment %d: %w", i, err)
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
				if _, err := f.ReadAt(oldBuf, int64(pgno-1)*int64(pageSize)); err != nil {
					return 0, 0, fmt.Errorf("ltxio: read dropped page %d: %w", pgno, err)
				}
				running = UpdateChecksum(running, pgno, oldBuf, nil)
			}
		}

		if err := f.Truncate(int64(shdr.Commit) * int64(pageSize)); err != nil {
			return 0, 0, fmt.Errorf("ltxio: truncate to commit size: %w", err)
		}

		// No growth fold is needed here: checkGrowth above guarantees every
		// page a growing commit adds (other than the lock page, which the
		// checksum skips) was carried and already folded in.

		declared := dec.Trailer().PostApplyChecksum
		if actual := ltx.Checksum(running); actual != declared {
			return 0, 0, fmt.Errorf("ltxio: segment %d post-apply checksum mismatch: computed %s, declared %s", i, actual, declared)
		}

		runningChecksum = declared
		prevMaxTXID = uint64(shdr.MaxTXID)
		prevCommit = shdr.Commit
	}

	return prevMaxTXID, uint64(runningChecksum), nil
}

// ErrUncarriedGrowth reports a commit that grows the database past pages
// the segment (or, in session, the transaction) does not carry.
var ErrUncarriedGrowth = errors.New("commit grows the database beyond the pages it carries")

// checkGrowth enforces "a commit grows the database only by pages it
// carries": when commit > prevCommit, every page number in
// (prevCommit, commit] except lockPgno must be in carried. Exported to
// session's write path through CheckGrowth so the two sides agree.
func checkGrowth(commit, prevCommit, lockPgno uint32, carried map[uint32]bool) error {
	if commit <= prevCommit {
		return nil
	}
	need := commit - prevCommit
	if lockPgno > prevCommit && lockPgno <= commit {
		need--
	}
	var have uint32
	for pgno := range carried {
		if pgno > prevCommit && pgno <= commit && pgno != lockPgno {
			have++
		}
	}
	if have != need {
		return fmt.Errorf("%w: commit %d after %d pages needs %d new pages, has %d",
			ErrUncarriedGrowth, commit, prevCommit, need, have)
	}
	return nil
}

// CheckGrowth is checkGrowth for callers outside this package that build
// segments (session.recordApply): it must refuse a transaction exactly when
// applySegments would refuse the segment carrying it.
func CheckGrowth(commit, prevCommit uint32, pageSize uint32, carried map[uint32]bool) error {
	return checkGrowth(commit, prevCommit, ltx.LockPgno(pageSize), carried)
}

// ApplySegments writes into dstPath the database formed by applying
// segments, in order, on top of the database at startPath, whose LTX
// rolling checksum is startChecksum (as recorded when it was
// materialized). It is MaterializeChain's segment half, starting from an
// existing file instead of a snapshot: the first segment's
// PreApplyChecksum must equal startChecksum, and every check
// MaterializeChain makes per segment is made here too.
//
// startPath is never modified. Its content is copied into a temp file in
// dstPath's directory — a copy-on-write clone where the filesystem supports
// one (reflink.CopyFile), so only the pages the segments touch are ever
// written — and the result is renamed over dstPath only after every segment
// verified, the same finalizeDestination discipline MaterializeChain uses:
// a failure anywhere leaves dstPath untouched and no temp file behind.
// Returns the last segment's MaxTXID and the resulting content's checksum
// (what ChecksumDatabase would compute over dstPath).
func ApplySegments(startPath string, startChecksum uint64, segments []io.Reader, dstPath string) (txid, checksum uint64, err error) {
	if len(segments) == 0 {
		return 0, 0, fmt.Errorf("ltxio: ApplySegments needs at least one segment")
	}
	src, err := os.Open(startPath)
	if err != nil {
		return 0, 0, err
	}
	pageSize, nPages, err := readDBHeader(src)
	src.Close()
	if err != nil {
		return 0, 0, err
	}

	tmp, err := os.CreateTemp(filepath.Dir(dstPath), filepath.Base(dstPath)+".tmp-*")
	if err != nil {
		return 0, 0, fmt.Errorf("ltxio: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	if err := os.Remove(tmpPath); err != nil {
		return 0, 0, err
	}
	var f *os.File
	defer func() {
		if err != nil {
			if f != nil {
				f.Close()
			}
			os.Remove(tmpPath)
		}
	}()
	if _, err = reflink.CopyFile(tmpPath, startPath); err != nil {
		return 0, 0, fmt.Errorf("ltxio: copy start file: %w", err)
	}
	// A clone carries src's mode (a read-only cache entry is 0444); the
	// working copy must be writable, and match MaterializeChain's temp mode.
	if err = os.Chmod(tmpPath, 0o600); err != nil {
		return 0, 0, err
	}
	if f, err = os.OpenFile(tmpPath, os.O_RDWR, 0); err != nil {
		return 0, 0, err
	}
	txid, checksum, err = applySegments(f, pageSize, 0, nPages, ltx.Checksum(startChecksum), segments)
	if err != nil {
		return 0, 0, err
	}
	if err = finalizeDestination(f, tmpPath, dstPath); err != nil {
		return 0, 0, err
	}
	return txid, checksum, nil
}
