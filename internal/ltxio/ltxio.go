// Package ltxio encodes SQLite databases as full-snapshot LTX files and
// materializes them back, wrapping github.com/superfly/ltx behind a stable
// interface (the ltx Go API carries no stability promise; the format spec is
// the contract).
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

const dbHeaderSize = 100

// readDBHeader reads a quiesced SQLite database's page size and page count
// (database size in pages) from its 100-byte header. r must be positioned at
// the start of the file. Shared by EncodeSnapshot and ChecksumDatabase, which
// both need exactly this parse.
func readDBHeader(r io.Reader) (pageSize, nPages uint32, err error) {
	hdr := make([]byte, dbHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, 0, fmt.Errorf("ltxio: read db header: %w", err)
	}
	pageSize = uint32(binary.BigEndian.Uint16(hdr[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	nPages = binary.BigEndian.Uint32(hdr[28:32])
	return pageSize, nPages, nil
}

// EncodeSnapshot writes a full-snapshot LTX of the SQLite main database at
// dbPath, covering TXIDs [1, txid]. Caller must have fully checkpointed the
// WAL (TRUNCATE) first; EncodeSnapshot returns an error if a non-empty -wal
// file exists next to dbPath.
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
func EncodeSnapshot(dbPath string, txid uint64, w io.Writer) error {
	if fi, err := os.Stat(dbPath + "-wal"); err == nil && fi.Size() > 0 {
		return fmt.Errorf("ltxio: %s has a non-empty WAL; checkpoint(TRUNCATE) first", dbPath)
	}
	f, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer f.Close()

	pageSize, nPages, err := readDBHeader(f)
	if err != nil {
		return err
	}

	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return fmt.Errorf("ltxio: new encoder: %w", err)
	}
	lhdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  pageSize,
		Commit:    nPages,
		MinTXID:   1,
		MaxTXID:   ltx.TXID(txid),
		Timestamp: time.Now().UnixMilli(),
	}
	if err := enc.EncodeHeader(lhdr); err != nil {
		return fmt.Errorf("ltxio: encode header: %w", err)
	}

	lockPgno := lhdr.LockPgno()
	buf := make([]byte, pageSize)
	chksum := ltx.ChecksumFlag
	for pgno := uint32(1); pgno <= nPages; pgno++ {
		if pgno == lockPgno {
			// The lock page carries no real data and the encoder rejects it.
			continue
		}
		if _, err := f.ReadAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
			return fmt.Errorf("ltxio: read page %d: %w", pgno, err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return fmt.Errorf("ltxio: encode page %d: %w", pgno, err)
		}
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(pgno, buf))
	}
	enc.SetPostApplyChecksum(chksum)
	return enc.Close()
}

// Materialize decodes a full-snapshot LTX from r into dbPath, verifying the
// trailer checksum. On any error the destination is left untouched (write to
// temp file + rename). Returns the snapshot's MaxTXID.
func Materialize(r io.Reader, dbPath string) (txid uint64, err error) {
	dir := filepath.Dir(dbPath)
	tmp, err := os.CreateTemp(dir, filepath.Base(dbPath)+".tmp-*")
	if err != nil {
		return 0, fmt.Errorf("ltxio: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	hdr, _, err := decodeSnapshot(r, tmp)
	if err != nil {
		return 0, err
	}

	if err = finalizeDestination(tmp, tmpPath, dbPath); err != nil {
		return 0, err
	}
	return uint64(hdr.MaxTXID), nil
}

// decodeSnapshot decodes a full-snapshot LTX from r into w (typically a temp
// file destined to become the materialized database). DecodeDatabaseTo
// streams every page (verifying the per-page checksum as it goes for
// snapshot files) and calls Close(), which verifies both the whole-file
// CRC64 checksum and the trailer's post-apply checksum. Any mismatch —
// including a mid-file bit flip — surfaces here before the caller ever
// touches its real destination. Returns the decoded header and trailer so
// callers (e.g. MaterializeChain) can continue a chain from this state.
func decodeSnapshot(r io.Reader, w io.Writer) (ltx.Header, ltx.Trailer, error) {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeDatabaseTo(w); err != nil {
		return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("ltxio: decode snapshot: %w", err)
	}
	return dec.Header(), dec.Trailer(), nil
}

// finalizeDestination syncs tmp, closes it, and renames it into place at
// dbPath, then removes any stale -wal/-shm siblings. Called only once every
// prior decode/verify step has succeeded, so a failure before this point
// leaves dbPath untouched.
func finalizeDestination(tmp *os.File, tmpPath, dbPath string) error {
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dbPath); err != nil {
		return err
	}
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")
	return nil
}
