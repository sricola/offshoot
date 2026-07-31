package wal

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openWALDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, p := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	return db, path
}

// readWALHeader reads and parses the WAL header directly from disk, for
// tests that need the real salts/checksum without going through a Reader.
func readWALHeader(t *testing.T, walPath string) Header {
	t.Helper()
	hb, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	hdr, err := ParseHeader(hb[:HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	return hdr
}

// appendFrame writes one raw WAL frame (header + page) to the end of the
// WAL file, using wal.go's big-endian field layout, so tests can fabricate
// torn or corrupt tail frames.
func appendFrame(t *testing.T, walPath string, pgno, commitSize, salt1, salt2, cksum1, cksum2 uint32, page []byte) {
	t.Helper()
	buf := make([]byte, FrameHeaderSize+len(page))
	binary.BigEndian.PutUint32(buf[0:4], pgno)
	binary.BigEndian.PutUint32(buf[4:8], commitSize)
	binary.BigEndian.PutUint32(buf[8:12], salt1)
	binary.BigEndian.PutUint32(buf[12:16], salt2)
	binary.BigEndian.PutUint32(buf[16:20], cksum1)
	binary.BigEndian.PutUint32(buf[20:24], cksum2)
	copy(buf[24:], page)

	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
}

func TestReaderSeesEachCommit(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")

	drain := func() int {
		n := 0
		for {
			tx, err := r.Next()
			if err != nil {
				t.Fatal(err)
			}
			if tx == nil {
				return n
			}
			if tx[len(tx)-1].Header.CommitSize == 0 {
				t.Fatal("returned txn does not end in commit frame")
			}
			n++
		}
	}
	drain() // consume CREATE TABLE

	for i := 0; i < 5; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
			t.Fatal(err)
		}
	}
	if got := drain(); got != 5 {
		t.Errorf("committed txns = %d, want 5", got)
	}
}

func TestReaderIgnoresTornTail(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}
	// Simulate a torn write: append half a frame of garbage to the WAL.
	f, err := os.OpenFile(path+"-wal", os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	tx, err := r.Next()
	if err != nil {
		t.Fatalf("torn tail must not error, got %v", err)
	}
	if tx != nil {
		t.Fatal("torn tail must not produce a transaction")
	}
	_ = db
}

func TestReaderDetectsRestart(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}
	// RESTART the WAL (new salts), then write again.
	if _, err := db.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != ErrWALRestarted {
		t.Fatalf("want ErrWALRestarted, got %v", err)
	}
}

// TestReaderBindAtHeaderSizeSeedsChecksum covers the only real contract for
// Bind: resuming a fresh Reader at HeaderSize with the WAL header's own
// salts. The running checksum seed for a frame at HeaderSize must come from
// the header's Cksum1/Cksum2 (that's how SQLite chains frame checksums from
// the header); if Bind doesn't establish that seed, every frame checksum
// will mismatch and committed transactions will be silently dropped instead
// of returned.
func TestReaderBindAtHeaderSizeSeedsChecksum(t *testing.T) {
	db, path := openWALDB(t)
	walPath := path + "-wal"
	hdr := readWALHeader(t, walPath)

	r := NewReader(walPath)
	r.Bind(HeaderSize, hdr.Salt1, hdr.Salt2)

	n := 0
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
		if tx[len(tx)-1].Header.CommitSize == 0 {
			t.Fatal("returned txn does not end in commit frame")
		}
		n++
	}
	if n == 0 {
		t.Fatal("reader bound at HeaderSize with header salts dropped all committed transactions (checksum seed not derived from header)")
	}
	_ = db
}

// TestReaderIgnoresTornTailBadFrameSalt covers the frame-salt-mismatch
// branch of the torn-tail handling: a full-sized frame whose header carries
// salts that don't match the WAL header (e.g. a stale/unwritten region past
// the valid frames) must be treated as "nothing more to read yet", not an
// error.
func TestReaderIgnoresTornTailBadFrameSalt(t *testing.T) {
	db, path := openWALDB(t)
	walPath := path + "-wal"
	r := NewReader(walPath)
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}

	hdr := readWALHeader(t, walPath)
	page := make([]byte, hdr.PageSize)
	appendFrame(t, walPath, 1, uint32(hdr.PageSize), hdr.Salt1+1, hdr.Salt2+1, 0, 0, page)

	tx, err := r.Next()
	if err != nil {
		t.Fatalf("torn tail (bad frame salt) must not error, got %v", err)
	}
	if tx != nil {
		t.Fatal("torn tail (bad frame salt) must not produce a transaction")
	}
	_ = db
}

// TestReaderIgnoresTornTailBadChecksum covers the checksum-mismatch branch
// of the torn-tail handling: a full-sized frame with the CORRECT salts but
// a corrupt/garbage checksum and page (as a partially-flushed OS write could
// leave behind) must be treated as "nothing more to read yet", not an
// error.
func TestReaderIgnoresTornTailBadChecksum(t *testing.T) {
	db, path := openWALDB(t)
	walPath := path + "-wal"
	r := NewReader(walPath)
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}

	hdr := readWALHeader(t, walPath)
	page := make([]byte, hdr.PageSize)
	for i := range page {
		page[i] = 0xAB
	}
	appendFrame(t, walPath, 1, uint32(hdr.PageSize), hdr.Salt1, hdr.Salt2, 0xDEADBEEF, 0xC0FFEE00, page)

	tx, err := r.Next()
	if err != nil {
		t.Fatalf("torn tail (bad checksum) must not error, got %v", err)
	}
	if tx != nil {
		t.Fatal("torn tail (bad checksum) must not produce a transaction")
	}
	_ = db
}
