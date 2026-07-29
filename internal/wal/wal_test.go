package wal

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// makeWAL creates a DB in WAL mode, writes n transactions, and returns the
// raw -wal bytes while the connection is still open (close would checkpoint).
func makeWAL(t *testing.T, n int) []byte {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, p := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0", "PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseHeader(t *testing.T) {
	b := makeWAL(t, 3)
	h, err := ParseHeader(b[:HeaderSize])
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.Version != 3007000 {
		t.Errorf("version = %d", h.Version)
	}
	if h.PageSize != 4096 {
		t.Errorf("page size = %d", h.PageSize)
	}
}

func TestFrameChainValid(t *testing.T) {
	b := makeWAL(t, 3)
	h, err := ParseHeader(b[:HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	bo := h.ChecksumByteOrder()
	s1, s2 := h.Cksum1, h.Cksum2
	commits := 0
	for off := HeaderSize; off+h.FrameSize() <= len(b); off += h.FrameSize() {
		fh := ParseFrameHeader(b[off : off+FrameHeaderSize])
		if fh.Salt1 != h.Salt1 || fh.Salt2 != h.Salt2 {
			t.Fatalf("salt mismatch at %d", off)
		}
		s1, s2 = Checksum(bo, s1, s2, b[off:off+8])
		s1, s2 = Checksum(bo, s1, s2, b[off+FrameHeaderSize:off+h.FrameSize()])
		if s1 != fh.Cksum1 || s2 != fh.Cksum2 {
			t.Fatalf("checksum mismatch at %d", off)
		}
		if fh.CommitSize != 0 {
			commits++
		}
	}
	// CREATE TABLE + 3 INSERTs, each autocommitted
	if commits != 4 {
		t.Errorf("commit frames = %d, want 4", commits)
	}
}

func TestParseHeaderRejectsGarbage(t *testing.T) {
	if _, err := ParseHeader(make([]byte, HeaderSize)); err == nil {
		t.Fatal("want error on zero header")
	}
}

func TestChecksumKnownProperty(t *testing.T) {
	// checksum must be order-sensitive and seed-sensitive
	b := []byte{1, 0, 0, 0, 2, 0, 0, 0}
	a1, a2 := Checksum(binary.LittleEndian, 0, 0, b)
	b1, b2 := Checksum(binary.LittleEndian, 1, 1, b)
	if a1 == b1 && a2 == b2 {
		t.Fatal("seed ignored")
	}
}
