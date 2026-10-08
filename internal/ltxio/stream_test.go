package ltxio

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// makePagedDB builds a WAL-mode database with the given page size and enough
// rows of 300 random bytes to span several pages, checkpointed TRUNCATE so the
// main file holds every page, and returns its path.
func makePagedDB(t *testing.T, pageSize, rows int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "paged.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		"PRAGMA page_size=" + strconv.Itoa(pageSize),
		"PRAGMA journal_mode=WAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(300))"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func sha(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

// streamFile runs StreamChecksum over the bytes at path with a SHA-256 tee
// and returns the checksum, the tee's hex digest and the error.
func streamFile(t *testing.T, path string) (uint64, string, error) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	c, err := StreamChecksum(f, h)
	return c, fmt.Sprintf("%x", h.Sum(nil)), err
}

// TestStreamChecksumMatchesChecksumDatabase: for 4 KiB and 64 KiB pages,
// with the block lowered to 128 KiB so the 64 KiB-page file spans several
// blocks, the streamed checksum equals ChecksumDatabase's and the tee's
// digest equals the file's.
func TestStreamChecksumMatchesChecksumDatabase(t *testing.T) {
	prev := streamBlockSize
	streamBlockSize = 128 << 10
	t.Cleanup(func() { streamBlockSize = prev })
	for _, pageSize := range []int{4096, 65536} {
		path := makePagedDB(t, pageSize, 2000) // 2000 x 300 B is about 600 KiB of rows
		want, err := ChecksumDatabase(path)
		if err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(path); fi.Size() <= int64(streamBlockSize) {
			t.Fatalf("page size %d: file is %d bytes, not larger than the %d-byte block; the test would not cross a block boundary", pageSize, fi.Size(), streamBlockSize)
		}
		got, digest, err := streamFile(t, path)
		if err != nil {
			t.Fatalf("page size %d: %v", pageSize, err)
		}
		if got != want {
			t.Fatalf("page size %d: StreamChecksum %016x, ChecksumDatabase %016x", pageSize, got, want)
		}
		data, _ := os.ReadFile(path)
		if digest != sha(data) {
			t.Fatalf("page size %d: tee digest %s, file digest %s", pageSize, digest, sha(data))
		}
	}
}

// TestStreamChecksumTrailingBytes: bytes past nPages*pageSize reach the tee
// and do not change the checksum, as ChecksumDatabase ignores them.
func TestStreamChecksumTrailingBytes(t *testing.T) {
	path := makePagedDB(t, 4096, 50)
	want, err := ChecksumDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	data = append(data, bytes.Repeat([]byte{0xAB}, 777)...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, digest, err := streamFile(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || digest != sha(data) {
		t.Fatalf("checksum %016x want %016x; digest %s want %s", got, want, digest, sha(data))
	}
}

// TestStreamChecksumTruncatedDatabase: a header that promises more pages than
// the file holds is an error, and the tee still saw every byte.
func TestStreamChecksumTruncatedDatabase(t *testing.T) {
	path := makePagedDB(t, 4096, 50)
	data, _ := os.ReadFile(path)
	data = data[:len(data)-4096-100]
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, digest, err := streamFile(t, path)
	if !errors.Is(err, ErrNotWholeDatabase) {
		t.Fatalf("err = %v, want errors.Is(err, ErrNotWholeDatabase)", err)
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("err = %v, want a truncation error", err)
	}
	if digest != sha(data) {
		t.Fatalf("tee digest %s, want %s: the tee did not receive every byte", digest, sha(data))
	}
}

// TestStreamChecksumNotADatabase: garbage, a 50-byte file and an empty file
// all error from the header read, with the tee holding exactly the bytes.
func TestStreamChecksumNotADatabase(t *testing.T) {
	for name, data := range map[string][]byte{
		"garbage": bytes.Repeat([]byte("not a database "), 40),
		"short":   []byte("SQLite format 3\x00 but far too short"),
		"empty":   nil,
	} {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		_, digest, err := streamFile(t, path)
		if !errors.Is(err, ErrNotWholeDatabase) {
			t.Fatalf("%s: err = %v, want errors.Is(err, ErrNotWholeDatabase)", name, err)
		}
		if digest != sha(data) {
			t.Fatalf("%s: tee digest %s, want %s", name, digest, sha(data))
		}
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write(p []byte) (int, error) { return 0, w.err }

// TestStreamChecksumTeeError: the tee's error comes back unchanged.
func TestStreamChecksumTeeError(t *testing.T) {
	path := makePagedDB(t, 4096, 50)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	boom := errors.New("boom")
	if _, err := StreamChecksum(f, failingWriter{boom}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
