package ltxio

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

// withStreamBlockSize lowers streamBlockSize for the duration of a test (or
// subtest) and restores it afterward.
func withStreamBlockSize(t *testing.T, n int) {
	t.Helper()
	prev := streamBlockSize
	streamBlockSize = n
	t.Cleanup(func() { streamBlockSize = prev })
}

// TestStreamChecksumMatchesChecksumDatabase: for 4 KiB and 64 KiB pages,
// with the block lowered to 128 KiB so the 64 KiB-page file spans several
// blocks, the streamed checksum equals ChecksumDatabase's and the tee's
// digest equals the file's.
func TestStreamChecksumMatchesChecksumDatabase(t *testing.T) {
	withStreamBlockSize(t, 128<<10)
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

// TestStreamChecksumTruncatedDatabase: a header that promises more pages
// than the file holds is an error, and the tee still saw every byte. Run at
// the default block size and at 65536 (the file is about 624 KiB, so the
// smaller size drains the truncation verdict across several blocks).
func TestStreamChecksumTruncatedDatabase(t *testing.T) {
	for _, blockSize := range []int{1 << 20, 65536} {
		t.Run(fmt.Sprintf("block=%d", blockSize), func(t *testing.T) {
			withStreamBlockSize(t, blockSize)
			path := makePagedDB(t, 4096, 2000)
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
		})
	}
}

// buildOddHeaderDB returns the bytes of a file shaped like a whole SQLite
// database of nPages pages of pageSize bytes each — the correct 16-byte
// magic, pageSize and nPages encoded at their header offsets, and a file
// length that genuinely matches nPages*pageSize — but whose page size is
// one SQLite itself never produces. checkStreamHeader (the fix for C1)
// must reject this from the header alone, before StreamChecksum reads or
// folds a single page; without that check the per-block fold misaligns
// against pageSize and produces a wrong checksum or a false truncation
// error instead.
func buildOddHeaderDB(pageSize uint16, nPages uint32) []byte {
	data := make([]byte, int64(nPages)*int64(pageSize))
	copy(data, dbHeaderMagic)
	binary.BigEndian.PutUint16(data[16:18], pageSize)
	binary.BigEndian.PutUint32(data[28:32], nPages)
	return data
}

// TestStreamChecksumNotADatabase: garbage, a too-short file, an empty file,
// an all-zero file and a file whose header declares an invalid page size
// all fail the header check — checkStreamHeader's magic or page-size test,
// or readDBHeader's own 100-byte minimum for the too-short and empty cases
// — rather than being folded as if they were real pages or mistaken for a
// truncated database. The tee holds exactly the bytes in every case. Run at
// two block sizes: the all-zero and odd-page-size files are large enough
// (200 bytes and 2,000,000 bytes) that the smaller size drains the verdict
// across more than one block for at least the odd-page-size case.
func TestStreamChecksumNotADatabase(t *testing.T) {
	cases := map[string][]byte{
		// "not a database " (repeated) does not match the 16-byte SQLite
		// magic, so this fails checkStreamHeader's magic check, not the
		// truncation path a large decoded nPages might otherwise suggest.
		"garbage": bytes.Repeat([]byte("not a database "), 40),
		// 34 bytes: short of readDBHeader's own 100-byte minimum.
		"short": []byte("SQLite format 3\x00 but far too short"),
		"empty": nil,
		// A 200-byte all-zero file decodes to page size 0 (C1): without
		// checkStreamHeader this reaches ltx.LockPgno(0), a division by
		// zero. It also fails the magic check on its own, independently.
		"zero page size (all-zero file)": make([]byte, 200),
		// The right magic, a file length that matches nPages*pageSize, but
		// pageSize=1000 is not a power of two: before C1 this produced a
		// wrong checksum with a nil error (pages straddling block
		// boundaries) rather than any error at all.
		"odd page size": buildOddHeaderDB(1000, 2000),
	}
	for _, blockSize := range []int{1 << 20, 65536} {
		for name, data := range cases {
			t.Run(fmt.Sprintf("%s/block=%d", name, blockSize), func(t *testing.T) {
				withStreamBlockSize(t, blockSize)
				path := filepath.Join(t.TempDir(), "notadb")
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
				_, digest, err := streamFile(t, path)
				if !errors.Is(err, ErrNotWholeDatabase) {
					t.Fatalf("%s: err = %v, want errors.Is(err, ErrNotWholeDatabase)", name, err)
				}
				if !strings.Contains(err.Error(), "header") || strings.Contains(err.Error(), "truncated") {
					t.Fatalf("%s: err = %v, want a header error, not a truncation error", name, err)
				}
				if digest != sha(data) {
					t.Fatalf("%s: tee digest %s, want %s", name, digest, sha(data))
				}
			})
		}
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write(p []byte) (int, error) { return 0, w.err }

// TestStreamChecksumTeeError: the tee's hard write error comes back
// unchanged.
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

// shortWriter reports success but writes one byte fewer than it was given,
// breaking the io.Writer contract the way a misbehaving tee might.
type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

// TestStreamChecksumTeeShortWrite: a tee that silently drops a byte (n <
// len(p), nil error) is caught the way io.Copy catches it, via
// io.ErrShortWrite (M2), rather than StreamChecksum reporting success over
// a tee that is missing data.
func TestStreamChecksumTeeShortWrite(t *testing.T) {
	path := makePagedDB(t, 4096, 50)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := StreamChecksum(f, shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err = %v, want io.ErrShortWrite", err)
	}
}

// erroringReader serves data's bytes and then returns err instead of
// io.EOF once data is exhausted, simulating a read failure that arrives
// before the stream would otherwise have ended legitimately.
type erroringReader struct {
	data []byte
	err  error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// TestStreamChecksumReadErrorMidStream: a read error that arrives before r
// would otherwise have reached EOF comes back unchanged (M4) and is not
// folded into a header or truncation verdict, so it must not be
// ErrNotWholeDatabase.
func TestStreamChecksumReadErrorMidStream(t *testing.T) {
	withStreamBlockSize(t, 65536)
	path := makePagedDB(t, 4096, 2000) // about 624 KiB; see TestStreamChecksumTruncatedDatabase
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cut := 2*streamBlockSize + 1000
	if len(full) < cut+streamBlockSize {
		t.Fatalf("file is %d bytes, want enough past %d to fail before EOF", len(full), cut)
	}
	boom := errors.New("boom")
	r := &erroringReader{data: full[:cut], err: boom}
	if _, err := StreamChecksum(r, io.Discard); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	} else if errors.Is(err, ErrNotWholeDatabase) {
		t.Fatalf("err = %v, should not also be ErrNotWholeDatabase", err)
	}
}
