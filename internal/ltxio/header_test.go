package ltxio

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// badHeaderFiles writes two files that parse syntactically as database
// headers but are not one: 200 zero bytes (no magic, page size 0, which
// divided by zero in ltx.LockPgno before the header was validated) and a
// file with the magic, page size 1000 and a matching length. Each case names
// the word the shared check's error must carry, which the old errors from
// further down the pipeline ("invalid page size", "EOF") did not.
func badHeaderFiles(t *testing.T) map[string]struct {
	path string
	want string
} {
	t.Helper()
	dir := t.TempDir()
	zero := filepath.Join(dir, "zero.db")
	if err := os.WriteFile(zero, make([]byte, 200), 0o644); err != nil {
		t.Fatal(err)
	}
	odd := filepath.Join(dir, "odd.db")
	if err := os.WriteFile(odd, buildOddHeaderDB(1000, 2000), 0o644); err != nil {
		t.Fatal(err)
	}
	return map[string]struct {
		path string
		want string
	}{
		"zero page size": {zero, "magic"},
		"odd page size":  {odd, "page size 1000 is not a power of two"},
	}
}

// TestReadDBHeaderValidatesMagicAndPageSize: the shared header parser
// rejects a missing magic and a page size that is not a power of two in
// [512, 65536], maps SQLite's encoding of 65536, and still parses a real
// database's header.
func TestReadDBHeaderValidatesMagicAndPageSize(t *testing.T) {
	if _, _, err := readDBHeader(bytes.NewReader(make([]byte, 200))); err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("all-zero header: err = %v, want a magic error", err)
	}
	odd := buildOddHeaderDB(1000, 2000)
	if _, _, err := readDBHeader(bytes.NewReader(odd)); err == nil || !strings.Contains(err.Error(), "page size 1000 is not a power of two") {
		t.Fatalf("page size 1000: err = %v, want a page size error", err)
	}
	hdr := make([]byte, dbHeaderSize)
	copy(hdr, dbHeaderMagic)
	binary.BigEndian.PutUint16(hdr[16:18], 1) // SQLite's encoding of 65536
	binary.BigEndian.PutUint32(hdr[28:32], 3)
	pageSize, nPages, err := readDBHeader(bytes.NewReader(hdr))
	if err != nil || pageSize != 65536 || nPages != 3 {
		t.Fatalf("64 KiB header: pageSize %d nPages %d err %v, want 65536 3 nil", pageSize, nPages, err)
	}
	path := makeDB(t, 10)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, _, err := readDBHeader(f); err != nil {
		t.Fatalf("real database header rejected: %v", err)
	}
}

// TestChecksumDatabaseRejectsBadHeader: ChecksumDatabase (the session's
// replica checksum) returns the shared check's error instead of dividing
// by zero on page size 0 or misreading odd pages.
func TestChecksumDatabaseRejectsBadHeader(t *testing.T) {
	for name, tc := range badHeaderFiles(t) {
		t.Run(name, func(t *testing.T) {
			if _, err := ChecksumDatabase(tc.path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestEncodeSnapshotRejectsBadHeader: the snapshot encoder returns the
// shared check's error. The ltx encoder already refused a page size of 0
// before, so this pins the message, not a former crash.
func TestEncodeSnapshotRejectsBadHeader(t *testing.T) {
	for name, tc := range badHeaderFiles(t) {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if _, err := EncodeSnapshot(tc.path, 1, &buf); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestApplySegmentsRejectsBadStartHeader: a start file that is not a
// database is refused by the shared check before any segment is read.
func TestApplySegmentsRejectsBadStartHeader(t *testing.T) {
	for name, tc := range badHeaderFiles(t) {
		t.Run(name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "out.db")
			if _, _, err := ApplySegments(tc.path, 0, []io.Reader{bytes.NewReader(nil)}, dst); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
