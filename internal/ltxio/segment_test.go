package ltxio

import (
	"bytes"
	"database/sql"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// buildDB makes a quiesced SQLite file and returns its path.
func buildDB(t *testing.T, stmts ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range append([]string{"PRAGMA journal_mode=WAL"}, stmts...) {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func dumpOf(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", path, ".dump").Output()
	if err != nil {
		t.Fatalf("dump %s: %v", path, err)
	}
	return string(out)
}

// readPages returns every page of a quiesced database.
func readPages(t *testing.T, path string) (pageSize uint32, commit uint32, pages []Page) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize = uint32(data[16])<<8 | uint32(data[17])
	if pageSize == 1 {
		pageSize = 65536
	}
	commit = uint32(data[28])<<24 | uint32(data[29])<<16 | uint32(data[30])<<8 | uint32(data[31])
	for i := uint32(0); i < commit; i++ {
		off := int(i) * int(pageSize)
		pages = append(pages, Page{Pgno: i + 1, Data: data[off : off+int(pageSize)]})
	}
	return pageSize, commit, pages
}

// checksumOf is a small test wrapper around ChecksumDatabase that fails the
// test on error, used to build the preApplyChecksum/postApplyChecksum values
// EncodeSegment requires.
func checksumOf(t *testing.T, path string) uint64 {
	t.Helper()
	c, err := ChecksumDatabase(path)
	if err != nil {
		t.Fatalf("checksum %s: %v", path, err)
	}
	return c
}

func TestSegmentAppliedToSnapshotEqualsTheLaterDatabase(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	// v1: the snapshot's state. v2: after more writes.
	v1 := buildDB(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
		"INSERT INTO t (v) VALUES ('a'), ('b')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	_, _, before := readPages(t, v1)

	// Apply more writes to a copy, then diff pages to build the segment.
	v2 := filepath.Join(t.TempDir(), "v2.sqlite")
	if err := copyFileForTest(v1, v2); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", v2,
		"INSERT INTO t (v) VALUES ('c'); UPDATE t SET v='A' WHERE id=1;").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	pageSize, commit, after := readPages(t, v2)

	var changed []Page
	for i, p := range after {
		if i >= len(before) || !bytes.Equal(p.Data, before[i].Data) {
			changed = append(changed, p)
		}
	}
	if len(changed) == 0 {
		t.Fatal("expected some pages to differ")
	}
	if len(changed) == len(after) {
		t.Fatal("every page changed; this test cannot show a segment is smaller")
	}

	// The ltx format requires a non-snapshot file to carry the rolling
	// checksum of the state it applies onto (PreApplyChecksum) and of the
	// resulting state (PostApplyChecksum, on the trailer). Both are the same
	// ChecksumDatabase rolling checksum EncodeSnapshot embeds.
	preApply := checksumOf(t, v1)
	postApply := checksumOf(t, v2)

	var seg bytes.Buffer
	if err := EncodeSegment(pageSize, commit, 2, 2, preApply, postApply, changed, &seg); err != nil {
		t.Fatal(err)
	}
	if seg.Len() >= snap.Len() {
		t.Errorf("a partial segment (%d bytes) should be smaller than a full snapshot (%d)",
			seg.Len(), snap.Len())
	}

	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	txid, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(seg.Bytes())}, out)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 2 {
		t.Errorf("txid = %d, want 2", txid)
	}
	if dumpOf(t, out) != dumpOf(t, v2) {
		t.Fatal("snapshot+segment does not reproduce the later database")
	}
}

func TestMaterializeChainWithNoSegmentsIsJustTheSnapshot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	v1 := buildDB(t, "CREATE TABLE t (v)", "INSERT INTO t VALUES ('only')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 5, &snap); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	txid, err := MaterializeChain(bytes.NewReader(snap.Bytes()), nil, out)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 5 {
		t.Errorf("txid = %d, want 5", txid)
	}
	if dumpOf(t, out) != dumpOf(t, v1) {
		t.Fatal("chain with no segments must equal the snapshot")
	}
}

func TestChainRejectsAGap(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	v1 := buildDB(t, "CREATE TABLE t (v)", "INSERT INTO t VALUES ('x')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	pageSize, commit, pages := readPages(t, v1)
	// A no-op segment (page 1 unchanged), so pre == post; only the TXID gap
	// should cause MaterializeChain to fail.
	chk := checksumOf(t, v1)
	var seg bytes.Buffer
	// Covers txids 5..5 — a gap after the snapshot's txid 1.
	if err := EncodeSegment(pageSize, commit, 5, 5, chk, chk, pages[:1], &seg); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	if _, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(seg.Bytes())}, out); err == nil {
		t.Fatal("a TXID gap in the chain must be an error, never a silent skip")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("a failed chain must leave no destination file")
	}
}

func TestEncodeSegmentRejectsUnsortedOrDuplicatePages(t *testing.T) {
	p := func(n uint32) Page { return Page{Pgno: n, Data: make([]byte, 4096)} }
	var buf bytes.Buffer
	// The sortedness/duplicate check happens before checksums are used, so
	// placeholder checksum values are fine here.
	if err := EncodeSegment(4096, 3, 2, 2, 1, 1, []Page{p(2), p(1)}, &buf); err == nil {
		t.Error("unsorted pages must be rejected")
	}
	if err := EncodeSegment(4096, 3, 2, 2, 1, 1, []Page{p(1), p(1)}, &buf); err == nil {
		t.Error("duplicate pages must be rejected")
	}
}

func TestCorruptSegmentFailsClosed(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	v1 := buildDB(t, "CREATE TABLE t (v)", "INSERT INTO t VALUES ('x')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	pageSize, commit, pages := readPages(t, v1)
	chk := checksumOf(t, v1)
	var seg bytes.Buffer
	if err := EncodeSegment(pageSize, commit, 2, 2, chk, chk, pages[:1], &seg); err != nil {
		t.Fatal(err)
	}
	b := seg.Bytes()
	b[len(b)/2] ^= 0xFF
	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	if _, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(b)}, out); err == nil {
		t.Fatal("a corrupt segment must fail closed")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("a failed chain must leave no destination file")
	}
}

func copyFileForTest(from, to string) error {
	b, err := os.ReadFile(from)
	if err != nil {
		return err
	}
	return os.WriteFile(to, b, 0o644)
}
