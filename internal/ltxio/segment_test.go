package ltxio

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/superfly/ltx"
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

// diffPages returns the pages of after that are new or differ from before at
// the same index — the same "changed pages" a caller would feed to
// EncodeSegment when building a segment from a diff of two on-disk database
// states. A shrinking after (fewer pages than before) naturally omits the
// dropped trailing pages, since the loop only ranges over after; the
// resulting segment's smaller Commit is what tells a reader those pages are
// gone.
func diffPages(before, after []Page) []Page {
	var changed []Page
	for i, p := range after {
		if i >= len(before) || !bytes.Equal(p.Data, before[i].Data) {
			changed = append(changed, p)
		}
	}
	return changed
}

// insertManySQL returns n INSERT statements, each writing a blobSize-byte
// blob, concatenated into one string suitable for a single sqlite3 CLI
// invocation. Used to force a database to grow by several pages.
func insertManySQL(n, blobSize int) string {
	return strings.Repeat(fmt.Sprintf("INSERT INTO t (v) VALUES (randomblob(%d));", blobSize), n)
}

// TestUpdateChecksumMatchesFullRescan proves the incremental-update math
// MaterializeChain relies on (UpdateChecksum, built on ChecksumPage) is
// equivalent to ChecksumDatabase's O(database size) full rescan: given only
// the "before" checksum and the pages that differ between two on-disk
// states, folding each page's old contribution out and new contribution in
// must land on exactly the same value as rescanning "after" from scratch.
// Exercised across both a database growing (new trailing pages appear) and
// shrinking (trailing pages are dropped), since those are the two cases
// MaterializeChain's incremental checksum maintenance has to get right.
func TestUpdateChecksumMatchesFullRescan(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}

	assertIncrementalMatchesRescan := func(t *testing.T, before, after string) {
		t.Helper()
		oldChecksum := checksumOf(t, before)
		pageSize, commitOld, pagesOld := readPages(t, before)
		_, commitNew, pagesNew := readPages(t, after)
		lockPgno := ltx.LockPgno(pageSize)

		top := commitOld
		if commitNew > top {
			top = commitNew
		}
		running := oldChecksum
		for pgno := uint32(1); pgno <= top; pgno++ {
			if pgno == lockPgno {
				continue
			}
			var oldData, newData []byte
			if pgno <= commitOld {
				oldData = pagesOld[pgno-1].Data
			}
			if pgno <= commitNew {
				newData = pagesNew[pgno-1].Data
			}
			running = UpdateChecksum(running, pgno, oldData, newData)
		}

		want := checksumOf(t, after)
		if running != want {
			t.Fatalf("incremental checksum %016x != full rescan of %s: %016x", running, after, want)
		}
	}

	v1 := buildDB(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
		"INSERT INTO t (v) VALUES (randomblob(50))")

	// Growth: append enough rows to add several new trailing pages.
	v2 := filepath.Join(t.TempDir(), "v2.sqlite")
	if err := copyFileForTest(v1, v2); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", v2,
		insertManySQL(60, 300)+"PRAGMA wal_checkpoint(TRUNCATE);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	_, c1, _ := readPages(t, v1)
	_, c2, _ := readPages(t, v2)
	if c2 <= c1 {
		t.Fatalf("expected v2 (%d pages) to have more pages than v1 (%d)", c2, c1)
	}
	assertIncrementalMatchesRescan(t, v1, v2)

	// Shrink: delete almost everything and VACUUM to drop trailing pages.
	v3 := filepath.Join(t.TempDir(), "v3.sqlite")
	if err := copyFileForTest(v2, v3); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", v3,
		"DELETE FROM t WHERE id > 1; VACUUM; PRAGMA wal_checkpoint(TRUNCATE);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	_, c3, _ := readPages(t, v3)
	if c3 >= c2 {
		t.Fatalf("expected v3 (%d pages) to have fewer pages than v2 (%d)", c3, c2)
	}
	assertIncrementalMatchesRescan(t, v2, v3)
}

// TestMaterializeChainIncrementalChecksumMatchesFullRescan builds a chain
// (snapshot, then a segment that grows the database, then a segment that
// shrinks it) and materializes it. MaterializeChain verifies each segment's
// incrementally-maintained checksum against that segment's declared
// post-apply checksum internally and fails closed on a mismatch, so a
// successful, correct-data materialization already demonstrates the
// incremental path agrees with the full-rescan values used to build the
// segments. This test also makes that explicit: it rescans the actual
// materialized result with ChecksumDatabase and checks it against the last
// segment's declared post-apply checksum.
func TestMaterializeChainIncrementalChecksumMatchesFullRescan(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}

	v1 := buildDB(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
		"INSERT INTO t (v) VALUES (randomblob(50))")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	_, commit0, before1 := readPages(t, v1)

	// Segment 1 (txid 2): grow the database by several pages.
	v2 := filepath.Join(t.TempDir(), "v2.sqlite")
	if err := copyFileForTest(v1, v2); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", v2,
		insertManySQL(60, 300)+"PRAGMA wal_checkpoint(TRUNCATE);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	pageSize, commit1, after1 := readPages(t, v2)
	if commit1 <= commit0 {
		t.Fatalf("expected v2 (%d pages) to have more pages than v1 (%d)", commit1, commit0)
	}
	pre1 := checksumOf(t, v1)
	post1 := checksumOf(t, v2)
	var seg1 bytes.Buffer
	if err := EncodeSegment(pageSize, commit1, 2, 2, pre1, post1, diffPages(before1, after1), &seg1); err != nil {
		t.Fatal(err)
	}

	// Segment 2 (txid 3): delete almost everything and VACUUM, shrinking the
	// database back down.
	v3 := filepath.Join(t.TempDir(), "v3.sqlite")
	if err := copyFileForTest(v2, v3); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", v3,
		"DELETE FROM t WHERE id > 1; VACUUM; PRAGMA wal_checkpoint(TRUNCATE);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	_, commit2, after2 := readPages(t, v3)
	if commit2 >= commit1 {
		t.Fatalf("expected v3 (%d pages) to have fewer pages than v2 (%d)", commit2, commit1)
	}
	pre2 := checksumOf(t, v2)
	post2 := checksumOf(t, v3)
	var seg2 bytes.Buffer
	if err := EncodeSegment(pageSize, commit2, 3, 3, pre2, post2, diffPages(after1, after2), &seg2); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	txid, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(seg1.Bytes()), bytes.NewReader(seg2.Bytes())}, out)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 3 {
		t.Errorf("txid = %d, want 3", txid)
	}
	if dumpOf(t, out) != dumpOf(t, v3) {
		t.Fatal("snapshot+segments does not reproduce the later database")
	}

	if rescan := checksumOf(t, out); rescan != post2 {
		t.Fatalf("full rescan of materialized result (%016x) != declared post-apply checksum (%016x)", rescan, post2)
	}
}
