package ltxio

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// cutPoints returns the two truncation lengths the tests below use for an
// object of n bytes: 60% through the pages, and one byte short of the
// trailer (so every page and most of the trailer are present).
func cutPoints(n int) map[string]int {
	return map[string]int{"at-60-percent": n * 6 / 10, "at-len-minus-1": n - 1}
}

// assertNothingWritten: dst does not exist and dir holds exactly the
// entries it held before the call (so no .tmp- file was left behind).
func assertNothingWritten(t *testing.T, dir, dst string, wantEntries []string) {
	t.Helper()
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("destination %s exists after a failed materialize (stat err %v)", dst, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != len(wantEntries) {
		t.Fatalf("directory after a failed materialize holds %v, want exactly %v", names, wantEntries)
	}
	for i := range names {
		if names[i] != wantEntries[i] {
			t.Fatalf("directory after a failed materialize holds %v, want exactly %v", names, wantEntries)
		}
	}
}

// TestMaterializeTruncatedSnapshotFailsClosed pins docs/architecture.md
// #Invariants §6 (every read path verifies the whole object before the
// destination exists): a snapshot object cut short — mid-pages or one byte
// before its trailer ends — fails Materialize AND MaterializeChain with an
// error, and neither a destination file nor a temp file is left in the
// destination directory.
//
// Mutation verified against: decodeSnapshot returning (hdr, trailer, nil)
// when DecodeDatabaseTo fails with io.ErrUnexpectedEOF (ltxio.go) — both
// entry points then rename the partial file into place.
func TestMaterializeTruncatedSnapshotFailsClosed(t *testing.T) {
	src := makeDB(t, 20)
	var buf bytes.Buffer
	if _, err := EncodeSnapshot(src, 3, &buf); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	if len(full) < 100 {
		t.Fatalf("precondition: snapshot is only %d bytes", len(full))
	}
	for name, n := range cutPoints(len(full)) {
		t.Run("Materialize/"+name, func(t *testing.T) {
			dir := t.TempDir()
			dst := filepath.Join(dir, "restored.db")
			txid, err := Materialize(bytes.NewReader(full[:n]), dst)
			if err == nil {
				t.Fatalf("Materialize of a snapshot truncated to %d/%d bytes returned nil (txid %d)", n, len(full), txid)
			}
			assertNothingWritten(t, dir, dst, nil)
		})
		t.Run("MaterializeChain/"+name, func(t *testing.T) {
			dir := t.TempDir()
			dst := filepath.Join(dir, "restored.db")
			txid, _, err := MaterializeChain(bytes.NewReader(full[:n]), nil, dst)
			if err == nil {
				t.Fatalf("MaterializeChain of a snapshot truncated to %d/%d bytes returned nil (txid %d)", n, len(full), txid)
			}
			assertNothingWritten(t, dir, dst, nil)
		})
	}
	// The untruncated object still materializes: the cuts, not the fixture,
	// are what the assertions above reject.
	dst := filepath.Join(t.TempDir(), "ok.db")
	if txid, err := Materialize(bytes.NewReader(full), dst); err != nil || txid != 3 {
		t.Fatalf("control: the full snapshot must materialize (txid %d, err %v)", txid, err)
	}
}

// TestMaterializeChainTruncatedSegmentFailsClosed: a chain whose LAST
// segment is cut short (after two good segments were already applied to
// the temp file) fails closed on MaterializeChain — error, no destination,
// no temp file — so partial progress is discarded, never renamed into
// place.
//
// Mutation verified against: applySegments treating io.ErrUnexpectedEOF
// from DecodePage as end-of-pages, ignoring it from Close, and skipping the
// post-apply checksum comparison (segment.go) — the truncated chain then
// lands.
func TestMaterializeChainTruncatedSegmentFailsClosed(t *testing.T) {
	testutil.RequireSQLite3(t)
	snap, segs, _ := buildThreeSegmentChain(t)
	last := segs[len(segs)-1]
	for name, n := range cutPoints(len(last)) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			dst := filepath.Join(dir, "chain.db")
			readers := readersOf(segs[:len(segs)-1])
			readers = append(readers, bytes.NewReader(last[:n]))
			txid, _, err := MaterializeChain(bytes.NewReader(snap), readers, dst)
			if err == nil {
				t.Fatalf("MaterializeChain with the last segment truncated to %d/%d bytes returned nil (txid %d)", n, len(last), txid)
			}
			assertNothingWritten(t, dir, dst, nil)
		})
	}
	// Control: the same chain untruncated materializes to txid 4.
	dst := filepath.Join(t.TempDir(), "ok.db")
	if txid, _, err := MaterializeChain(bytes.NewReader(snap), readersOf(segs), dst); err != nil || txid != 4 {
		t.Fatalf("control: the full chain must materialize (txid %d, err %v)", txid, err)
	}
}

// TestApplySegmentsTruncatedSegmentFailsClosed: the same cut applied through
// ApplySegments (the cached-checkout extension path) fails closed — error,
// no destination, the start file and nothing else in the directory, the
// start file's bytes untouched.
//
// Mutation verified against: the same applySegments mutation as
// TestMaterializeChainTruncatedSegmentFailsClosed.
func TestApplySegmentsTruncatedSegmentFailsClosed(t *testing.T) {
	testutil.RequireSQLite3(t)
	snap, segs, _ := buildThreeSegmentChain(t)
	last := segs[len(segs)-1]
	for name, n := range cutPoints(len(last)) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			start := filepath.Join(dir, "start.sqlite")
			_, startSum, err := MaterializeChain(bytes.NewReader(snap), nil, start)
			if err != nil {
				t.Fatal(err)
			}
			startBytes, err := os.ReadFile(start)
			if err != nil {
				t.Fatal(err)
			}
			dst := filepath.Join(dir, "dst.sqlite")
			readers := readersOf(segs[:len(segs)-1])
			readers = append(readers, io.Reader(bytes.NewReader(last[:n])))
			txid, _, err := ApplySegments(start, startSum, readers, dst)
			if err == nil {
				t.Fatalf("ApplySegments with the last segment truncated to %d/%d bytes returned nil (txid %d)", n, len(last), txid)
			}
			assertNothingWritten(t, dir, dst, []string{"start.sqlite"})
			if got, _ := os.ReadFile(start); !bytes.Equal(got, startBytes) {
				t.Fatal("a failed ApplySegments modified its start file")
			}
		})
	}
}
