package ops

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

// requireSQLite3 skips the test if the sqlite3 CLI isn't on PATH — every
// test in this file drives a checkout through it, mirroring
// materialize_test.go's own convention.
func requireSQLite3(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
}

func rowCount(t *testing.T, dbPath string) int {
	t.Helper()
	out, err := exec.Command("sqlite3", dbPath, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatalf("sqlite3 %s: %v", dbPath, err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("sqlite3 %s: unparseable count %q: %v", dbPath, out, err)
	}
	return n
}

func TestExportHeadWritesAPlainFileWithNoSidecar(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('one');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "out.db")
	if err := w.Export("app", "main", "", dst, false); err != nil {
		t.Fatalf("export: %v", err)
	}
	if got := rowCount(t, dst); got != 1 {
		t.Fatalf("exported rows = %d, want 1", got)
	}
	if _, err := os.Stat(dst + ".sum"); !os.IsNotExist(err) {
		t.Fatalf("export must not write a .sum sidecar, got err=%v", err)
	}
}

func TestExportNamedCheckpointExportsHistoricalContentNotHead(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('one');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"INSERT INTO t VALUES ('two');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "out.db")
	if err := w.Export("app", "main", "v1", dst, false); err != nil {
		t.Fatalf("export at v1: %v", err)
	}
	if got := rowCount(t, dst); got != 1 {
		t.Fatalf("export at v1 rows = %d, want 1 (must not include v2's write)", got)
	}

	dst2 := filepath.Join(t.TempDir(), "out2.db")
	if err := w.Export("app", "main", "", dst2, false); err != nil {
		t.Fatalf("export at head: %v", err)
	}
	if got := rowCount(t, dst2); got != 2 {
		t.Fatalf("export at head rows = %d, want 2", got)
	}
}

func TestExportRefusesToOverwriteWithoutForce(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('one');").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "out.db")
	if err := w.Export("app", "main", "", dst, false); err != nil {
		t.Fatalf("first export: %v", err)
	}
	before, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}

	// Advance head so a silent overwrite would be observable.
	exec.Command("sqlite3", path, "INSERT INTO t VALUES ('two');").Run()
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}

	if err := w.Export("app", "main", "", dst, false); err == nil {
		t.Fatal("export must refuse to overwrite an existing destination without force")
	}
	after, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("a refused export must not touch the existing destination file")
	}

	if err := w.Export("app", "main", "", dst, true); err != nil {
		t.Fatalf("export with force: %v", err)
	}
	if got := rowCount(t, dst); got != 2 {
		t.Fatalf("forced export rows = %d, want 2 (head, post-overwrite)", got)
	}
}

func TestExportErrorsOnMissingCheckpoint(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "out.db")
	err := w.Export("app", "main", "nope", dst, false)
	if err == nil {
		t.Fatal("export at a nonexistent checkpoint must error")
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Fatal("a failed export must not create the destination")
	}
}

func TestExportErrorsOnMissingBranch(t *testing.T) {
	w := newWS(t)
	dst := filepath.Join(t.TempDir(), "out.db")
	if err := w.Export("nope", "main", "", dst, false); err == nil {
		t.Fatal("export of a nonexistent db@branch must error")
	}
}

func TestExportCreatesDestinationDirectory(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "nested", "deeper", "out.db")
	if err := w.Export("app", "main", "", dst, false); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("export did not create its destination directory: %v", err)
	}
}

// TestExportAtomicOnMidWriteFailureLeavesNoTempOrPartialFile forces a
// failure INSIDE ltxio.MaterializeChain (after its temp file already
// exists, mid-chain — not a fetch failure that never gets that far) by
// corrupting a hand-built segment's declared post-apply checksum, so its
// header decodes fine (the snapshot is already decoded into the temp file
// by then) but applying the segment's pages fails checksum verification.
// Export must surface the error, leave no file at dst, and leave dst's
// directory completely empty — proving the atomic temp+rename cleans up on
// error, not just that it succeeds on the happy path (see
// ltxio.MaterializeChain's doc comment: the temp file lives in dst's own
// directory and is removed on any error return).
func TestExportAtomicOnMidWriteFailureLeavesNoTempOrPartialFile(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('base');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "base", nil); err != nil {
		t.Fatal(err)
	}

	// Hand-build a second state as a SEGMENT (mirrors
	// TestCheckoutReadsASnapshotPlusSegments in materialize_test.go), then
	// corrupt its declared post-apply checksum so decoding the header
	// succeeds but applying its pages fails verification.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	next := path + ".next"
	if err := copyFile(path, next); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", next,
		"INSERT INTO t VALUES ('from-segment');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	segTXID := ref.HeadTXID + 1
	pageSize, commit, changed := changedPagesForTest(t, path, next)
	pre, err := ltxio.ChecksumDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	post, err := ltxio.ChecksumDatabase(next)
	if err != nil {
		t.Fatal(err)
	}
	var seg bytes.Buffer
	// post+1: a declared post-apply checksum that does NOT match what
	// applying `changed`'s pages actually produces.
	if err := ltxio.EncodeSegment(pageSize, commit, segTXID, segTXID, pre, post+1, changed, &seg); err != nil {
		t.Fatal(err)
	}
	segKey := store.SegmentKey(ref.Lineage, ref.Epoch, segTXID, segTXID)
	if _, err := w.Store.B.PutIf(segKey, seg.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID, ref.HeadEpoch = segTXID, ref.Epoch
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "out.db")
	if err := w.Export("app", "main", "", dst, false); err == nil {
		t.Fatal("export over a chain with a bad segment checksum must fail")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("a failed export must not leave a file at the destination")
	}
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Fatalf("a failed export must leave dst's directory empty, found %q", e.Name())
	}
}

func TestExportDiscardsChecksumRegardlessOfChainLength(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "out.db")
	if err := w.Export("app", "main", "v1", dst, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dst + ".sum"); !os.IsNotExist(err) {
		t.Fatal("export must never write a .sum sidecar (it discards the checksum)")
	}
}

// --- checkout-at (read-only historical checkout) ---

func TestCheckoutAtMaterializesHistoricalCheckpointReadOnly(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('one');").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", path, "INSERT INTO t VALUES ('two');").Run()
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}

	roPath, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatalf("checkout-at: %v", err)
	}
	wantPath := w.CheckoutAtPath("app", "main", "v1")
	if roPath != wantPath {
		t.Fatalf("checkout-at path = %q, want %q", roPath, wantPath)
	}
	if roPath == w.CheckoutPath("app", "main") {
		t.Fatal("the ro-cache path must never equal the writable checkout path")
	}
	if got := rowCount(t, roPath); got != 1 {
		t.Fatalf("ro checkout at v1 rows = %d, want 1", got)
	}
	fi, err := os.Stat(roPath)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o444 {
		t.Fatalf("ro checkout perm = %o, want 0444", perm)
	}
	if _, err := os.Stat(roPath + ".sum"); !os.IsNotExist(err) {
		t.Fatal("checkout-at must not write a .sum sidecar")
	}

	// The writable checkout (still at head, v2) must be untouched.
	if got := rowCount(t, path); got != 2 {
		t.Fatalf("writable checkout rows = %d, want 2 (checkout-at must not touch it)", got)
	}
}

func TestCheckoutAtRequiresACheckpointName(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.CheckoutAt("app", "main", "", false); err == nil {
		t.Fatal("checkout-at with no checkpoint name must error")
	}
}

func TestCheckoutAtErrorsOnMissingCheckpoint(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.CheckoutAt("app", "main", "nope", false); err == nil {
		t.Fatal("checkout-at at a nonexistent checkpoint must error")
	}
}

// TestCheckoutAtWithoutForceIsACacheHitWithNoStoreAccess proves the
// documented "cheap idempotence": once a ro-cache file exists, a repeat
// call with force=false returns it without touching the store at all — even
// once the underlying branch has been destroyed entirely.
func TestCheckoutAtWithoutForceIsACacheHitWithNoStoreAccess(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	first, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatal(err)
	}

	// Destroy the branch entirely — a store-touching call would now fail.
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}

	second, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatalf("cache hit must not touch the (now-gone) store: %v", err)
	}
	if second != first {
		t.Fatalf("cache hit path = %q, want %q", second, first)
	}
}

// TestCheckoutAtForceRematerializesAndCanFail proves force=true actually
// re-reads the store (unlike the force=false cache-hit path above): once
// the branch is destroyed, a forced re-materialize of the same checkpoint
// must fail, not silently keep serving the stale cached file.
func TestCheckoutAtForceRematerializesAndCanFail(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.CheckoutAt("app", "main", "v1", false); err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, err := w.CheckoutAt("app", "main", "v1", true); err == nil {
		t.Fatal("force=true must re-read the store and fail once the branch is gone")
	}
}

// TestCheckoutAtRejectsPathTraversalCheckpoint is the fix for a CRITICAL
// finding: CheckoutAt used to validate db and branch but not checkpoint
// before handing it straight to CheckoutAtPath's filepath.Join. A crafted
// checkpoint value containing ".." segments could resolve OUTSIDE
// checkouts-ro entirely — including onto the branch's own WRITABLE
// checkout path — and, worse, the force=false fast path would return that
// path as a "cache hit" purely because something existed there, before
// ref.Checkpoints was ever consulted. store.ValidateName must reject every
// such value before CheckoutAtPath is ever computed.
func TestCheckoutAtRejectsPathTraversalCheckpoint(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Give the branch a real, writable checkout with known content, so a
	// traversal that reached it would be observable.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('writable-checkout');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	malicious := []string{
		"../../../etc/passwd",
		"..",
		"a/b",
		"/etc/passwd",
		// Crafted to walk back out of checkouts-ro/app/ and land on
		// CheckoutPath's own file: checkouts-ro/app/main@<X>.db where X =
		// "../../checkouts/app/main" resolves (Join+Clean) to
		// checkouts/app/main.db — the writable checkout computed above.
		"../../checkouts/app/main",
	}

	for _, cp := range malicious {
		if _, err := w.CheckoutAt("app", "main", cp, false); err == nil {
			t.Fatalf("checkout-at with checkpoint %q must be refused (path traversal)", cp)
		}
	}

	// The writable checkout must be completely untouched by every attempt
	// above: same content, and — the sharpest possible check — it must
	// still be writable (a traversal that succeeded in "caching" onto it
	// would have chmod'd it 0444).
	if got := rowCount(t, path); got != 1 {
		t.Fatalf("writable checkout rows = %d, want 1 (a traversal attempt corrupted it)", got)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o200 == 0 {
		t.Fatal("the writable checkout must still be writable — a path-traversal checkout-at must not have chmod'd it read-only")
	}
}

// TestCheckoutAtChmodFailureRemovesFileSoLaterCallsRematerialize is the fix
// for a MINOR finding: if the post-materialize os.Chmod(path, 0o444) call
// fails, the file it was trying to lock down must not be left in place —
// the force=false fast path is a bare os.Stat (existence only, never a
// mode re-check), so a leftover writable file would be silently served as
// a "read-only checkout" by every future force=false call. Forces the
// chmod to fail via the checkoutAtChmod test hook and proves: (1) the call
// itself errors, (2) no file is left at the cache path, (3) a subsequent
// call (chmod restored) succeeds cleanly rather than tripping over a
// leftover.
func TestCheckoutAtChmodFailureRemovesFileSoLaterCallsRematerialize(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}

	orig := checkoutAtChmod
	checkoutAtChmod = func(string, os.FileMode) error { return os.ErrPermission }
	t.Cleanup(func() { checkoutAtChmod = orig })

	if _, err := w.CheckoutAt("app", "main", "v1", false); err == nil {
		t.Fatal("checkout-at must surface a chmod failure as an error")
	}
	roPath := w.CheckoutAtPath("app", "main", "v1")
	if _, err := os.Stat(roPath); !os.IsNotExist(err) {
		t.Fatalf("a chmod failure must leave no file at the cache path, got stat err=%v", err)
	}

	checkoutAtChmod = orig
	got, err := w.CheckoutAt("app", "main", "v1", false)
	if err != nil {
		t.Fatalf("a retry after the chmod hook is restored must succeed: %v", err)
	}
	if got != roPath {
		t.Fatalf("path = %q, want %q", got, roPath)
	}
	fi, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o444 {
		t.Fatalf("perm = %o, want 0444", perm)
	}
}
