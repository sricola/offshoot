package ops

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/store/storetest"
)

// captureStderr redirects os.Stderr for the duration of fn and returns
// everything written to it.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = wr
	fn()
	wr.Close()
	os.Stderr = orig
	out, _ := io.ReadAll(r)
	return string(out)
}

func newWS(t *testing.T) *Workspace {
	t.Helper()
	w, err := Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// newWSOnFakeS3 mirrors newWS but backs the Workspace with an in-process
// fake S3 server instead of a local directory. It exists so the SAME test
// bodies that exercise ops invariants against the local backend (see
// TestConcurrentCheckpointsOnlyOneWinsOnS3, TestDestroyAndGCOnS3) can be
// run again against S3, proving those invariants hold across backends
// rather than being accidents of the local filesystem's Put/rename
// semantics. Each call gets its own fake server and a unique key prefix.
func newWSOnFakeS3(t *testing.T) *Workspace {
	t.Helper()
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")
	t.Setenv("OFFSHOOT_CHECKOUTS", t.TempDir())

	spec := "s3://" + f.Bucket() + "/" + strings.ReplaceAll(t.Name(), "/", "-")
	w, err := Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestInitAndOpen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "s")
	if _, err := Open(root); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("open before init: want ErrNotFound, got %v", err)
	}
	if _, err := Init(root); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAndCheckout(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err == nil {
		t.Fatal("duplicate create must fail")
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("checkout not writable: %v", err)
	}
	r, _, err := w.Store.GetRef("app", "main")
	if err != nil || !r.Protected || r.Checkpoints["init"].TXID != 1 || r.HeadTXID != 1 {
		t.Fatalf("ref: %+v err=%v", r, err)
	}
}

func TestCreateFromImportsWithoutTouchingSource(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "legacy.db")
	if out, err := exec.Command("sqlite3", src,
		"CREATE TABLE t (v); INSERT INTO t VALUES (42);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	before, _ := os.ReadFile(src)

	w := newWS(t)
	if err := w.CreateFrom("legacy", src); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(src)
	if string(before) != string(after) {
		t.Fatal("import mutated the source file")
	}
	path, err := w.Checkout("legacy", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "42\n" {
		t.Fatalf("imported content: %q err=%v", out, err)
	}
}

func TestParseTarget(t *testing.T) {
	db, br, err := ParseTarget("app")
	if err != nil || db != "app" || br != "main" {
		t.Fatalf("%s %s %v", db, br, err)
	}
	db, br, err = ParseTarget("app@x-1")
	if err != nil || db != "app" || br != "x-1" {
		t.Fatalf("%s %s %v", db, br, err)
	}
	if _, _, err := ParseTarget("a@b@c"); err == nil {
		t.Fatal("want error")
	}
	if _, _, err := ParseTarget("Bad@x"); err == nil {
		t.Fatal("want name validation error")
	}
}

func TestCheckpointAndRematerialize(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2),(3);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	txid, err := w.Checkpoint("app", "main", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if txid != 2 {
		t.Errorf("txid = %d, want 2", txid)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err == nil {
		t.Fatal("duplicate checkpoint name must fail")
	}

	// A fresh checkout must contain the checkpointed data.
	want, _ := exec.Command("sqlite3", path, ".dump").Output()
	path2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", path2, ".dump").Output()
	if string(want) != string(got) {
		t.Fatal("re-checkout does not match checkpointed state")
	}
}

// TestCheckpointRecoversFromOrphanedSnapshot simulates a crashed prior
// Checkpoint attempt: the snapshot object at the deterministic key
// (lineage, epoch, HeadTXID+1) was uploaded but the ref update never landed
// (process died, CAS lost, I/O error). A subsequent Checkpoint's create-only
// put at that same key must not wedge forever behind the orphan; it must
// recover and succeed with the real (non-garbage) data.
func TestCheckpointRecoversFromOrphanedSnapshot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2),(3);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	// Pre-seed the snapshot key Checkpoint is about to write, with garbage
	// bytes, simulating a crashed prior attempt whose ref write never landed.
	orphanKey := store.SnapshotKey(ref.Lineage, ref.Epoch, ref.HeadTXID+1)
	if err := w.Store.B.Put(orphanKey, []byte("garbage-not-a-valid-ltx-snapshot")); err != nil {
		t.Fatal(err)
	}

	txid, err := w.Checkpoint("app", "main", "v1")
	if err != nil {
		t.Fatalf("Checkpoint must recover from an orphaned snapshot object, got: %v", err)
	}
	if txid != ref.HeadTXID+1 {
		t.Errorf("txid = %d, want %d", txid, ref.HeadTXID+1)
	}

	// A fresh checkout must contain the real data, not the garbage that was
	// squatting on the snapshot key.
	want, _ := exec.Command("sqlite3", path, ".dump").Output()
	path2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", path2, ".dump").Output()
	if string(want) != string(got) {
		t.Fatal("checkout after recovered checkpoint does not match real data")
	}
}

func TestCheckpointFailsCleanlyUnderLiveWriter(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Exec("PRAGMA journal_mode=WAL; CREATE TABLE t (v)"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	// Open write transaction -> checkpoint must fail cleanly, not hang forever.
	if _, err := w.Checkpoint("app", "main", "x"); err == nil {
		t.Fatal("checkpoint under live write txn must fail")
	}
	tx.Rollback()
}

func TestForkIsIndependentOfParent(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	txid, err := w.Fork("app", "main", "attempt-1", "", 0)
	if err != nil || txid != 2 {
		t.Fatalf("fork: txid=%d err=%v", txid, err)
	}
	// Child materializes and matches the parent's checkpointed state.
	cpath, err := w.Checkout("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", cpath, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("child content: %q", got)
	}

	// Storage independence: delete the ENTIRE parent lineage; child must
	// still materialize (spec: children never reference parent segments).
	parentRef, _, _ := w.Store.GetRef("app", "main")
	keys, _ := w.Store.B.List(store.LineagePrefix(parentRef.Lineage))
	for _, k := range keys {
		if err := w.Store.B.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Checkout("app", "attempt-1"); err != nil {
		t.Fatalf("child not independent of parent lineage: %v", err)
	}

	// Child ref shape.
	cref, _, _ := w.Store.GetRef("app", "attempt-1")
	if cref.Parent != "app@main@2" || cref.Protected ||
		cref.Checkpoints["fork"].TXID != 2 || len(cref.Checkpoints) != 1 {
		t.Fatalf("child ref: %+v", cref)
	}
	if cref.Lineage == parentRef.Lineage {
		t.Fatal("child must have its own lineage")
	}
}

func TestForkAtCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")
	exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").Run()
	w.Checkpoint("app", "main", "v2")

	if _, err := w.Fork("app", "main", "old", "v1", 0); err != nil {
		t.Fatal(err)
	}
	p, _ := w.Checkout("app", "old")
	got, _ := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("fork --at v1 content: %q", got)
	}
	if _, err := w.Fork("app", "main", "bad", "nope", 0); err == nil {
		t.Fatal("unknown checkpoint must fail")
	}
	if _, err := w.Fork("app", "main", "old", "", 0); err == nil {
		t.Fatal("existing branch name must fail")
	}
}

// TestForkWarnsOnUncheckpointedChanges verifies the plan promise: forking a
// branch whose checkout has un-checkpointed changes proceeds from the last
// committed state and prints a warning naming the txid used. Forking right
// after a fresh checkpoint (no drift) must stay silent.
func TestForkWarnsOnUncheckpointedChanges(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	// No-warning case: fork immediately after a fresh checkpoint (checkout
	// matches the committed state exactly).
	stderr := captureStderr(t, func() {
		if _, err := w.Fork("app", "main", "clean", "", 0); err != nil {
			t.Fatal(err)
		}
	})
	if stderr != "" {
		t.Fatalf("unexpected warning forking a freshly-checkpointed branch: %q", stderr)
	}

	// Write MORE data without checkpointing.
	if out, err := exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	var txid uint64
	stderr = captureStderr(t, func() {
		txid, err = w.Fork("app", "main", "dirty", "", 0)
		if err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(stderr, "un-checkpointed changes") {
		t.Fatalf("warning missing expected text: %q", stderr)
	}
	if !strings.Contains(stderr, "txid 2") {
		t.Fatalf("warning does not name the txid used: %q", stderr)
	}
	if txid != 2 {
		t.Fatalf("fork txid = %d, want 2 (last committed, not the dirty write)", txid)
	}

	// The fork's content must be the v1 (committed) state, not the
	// uncommitted second insert.
	cpath, err := w.Checkout("app", "dirty")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", cpath, "SELECT v FROM t ORDER BY v;").Output()
	if string(got) != "1\n" {
		t.Fatalf("fork content: %q, want only the checkpointed row", got)
	}
}

// TestForkWarnsOnStaleCheckout verifies the fix for a checkout sidecar that
// used to be a bare content hash: it couldn't detect a STALE checkout, one
// whose branch ref was repointed (e.g. rollback/promote with a skipped
// refresh on a busy checkout) after the checkout was last materialized. We
// simulate that "repoint without refresh" directly at the store layer (the
// same end state Rollback/Promote leave behind when their best-effort
// checkout refresh is skipped or fails): fork a scratch branch, advance it,
// then CAS main's ref to point at the scratch branch's new state without
// touching main's checkout file or its .sum sidecar. Forking main afterward
// must warn that the checkout is stale (not silently treat it as clean) and
// must still fork from the branch's actual (new) head.
func TestForkWarnsOnStaleCheckout(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	// Build a "new state" for main to be silently repointed to: fork a
	// scratch branch off main, add more data, and checkpoint it there.
	if _, err := w.Fork("app", "main", "scratch", "", 0); err != nil {
		t.Fatal(err)
	}
	scratchPath, err := w.Checkout("app", "scratch")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", scratchPath, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	newTXID, err := w.Checkpoint("app", "scratch", "v2")
	if err != nil {
		t.Fatal(err)
	}
	scratchRef, _, err := w.Store.GetRef("app", "scratch")
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a repoint-without-refresh: CAS main's ref directly at the
	// store layer to the scratch branch's new (lineage, txid), exactly as
	// Rollback/Promote would, but WITHOUT touching main's checkout file or
	// its .sum sidecar (as if the refresh had been skipped because the
	// checkout was busy).
	mainRef, mainEtag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mainRef.Lineage = scratchRef.Lineage
	mainRef.Epoch = scratchRef.Epoch
	mainRef.HeadTXID = newTXID
	if _, err := w.Store.PutRef("app", "main", mainRef, mainEtag); err != nil {
		t.Fatal(err)
	}

	var forkTXID uint64
	stderr := captureStderr(t, func() {
		forkTXID, err = w.Fork("app", "main", "child", "", 0)
		if err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(stderr, "stale") {
		t.Fatalf("warning missing expected text: %q", stderr)
	}
	if !strings.Contains(stderr, "repointed") {
		t.Fatalf("warning missing expected text: %q", stderr)
	}
	if !strings.Contains(stderr, "offshoot checkout") {
		t.Fatalf("warning missing refresh hint: %q", stderr)
	}
	if forkTXID != newTXID {
		t.Fatalf("fork txid = %d, want %d (the repointed-to branch head, not the stale checkout's txid)", forkTXID, newTXID)
	}

	// The fork's content must be the NEW (repointed-to) branch head, not
	// whatever the stale checkout file on disk happened to contain.
	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", cpath, "SELECT v FROM t ORDER BY v;").Output()
	if string(got) != "1\n2\n" {
		t.Fatalf("fork content: %q, want the new branch head content", got)
	}
}

func TestRollback(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "good")
	exec.Command("sqlite3", path, "DROP TABLE t;").Run()
	w.Checkpoint("app", "main", "bad")

	before, _, _ := w.Store.GetRef("app", "main")
	p, err := w.Rollback("app", "main", "good")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", p, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("rolled-back content: %q", got)
	}
	after, _, _ := w.Store.GetRef("app", "main")
	if after.Lineage == before.Lineage {
		t.Fatal("rollback must move to a new lineage")
	}
	if _, ok := after.Checkpoints["bad"]; ok {
		t.Fatal("later checkpoint must be dropped")
	}
	if after.Checkpoints["good"] != before.Checkpoints["good"] {
		t.Fatal("earlier checkpoint must be kept")
	}
	if !after.Protected {
		t.Fatal("protected flag must survive rollback")
	}
}

// TestRollbackToEarlierCheckpointTwice guards against dangling checkpoints:
// Rollback used to copy only the `to` checkpoint's snapshot into the new
// lineage, dropping every OTHER kept (earlier) checkpoint's snapshot on the
// floor in the old, now-orphaned lineage. A second rollback to one of those
// earlier checkpoints would then fail "not found" (and GC would eventually
// delete the data for good). Rolling back to v2 then to v1 must both
// succeed, with v1's content correct.
func TestRollbackToEarlierCheckpointTwice(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").Run()
	if _, err := w.Checkpoint("app", "main", "v2"); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Rollback("app", "main", "v2"); err != nil {
		t.Fatalf("rollback --to v2: %v", err)
	}
	p, err := w.Rollback("app", "main", "v1")
	if err != nil {
		t.Fatalf("rollback --to v1 after rollback --to v2: %v", err)
	}
	got, _ := exec.Command("sqlite3", p, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("content after second rollback: %q, want just the v1 row", got)
	}
}

// TestForkAtCheckpointAfterRollback is the fork-side counterpart of
// TestRollbackToEarlierCheckpointTwice: after a rollback, `fork --at` an
// earlier checkpoint that rollback kept must still find its snapshot object
// (not "not found") and must produce the correct (earlier) content.
func TestForkAtCheckpointAfterRollback(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").Run()
	if _, err := w.Checkpoint("app", "main", "v2"); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Rollback("app", "main", "v2"); err != nil {
		t.Fatalf("rollback --to v2: %v", err)
	}
	if _, err := w.Fork("app", "main", "old", "v1", 0); err != nil {
		t.Fatalf("fork --at v1 after rollback --to v2: %v", err)
	}
	p, err := w.Checkout("app", "old")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", p, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("fork --at v1 content: %q, want just the v1 row", got)
	}
}

// TestCheckoutQuiescesAndWarnsOnDirtyOverwrite verifies the fix for
// Checkout's missing busy probe and dirty-overwrite warning: re-checking-out
// a branch whose checkout has un-checkpointed local edits must warn on
// stderr before silently discarding them, and the resulting file must equal
// the branch's actual committed head, not the discarded local edit.
func TestCheckoutQuiescesAndWarnsOnDirtyOverwrite(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	// No-warning case: re-checkout right after a fresh checkpoint (checkout
	// matches committed state exactly).
	stderr := captureStderr(t, func() {
		if _, err := w.Checkout("app", "main"); err != nil {
			t.Fatal(err)
		}
	})
	if stderr != "" {
		t.Fatalf("unexpected warning re-checking-out a clean checkout: %q", stderr)
	}

	// Modify the checkout WITHOUT checkpointing.
	if out, err := exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	var newPath string
	stderr = captureStderr(t, func() {
		newPath, err = w.Checkout("app", "main")
		if err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(stderr, "un-checkpointed changes") {
		t.Fatalf("warning missing expected text: %q", stderr)
	}
	if !strings.Contains(stderr, "app@main") {
		t.Fatalf("warning does not name the checkout: %q", stderr)
	}

	got, _ := exec.Command("sqlite3", newPath, "SELECT v FROM t ORDER BY v;").Output()
	if string(got) != "1\n" {
		t.Fatalf("checkout content = %q, want reset to head (just the checkpointed row)", got)
	}
}

// TestPromoteWarnsOnUncheckpointedSourceChanges verifies Promote warns (like
// Fork) when the source branch's checkout has un-checkpointed local edits:
// promote always proceeds from the source's last committed head, and the
// operator should be told their dirty edits weren't included.
func TestPromoteWarnsOnUncheckpointedSourceChanges(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}
	ap, err := w.Checkout("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", ap, "INSERT INTO t VALUES (99);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "attempt-1", "winner"); err != nil {
		t.Fatal(err)
	}
	// Dirty edit on the SOURCE after its last checkpoint, without
	// checkpointing it.
	if out, err := exec.Command("sqlite3", ap, "INSERT INTO t VALUES (100);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	stderr := captureStderr(t, func() {
		if _, err := w.Promote("app", "attempt-1", "main", true); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(stderr, "un-checkpointed changes") {
		t.Fatalf("warning missing expected text: %q", stderr)
	}
	if !strings.Contains(stderr, "app@attempt-1") {
		t.Fatalf("warning does not name the source: %q", stderr)
	}

	mp, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", mp, "SELECT count(*) FROM t;").Output()
	if string(got) != "2\n" {
		t.Fatalf("promoted content = %q, want the checkpointed count only (2 rows)", got)
	}
}

func TestPromote(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")
	w.Fork("app", "main", "attempt-1", "", 0)

	ap, _ := w.Checkout("app", "attempt-1")
	exec.Command("sqlite3", ap, "INSERT INTO t VALUES (99);").Run()
	w.Checkpoint("app", "attempt-1", "winner")

	// Protected target requires force.
	if _, err := w.Promote("app", "attempt-1", "main", false); err == nil {
		t.Fatal("promote onto protected main without force must fail")
	}
	txid, err := w.Promote("app", "attempt-1", "main", true)
	if err != nil {
		t.Fatal(err)
	}
	mp, _ := w.Checkout("app", "main")
	got, _ := exec.Command("sqlite3", mp, "SELECT count(*) FROM t;").Output()
	if string(got) != "2\n" {
		t.Fatalf("promoted main content: %q", got)
	}
	mref, _, _ := w.Store.GetRef("app", "main")
	aref, _, _ := w.Store.GetRef("app", "attempt-1")
	if mref.Lineage == aref.Lineage {
		t.Fatal("promote must seed a NEW lineage (one writer per lineage)")
	}
	if mref.Checkpoints["promote"].TXID != txid || !mref.Protected {
		t.Fatalf("target ref: %+v", mref)
	}
	// Source survives; destroying it later must not affect main (independence).
	keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage))
	for _, k := range keys {
		w.Store.B.Delete(k)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatalf("promoted main not independent of source lineage: %v", err)
	}
}

// TestPromoteOntoSelfRejected verifies that promoting a branch onto itself
// is rejected outright: doing so would reset the branch's own checkpoint
// map to {"promote": txid} against its own head, silently wiping its
// checkpoint history.
func TestPromoteOntoSelfRejected(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.Promote("app", "main", "main", true); err == nil {
		t.Fatal("promote onto self must fail")
	}

	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("ref must be unchanged after a rejected self-promote: before=%+v after=%+v", before, after)
	}
}

// TestRollbackReportsRepointOnRefreshFailure verifies that when Rollback's
// ref CAS succeeds but the subsequent checkout refresh fails, the branch's
// repointed state is not silently lost behind a plain error: the error
// names the partial success, and the ref itself really has moved.
func TestRollbackReportsRepointOnRefreshFailure(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "good"); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path, "DROP TABLE t;").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "bad"); err != nil {
		t.Fatal(err)
	}

	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Replace the checkout file with a directory at the exact same path, so
	// the post-CAS refresh (busy probe / materialize rename) cannot succeed.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	_, err = w.Rollback("app", "main", "good")
	if err == nil {
		t.Fatal("rollback must report the refresh failure")
	}
	if !strings.Contains(err.Error(), "repointed") {
		t.Fatalf("error must mention the repoint: %v", err)
	}
	if !strings.Contains(err.Error(), "checkout could not be refreshed") {
		t.Fatalf("error must mention the refresh failure: %v", err)
	}

	// Partial success is real: the ref must have actually moved, even though
	// the checkout refresh failed.
	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage == before.Lineage {
		t.Fatal("ref must have repointed to a new lineage despite the refresh failure")
	}
	if after.HeadTXID != before.Checkpoints["good"].TXID {
		t.Fatalf("ref HeadTXID = %d, want checkpoint %q's txid %d", after.HeadTXID, "good", before.Checkpoints["good"].TXID)
	}
	if _, ok := after.Checkpoints["bad"]; ok {
		t.Fatal("later checkpoint must still be dropped")
	}
}

func TestConcurrentForksFromSameParent(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, errs[n] = w.Fork("app", "main", fmt.Sprintf("f-%d", n), "", 0)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("fork %d: %v", i, err)
		}
	}
	// All 8 children have distinct lineages and materialize correctly.
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		r, _, err := w.Store.GetRef("app", fmt.Sprintf("f-%d", i))
		if err != nil || seen[r.Lineage] {
			t.Fatalf("child %d: err=%v dup-lineage=%v", i, err, seen[r.Lineage])
		}
		seen[r.Lineage] = true
		if _, err := w.Checkout("app", fmt.Sprintf("f-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentCheckpointsOnlyOneWins(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()

	var wg sync.WaitGroup
	okCount := 0
	var mu sync.Mutex
	var loserErrs []error
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := w.Checkpoint("app", "main", fmt.Sprintf("cp-%d", n)); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			} else {
				mu.Lock()
				loserErrs = append(loserErrs, err)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	// At least one wins; losers fail loudly with the CAS-race error, and the
	// ref must remain internally consistent (head >= every recorded checkpoint).
	if okCount == 0 {
		t.Fatal("no checkpoint succeeded")
	}
	for _, e := range loserErrs {
		t.Logf("loser error: %v", e)
	}
	r, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	for name, cp := range r.Checkpoints {
		if cp.TXID > r.HeadTXID {
			t.Fatalf("checkpoint %s@txid %d beyond head %d", name, cp.TXID, r.HeadTXID)
		}
		if _, _, err := w.Store.B.Get(store.SnapshotKey(r.Lineage, r.Epoch, cp.TXID)); err != nil {
			t.Fatalf("recorded checkpoint %s has no snapshot object: %v", name, err)
		}
	}
}

// TestConcurrentCheckpointsOnlyOneWinsOnS3 is TestConcurrentCheckpointsOnlyOneWins
// run against the S3 backend instead of Local. It matters specifically
// because losing racers in Checkpoint fall through to an UNCONDITIONAL
// Store.B.Put to overwrite an orphaned/rival snapshot object at the
// deterministic snapshot key (see the comment in ops.go's Checkpoint) —
// a code path RunConformance never exercised before PutOverwritesExistingKey
// was added, and Local's Put (rename-over-existing-file) could silently
// diverge from S3's Put (PutObject, unconditional overwrite) without any
// test noticing. Same assertions as the Local version: at least one
// checkpoint wins, the ref stays internally consistent, and every recorded
// checkpoint's snapshot object is actually present.
func TestConcurrentCheckpointsOnlyOneWinsOnS3(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWSOnFakeS3(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()

	var wg sync.WaitGroup
	okCount := 0
	var mu sync.Mutex
	var loserErrs []error
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := w.Checkpoint("app", "main", fmt.Sprintf("cp-%d", n)); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			} else {
				mu.Lock()
				loserErrs = append(loserErrs, err)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	// At least one wins; losers fail loudly with the CAS-race error, and the
	// ref must remain internally consistent (head >= every recorded checkpoint).
	if okCount == 0 {
		t.Fatal("no checkpoint succeeded")
	}
	for _, e := range loserErrs {
		t.Logf("loser error: %v", e)
	}
	r, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	for name, cp := range r.Checkpoints {
		if cp.TXID > r.HeadTXID {
			t.Fatalf("checkpoint %s@txid %d beyond head %d", name, cp.TXID, r.HeadTXID)
		}
		if _, _, err := w.Store.B.Get(store.SnapshotKey(r.Lineage, r.Epoch, cp.TXID)); err != nil {
			t.Fatalf("recorded checkpoint %s has no snapshot object: %v", name, err)
		}
	}
}

func TestInitAndOpenAgainstS3Spec(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")
	t.Setenv("OFFSHOOT_CHECKOUTS", t.TempDir())

	spec := "s3://" + f.Bucket() + "/p"
	w, err := Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(path, os.Getenv("OFFSHOOT_CHECKOUTS")) {
		t.Fatalf("remote-store checkout must live under OFFSHOOT_CHECKOUTS, got %s", path)
	}
	// Re-open the same store and see the database.
	w2, err := Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	sts, err := w2.Status()
	if err != nil || len(sts) != 1 || sts[0].DB != "app" {
		t.Fatalf("status=%v err=%v", sts, err)
	}
}

// TestCheckoutRootSeparatesDistinctEndpoints guards the fix for the checkout
// cache collision: checkoutRoot must key off the RESOLVED store identity
// (store.StoreIdentity), not the raw spec string. For one fixed
// "s3://bucket/p" spec, two different OFFSHOOT_S3_ENDPOINT values must
// produce two different checkout roots (otherwise a session against MinIO
// and a later session against real AWS, using the identical spec string,
// would land in the same local checkout cache dir and silently discard
// un-checkpointed edits). The trailing-slash spelling of the same spec under
// the same endpoint must land on the SAME checkout root.
func TestCheckoutRootSeparatesDistinctEndpoints(t *testing.T) {
	// OFFSHOOT_CHECKOUTS must be unset for this test: checkoutRoot only
	// consults StoreIdentity when it falls through to the hashed-cache-dir
	// path, i.e. when OFFSHOOT_CHECKOUTS isn't overriding it.
	prevCheckouts, hadCheckouts := os.LookupEnv("OFFSHOOT_CHECKOUTS")
	os.Unsetenv("OFFSHOOT_CHECKOUTS")
	t.Cleanup(func() {
		if hadCheckouts {
			os.Setenv("OFFSHOOT_CHECKOUTS", prevCheckouts)
		} else {
			os.Unsetenv("OFFSHOOT_CHECKOUTS")
		}
	})

	t.Setenv("OFFSHOOT_S3_ENDPOINT", "http://minio.local:9000")
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")

	rootMinio, err := checkoutRoot("s3://bucket/p")
	if err != nil {
		t.Fatal(err)
	}
	rootMinioTrailingSlash, err := checkoutRoot("s3://bucket/p/")
	if err != nil {
		t.Fatal(err)
	}
	if rootMinio != rootMinioTrailingSlash {
		t.Fatalf("s3://bucket/p and s3://bucket/p/ under the same endpoint must share a checkout root: %q vs %q", rootMinio, rootMinioTrailingSlash)
	}

	t.Setenv("OFFSHOOT_S3_ENDPOINT", "http://real-aws.example.com")
	rootAWS, err := checkoutRoot("s3://bucket/p")
	if err != nil {
		t.Fatal(err)
	}
	if rootAWS == rootMinio {
		t.Fatalf("the same spec string under different OFFSHOOT_S3_ENDPOINT values must NOT share a checkout root, got %q for both", rootMinio)
	}
}

func TestCorruptSnapshotFailsClosedEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	r, _, _ := w.Store.GetRef("app", "main")
	key := store.SnapshotKey(r.Lineage, r.Epoch, 1)
	data, _, _ := w.Store.B.Get(key)
	data[len(data)/2] ^= 0xFF
	w.Store.B.Put(key, data)
	if _, err := w.Checkout("app", "main"); err == nil {
		t.Fatal("checkout of corrupt snapshot must fail closed")
	}
}

func TestMaterializeUsesCheckpointEpochNotRefEpoch(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	// Simulate a later epoch bump: the ref advances, but the objects written
	// under the old epoch stay exactly where they are.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.Epoch++
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	// Checkout and fork --at must both still find the old-epoch objects.
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout after epoch bump: %v", err)
	}
	got, _ := exec.Command("sqlite3", p2, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("content after epoch bump: %q", got)
	}
	if _, err := w.Fork("app", "main", "child", "v1", 0); err != nil {
		t.Fatalf("fork --at across an epoch bump: %v", err)
	}
}

// TestCheckpointAfterEpochBumpIsMaterializable guards against a checkpoint
// taken after an epoch bump leaving the ref's HeadEpoch stale. Checkpoint
// advances HeadTXID and records the new checkpoint at ref.Epoch (the
// current epoch), but if it doesn't also advance ref.HeadEpoch to match,
// headCheckpoint(ref) — used by Checkout — points at (stale epoch, new
// txid): an object that was never written. A fresh Checkout would then
// fail "not found" even though the checkpoint just succeeded.
func TestCheckpointAfterEpochBumpIsMaterializable(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	// Simulate an epoch bump (e.g. a lease reclaim): the ref advances, but
	// objects written under the old epoch stay exactly where they are.
	// This mirrors TestMaterializeUsesCheckpointEpochNotRefEpoch.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.Epoch++
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	// Write more data under the new epoch and take a second checkpoint.
	if out, err := exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2"); err != nil {
		t.Fatal(err)
	}

	// A fresh checkout must succeed and contain the v2 data — this fails
	// with "not found" if HeadEpoch was left stale at the pre-bump epoch.
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout after checkpoint following epoch bump: %v", err)
	}
	got, err := exec.Command("sqlite3", p2, "SELECT v FROM t ORDER BY v;").Output()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if string(got) != "1\n2\n" {
		t.Fatalf("content after checkpoint following epoch bump: %q", got)
	}

	// The pre-bump checkpoint must still be reachable: its object lives
	// under the old epoch and was never touched by the bump or by v2.
	if _, err := w.Fork("app", "main", "child", "v1", 0); err != nil {
		t.Fatalf("fork --at v1 (pre-bump checkpoint) after later checkpoint: %v", err)
	}
}

// TestOpsRejectEscapingNames pins the fix for the MCP-facing path-traversal
// finding: Checkout, Checkpoint, Rollback, Promote, and Destroy must reject
// every db/branch/checkpoint-name argument that fails store.ValidateName
// BEFORE touching the filesystem, exactly as ops.ParseTarget already gates
// the CLI. Without this, an agent-supplied name (the MCP tools pass
// database/branch straight from JSON, with no ParseTarget in between) flows
// straight into Workspace.CheckoutPath's bare filepath.Join ahead of a real
// os.MkdirAll/materialize/os.Remove.
func TestOpsRejectEscapingNames(t *testing.T) {
	badNames := []string{"../x", "a/b", "..", ".", "UPPER", ""}

	setup := func(t *testing.T) *Workspace {
		t.Helper()
		w := newWS(t)
		if err := w.Create("app"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Checkout("app", "main"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
			t.Fatal(err)
		}
		return w
	}

	assertRejected := func(t *testing.T, err error, label string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: want a validation error, got nil", label)
		}
		if !strings.Contains(err.Error(), "invalid name") {
			t.Fatalf("%s: want a store.ValidateName rejection, got: %v", label, err)
		}
	}

	for _, bad := range badNames {
		label := bad
		if label == "" {
			label = "<empty>"
		}
		label = strings.ReplaceAll(label, "/", "_")

		t.Run("Checkout/db="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Checkout(bad, "main")
			assertRejected(t, err, "Checkout db")
		})
		t.Run("Checkout/branch="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Checkout("app", bad)
			assertRejected(t, err, "Checkout branch")
		})

		t.Run("Checkpoint/db="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Checkpoint(bad, "main", "v2")
			assertRejected(t, err, "Checkpoint db")
		})
		t.Run("Checkpoint/branch="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Checkpoint("app", bad, "v2")
			assertRejected(t, err, "Checkpoint branch")
		})
		t.Run("Checkpoint/name="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Checkpoint("app", "main", bad)
			assertRejected(t, err, "Checkpoint name")
		})

		t.Run("Rollback/db="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Rollback(bad, "main", "v1")
			assertRejected(t, err, "Rollback db")
		})
		t.Run("Rollback/branch="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Rollback("app", bad, "v1")
			assertRejected(t, err, "Rollback branch")
		})
		t.Run("Rollback/to="+label, func(t *testing.T) {
			w := setup(t)
			_, err := w.Rollback("app", "main", bad)
			assertRejected(t, err, "Rollback to")
		})

		t.Run("Promote/db="+label, func(t *testing.T) {
			w := setup(t)
			if err := w.Create("target"); err != nil {
				t.Fatal(err)
			}
			_, err := w.Promote(bad, "main", "target", true)
			assertRejected(t, err, "Promote db")
		})

		t.Run("Destroy/db="+label, func(t *testing.T) {
			w := setup(t)
			err := w.Destroy(bad, "main", true)
			assertRejected(t, err, "Destroy db")
		})
		t.Run("Destroy/branch="+label, func(t *testing.T) {
			w := setup(t)
			err := w.Destroy("app", bad, true)
			assertRejected(t, err, "Destroy branch")
		})
	}
}

// TestCheckoutSkipsRematerializeWhenCleanAndCurrent pins the optimization
// that Checkout must not re-materialize (temp file + rename over the fixed
// checkout path) when the existing checkout is already byte-correct: clean
// (matches its .sum sidecar fingerprint) AND current (the sidecar's recorded
// lineage+txid equals the ref's CURRENT head, which checkoutState already
// compares against — see checkoutState's doc comment). materializeChainAt
// (internal/ltxio.MaterializeChain) always writes via os.CreateTemp + rename,
// so a skipped re-materialization is observable as the checkout path keeping
// the SAME inode across calls; a re-materialized one gets a fresh inode.
func TestCheckoutSkipsRematerializeWhenCleanAndCurrent(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	p1, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st1, err := os.Stat(p1)
	if err != nil {
		t.Fatal(err)
	}
	// The checkout is clean and current immediately after materializing: a
	// second Checkout must return the SAME file, not rebuild it.
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st2, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(st1, st2) {
		t.Fatal("a clean, current checkout must not be re-materialized (same inode expected)")
	}

	// A checkpoint advances the head; Checkpoint itself refreshes the .sum
	// sidecar to the new (lineage, txid) in place (see writeSum in
	// Checkpoint), so the checkout is STILL clean and current at the new
	// head without Checkout doing anything — still no re-materialize.
	mustExecSQL(t, p2, "CREATE TABLE t (v);")
	if _, err := w.Checkpoint("app", "main", "cp1"); err != nil {
		t.Fatal(err)
	}
	p3, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st3, err := os.Stat(p3)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(st2, st3) {
		t.Fatal("checkpoint leaves the checkout clean at the new head; still no re-materialize")
	}

	// An unverifiable checkout (sidecar present but not a valid current-format
	// record) must fall back to checkoutState == "unknown" and re-materialize
	// rather than risk skipping over something it can't prove is clean.
	// checkoutState treats {} as unknown: it decodes fine but rec.Hash == "".
	sum := p3 + ".sum"
	if _, err := os.Stat(sum); err != nil {
		t.Fatalf("expected a .sum sidecar at %s: %v", sum, err)
	}
	if err := os.WriteFile(sum, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	p4, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st4, err := os.Stat(p4)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(st3, st4) {
		t.Fatal("an unverifiable checkout (bad sidecar) must be re-materialized")
	}

	// A modified checkout (un-checkpointed local edits) must still warn and
	// be overwritten, exactly as before this change — the "clean" fast path
	// must not swallow the "modified" case.
	mustExecSQL(t, p4, "INSERT INTO t VALUES (1);")
	var p5 string
	stderr := captureStderr(t, func() {
		p5, err = w.Checkout("app", "main")
		if err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(stderr, "un-checkpointed changes") {
		t.Fatalf("warning missing expected text: %q", stderr)
	}
	st5, err := os.Stat(p5)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(st4, st5) {
		t.Fatal("a modified checkout must still be re-materialized (and warned about)")
	}
}

// TestCheckoutRematerializesOnEpochMismatchOrOldFormatSidecar guards the gap
// flagged in review of the clean-fast-path change above: checkoutState's
// identity check must compare Epoch, not just (Lineage, TXID). Chain
// resolution (store.keepHighestEpoch) exists precisely because a fenced-out
// writer's orphaned object can share a TXID with the live object under a
// different epoch, so lineage+txid alone does not prove two checkouts hold
// the same committed bytes. A sidecar with the right Lineage+TXID but a
// stale Epoch must read as "stale" (rebuild, not skip); so must an
// OLD-FORMAT sidecar that predates the Epoch field entirely (decodes Epoch
// to its zero value) — it must not be trusted as clean just because a real
// ref's HeadEpoch is never 0.
func TestCheckoutRematerializesOnEpochMismatchOrOldFormatSidecar(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	p1, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st1, err := os.Stat(p1)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.HeadEpoch == 0 {
		t.Fatal("test assumption violated: a real ref's HeadEpoch must be >= 1 (see decodeRef)")
	}
	hash, err := fileSum(p1)
	if err != nil {
		t.Fatal(err)
	}

	// Right lineage, right txid, WRONG epoch: checkoutState must call this
	// "stale" (not "clean"), and Checkout must rebuild rather than skip.
	staleEpoch := fmt.Sprintf(`{"hash":%q,"lineage":%q,"epoch":%d,"txid":%d}`,
		hash, ref.Lineage, ref.HeadEpoch+1, ref.HeadTXID)
	if err := os.WriteFile(p1+".sum", []byte(staleEpoch), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := checkoutState(p1, ref); got != "stale" {
		t.Fatalf("checkoutState with mismatched epoch = %q, want %q", got, "stale")
	}
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st2, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(st1, st2) {
		t.Fatal("a sidecar with a stale epoch (right lineage+txid) must be re-materialized, not skipped")
	}

	// Old-format sidecar: no "epoch" field at all, as any sidecar written
	// before this fix would be. Decodes Epoch to its zero value, which must
	// NOT be treated as matching a real ref's HeadEpoch (always >= 1) —
	// that's the safe direction (rebuild) rather than the unsafe one (trust
	// a pre-epoch-tracking sidecar as clean).
	ref2, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	hash2, err := fileSum(p2)
	if err != nil {
		t.Fatal(err)
	}
	oldFormat := fmt.Sprintf(`{"hash":%q,"lineage":%q,"txid":%d}`, hash2, ref2.Lineage, ref2.HeadTXID)
	if err := os.WriteFile(p2+".sum", []byte(oldFormat), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := checkoutState(p2, ref2); got != "stale" {
		t.Fatalf("checkoutState for an old-format (no epoch) sidecar = %q, want %q", got, "stale")
	}
	p3, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	st3, err := os.Stat(p3)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(st2, st3) {
		t.Fatal("an old-format sidecar (no epoch field) must be re-materialized, not skipped")
	}
}
