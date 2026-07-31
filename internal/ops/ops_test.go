package ops

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/store"
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
	if err != nil || !r.Protected || r.Checkpoints["init"] != 1 || r.HeadTXID != 1 {
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

	txid, err := w.Fork("app", "main", "attempt-1", "")
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
		cref.Checkpoints["fork"] != 2 || len(cref.Checkpoints) != 1 {
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

	if _, err := w.Fork("app", "main", "old", "v1"); err != nil {
		t.Fatal(err)
	}
	p, _ := w.Checkout("app", "old")
	got, _ := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("fork --at v1 content: %q", got)
	}
	if _, err := w.Fork("app", "main", "bad", "nope"); err == nil {
		t.Fatal("unknown checkpoint must fail")
	}
	if _, err := w.Fork("app", "main", "old", ""); err == nil {
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
		if _, err := w.Fork("app", "main", "clean", ""); err != nil {
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
		txid, err = w.Fork("app", "main", "dirty", "")
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
