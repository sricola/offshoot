package daemon

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// dRowCount runs a row-count query against a plain SQLite file via the
// sqlite3 CLI — used here (rather than a Go driver) so this file has no new
// import beyond what server_test.go's own sqlite3-CLI-driven tests already
// require (newServer skips cleanly when sqlite3 isn't on PATH).
func dRowCount(t *testing.T, dbPath string) int {
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

// TestOpExportRefusesRelativePath pins the daemon-specific trust-model
// guard documented on Request.Path and opExport: a relative path is refused
// outright, never resolved against the daemon's own (client-invisible)
// working directory.
func TestOpExportRefusesRelativePath(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	r := call(t, sock, Request{Op: "export", DB: "app", Branch: "main", Path: "relative-out.db"})
	if r.OK {
		t.Fatal("export with a relative path must be refused")
	}
}

// TestOpExportRefusesOverwriteWithoutForceThenForceSucceeds exercises the
// daemon's export op end to end against a live checkout: the same
// refuse-then-force semantics ops.Workspace.Export itself already has
// unit-level coverage for (internal/ops/export_ops_test.go), here proven
// over the wire.
func TestOpExportRefusesOverwriteWithoutForceThenForceSucceeds(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

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
	r := call(t, sock, Request{Op: "export", DB: "app", Branch: "main", Path: dst})
	if !r.OK {
		t.Fatalf("export failed: %+v", r)
	}
	if got := dRowCount(t, dst); got != 1 {
		t.Fatalf("exported rows = %d, want 1", got)
	}

	r = call(t, sock, Request{Op: "export", DB: "app", Branch: "main", Path: dst})
	if r.OK {
		t.Fatal("export must refuse to overwrite an existing destination without force")
	}

	r = call(t, sock, Request{Op: "export", DB: "app", Branch: "main", Path: dst, Force: true})
	if !r.OK {
		t.Fatalf("export with force failed: %+v", r)
	}
}

// TestOpExportMissesUnflushedSessionWrites is the load-bearing assertion
// from the Milestone 3 Task 2 brief: export reads the STORE (the branch's
// last durable chain), never the live checkout, so a write an open session
// has made but never flushed is NOT in the export. This proves it directly
// — open a session, write through its checkout, export WITHOUT flushing,
// and assert the exported row count reflects only the pre-write (flushed)
// state, not the live write.
func TestOpExportMissesUnflushedSessionWrites(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	// Durable baseline before the session ever opens: one row, checkpointed.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('durable');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open failed: %+v", open)
	}
	t.Cleanup(func() { call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}) })

	// Write through the LIVE session's checkout, deliberately never flushed.
	if out, err := exec.Command("sqlite3", open.Checkout,
		"INSERT INTO t VALUES ('unflushed');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	dst := filepath.Join(t.TempDir(), "no-unflushed.db")
	r := call(t, sock, Request{Op: "export", DB: "app", Branch: "main", Path: dst})
	if !r.OK {
		t.Fatalf("export failed: %+v", r)
	}
	if got := dRowCount(t, dst); got != 1 {
		t.Fatalf("export while a session held an unflushed write: rows = %d, want 1 "+
			"(the unflushed row must not appear — export reads the store, not the checkout)", got)
	}

	// Now flush, and prove the SAME export op picks the write up once it's
	// actually durable — confirming the miss above was about durability,
	// not some unrelated bug swallowing rows.
	flush := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
	if !flush.OK {
		t.Fatalf("flush failed: %+v", flush)
	}
	dst2 := filepath.Join(t.TempDir(), "after-flush.db")
	r = call(t, sock, Request{Op: "export", DB: "app", Branch: "main", Path: dst2})
	if !r.OK {
		t.Fatalf("export after flush failed: %+v", r)
	}
	if got := dRowCount(t, dst2); got != 2 {
		t.Fatalf("export after flush: rows = %d, want 2", got)
	}
}

// TestOpCheckoutAtMaterializesSeparateReadOnlyPath exercises the daemon's
// checkout-at op end to end: it must materialize a historical checkpoint
// into a path distinct from the branch's writable checkout, and leave that
// writable checkout untouched.
func TestOpCheckoutAtMaterializesSeparateReadOnlyPath(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

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

	r := call(t, sock, Request{Op: "checkout-at", DB: "app", Branch: "main", Name: "v1"})
	if !r.OK {
		t.Fatalf("checkout-at failed: %+v", r)
	}
	if r.Checkout == "" {
		t.Fatal("checkout-at must return a path")
	}
	if r.Checkout == path {
		t.Fatal("checkout-at's path must never equal the writable checkout path")
	}
	if got := dRowCount(t, r.Checkout); got != 1 {
		t.Fatalf("checkout-at v1 rows = %d, want 1", got)
	}
	fi, err := os.Stat(r.Checkout)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o444 {
		t.Fatalf("checkout-at file perm = %o, want 0444", perm)
	}
	// The writable checkout (still head, v2) must be untouched.
	if got := dRowCount(t, path); got != 2 {
		t.Fatalf("writable checkout rows = %d, want 2 (checkout-at must not touch it)", got)
	}
}

// TestOpCheckoutAtSafeAlongsideOpenSessionOnSameBranch proves checkout-at
// needs no refuseIfClaimed guard: it runs successfully even with a live
// session open on the exact same branch, unlike opCheckoutAtRest/
// opRollback/opPromote, which all refuse in that situation.
func TestOpCheckoutAtSafeAlongsideOpenSessionOnSameBranch(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

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

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open failed: %+v", open)
	}
	t.Cleanup(func() { call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}) })

	r := call(t, sock, Request{Op: "checkout-at", DB: "app", Branch: "main", Name: "v1"})
	if !r.OK {
		t.Fatalf("checkout-at alongside an open session must succeed: %+v", r)
	}
}

// TestOpCheckoutAtRequiresAName pins that checkout-at, unlike export, has
// no "head" alias — an empty Name must be refused.
func TestOpCheckoutAtRequiresAName(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	r := call(t, sock, Request{Op: "checkout-at", DB: "app", Branch: "main"})
	if r.OK {
		t.Fatal("checkout-at with no checkpoint name must be refused")
	}
}

// TestOpCheckoutAtRejectsPathTraversalCheckpoint pins the CRITICAL fix over
// the wire: the daemon's checkout-at op forwards req.Name straight to
// ops.Workspace.CheckoutAt, which must refuse a '/'- or '..'-containing
// value before it ever reaches CheckoutAtPath's filepath.Join — otherwise a
// crafted Name could resolve outside checkouts-ro, including onto the
// branch's own writable checkout path.
func TestOpCheckoutAtRejectsPathTraversalCheckpoint(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('writable');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	for _, cp := range []string{"../../../etc/passwd", "..", "a/b", "../../checkouts/app/main"} {
		r := call(t, sock, Request{Op: "checkout-at", DB: "app", Branch: "main", Name: cp})
		if r.OK {
			t.Fatalf("checkout-at with name %q must be refused (path traversal), got checkout=%q", cp, r.Checkout)
		}
	}

	// The writable checkout must be untouched: still there, still writable.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o200 == 0 {
		t.Fatal("the writable checkout must still be writable")
	}
	if got := dRowCount(t, path); got != 1 {
		t.Fatalf("writable checkout rows = %d, want 1", got)
	}
}
