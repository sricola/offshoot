package mcp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/daemon"
	"github.com/offshoot-db/offshoot/internal/ops"
)

// newDaemonTools starts a real daemon server (daemon.NewServer, the exact
// type `offshoot serve` runs) on a temp socket, bound to a fresh workspace
// with one db "app", and returns an OffshootTools wired to that same
// socket. This is the harness-opens-a-session setup the daemon-aware
// handlers (checkpoint/fork/checkout) are meant to ride — see
// task-4-brief.md: internal/daemon's own newServer (server_test.go) is
// unexported and unreachable from this package, so this rebuilds the same
// shape from daemon's exported surface (NewServer/Serve/Shutdown).
func newDaemonTools(t *testing.T) (ts *OffshootTools, w *ops.Workspace, sock string) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	spec := filepath.Join(dir, "store")
	w, err := ops.Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// The socket lives in its own short-named temp dir, independent of
	// t.TempDir() (which nests under the test's full name): unix socket
	// paths are capped at ~104-108 bytes on several platforms, and a
	// t.TempDir() path can exceed that — see
	// internal/daemon/server_test.go's newServer for the same workaround.
	sockDir, err := os.MkdirTemp("", "offshoot-mcp-daemon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock = filepath.Join(sockDir, "sock")
	srv, err := daemon.NewServer(w, sock)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	ts = NewOffshootTools(w, spec, 0, sock)
	return ts, w, sock
}

// openSession opens a daemon session on db@branch directly against the
// socket (standing in for whatever harness opened it — the Python/TS SDK,
// `offshoot session open`, or a custom agent loop; no MCP tool opens
// sessions itself) and returns its live checkout path.
func openSession(t *testing.T, sock, db, branch string) string {
	t.Helper()
	resp, err := daemon.Call(sock, daemon.Request{Op: "open", DB: db, Branch: branch})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Checkout == "" {
		t.Fatalf("open %s@%s = %+v", db, branch, resp)
	}
	return resp.Checkout
}

// sqliteExec runs sql against the SQLite file at path via the sqlite3 CLI,
// failing the test with the command's combined output on error.
func sqliteExec(t *testing.T, path, sql string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path, sql).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

// sqliteCount runs a scalar-returning query against path via the sqlite3
// CLI and parses the single-line result as an int, failing the test with
// the command's combined output on error.
func sqliteCount(t *testing.T, path, query string) int {
	t.Helper()
	out, err := exec.Command("sqlite3", path, query).CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	n, convErr := strconv.Atoi(strings.TrimSpace(string(out)))
	if convErr != nil {
		t.Fatalf("sqlite3 output %q: %v", out, convErr)
	}
	return n
}

// TestCheckpointCapturesUnflushedWriteViaDaemon proves offshoot_checkpoint
// rides an open daemon session: a write made directly against the
// session's live checkout (never explicitly flushed over the socket) must
// still be durable after the MCP checkpoint tool runs, because that tool
// routes to the daemon's "flush" op rather than the at-rest snapshot path.
// Verification reads the checkpoint back through a completely separate
// materialization (fork "verify" at the checkpoint, then checkout it) so
// it never touches main's live checkout file — ops.Workspace.Checkpoint's
// own doc comment warns that path is unsafe against a live in-process
// session, exactly the hazard this test must not reintroduce.
func TestCheckpointCapturesUnflushedWriteViaDaemon(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	checkoutPath := openSession(t, sock, "app", "main")

	sqliteExec(t, checkoutPath, "CREATE TABLE t (v); INSERT INTO t VALUES ('unflushed');")

	r := call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "v1"})
	if r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}

	if _, err := w.Fork("app", "main", "verify", "v1", 0); err != nil {
		t.Fatal(err)
	}
	verifyPath, err := w.Checkout("app", "verify")
	if err != nil {
		t.Fatal(err)
	}
	if n := sqliteCount(t, verifyPath, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("checkpoint v1 row count = %d, want 1 (the unflushed write)", n)
	}
}

// TestForkFromOpenSessionViaDaemon proves offshoot_fork rides an open
// daemon session too: forking main while a write sits unflushed in its
// live session must still carry that write into the new branch, since the
// daemon's fork op flushes an open source first.
func TestForkFromOpenSessionViaDaemon(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	checkoutPath := openSession(t, sock, "app", "main")

	sqliteExec(t, checkoutPath, "CREATE TABLE t (v); INSERT INTO t VALUES ('unflushed');")

	r := call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-1"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}

	path, err := w.Checkout("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if n := sqliteCount(t, path, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("forked branch row count = %d, want 1 (unflushed write via daemon flush-then-fork)", n)
	}
}

// TestCheckoutReturnsSessionPathWhileOpen proves offshoot_checkout, called
// against a branch with an open daemon session, reports that SESSION's
// live checkout path rather than materializing a separate at-rest copy.
func TestCheckoutReturnsSessionPathWhileOpen(t *testing.T) {
	ts, _, sock := newDaemonTools(t)
	checkoutPath := openSession(t, sock, "app", "main")

	r := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if r.IsError {
		t.Fatalf("checkout: %s", text(r))
	}
	got := strings.TrimSpace(lastPath(text(r)))
	if got != checkoutPath {
		t.Fatalf("checkout while a session is open = %q, want the session's live path %q\nfull message: %s",
			got, checkoutPath, text(r))
	}
}

// TestNoDaemonBehavesExactlyAtRest spot-checks checkpoint and fork against
// the package's existing at-rest harness (newTools, shared with
// tools_test.go): its socket points at nothing, so every tool call's
// per-call daemon probe must fail fast and silently fall back to exactly
// today's at-rest behavior — the same behavior the rest of tools_test.go
// already pins, now proven to survive a nonexistent daemon rather than
// merely the absence of daemon-aware code.
func TestNoDaemonBehavesExactlyAtRest(t *testing.T) {
	ts, w := newTools(t)

	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if co.IsError {
		t.Fatalf("checkout: %s", text(co))
	}
	path := strings.TrimSpace(lastPath(text(co)))
	sqliteExec(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")

	if r := call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "v1"}); r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}
	if r := call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-1"}); r.IsError {
		t.Fatalf("fork: %s", text(r))
	}

	fpath, err := w.Checkout("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if n := sqliteCount(t, fpath, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("forked branch (no daemon) row count = %d, want 1", n)
	}
}
