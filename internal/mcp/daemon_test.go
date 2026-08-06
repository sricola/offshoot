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
//
// defaultTTL is variadic, mirroring newTools in tools_test.go: no argument
// means 0 (agent forks have no TTL unless a call passes one), a single
// argument sets OffshootTools' configured default.
func newDaemonTools(t *testing.T, defaultTTL ...time.Duration) (ts *OffshootTools, w *ops.Workspace, sock string) {
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
	var ttl time.Duration
	if len(defaultTTL) > 0 {
		ttl = defaultTTL[0]
	}
	ts = NewOffshootTools(w, spec, ttl, sock)
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

// TestCheckpointWithDaemonUpButNoSessionIsAtRest pins the brief's other
// negative case, distinct from TestNoDaemonBehavesExactlyAtRest: a REAL
// daemon is reachable (daemonStatus's up=true), but nothing has opened a
// session on this specific branch, so openSession must report ok=false and
// checkpoint must fall to the plain at-rest snapshot — not silently
// "succeed" via some path that only happens to look right because no
// daemon was involved at all.
func TestCheckpointWithDaemonUpButNoSessionIsAtRest(t *testing.T) {
	ts, _, _ := newDaemonTools(t)

	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if co.IsError {
		t.Fatalf("checkout: %s", text(co))
	}
	path := strings.TrimSpace(lastPath(text(co)))
	sqliteExec(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES ('x');")

	r := call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "name": "v1"})
	if r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}
	if strings.Contains(text(r), "captured live") {
		t.Fatalf("checkpoint with a daemon up but no open session must not claim a live "+
			"daemon capture: %s", text(r))
	}
}

// TestForkTTLAppliesThroughDaemonWithDefault proves the daemon-routed fork
// path (tools.go's fork, when daemonStatus reports up) still carries
// OffshootTools' configured defaultTTL — not just that the fork succeeds.
// Deleting the `req.TTL = ttl.String()` line on that path would leave every
// pre-existing test green (none of them asserted on TTL through the
// daemon), silently making every agent fork immortal whenever a daemon
// happens to be running; this asserts both the response text (what an
// agent actually sees in its transcript) and the ref written to the store
// (what's actually durable).
func TestForkTTLAppliesThroughDaemonWithDefault(t *testing.T) {
	ts, w, _ := newDaemonTools(t, time.Hour)

	r := call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-1"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	got := text(r)
	if !strings.Contains(got, "ttl=1h") || !strings.Contains(got, "expires_at=") {
		t.Fatalf("fork through the daemon with a configured defaultTTL must echo ttl+expiry: %s", got)
	}
	ref, _, err := w.Store.GetRef("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if ref.TTL == "" {
		t.Fatalf("forked branch ref has no TTL, want the default applied through the daemon: %+v", ref)
	}
}

// TestForkTTLAppliesThroughDaemonWithExplicitArg is
// TestForkTTLAppliesThroughDaemonWithDefault's sibling for an explicit
// `ttl` call argument (no configured default), covering the other input to
// resolveForkTTL on the same daemon-routed path.
func TestForkTTLAppliesThroughDaemonWithExplicitArg(t *testing.T) {
	ts, w, _ := newDaemonTools(t)

	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1", "ttl": "2h"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	got := text(r)
	if !strings.Contains(got, "ttl=2h") || !strings.Contains(got, "expires_at=") {
		t.Fatalf("fork through the daemon with an explicit ttl must echo it: %s", got)
	}
	ref, _, err := w.Store.GetRef("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if ref.TTL == "" {
		t.Fatalf("forked branch ref has no TTL, want the explicit ttl applied through the daemon: %+v", ref)
	}
}

// TestCheckoutSkipsFencedSession proves offshoot_checkout does not hand
// over a fenced (lease-lost) session's checkout path: that session stays
// listed in the daemon's "status" response (SessionInfo.Error set, per
// server.go's opStatus) even though it has stopped capturing, so naively
// trusting "a session is listed" would promise continuous capture from a
// session that's actually dead. The lease is stolen the same way
// internal/session's own fencing tests do it (see
// internal/session/flush_test.go's TestFlushAfterFencingIsRefused): back-
// date the ref's LeaseExpiry so a fresh AcquireLease can reclaim it without
// waiting out the real 30s default LeaseTTL, then force the daemon's live
// session to notice synchronously (a "flush" op checks the ref's
// lease/epoch against what it acquired, rather than waiting on the
// session's own background renewal loop).
func TestCheckoutSkipsFencedSession(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	openSession(t, sock, "app", "main")

	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.LeaseExpiry = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	if resp, err := daemon.Call(sock, daemon.Request{Op: "flush", DB: "app", Branch: "main"}); err == nil {
		t.Fatalf("flush after lease theft must fail (fencing), got %+v", resp)
	}

	st, err := daemon.Call(sock, daemon.Request{Op: "status"})
	if err != nil || !st.OK {
		t.Fatalf("status: err=%v resp=%+v", err, st)
	}
	var found bool
	for _, s := range st.Sessions {
		if s.DB == "app" && s.Branch == "main" {
			found = true
			if s.Error == "" {
				t.Fatalf("session should be fenced (Error set) after lease theft: %+v", s)
			}
		}
	}
	if !found {
		t.Fatalf("a fenced session must still be listed in status (see opStatus): %+v", st.Sessions)
	}

	r := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if r.IsError {
		t.Fatalf("checkout: %s", text(r))
	}
	got := text(r)
	if strings.Contains(got, "live checkout") {
		t.Fatalf("checkout must not hand over a fenced session's path: %s", got)
	}
	if !strings.Contains(got, "unhealthy") || !strings.Contains(got, "warning") {
		t.Fatalf("checkout must warn about the fenced session, naming its error: %s", got)
	}
}

// TestRollbackRefusesWhenSessionOpen proves offshoot_rollback refuses
// against a branch with an open daemon session rather than repointing it
// out from under the session (see refuseIfSessionOpen) — MCP calls
// ops.Workspace.Rollback directly, bypassing the daemon's own equivalent
// guard entirely, so without this fix a rollback here would silently fence
// a session the daemon still believes owns the branch.
func TestRollbackRefusesWhenSessionOpen(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}
	openSession(t, sock, "app", "attempt-1")

	if r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "branch": "attempt-1", "name": "v1"}); r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}

	r := call(t, ts, "offshoot_rollback", map[string]any{
		"database": "app", "branch": "attempt-1", "to": "v1"})
	if !r.IsError {
		t.Fatalf("rollback against an open daemon session must be refused, got: %s", text(r))
	}
	if !strings.Contains(text(r), "attempt-1") {
		t.Fatalf("refusal should name the branch: %s", text(r))
	}
}

// TestRollbackProceedsAtRestWhenNoSessionOpen is
// TestRollbackRefusesWhenSessionOpen's negative case: a daemon is up, but
// nothing has opened a session on this branch, so rollback must proceed at
// rest exactly as it always has.
func TestRollbackProceedsAtRestWhenNoSessionOpen(t *testing.T) {
	ts, w, _ := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkout("app", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "branch": "attempt-1", "name": "v1"}); r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}

	r := call(t, ts, "offshoot_rollback", map[string]any{
		"database": "app", "branch": "attempt-1", "to": "v1"})
	if r.IsError {
		t.Fatalf("rollback with daemon up but no open session must proceed at rest: %s", text(r))
	}
}

// TestDestroyRefusesWhenSessionOpen mirrors
// TestRollbackRefusesWhenSessionOpen for offshoot_destroy.
func TestDestroyRefusesWhenSessionOpen(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}
	openSession(t, sock, "app", "attempt-1")

	r := call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "attempt-1"})
	if !r.IsError {
		t.Fatalf("destroy against an open daemon session must be refused, got: %s", text(r))
	}
	if !strings.Contains(text(r), "attempt-1") {
		t.Fatalf("refusal should name the branch: %s", text(r))
	}
}

// TestDestroyProceedsAtRestWhenNoSessionOpen mirrors
// TestRollbackProceedsAtRestWhenNoSessionOpen for offshoot_destroy.
func TestDestroyProceedsAtRestWhenNoSessionOpen(t *testing.T) {
	ts, w, _ := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}

	r := call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "attempt-1"})
	if r.IsError {
		t.Fatalf("destroy with daemon up but no open session must proceed at rest: %s", text(r))
	}
}

// TestPromoteRefusesWhenTargetSessionOpen mirrors
// TestRollbackRefusesWhenSessionOpen for offshoot_promote, guarding the
// TARGET specifically (see promote's doc comment for why the source isn't
// guarded the same way).
func TestPromoteRefusesWhenTargetSessionOpen(t *testing.T) {
	ts, w, sock := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "attempt-2", "", 0); err != nil {
		t.Fatal(err)
	}
	openSession(t, sock, "app", "attempt-2") // the promote TARGET

	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "attempt-2"})
	if !r.IsError {
		t.Fatalf("promote onto an open-session target must be refused, got: %s", text(r))
	}
	if !strings.Contains(text(r), "attempt-2") {
		t.Fatalf("refusal should name the target: %s", text(r))
	}
}

// TestPromoteProceedsAtRestWhenNoSessionOpen mirrors
// TestRollbackProceedsAtRestWhenNoSessionOpen for offshoot_promote.
func TestPromoteProceedsAtRestWhenNoSessionOpen(t *testing.T) {
	ts, w, _ := newDaemonTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "attempt-2", "", 0); err != nil {
		t.Fatal(err)
	}

	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "attempt-2"})
	if r.IsError {
		t.Fatalf("promote with daemon up but no open session must proceed at rest: %s", text(r))
	}
}
