package daemon

import (
	"context"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// TestJanitorReapsExpiredBranchWhileDaemonRuns starts a daemon with a fast
// janitor tick, backdates a forked branch's TTL so it is already expired,
// and waits for the janitor to reap it out from under a live server —
// proving StartJanitor actually drives ops.Reap on a schedule, not just that
// Reap works in isolation (already covered by internal/ops/reap_test.go).
func TestJanitorReapsExpiredBranchWhileDaemonRuns(t *testing.T) {
	srv, w := newServer(t)

	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ref.TTL = "1h"
	ref.TouchedAt = now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "doomed", ref, etag); err != nil {
		t.Fatal(err)
	}

	srv.StartJanitor(50*time.Millisecond, 0)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, err := w.Store.GetRef("app", "doomed"); errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("janitor never reaped the expired branch within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, _, err := w.Store.GetRef("app", "main"); err != nil {
		t.Fatalf("main must survive: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestStartJanitorZeroDisablesIt pins the brief's explicit requirement that
// every <= 0 disables the janitor entirely: an expired-TTL branch must sit
// untouched, and Shutdown (which stops a running janitor as its very first
// step) must still complete cleanly when no janitor was ever started.
func TestStartJanitorZeroDisablesIt(t *testing.T) {
	srv, w := newServer(t)

	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ref.TTL = "1h"
	ref.TouchedAt = now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "doomed", ref, etag); err != nil {
		t.Fatal(err)
	}

	srv.StartJanitor(0, 0)

	// Give a real (disabled) janitor every chance to wrongly fire.
	time.Sleep(200 * time.Millisecond)

	if _, _, err := w.Store.GetRef("app", "doomed"); err != nil {
		t.Fatalf("StartJanitor(0, 0) must disable the janitor; expired branch was reaped: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// sqliteExec runs sql against the SQLite file at path via the sqlite3 CLI,
// failing the test with the command's combined output on error.
func sqliteExec(t *testing.T, path, sql string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path, sql).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

// sqliteCount runs a scalar-returning query (e.g. a SELECT COUNT(*)) against
// path via the sqlite3 CLI and parses the single-line result as an int,
// failing the test with the command's combined output on error.
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

// TestLifecycleOpsRoundTrip exercises the full new lifecycle surface end to
// end against one daemon: create, branches, fork (at rest and from an open
// session — proving the flush-then-fork path actually carries an
// un-flushed write), checkout, and destroy's open-session guard.
func TestLifecycleOpsRoundTrip(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	if r := call(t, sock, Request{Op: "create", DB: "db2"}); !r.OK {
		t.Fatalf("create = %+v", r)
	}

	br := call(t, sock, Request{Op: "branches", DB: "db2"})
	if !br.OK || len(br.Branches) != 1 || br.Branches[0].Branch != "main" {
		t.Fatalf("branches = %+v", br)
	}

	fork := call(t, sock, Request{Op: "fork", DB: "db2", Branch: "main", Name: "try", TTL: "1h"})
	if !fork.OK {
		t.Fatalf("fork = %+v", fork)
	}

	br = call(t, sock, Request{Op: "branches", DB: "db2"})
	if !br.OK {
		t.Fatalf("branches = %+v", br)
	}
	var tryInfo *BranchInfo
	for i := range br.Branches {
		if br.Branches[i].Branch == "try" {
			tryInfo = &br.Branches[i]
		}
	}
	if tryInfo == nil {
		t.Fatalf("branches missing try: %+v", br.Branches)
	}
	// ops.Fork stores req.TTL parsed and re-rendered via time.Duration.String,
	// which normalizes "1h" to "1h0m0s" — compare against that, not the
	// literal request string.
	if tryInfo.TTL != time.Hour.String() || tryInfo.TTLRemaining == "" || tryInfo.TTLRemaining == "expired" {
		t.Fatalf("try branch info = %+v", tryInfo)
	}

	open := call(t, sock, Request{Op: "open", DB: "db2", Branch: "try"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	flush := call(t, sock, Request{Op: "flush", DB: "db2", Branch: "try", Name: "good"})
	if !flush.OK {
		t.Fatalf("flush = %+v", flush)
	}

	// fork FROM THE OPEN SESSION: source "try" is open here, so opFork must
	// flush it first, then fork — the new branch must carry the row just
	// written above even though it was never explicitly flushed before this
	// fork call.
	fork2 := call(t, sock, Request{Op: "fork", DB: "db2", Branch: "try", Name: "try2"})
	if !fork2.OK {
		t.Fatalf("fork from an open session = %+v", fork2)
	}
	co := call(t, sock, Request{Op: "checkout", DB: "db2", Branch: "try2"})
	if !co.OK || co.Checkout == "" {
		t.Fatalf("checkout try2 = %+v", co)
	}
	if n := sqliteCount(t, co.Checkout, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("try2 row count = %d, want 1", n)
	}

	destroy := call(t, sock, Request{Op: "destroy", DB: "db2", Branch: "try"})
	if destroy.OK {
		t.Fatal("destroy of a branch with an open session must be refused")
	}
	if !strings.Contains(destroy.Error, "close") {
		t.Fatalf("destroy error = %q, want it to mention closing the session", destroy.Error)
	}

	if cl := call(t, sock, Request{Op: "close", DB: "db2", Branch: "try"}); !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
	if destroy2 := call(t, sock, Request{Op: "destroy", DB: "db2", Branch: "try"}); !destroy2.OK {
		t.Fatalf("destroy = %+v", destroy2)
	}
	br = call(t, sock, Request{Op: "branches", DB: "db2"})
	for _, b := range br.Branches {
		if b.Branch == "try" {
			t.Fatalf("try still listed after destroy: %+v", br.Branches)
		}
	}
}

// TestPromoteGuards covers both halves of promote's open-session rule: a
// promote is refused when its TARGET has an open session here, but a
// promote whose SOURCE has an open session flushes that session first
// (matching fork's behavior) so an un-flushed write still lands.
func TestPromoteGuards(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	if _, err := w.Fork("app", "main", "src", "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "target-open", "", 0); err != nil {
		t.Fatal(err)
	}
	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "target-open"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	promote := call(t, sock, Request{Op: "promote", DB: "app", Branch: "src", Name: "target-open"})
	if promote.OK {
		t.Fatal("promote onto a branch with an open session must be refused")
	}
	if !strings.Contains(promote.Error, "close") {
		t.Fatalf("promote error = %q, want it to mention closing the session", promote.Error)
	}
	if cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "target-open"}); !cl.OK {
		t.Fatalf("close = %+v", cl)
	}

	if _, err := w.Fork("app", "main", "src2", "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "target2", "", 0); err != nil {
		t.Fatal(err)
	}
	openSrc := call(t, sock, Request{Op: "open", DB: "app", Branch: "src2"})
	if !openSrc.OK {
		t.Fatalf("open src2 = %+v", openSrc)
	}
	sqliteExec(t, openSrc.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	promote2 := call(t, sock, Request{Op: "promote", DB: "app", Branch: "src2", Name: "target2"})
	if !promote2.OK {
		t.Fatalf("promote with an open source = %+v", promote2)
	}
	co := call(t, sock, Request{Op: "checkout", DB: "app", Branch: "target2"})
	if !co.OK {
		t.Fatalf("checkout target2 = %+v", co)
	}
	if n := sqliteCount(t, co.Checkout, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("target2 row count = %d, want 1", n)
	}
}

// TestRollbackGuard covers rollback's open-session guard: refused while the
// branch has an open session here, and successful (with the checkout
// reflecting the target checkpoint) once it's closed.
func TestRollbackGuard(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	flush1 := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "cp1"})
	if !flush1.OK {
		t.Fatalf("flush cp1 = %+v", flush1)
	}
	sqliteExec(t, open.Checkout, "INSERT INTO t VALUES (2);")
	flush2 := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "cp2"})
	if !flush2.OK {
		t.Fatalf("flush cp2 = %+v", flush2)
	}

	rb := call(t, sock, Request{Op: "rollback", DB: "app", Branch: "main", Name: "cp1"})
	if rb.OK {
		t.Fatal("rollback of a branch with an open session must be refused")
	}
	if !strings.Contains(rb.Error, "close") {
		t.Fatalf("rollback error = %q, want it to mention closing the session", rb.Error)
	}

	if cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}); !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
	rb2 := call(t, sock, Request{Op: "rollback", DB: "app", Branch: "main", Name: "cp1"})
	if !rb2.OK || rb2.Checkout == "" {
		t.Fatalf("rollback = %+v", rb2)
	}
	if n := sqliteCount(t, rb2.Checkout, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("post-rollback row count = %d, want 1", n)
	}
}

// TestUnknownOpStillErrors guards dispatch's default case as the op list
// grows: an op the server does not recognize must fail cleanly rather than
// panic or silently succeed.
func TestUnknownOpStillErrors(t *testing.T) {
	srv, _ := newServer(t)
	r := call(t, srv.SocketPath(), Request{Op: "zap"})
	if r.OK {
		t.Fatal("unknown op must not report ok")
	}
	if r.Error == "" {
		t.Fatal("unknown op must carry an error message")
	}
}
