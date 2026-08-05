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

// branchInfo finds branch in a "branches" Response, failing the test if it
// is not listed.
func branchInfo(t *testing.T, resp Response, branch string) BranchInfo {
	t.Helper()
	for _, b := range resp.Branches {
		if b.Branch == branch {
			return b
		}
	}
	t.Fatalf("branches missing %q: %+v", branch, resp.Branches)
	return BranchInfo{}
}

// TestGuardsRefuseDuringInFlightOpen closes the gap where a branch reserved
// (key present, nil value) by an in-flight opOpen — still inside its slow,
// unlocked session.Open, which is materializing the checkout file — was
// invisible to every open-session guard because they only checked for a
// non-nil session. That let checkout/destroy/rollback/promote's target
// guard, and fork/promote's "is the source open" check, all sail straight
// past a reservation and run concurrently with session.Open's write to the
// exact same checkout file: the very on-disk race opOpen's reservation
// exists to prevent (see opOpen's doc comment). Uses the same openDelay
// test hook TestShutdownDuringInFlightOpenLeavesNoLease uses to
// deterministically hold an open in that reserved-but-not-yet-live window.
func TestGuardsRefuseDuringInFlightOpen(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	if _, err := w.Fork("app", "main", "mid", "", 0); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	proceed := make(chan struct{})
	openDelay = func() {
		close(entered)
		<-proceed
	}
	defer func() { openDelay = nil }()

	openDone := make(chan Response, 1)
	go func() {
		resp, err := rawCall(sock, Request{Op: "open", DB: "app", Branch: "mid"})
		if err != nil {
			resp = errResp(err)
		}
		openDone <- resp
	}()
	<-entered // opOpen has reserved app@mid and is blocked before session.Open

	if r := call(t, sock, Request{Op: "checkout", DB: "app", Branch: "mid"}); r.OK || !strings.Contains(r.Error, "close") {
		t.Fatalf("checkout during in-flight open = %+v, want a close-session refusal", r)
	}
	if r := call(t, sock, Request{Op: "destroy", DB: "app", Branch: "mid"}); r.OK || !strings.Contains(r.Error, "close") {
		t.Fatalf("destroy during in-flight open = %+v, want a close-session refusal", r)
	}
	if r := call(t, sock, Request{Op: "rollback", DB: "app", Branch: "mid", Name: "fork"}); r.OK || !strings.Contains(r.Error, "close") {
		t.Fatalf("rollback during in-flight open = %+v, want a close-session refusal", r)
	}
	if r := call(t, sock, Request{Op: "promote", DB: "app", Branch: "main", Name: "mid"}); r.OK || !strings.Contains(r.Error, "close") {
		t.Fatalf("promote onto an in-flight-open target = %+v, want a close-session refusal", r)
	}
	if r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "mid", Name: "mid-fork"}); r.OK || !strings.Contains(r.Error, "close") {
		t.Fatalf("fork from an in-flight-open source = %+v, want a close-session refusal", r)
	}

	openDelay = nil
	close(proceed)
	var openResp Response
	select {
	case openResp = <-openDone:
	case <-time.After(10 * time.Second):
		t.Fatal("open never returned after being released")
	}
	if !openResp.OK {
		t.Fatalf("open eventually = %+v", openResp)
	}
	if cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "mid"}); !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
}

// TestOpPromoteDoesNotDefaultTarget pins the fix for a real footgun: an
// omitted req.Name used to default to "main" the same way req.Branch
// (source) does, so a client bug that dropped the target field silently
// became "promote onto main" instead of a clean validation error — and
// once req.Force can override main's protected-branch guard, that default
// is destructive. An omitted target must fail via ops.Promote's own
// store.ValidateName("") refusal, and must leave main completely untouched.
func TestOpPromoteDoesNotDefaultTarget(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	before := call(t, sock, Request{Op: "branches", DB: "app"})
	mainBefore := branchInfo(t, before, "main")

	r := call(t, sock, Request{Op: "promote", DB: "app", Branch: "main"}) // Name omitted
	if r.OK {
		t.Fatal("promote with an omitted target must not silently default to main")
	}
	if r.Error == "" {
		t.Fatal("a refused promote must carry an error message")
	}

	after := call(t, sock, Request{Op: "branches", DB: "app"})
	mainAfter := branchInfo(t, after, "main")
	if mainAfter.HeadTXID != mainBefore.HeadTXID {
		t.Fatalf("main was mutated by a promote with no target: before=%+v after=%+v", mainBefore, mainAfter)
	}
}

// TestTouchOp covers all four of touch's ttl arms end to end through the
// daemon, asserting the result via the "branches" op: "" keeps the current
// TTL, "none" clears it, a positive duration sets it, and both an
// unparseable string and a non-positive parsed duration are refused
// (ok=false) rather than silently accepted or aliased to "none".
func TestTouchOp(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	// "2h" sets.
	if r := call(t, sock, Request{Op: "touch", DB: "app", Branch: "main", TTL: "2h"}); !r.OK {
		t.Fatalf("touch 2h = %+v", r)
	}
	want := (2 * time.Hour).String()
	br := call(t, sock, Request{Op: "branches", DB: "app"})
	if got := branchInfo(t, br, "main").TTL; got != want {
		t.Fatalf("ttl after touch 2h = %q, want %q", got, want)
	}

	// "" keeps the current TTL unchanged.
	if r := call(t, sock, Request{Op: "touch", DB: "app", Branch: "main", TTL: ""}); !r.OK {
		t.Fatalf("touch \"\" = %+v", r)
	}
	br = call(t, sock, Request{Op: "branches", DB: "app"})
	if got := branchInfo(t, br, "main").TTL; got != want {
		t.Fatalf("touch \"\" changed the ttl: got %q, want unchanged %q", got, want)
	}

	// "none" clears.
	if r := call(t, sock, Request{Op: "touch", DB: "app", Branch: "main", TTL: "none"}); !r.OK {
		t.Fatalf("touch none = %+v", r)
	}
	br = call(t, sock, Request{Op: "branches", DB: "app"})
	if got := branchInfo(t, br, "main").TTL; got != "" {
		t.Fatalf("touch none did not clear the ttl, got %q", got)
	}

	// Unparseable -> ok=false with an error.
	r := call(t, sock, Request{Op: "touch", DB: "app", Branch: "main", TTL: "not-a-duration"})
	if r.OK {
		t.Fatal("touch with an unparseable ttl must be refused")
	}
	if r.Error == "" {
		t.Fatal("a failed touch must carry an error message")
	}

	// A non-positive parsed duration must not silently alias "none".
	r = call(t, sock, Request{Op: "touch", DB: "app", Branch: "main", TTL: "0s"})
	if r.OK {
		t.Fatal("touch with a zero ttl must be refused, not aliased to \"none\"")
	}
	if !strings.Contains(r.Error, "none") {
		t.Fatalf("zero-ttl error = %q, want it to name \"none\" as the way to clear", r.Error)
	}
	r = call(t, sock, Request{Op: "touch", DB: "app", Branch: "main", TTL: "-1h"})
	if r.OK {
		t.Fatal("touch with a negative ttl must be refused, not aliased to \"none\"")
	}
	if !strings.Contains(r.Error, "none") {
		t.Fatalf("negative-ttl error = %q, want it to name \"none\" as the way to clear", r.Error)
	}
}

// TestCheckoutRefusesOpenSession covers the checkout op's open-session
// guard directly (previously exercised only incidentally as a side effect
// of other tests): a checkout of a branch this daemon already has open
// must be refused, mentioning "close".
func TestCheckoutRefusesOpenSession(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	r := call(t, sock, Request{Op: "checkout", DB: "app", Branch: "main"})
	if r.OK {
		t.Fatal("checkout of an open branch must be refused")
	}
	if !strings.Contains(r.Error, "close") {
		t.Fatalf("checkout error = %q, want it to mention closing the session", r.Error)
	}
}

// TestForkFromNamedCheckpoint covers fork's req.From: forking at an earlier
// named checkpoint (rather than the branch's current head) must carry
// exactly that checkpoint's state, not anything written after it.
func TestForkFromNamedCheckpoint(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "cp1"}); !r.OK {
		t.Fatalf("flush cp1 = %+v", r)
	}
	sqliteExec(t, open.Checkout, "INSERT INTO t VALUES (2);")
	if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "cp2"}); !r.OK {
		t.Fatalf("flush cp2 = %+v", r)
	}
	if cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}); !cl.OK {
		t.Fatalf("close = %+v", cl)
	}

	fork := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "fromcp1", From: "cp1"})
	if !fork.OK {
		t.Fatalf("fork from=cp1 = %+v", fork)
	}
	co := call(t, sock, Request{Op: "checkout", DB: "app", Branch: "fromcp1"})
	if !co.OK {
		t.Fatalf("checkout fromcp1 = %+v", co)
	}
	if n := sqliteCount(t, co.Checkout, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("fromcp1 row count = %d, want 1 (cp1's state, not cp2's)", n)
	}
}

// TestDestroyForceOnProtectedBranch covers destroy's req.Force: main is
// protected by default (ops.Create), so destroying it without force must
// fail, and with force must succeed.
func TestDestroyForceOnProtectedBranch(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	if r := call(t, sock, Request{Op: "destroy", DB: "app", Branch: "main"}); r.OK {
		t.Fatal("destroy of a protected branch without force must be refused")
	}
	r := call(t, sock, Request{Op: "destroy", DB: "app", Branch: "main", Force: true})
	if !r.OK {
		t.Fatalf("destroy with force = %+v", r)
	}
	br := call(t, sock, Request{Op: "branches", DB: "app"})
	for _, b := range br.Branches {
		if b.Branch == "main" {
			t.Fatalf("main still listed after forced destroy: %+v", br.Branches)
		}
	}
}

// TestBranchesUnknownDBErrors pins the fix for "branches" on a db that was
// never created (or has had every branch destroyed): it must return an
// error, not ok=true with an empty list — a client asking for a specific
// db's branches should be told plainly that the db doesn't exist rather
// than silently seeing "no branches".
func TestBranchesUnknownDBErrors(t *testing.T) {
	srv, _ := newServer(t)
	r := call(t, srv.SocketPath(), Request{Op: "branches", DB: "does-not-exist"})
	if r.OK {
		t.Fatal("branches of a nonexistent db must be refused, not ok=true with an empty list")
	}
	if r.Error == "" {
		t.Fatal("a refused branches call must carry an error message")
	}
}

// TestJanitorNeverReapsThisDaemonsOwnSession pins the spec sentence "a
// branch with an active lease is never reaped — expiry defers until the
// lease is released" against a REAL running daemon and REAL janitor loop,
// not just ops.Reap in isolation (TestExpiredLeaseDefersToTTLNotBlocksForever
// in internal/ops/reap_test.go already covers the deferral arithmetic in
// isolation; this test is about whether the daemon's own live session
// actually produces that live lease and survives the janitor because of it).
//
// "live" is opened here (giving it a real session lease, acquired for
// ops.DefaultLeaseTTL = 30s) and then backdated the way
// TestJanitorReapsExpiredBranchWhileDaemonRuns backdates an unleased branch:
// TouchedAt two hours in the past — already reap-eligible by the activity
// clock alone. The TTL itself is deliberately short (200ms), not the "1h"
// TestJanitorReapsExpiredBranchWhileDaemonRuns uses: store.ReleaseLease
// stamps a fresh Touch(now) the moment it clears a lease (see
// internal/store/lease.go — "a lease that was just live counts as
// activity"), so this test's own post-close deadline is close-time+TTL, not
// close-time+0. A 200ms TTL keeps that comfortably inside the 5s window this
// test waits for the post-close reap in, while a 1h TTL would not reap for
// another hour. A sibling branch, "doomed", gets the identical backdating
// but is never opened, so it carries no lease at all. Waiting for "doomed"
// to disappear under a 50ms janitor is how this test knows the janitor is
// actually cycling against live server state, not merely that wall-clock
// time passed: reaping "doomed" requires a real Reap pass to run and tell
// the two apart. The test then keeps the janitor cycling for ~20 more ticks
// (a further ~1s at 50ms) — "live" must survive every single one, and a
// flush through the still-open session must keep succeeding. Both survive
// because the live lease's Expiry (~30s out, acquired at Open) dominates the
// deadline regardless of the short TTL — this is what proves it is the
// lease, not the TTL's length, doing the deferring. Only after Close
// releases the lease does the deadline collapse to close-time+200ms: the
// janitor must reap it within 5s.
func TestJanitorNeverReapsThisDaemonsOwnSession(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	if _, err := w.Fork("app", "main", "live", "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "live"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}

	now := time.Now().UTC()
	backdate := func(branch string) {
		ref, etag, err := w.Store.GetRef("app", branch)
		if err != nil {
			t.Fatal(err)
		}
		ref.TTL = "200ms"
		ref.TouchedAt = now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
		if _, err := w.Store.PutRef("app", branch, ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	backdate("live")
	backdate("doomed")

	srv.StartJanitor(50*time.Millisecond, 0)

	// Proof the janitor is actually cycling against real state: "doomed"
	// (unleased) must go, distinguishing it from "live" (leased) despite
	// both starting out identically backdated.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, err := w.Store.GetRef("app", "doomed"); errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("janitor never reaped the unleased sibling within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Keep the janitor cycling for ~20 more 50ms ticks. "live" must survive
	// every one of them, and the open session must keep working throughout.
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if _, _, err := w.Store.GetRef("app", "live"); err != nil {
			t.Fatalf("cycle %d: live session's branch was reaped out from under it: %v", i, err)
		}
	}
	if flush := call(t, sock, Request{Op: "flush", DB: "app", Branch: "live", Name: ""}); !flush.OK {
		t.Fatalf("flush on the still-open live session = %+v", flush)
	}

	if cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "live"}); !cl.OK {
		t.Fatalf("close = %+v", cl)
	}

	// Lease released: ReleaseLease's own Touch(now) resets the clock, but the
	// TTL is only 200ms, so the janitor must reap it well within 5s.
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, _, err := w.Store.GetRef("app", "live"); errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("janitor never reaped live after its session closed, within 5s")
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

// TestForkFromLiveSessionSeesLatestWrite is a regression pin for the
// flush-then-fork contract (already exercised incidentally inside
// TestLifecycleOpsRoundTrip's fork2 case): write a row through an open
// session's checkout, deliberately never flush it, then daemon-op fork that
// branch. opFork's flushIfOpen step must flush the live session before
// forking, so the child must carry the row even though the caller never
// explicitly flushed it. It also asserts the other half of the contract that
// a naive implementation could get wrong: flushing the source out from under
// its own open session must not leave that session damaged — a further
// write, an explicit flush, and a status check must all still succeed
// afterward.
func TestForkFromLiveSessionSeesLatestWrite(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	sqliteExec(t, open.Checkout, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")

	// No flush here: the point is that opFork's flush-then-fork step, not an
	// explicit flush from the caller, is what carries this write forward.
	fork := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "child"})
	if !fork.OK {
		t.Fatalf("fork from a live, unflushed session = %+v", fork)
	}

	co := call(t, sock, Request{Op: "checkout", DB: "app", Branch: "child"})
	if !co.OK || co.Checkout == "" {
		t.Fatalf("checkout child = %+v", co)
	}
	if n := sqliteCount(t, co.Checkout, "SELECT COUNT(*) FROM t;"); n != 1 {
		t.Fatalf("child row count = %d, want 1 (the unflushed write)", n)
	}

	// The SOURCE session must still be healthy after fork flushed it out
	// from under the caller: a further write, an explicit flush, and status
	// must all still succeed.
	sqliteExec(t, open.Checkout, "INSERT INTO t VALUES (2);")
	flush := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "after-fork"})
	if !flush.OK {
		t.Fatalf("flush on source after fork = %+v", flush)
	}
	st := call(t, sock, Request{Op: "status"})
	if !st.OK {
		t.Fatalf("status = %+v", st)
	}
	var found bool
	for _, in := range st.Sessions {
		if in.DB == "app" && in.Branch == "main" {
			found = true
			if in.Error != "" {
				t.Fatalf("source session unhealthy after fork: %+v", in)
			}
		}
	}
	if !found {
		t.Fatalf("source session missing from status after fork: %+v", st.Sessions)
	}
}
