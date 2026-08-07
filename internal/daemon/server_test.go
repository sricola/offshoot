package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
)

func newServer(t *testing.T) (*Server, *ops.Workspace) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	w, err := ops.Init(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// The socket path lives in its own short-named temp dir, independent of
	// t.TempDir() (which nests under the test's, and any subtest's, full
	// name): unix domain socket paths are capped at ~104-108 bytes on
	// several platforms (notably macOS), and a t.TempDir() path can exceed
	// that once the test name is long.
	sockDir, err := os.MkdirTemp("", "offshoot-daemon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	srv, err := NewServer(w, filepath.Join(sockDir, "sock"))
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, w
}

// call sends one request on a fresh connection and returns the response.
func call(t *testing.T, sock string, req Request) Response {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(c)).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestServerOpenFlushStatusClose(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket perm = %o, want 600", perm)
	}

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Flush may race capture; retry briefly until the row is durable.
	var txid uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: ""})
		if resp.OK {
			txid = resp.TXID
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			if ref.HeadTXID == txid {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if txid == 0 {
		t.Fatal("flush never succeeded")
	}

	st := call(t, sock, Request{Op: "status"})
	if !st.OK || len(st.Sessions) != 1 {
		t.Fatalf("status = %+v", st)
	}
	if st.Sessions[0].DB != "app" || st.Sessions[0].DurableTXID != txid {
		t.Fatalf("session info = %+v", st.Sessions[0])
	}

	cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("close must release the lease, holder = %q", ref.LeaseHolder)
	}
}

func TestServerRefusesDoubleOpen(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("first open = %+v", r)
	}
	r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if r.OK {
		t.Fatal("second open of the same branch must be refused")
	}
	if r.Error == "" {
		t.Fatal("a refusal must carry an error message")
	}
}

func TestShutdownReleasesEveryLease(t *testing.T) {
	srv, w := newServer(t)
	if r := call(t, srv.SocketPath(), Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("shutdown must release leases, holder = %q", ref.LeaseHolder)
	}
	if _, err := os.Stat(srv.SocketPath()); !os.IsNotExist(err) {
		t.Fatalf("shutdown must remove the socket, stat err = %v", err)
	}
}

// rawCall is like call but reports errors to the caller instead of failing
// the test directly, so it is safe to use from a goroutine other than the
// test's own (t.Fatal et al. must only be called from the test goroutine).
func rawCall(sock string, req Request) (Response, error) {
	c, err := net.Dial("unix", sock)
	if err != nil {
		return Response{}, err
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(c)).Decode(&resp); err != nil {
		return Response{}, err
	}
	return resp, nil
}

// TestShutdownDuringInFlightOpenLeavesNoLease exercises the fix for a race
// where Shutdown's drain loop used to delete every map entry, including a
// nil placeholder reserved by an opOpen still running session.Open. That let
// Shutdown return (and remove the socket) while a lease acquisition and a
// capture-engine startup were still in progress, unsupervised: if the
// process exited right after Shutdown returned, that lease would leak for
// real; even if it didn't, a second opOpen for the same branch could reuse
// the wiped key and start a second, concurrent session.Open against the same
// checkout file.
//
// Reproducing the original interleaving through real socket timing is not
// reliable (session.Open on a fresh temp dir is typically fast enough that
// Shutdown racing it is a narrow, flaky window). Instead this test uses the
// package-level openDelay hook to deterministically hold an open in flight
// — reserved in the map, blocked before session.Open runs — and starts
// Shutdown while it is stuck there. The core assertion is timing-based but
// coarse and reliable: with the fix, Shutdown must not return while that
// open is still held open by the hook (it is blocked on s.openWG.Wait), so a
// generous grace period with nothing arriving on shutdownDone is a reliable
// negative signal. Pre-fix, Shutdown drains and returns almost immediately
// regardless of the in-flight open, so this assertion fails reliably against
// the old code (verified by re-running this test against a checkout of the
// pre-fix server.go: it fails on the first iteration, Shutdown returning in
// well under a millisecond instead of waiting).
func TestShutdownDuringInFlightOpenLeavesNoLease(t *testing.T) {
	for i := 0; i < 5; i++ {
		srv, w := newServer(t)
		sock := srv.SocketPath()

		entered := make(chan struct{})
		proceed := make(chan struct{})
		openDelay = func() {
			close(entered)
			<-proceed
		}

		openDone := make(chan struct{})
		go func() {
			defer close(openDone)
			rawCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
		}()
		<-entered // opOpen has reserved app@main and is blocked before session.Open

		shutdownDone := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			shutdownDone <- srv.Shutdown(ctx)
		}()

		select {
		case <-shutdownDone:
			openDelay = nil
			close(proceed)
			<-openDone
			t.Fatalf("iteration %d: Shutdown returned while an open was still in flight for the same branch", i)
		case <-time.After(200 * time.Millisecond):
			// Expected: Shutdown is waiting on the in-flight open.
		}

		openDelay = nil
		close(proceed) // let the blocked open finish
		<-openDone

		select {
		case err := <-shutdownDone:
			if err != nil {
				t.Fatalf("iteration %d: shutdown = %v", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("iteration %d: shutdown did not return after the in-flight open resolved", i)
		}

		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if ref.LeaseHolder != "" {
			t.Fatalf("iteration %d: lease leaked, holder = %q", i, ref.LeaseHolder)
		}

		srv.mu.Lock()
		n := len(srv.sessions)
		srv.mu.Unlock()
		if n != 0 {
			t.Fatalf("iteration %d: %d session(s) still tracked after shutdown", i, n)
		}
	}
}

// TestShutdownRespondsBeforeClosingRequestingConn exercises the fix for a
// race between the "shutdown" response write and Shutdown's own connection-
// close pass: dispatch's "shutdown" case used to trigger Shutdown (which
// force-closes every live connection, including the one that just asked for
// the shutdown) via `go s.Shutdown(...)` and then immediately return, with
// no ordering between that goroutine and handle's subsequent write of this
// very response. Under normal scheduling the write usually won, so the bug
// only surfaced as an occasional CI flake on a loaded/CPU-constrained
// runner: the CLI's `session shutdown` would see "daemon: reading response:
// EOF" instead of the {OK: true} the server did send -- because Shutdown's
// connection-close won the race and tore down the socket before (or during)
// the write.
//
// Reproducing that interleaving through real socket timing is exactly the
// kind of narrow, load-dependent window that made this flake so rare
// locally. Instead this test uses the shutdownRespondDelay hook to force
// the losing interleaving deterministically: it delays handle's write of
// the shutdown response for a while after dispatch has already returned
// (and, pre-fix, already launched Shutdown). That gives Shutdown a
// generous head start to close every live connection, including this
// request's own, before the delayed write is allowed to run.
//
// Pre-fix, this reliably reproduces the bug: Shutdown wins the race, the
// delayed write fails against an already-closed connection, and the client
// sees an error instead of {OK: true} (verified by re-running this test
// against a checkout of the pre-fix server.go: it fails on every
// iteration). Post-fix, Shutdown is not triggered until after the response
// has actually been written, so the delay hook has nothing left to race:
// the client always gets its {OK: true}.
func TestShutdownRespondsBeforeClosingRequestingConn(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	// entered is closed from inside the hook closure itself, after the
	// package-level shutdownRespondDelay var has already been read and
	// invoked by handle -- so, per the same reasoning as openDelay's tests
	// (see TestShutdownDuringInFlightOpenLeavesNoLease), waiting on it
	// before this goroutine later nils the var out gives a real
	// happens-before edge instead of racing handle's read of it.
	entered := make(chan struct{})
	release := make(chan struct{})
	shutdownRespondDelay = func() {
		close(entered)
		<-release
	}
	defer func() { shutdownRespondDelay = nil }()

	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(Request{Op: "shutdown"}); err != nil {
		t.Fatal(err)
	}
	<-entered // handle has dispatched the request and is about to write its response

	// Give a pre-fix Shutdown -- already launched by dispatch, before this
	// response is written -- a generous head start to close every live
	// connection, including this one, before the delayed write is allowed
	// to proceed.
	time.Sleep(200 * time.Millisecond)
	close(release)

	var resp Response
	if err := json.NewDecoder(bufio.NewReader(c)).Decode(&resp); err != nil {
		t.Fatalf("shutdown response: %v (the daemon must finish writing the "+
			"shutdown response before Shutdown's connection-close pass can "+
			"touch this connection)", err)
	}
	if !resp.OK {
		t.Fatalf("shutdown response = %+v, want OK", resp)
	}
}

// TestConcurrentFlushAndCloseIsSafe exercises the fix for a race between
// Close and a concurrent Flush on the same session: Close's os.RemoveAll of
// the scratch dir used to run with no coordination against Flush's read of
// the replica file (under that same dir) via ltxio.EncodeSnapshot — so a
// close could delete the directory out from under an in-flight flush's
// encode. This is reachable through the daemon precisely because lookup()
// hands out the same *session.Session to concurrent "flush" and "close"
// requests for one branch; nothing in the daemon serializes them itself.
//
// Like TestShutdownDuringInFlightOpenLeavesNoLease, reproducing the original
// interleaving through real timing is unreliable (the race window is a few
// syscalls wide). Instead this test uses session.FlushEncodeHook to
// deterministically pause a flush immediately before its EncodeSnapshot
// call — inside the exact window the race lived in — while a concurrent
// "close" request runs. The core assertion is timing-based but coarse and
// reliable: with the fix, Close acquires flushMu before it removes the
// scratch dir, so "close" must not return while the flush is paused there;
// a generous grace period with nothing arriving on closeDone is a reliable
// negative signal.
//
// Verified against the pre-fix session.go (Close not taking flushMu around
// its RemoveAll): temporarily reverting that one change made this test's
// blocking assertion fail immediately — "close" returned in a few hundred
// microseconds instead of waiting out the paused flush, would-be evidence of
// exactly the unguarded race this test exists to catch (RemoveAll running
// while EncodeSnapshot was still reading files under the same directory).
func TestConcurrentFlushAndCloseIsSafe(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	entered := make(chan struct{})
	proceed := make(chan struct{})
	session.FlushEncodeHook = func() {
		close(entered)
		<-proceed
	}
	defer func() { session.FlushEncodeHook = nil }()

	flushDone := make(chan Response, 1)
	go func() {
		resp, err := rawCall(sock, Request{Op: "flush", DB: "app", Branch: "main"})
		if err != nil {
			resp = errResp(err)
		}
		flushDone <- resp
	}()
	select {
	case <-entered:
		// The flush is now paused inside Flush, holding flushMu, immediately
		// before its EncodeSnapshot call.
	case <-time.After(10 * time.Second):
		t.Fatal("flush never reached its encode hook")
	}

	closeDone := make(chan Response, 1)
	go func() {
		resp, err := rawCall(sock, Request{Op: "close", DB: "app", Branch: "main"})
		if err != nil {
			resp = errResp(err)
		}
		closeDone <- resp
	}()

	select {
	case r := <-closeDone:
		session.FlushEncodeHook = nil
		close(proceed)
		<-flushDone
		t.Fatalf("close returned (%+v) while a flush was paused mid-encode on the same session", r)
	case <-time.After(200 * time.Millisecond):
		// Expected: close is blocked behind the paused flush's flushMu.
	}

	close(proceed) // let the paused flush finish its encode

	var flushResp, closeResp Response
	select {
	case flushResp = <-flushDone:
	case <-time.After(10 * time.Second):
		t.Fatal("flush never returned after being released")
	}
	select {
	case closeResp = <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("close never returned after the paused flush finished")
	}

	if !closeResp.OK {
		t.Fatalf("close must succeed: %+v", closeResp)
	}

	// Whichever way the race between Close's ReleaseLease and Flush's own
	// PutRef fell, the outcome must be reported honestly: a flush reporting
	// success must have an actual snapshot behind the ref it claims — never
	// a success whose snapshot is missing.
	if flushResp.OK {
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if ref.HeadTXID != flushResp.TXID {
			t.Fatalf("flush reported success at txid %d but ref head is %d", flushResp.TXID, ref.HeadTXID)
		}
		key := store.SnapshotKey(ref.Lineage, ref.HeadEpoch, ref.HeadTXID)
		if data, _, err := w.Store.B.Get(key); err != nil || len(data) == 0 {
			t.Fatalf("flush reported success but its snapshot is missing: key=%s err=%v len=%d",
				key, err, len(data))
		}
	} else if flushResp.Error == "" {
		t.Fatal("a failed flush must carry an error message")
	}

	// No panic occurred (the test would already have crashed if one had),
	// and the session is gone from the daemon afterward.
	st := call(t, sock, Request{Op: "status"})
	for _, in := range st.Sessions {
		if in.DB == "app" && in.Branch == "main" {
			t.Fatalf("session still listed after close: %+v", in)
		}
	}
}
