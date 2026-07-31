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

	"github.com/offshoot-db/offshoot/internal/ops"
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
