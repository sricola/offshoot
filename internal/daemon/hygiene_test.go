package daemon

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
)

// TestNewServerRefusesASocketALiveDaemonServes: a second `serve` on a path
// a live daemon answers must fail, not unlink the live daemon's socket and
// take its place (which left the first daemon running with an open session
// nobody could reach, and its eventual Shutdown then unlinked the second
// daemon's socket too).
func TestNewServerRefusesASocketALiveDaemonServes(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if _, err := NewServer(w, sock); err == nil || !strings.Contains(err.Error(), "already serving") {
		t.Fatalf("NewServer on a live daemon's socket: err = %v, want 'already serving'", err)
	}
	if !Running(sock) {
		t.Fatal("the refused NewServer unlinked the live daemon's socket")
	}
	if r := call(t, sock, Request{Op: "status"}); !r.OK {
		t.Fatalf("live daemon no longer answers: %+v", r)
	}
}

// TestShutdownLeavesASocketItNoLongerOwns: if the path was replaced while
// this daemon ran (an operator removed what looked like a stale socket and
// started another daemon), Shutdown must not unlink the other daemon's
// socket. Closing a UnixListener unlinks its path by default; the server
// removes the path itself, and only while it is still its own.
func TestShutdownLeavesASocketItNoLongerOwns(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if err := os.Remove(sock); err != nil {
		t.Fatal(err)
	}
	next, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("Shutdown unlinked a socket another daemon owns: stat %s: %v", sock, err)
	}
}

// flakyListener fails its first Accept with the errno a descriptor-starved
// process sees, then delegates.
type flakyListener struct {
	net.Listener
	failed atomic.Bool
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.failed.CompareAndSwap(false, true) {
		return nil, &net.OpError{Op: "accept", Net: "unix", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}

// TestServeSurvivesATransientAcceptError: EMFILE/ENFILE from accept(2) is a
// burst of connections against the fd ceiling, not a dead listener. Serve
// used to return it, which exited the daemon with every lease held and
// every checkout un-stamped.
func TestServeSurvivesATransientAcceptError(t *testing.T) {
	srv, _ := newServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx) // newServer's own Serve; this test drives its own

	w := srv.ws
	sockDir, err := os.MkdirTemp("", "offshoot-daemon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "sock")
	s2, err := NewServer(w, sock)
	if err != nil {
		t.Fatal(err)
	}
	s2.ln = &flakyListener{Listener: s2.ln}
	served := make(chan error, 1)
	go func() { served <- s2.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s2.Shutdown(ctx)
	})

	// Serve returning is the old behaviour; check for it before dialling,
	// because a dial against an open listener nobody accepts on would hang.
	select {
	case err := <-served:
		t.Fatalf("Serve returned on a transient accept error: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if r := call(t, sock, Request{Op: "status"}); !r.OK {
		t.Fatalf("status after a transient accept error = %+v", r)
	}
}

// TestShutdownClosesSessionsConcurrently: Session.Close is slow in
// proportion to the checkout (engine shutdown hashes it), and Shutdown has
// one budget for all of them; closing them one after another turned a
// graceful SIGTERM into a timeout for the sessions that did not make the
// cut. Every close must be in flight at once.
func TestShutdownClosesSessionsConcurrently(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	const n = 3
	for _, b := range []string{"a", "b"} {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range []string{"main", "a", "b"} {
		if r := call(t, sock, Request{Op: "open", DB: "app", Branch: b}); !r.OK {
			t.Fatalf("open %s = %+v", b, r)
		}
	}

	entered := make(chan struct{}, n)
	release := make(chan struct{})
	session.CloseReleaseHook = func() {
		entered <- struct{}{}
		<-release
	}
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		session.CloseReleaseHook = nil
	})

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- srv.Shutdown(ctx)
	}()
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-entered:
		case <-deadline:
			t.Fatalf("only %d of %d session closes were in flight together: Shutdown closes serially", i, n)
		}
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestSocketConnectionCapIsPerRequest: both SDKs keep one socket
// connection for the client's life. The request-size cap used to be a
// per-connection lifetime cap, so a long-lived client's ~140k-th small
// request was answered with EOF. Three requests whose whitespace padding
// totals well over the cap must all be answered on one connection.
func TestSocketConnectionCapIsPerRequest(t *testing.T) {
	srv, _ := newServer(t)
	c, err := net.Dial("unix", srv.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pad := strings.Repeat(" ", maxSocketRequestBytes/2+1)
	dec := json.NewDecoder(c)
	for i := 0; i < 3; i++ {
		if _, err := c.Write([]byte(`{"op":"status"}` + pad + "\n")); err != nil {
			t.Fatalf("request %d: write: %v", i, err)
		}
		var r Response
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("request %d (%d bytes so far on this connection): %v", i, (i+1)*(len(pad)+16), err)
		}
		if !r.OK {
			t.Fatalf("request %d: %+v", i, r)
		}
	}
}

// gateListBackend blocks every List("refs/") — the janitor's reap starts
// with ListRefs — until release is closed, so a janitor pass can be held
// mid-flight deterministically.
type gateListBackend struct {
	store.Backend
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateListBackend) List(prefix string) ([]string, error) {
	if prefix == "refs/" {
		b.once.Do(func() { close(b.entered) })
		<-b.release
	}
	return b.Backend.List(prefix)
}

// TestShutdownOpBoundsTheJanitorWait pins docs/status.md ("serve exits
// after the shutdown op only once Shutdown has returned … with an error
// if a janitor pass was still running after half the shutdown budget;
// sessions are closed regardless"). A shutdown started by the `shutdown`
// op used to run with no deadline, so a janitor pass that never returned
// held every session open and every lease until the process was killed.
// With the budget applied, the open session's lease is released while the
// pass is still blocked. Verified against the pre-fix code (lease still
// held after the budget elapsed).
func TestShutdownOpBoundsTheJanitorWait(t *testing.T) {
	prev := ShutdownBudget
	ShutdownBudget = 2 * time.Second
	t.Cleanup(func() { ShutdownBudget = prev })

	srv, w := newServer(t)
	gate := &gateListBackend{Backend: w.Store.B, entered: make(chan struct{}), release: make(chan struct{})}
	w.Store.B = gate
	// Let the blocked pass finish before the temp store is removed (it
	// would otherwise write into a directory being deleted); registered
	// after newServer's TempDir, so it runs first.
	var released sync.Once
	t.Cleanup(func() {
		released.Do(func() { close(gate.release) })
		srv.janitorWG.Wait()
	})
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open: %+v", r)
	}
	srv.StartJanitor(10*time.Millisecond, 0)
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the janitor never reached its reap listing")
	}
	if r := call(t, sock, Request{Op: "shutdown"}); !r.OK {
		t.Fatalf("shutdown: %+v", r)
	}
	// Half the budget is the janitor's; the session close must land inside
	// the other half (plus slack for a loaded runner), not merely before
	// the whole budget lapses.
	deadline := time.Now().Add(ShutdownBudget/2 + 1500*time.Millisecond)
	for {
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}
		if ref.LeaseHolder == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("app@main still leased by %q %s after the shutdown op while a janitor pass blocks; the budget never applied", ref.LeaseHolder, ShutdownBudget)
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.WaitShutdown(ctx); err == nil || !strings.Contains(err.Error(), "janitor pass still running") {
		t.Fatalf("WaitShutdown = %v, want the janitor half-budget error", err)
	}
}
