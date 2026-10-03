package daemon

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
)

// within waits up to 10 s for ch, failing the test if nothing arrives. It
// bounds a wait for something that must happen; it never gives something
// that must not happen time to happen.
func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// goCall sends req on its own connection from a goroutine and delivers the
// response, or the transport error as an error response.
func goCall(sock string, req Request) <-chan Response {
	ch := make(chan Response, 1)
	go func() {
		r, err := rawCall(sock, req)
		if err != nil {
			r = errResp(err)
		}
		ch <- r
	}()
	return ch
}

// holdNextClose parks the next Session.Close to reach
// session.CloseReleaseHook — its engine shut down, its lease still live and
// renewed — until release is called; later closes pass straight through.
// The cleanup releases it and clears the hook. Cleanups run last-registered
// first, so it runs before newServer's Shutdown.
func holdNextClose(t *testing.T) (entered <-chan struct{}, release func()) {
	t.Helper()
	ent, rel := make(chan struct{}), make(chan struct{})
	var taken atomic.Bool
	session.CloseReleaseHook = func() {
		if taken.CompareAndSwap(false, true) {
			close(ent)
			<-rel
		}
	}
	var once sync.Once
	release = func() { once.Do(func() { close(rel) }) }
	t.Cleanup(func() {
		release()
		session.CloseReleaseHook = nil
	})
	return ent, release
}

// watchCloseWaits reports the deadline of every wait that opOpen, opClose or
// Shutdown starts on a closing slot.
func watchCloseWaits(t *testing.T) <-chan time.Time {
	t.Helper()
	ch := make(chan time.Time, 16)
	closeWaitEntered = func(_ string, deadline time.Time) {
		select {
		case ch <- deadline:
		default:
		}
	}
	t.Cleanup(func() { closeWaitEntered = nil })
	return ch
}

// TestReopenDuringCloseWaitsAndGetsAFreshEpoch is the FD-budget review's
// race, made deterministic. An open of a branch whose session is closing
// must wait for the close and then take the lease under a fresh epoch.
// Before the fix, opClose freed the slot before Close ran, so the open
// renewed the closing session's lease in place (same holder, same epoch),
// and the old session's release then cleared the new session's lease.
func TestReopenDuringCloseWaitsAndGetsAFreshEpoch(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	old := getStatus(t, sock, "app", "main")

	entered, release := holdNextClose(t)
	waits := watchCloseWaits(t)
	closed := goCall(sock, Request{Op: "close", DB: "app", Branch: "main"})
	within(t, entered, "the close to reach the release hook")

	reopened := goCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	select {
	case <-waits:
		// The reopen is waiting for the close, as it must.
	case r := <-reopened:
		release()
		<-closed
		ref, _, _ := w.Store.GetRef("app", "main")
		t.Fatalf("open returned %+v while the previous session was still closing; "+
			"afterwards the ref's holder is %q at epoch %d (the closing session's epoch was %d)",
			r, ref.LeaseHolder, ref.Epoch, old.Epoch)
	case <-time.After(10 * time.Second):
		t.Fatal("the reopen neither waited for the close nor returned")
	}

	release()
	if r := within(t, closed, "the close"); !r.OK {
		t.Fatalf("close = %+v", r)
	}
	if r := within(t, reopened, "the reopen"); !r.OK {
		t.Fatalf("reopen = %+v", r)
	}
	cur := getStatus(t, sock, "app", "main")
	if cur.Epoch <= old.Epoch {
		t.Fatalf("reopened at epoch %d, want above the closed session's %d", cur.Epoch, old.Epoch)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != cur.Holder || ref.Epoch != cur.Epoch {
		t.Fatalf("ref lease %q@%d, want the open session's %q@%d", ref.LeaseHolder, ref.Epoch, cur.Holder, cur.Epoch)
	}
}

// closingSession opens db@branch, starts closing it over the socket, and
// returns once that Close is parked in holdNextClose's hook. closed
// delivers the close's response after release.
func closingSession(t *testing.T, sock, db, branch string) (closed <-chan Response, release func()) {
	t.Helper()
	if r := call(t, sock, Request{Op: "open", DB: db, Branch: branch}); !r.OK {
		t.Fatalf("open %s@%s = %+v", db, branch, r)
	}
	entered, release := holdNextClose(t)
	closed = goCall(sock, Request{Op: "close", DB: db, Branch: branch})
	within(t, entered, "the close to reach the release hook")
	return closed, release
}

// slotAt returns the slot srv.sessions holds for k, or nil.
func slotAt(srv *Server, k string) *slot {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.sessions[k]
}

// shrinkCloseWait sets closeWaitBudget for one test. Call it before
// newServer, so that its restore runs after the server has shut down.
func shrinkCloseWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := closeWaitBudget
	closeWaitBudget = d
	t.Cleanup(func() { closeWaitBudget = old })
}

func TestReopenDuringCloseTimesOut(t *testing.T) {
	t.Run("budget", func(t *testing.T) {
		shrinkCloseWait(t, 200*time.Millisecond)
		srv, _ := newServer(t)
		sock := srv.SocketPath()
		closed, release := closingSession(t, sock, "app", "main")

		r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
		if r.OK || r.Error != "daemon: app@main is still closing; retry" {
			t.Fatalf("open during a held close = %+v, want the still-closing refusal", r)
		}
		if sl := slotAt(srv, "app@main"); sl == nil || !sl.isClosing() {
			t.Fatalf("after the timed-out open the slot is %+v, want the closing marker, not a reservation", sl)
		}
		release()
		if r := within(t, closed, "the close"); !r.OK {
			t.Fatalf("close = %+v", r)
		}
		if sl := slotAt(srv, "app@main"); sl != nil {
			t.Fatalf("slot still present after the close: %+v", sl)
		}
		if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
			t.Fatalf("open after the close = %+v", r)
		}
	})

	t.Run("one deadline across iterations", func(t *testing.T) {
		// The budget has to outlast the rest of the first close (renewal
		// join, release, scratch removal) under -race, or the open times out
		// on its first wait and never reaches the second. Equal deadlines
		// still prove the second wait reused the first's.
		shrinkCloseWait(t, 3*time.Second)
		srv, _ := newServer(t)
		sock := srv.SocketPath()
		waits := watchCloseWaits(t)
		closed, release := closingSession(t, sock, "app", "main")

		reopened := goCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
		first := within(t, waits, "the open to wait on the first close")

		// Swap in a second closing marker before the first close finishes, as
		// if another close of the branch had started. The open must wait on it
		// against the deadline it computed on entry, not a fresh one. This also
		// exercises closeSlot's "delete only if the key still holds my marker".
		srv.mu.Lock()
		m1 := srv.sessions["app@main"]
		m2 := &slot{sess: m1.sess, done: make(chan struct{})}
		srv.sessions["app@main"] = m2
		srv.mu.Unlock()
		t.Cleanup(func() { // registered after newServer, so it runs before Shutdown
			srv.mu.Lock()
			if srv.sessions["app@main"] == m2 {
				delete(srv.sessions, "app@main")
			}
			srv.mu.Unlock()
			close(m2.done)
		})

		release()
		if r := within(t, closed, "the first close"); !r.OK {
			t.Fatalf("close = %+v", r)
		}
		second := within(t, waits, "the open to wait on the second close")
		if !second.Equal(first) {
			t.Fatalf("second wait's deadline %v, want the first's %v: one deadline spans every iteration", second, first)
		}
		if r := within(t, reopened, "the open to time out"); r.OK || r.Error != "daemon: app@main is still closing; retry" {
			t.Fatalf("open = %+v, want the still-closing refusal", r)
		}
		if sl := slotAt(srv, "app@main"); sl != m2 {
			t.Fatalf("slot is %+v after the timed-out open, want the second marker untouched", sl)
		}
	})
}

// TestConcurrentReopensDuringCloseOpenOnce: a supervisor reopening the same
// branch from several clients during one close must get exactly one
// session; the rest see the slot the winner reserved.
func TestConcurrentReopensDuringCloseOpenOnce(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	waits := watchCloseWaits(t)
	closed, release := closingSession(t, sock, "app", "main")

	const n = 4
	opens := make([]<-chan Response, n)
	for i := range opens {
		opens[i] = goCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	}
	for i := 0; i < n; i++ {
		within(t, waits, "every reopen to wait on the close")
	}
	release()
	within(t, closed, "the close")

	ok := 0
	for i, ch := range opens {
		r := within(t, ch, "a reopen")
		switch {
		case r.OK:
			ok++
		case r.Error != "daemon: app@main is already open here":
			t.Fatalf("reopen %d = %+v, want success or the already-open refusal", i, r)
		}
	}
	if ok != 1 {
		t.Fatalf("%d reopens succeeded, want exactly 1", ok)
	}
}

// failReleases fails the next n lease-release writes: ref writes that leave
// no lease holder. Nothing else a daemon session writes after its open does
// that, so only Session.Close's release attempts hit it. Install it as
// w.Store.B before the daemon opens any session, and arm it later.
type failReleases struct {
	store.Backend
	n atomic.Int64
}

func (f *failReleases) arm(n int64) { f.n.Store(n) }

func (f *failReleases) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, "refs/") && !carriesLease(data) {
		for {
			cur := f.n.Load()
			if cur <= 0 {
				break
			}
			if f.n.CompareAndSwap(cur, cur-1) {
				return "", errors.New("test: injected release failure")
			}
		}
	}
	return f.Backend.PutIf(key, data, ifMatch)
}

func carriesLease(ref []byte) bool {
	var r struct {
		LeaseHolder string `json:"lease_holder"`
	}
	return json.Unmarshal(ref, &r) == nil && r.LeaseHolder != ""
}

func TestCloseOnClosingSlotWaits(t *testing.T) {
	t.Run("returns the first close's result", func(t *testing.T) {
		srv, _ := newServer(t)
		sock := srv.SocketPath()
		waits := watchCloseWaits(t)
		closed, release := closingSession(t, sock, "app", "main")

		again := goCall(sock, Request{Op: "close", DB: "app", Branch: "main"})
		select {
		case <-waits:
		case r := <-again:
			release()
			t.Fatalf("a second close returned %+v while the first was still running", r)
		case <-time.After(10 * time.Second):
			t.Fatal("the second close neither waited nor returned")
		}
		release()
		if r := within(t, closed, "the first close"); !r.OK {
			t.Fatalf("first close = %+v", r)
		}
		if r := within(t, again, "the second close"); !r.OK {
			t.Fatalf("second close = %+v, want the first close's OK", r)
		}
	})

	t.Run("returns the first close's error and frees the slot", func(t *testing.T) {
		srv, w := newServer(t)
		sock := srv.SocketPath()
		fr := &failReleases{Backend: w.Store.B}
		w.Store.B = fr // before any session exists
		waits := watchCloseWaits(t)
		closed, release := closingSession(t, sock, "app", "main")
		fr.arm(3) // every attempt Close's release makes

		again := goCall(sock, Request{Op: "close", DB: "app", Branch: "main"})
		within(t, waits, "the second close to wait")
		release()
		first := within(t, closed, "the first close")
		second := within(t, again, "the second close")
		if first.OK || !strings.Contains(first.Error, "injected release failure") {
			t.Fatalf("first close = %+v, want the injected release failure", first)
		}
		if second.OK || second.Error != first.Error {
			t.Fatalf("second close = %+v, want the first close's error %q", second, first.Error)
		}
		if sl := slotAt(srv, "app@main"); sl != nil {
			t.Fatalf("a failed close left the slot %+v", sl)
		}
		// The daemon has let go of the branch even though the release failed:
		// its map no longer refuses an open. (Under this daemon's one holder,
		// AcquireLease renews the unreleased lease in place.)
		if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
			t.Fatalf("open after a failed close = %+v", r)
		}
	})

	t.Run("budget", func(t *testing.T) {
		shrinkCloseWait(t, 200*time.Millisecond)
		srv, _ := newServer(t)
		sock := srv.SocketPath()
		closed, release := closingSession(t, sock, "app", "main")
		if r := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}); r.OK || r.Error != "daemon: app@main is still closing; retry" {
			t.Fatalf("second close = %+v, want the still-closing refusal", r)
		}
		release()
		within(t, closed, "the first close")
		if r := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"}); r.OK || r.Error != "daemon: app@main is not open" {
			t.Fatalf("close after the close = %+v, want is not open", r)
		}
	})
}
