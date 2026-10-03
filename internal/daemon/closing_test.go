package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
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
		old, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Fatal(err)
		}

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
		// its map no longer refuses an open. Under this daemon's one holder,
		// AcquireLease renews the unreleased lease in place, so the reopen
		// keeps the closed session's holder and epoch, as reference.md's
		// `session open` and events table say. Per-session holders will make
		// this open wait out the lease instead; update those docs then.
		if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
			t.Fatalf("open after a failed close = %+v", r)
		}
		if cur := getStatus(t, sock, "app", "main"); cur.Holder != old.LeaseHolder || cur.Epoch != old.Epoch {
			t.Fatalf("reopen after a failed release is %q@%d, want the closed session's %q@%d renewed in place",
				cur.Holder, cur.Epoch, old.LeaseHolder, old.Epoch)
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

func TestFlushOnClosingSlotIsRefused(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	closed, release := closingSession(t, sock, "app", "main")
	r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
	release()
	within(t, closed, "the close")
	if r.OK || r.Error != "daemon: app@main is closing" {
		t.Fatalf("flush during a close = %+v, want the closing refusal", r)
	}
}

// TestForkFromClosingSourceIsRefused: Close does not flush, and a flush may
// still be queued on flushMu ahead of it, so an at-rest fork or promote of a
// closing source could miss writes. Both refuse, as for a reserved source.
func TestForkFromClosingSourceIsRefused(t *testing.T) {
	const want = "daemon: app@main is closing; retry when the close finishes"
	for _, tc := range []struct {
		name string
		req  Request
	}{
		{"fork", Request{Op: "fork", DB: "app", Branch: "main", Name: "kid"}},
		{"promote source", Request{Op: "promote", DB: "app", Branch: "main", Name: "target", NoBackup: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, w := newServer(t)
			sock := srv.SocketPath()
			if _, err := w.Fork("app", "main", "target", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			before, _, err := w.Store.GetRef("app", "target")
			if err != nil {
				t.Fatal(err)
			}
			closed, release := closingSession(t, sock, "app", "main")
			r := call(t, sock, tc.req)
			release()
			within(t, closed, "the close")
			if r.OK || r.Error != want {
				t.Fatalf("%s = %+v, want %q", tc.name, r, want)
			}
			if _, _, err := w.Store.GetRef("app", "kid"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("an at-rest fork ran anyway: GetRef(kid) err = %v", err)
			}
			after, _, err := w.Store.GetRef("app", "target")
			if err != nil {
				t.Fatal(err)
			}
			if after.Lineage != before.Lineage || after.HeadTXID != before.HeadTXID {
				t.Fatalf("an at-rest promote ran anyway: target moved from %s@%d to %s@%d",
					before.Lineage, before.HeadTXID, after.Lineage, after.HeadTXID)
			}
		})
	}
}

func TestRollbackDuringCloseIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		closing string // the branch whose session is closing
		req     Request
	}{
		{"rollback", "main", Request{Op: "rollback", DB: "app", Branch: "main", Name: "v1"}},
		{"rollback backup", "main-pre-rollback", Request{Op: "rollback", DB: "app", Branch: "main", Name: "v1"}},
		{"promote target", "target", Request{Op: "promote", DB: "app", Branch: "main", Name: "target"}},
		{"promote target backup", "target-pre-promote", Request{Op: "promote", DB: "app", Branch: "main", Name: "target"}},
		{"compact", "main", Request{Op: "compact", DB: "app", Branch: "main"}},
		{"checkout at rest", "main", Request{Op: "checkout", DB: "app", Branch: "main"}},
		{"destroy", "target", Request{Op: "destroy", DB: "app", Branch: "target"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, w := newServer(t)
			sock := srv.SocketPath()
			for _, b := range []string{"target", "main-pre-rollback", "target-pre-promote"} {
				if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
					t.Fatal(err)
				}
			}
			closed, release := closingSession(t, sock, "app", tc.closing)
			r := call(t, sock, tc.req)
			release()
			within(t, closed, "the close")
			want := fmt.Sprintf("daemon: app@%s is closing; retry when the close finishes", tc.closing)
			if r.OK || r.Error != want {
				t.Fatalf("%s = %+v, want %q", tc.name, r, want)
			}
		})
	}
}

// TestSessionClosedEventFollowsSlotRelease: a client that acts on
// session_closed must find the branch free. The session's own "closed"
// transition fires inside Close, while the closing marker is still in the
// map. To make the ordering deterministic, this test holds srv.mu while the
// close finishes: Close and OnTransition never take it, so the session's
// transition still runs, but closeSlot cannot free the key. A session_closed
// seen at that moment came from inside Close.
func TestSessionClosedEventFollowsSlotRelease(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: "v1"}); !r.OK {
		t.Fatalf("flush v1 = %+v", r)
	}
	before := getStatus(t, sock, "app", "main")

	events, unsubscribe := srv.events.subscribe(64)
	defer unsubscribe()
	prev := session.OnTransition
	closedLogged := make(chan struct{})
	var once sync.Once
	session.OnTransition = func(db, branch, event string, kv []any) {
		prev(db, branch, event, kv)
		if event == "closed" && db == "app" && branch == "main" {
			once.Do(func() { close(closedLogged) })
		}
	}
	t.Cleanup(func() { session.OnTransition = prev })

	entered, release := holdNextClose(t)
	closed := goCall(sock, Request{Op: "close", DB: "app", Branch: "main"})
	within(t, entered, "the close to reach the release hook")

	srv.mu.Lock()
	release()
	select {
	case <-closedLogged:
	case <-time.After(10 * time.Second):
		srv.mu.Unlock()
		t.Fatal("Close never logged its closed transition")
	}
	for drained := false; !drained; {
		select {
		case ev := <-events:
			if ev.Type == "session_closed" {
				srv.mu.Unlock()
				t.Fatal("session_closed was published while app@main's closing marker was still in the session map")
			}
		default:
			drained = true
		}
	}
	srv.mu.Unlock()

	ev := waitForEventType(t, events, "session_closed", 10*time.Second)
	if r := call(t, sock, Request{Op: "rollback", DB: "app", Branch: "main", Name: "v1", NoBackup: true}); !r.OK {
		t.Fatalf("rollback right after session_closed = %+v", r)
	}
	if br := branchInfo(t, call(t, sock, Request{Op: "branches", DB: "app"}), "main"); br.State == "closing" {
		t.Fatalf("branches still says closing after session_closed")
	}
	if ev.Detail["holder"] != before.Holder || fmt.Sprint(ev.Detail["epoch"]) != fmt.Sprint(before.Epoch) {
		t.Fatalf("session_closed detail = %v, want holder %q epoch %d", ev.Detail, before.Holder, before.Epoch)
	}
	if r := within(t, closed, "the close"); !r.OK {
		t.Fatalf("close = %+v", r)
	}
}

// parkedIn reports whether, within 10 s, some goroutine's stack runs
// through every one of fns: a way to see that a goroutine is blocked at a
// particular call, which no channel or hook reports.
func parkedIn(fns ...string) bool {
	buf := make([]byte, 1<<20)
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		n := runtime.Stack(buf, true)
		for n == len(buf) {
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
	stacks:
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			for _, fn := range fns {
				if !strings.Contains(g, fn) {
					continue stacks
				}
			}
			return true
		}
	}
	return false
}

// TestSessionClosedPrecedesTheReopen: closeSlot publishes session_closed
// before it closes done and releases s.mu, so nothing can reserve the
// branch ahead of the event, and a reopen's session_opened always follows
// it. Published after the unlock, a reopen could reserve and publish
// session_opened first, and a subscriber tracking the branch from the
// stream would mark the new session closed. That took the closing
// goroutine being descheduled between the unlock and the publish, so the
// test makes that wait: it holds the bus's lock while the close finishes,
// and once closeSlot is blocked publishing, the slot must still be closing.
func TestSessionClosedPrecedesTheReopen(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	events, unsubscribe := srv.events.subscribe(64)
	defer unsubscribe()
	entered, release := holdNextClose(t)
	waits := watchCloseWaits(t)
	closed := goCall(sock, Request{Op: "close", DB: "app", Branch: "main"})
	within(t, entered, "the close to reach the release hook")
	m := slotAt(srv, "app@main")

	queued := make(chan bool, 1)
	openDelay = func() { // the reopen has just reserved app@main
		for {
			select {
			case ev := <-events:
				if ev.Type == "session_closed" && ev.DB == "app" && ev.Branch == "main" {
					queued <- true
					return
				}
			default:
				queued <- false
				return
			}
		}
	}
	t.Cleanup(func() { openDelay = nil })
	reopened := goCall(sock, Request{Op: "open", DB: "app", Branch: "main"})
	within(t, waits, "the reopen to wait for the close")

	srv.events.mu.Lock()
	release()
	if !parkedIn("daemon.(*eventBus).publish", "daemon.(*Server).closeSlot") {
		srv.events.mu.Unlock()
		t.Fatal("closeSlot never blocked publishing session_closed")
	}
	select {
	case <-m.done:
		srv.events.mu.Unlock()
		t.Fatal("closeSlot let go of app@main before its session_closed was published")
	default:
	}
	srv.events.mu.Unlock()

	if !within(t, queued, "the reopen to reserve app@main") {
		t.Fatal("the reopen reserved app@main before the closing session's session_closed was queued")
	}
	if r := within(t, closed, "the close"); !r.OK {
		t.Fatalf("close = %+v", r)
	}
	if r := within(t, reopened, "the reopen"); !r.OK {
		t.Fatalf("reopen = %+v", r)
	}
}

// TestShutdownDuringClose: a close opClose started is invisible to the old
// drain (the key was already gone), so Shutdown returned and the process
// could exit before that close released its lease. The close is driven by
// a direct srv.opClose, because Shutdown closes every connection.
func TestShutdownDuringClose(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	events, unsubscribe := srv.events.subscribe(64)
	defer unsubscribe()
	entered, release := holdNextClose(t)
	waits := watchCloseWaits(t)
	closed := make(chan Response, 1)
	go func() { closed <- srv.opClose(Request{Op: "close", DB: "app", Branch: "main"}) }()
	within(t, entered, "the close to reach the release hook")

	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- srv.Shutdown(ctx)
	}()
	select {
	case <-waits:
		// Shutdown is waiting on the close opClose started.
	case err := <-shut:
		release()
		t.Fatalf("Shutdown returned (%v) while a close was still in progress", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown neither waited on the close nor returned")
	}
	release()
	if r := within(t, closed, "the close"); !r.OK {
		t.Fatalf("close = %+v", r)
	}
	if err := within(t, shut, "Shutdown"); err != nil {
		t.Fatalf("shutdown = %v", err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("Shutdown returned with the lease still held by %q", ref.LeaseHolder)
	}
	n := 0
	for drained := false; !drained; {
		select {
		case ev := <-events:
			if ev.Type == "session_closed" && ev.DB == "app" && ev.Branch == "main" {
				n++
			}
		default:
			drained = true
		}
	}
	if n != 1 {
		t.Fatalf("session_closed published %d times, want once: Shutdown must not close a closing session again", n)
	}
}

// TestShutdownOpWaitsForSessions: the shutdown op runs Shutdown on a
// goroutine of its own, and WaitShutdown is what serve waits on. It must
// not return until every session has closed and released its lease.
func TestShutdownOpWaitsForSessions(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if _, err := w.Fork("app", "main", "b", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"main", "b"} {
		if r := call(t, sock, Request{Op: "open", DB: "app", Branch: b}); !r.OK {
			t.Fatalf("open %s = %+v", b, r)
		}
	}
	entered, release := holdNextClose(t)
	if r := call(t, sock, Request{Op: "shutdown"}); !r.OK {
		t.Fatalf("shutdown op = %+v", r)
	}
	waited := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		waited <- srv.WaitShutdown(ctx)
	}()
	within(t, entered, "Shutdown to reach a session's release")
	select {
	case err := <-waited:
		release()
		t.Fatalf("WaitShutdown returned (%v) while a session was still closing", err)
	default:
	}
	release()
	if err := within(t, waited, "WaitShutdown"); err != nil {
		t.Fatalf("WaitShutdown = %v", err)
	}
	for _, b := range []string{"main", "b"} {
		ref, _, err := w.Store.GetRef("app", b)
		if err != nil {
			t.Fatal(err)
		}
		if ref.LeaseHolder != "" {
			t.Fatalf("app@%s still leased by %q after WaitShutdown", b, ref.LeaseHolder)
		}
	}
}

// TestSecondShutdownWaitsForTheFirst: a signal arriving while the shutdown
// op's Shutdown is closing sessions calls Shutdown again. That call used to
// return nil at once, so serve exited before the leases were released.
func TestSecondShutdownWaitsForTheFirst(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	entered, release := holdNextClose(t)
	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		first <- srv.Shutdown(ctx)
	}()
	within(t, entered, "the first Shutdown to reach the session's release")

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := srv.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Shutdown during the first = %v, want it to wait and time out on its own ctx", err)
	}
	release()
	firstErr := within(t, first, "the first Shutdown")
	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := srv.Shutdown(ctx); err != firstErr {
		t.Fatalf("a later Shutdown = %v, want the first's result %v", err, firstErr)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("lease still held by %q", ref.LeaseHolder)
	}
}

// TestShutdownReturnsAFailedRelease: Shutdown's result is what serve exits
// with after the shutdown op (through WaitShutdown), and what a later
// Shutdown returns. A close whose release failed every attempt must show up
// in it, whether Shutdown closed that session itself or waited on a close
// opClose had started. Otherwise serve exits 0 and nothing tells the
// operator the branch stays leased for up to a TTL.
func TestShutdownReturnsAFailedRelease(t *testing.T) {
	check := func(t *testing.T, srv *Server, got error) {
		t.Helper()
		if got == nil || !strings.Contains(got.Error(), "injected release failure") {
			t.Fatalf("Shutdown = %v, want the injected release failure", got)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if again := srv.Shutdown(ctx); again != got {
			t.Fatalf("a later Shutdown = %v, want the first's result %v", again, got)
		}
	}

	t.Run("a session Shutdown closes", func(t *testing.T) {
		srv, w := newServer(t)
		sock := srv.SocketPath()
		fr := &failReleases{Backend: w.Store.B}
		w.Store.B = fr // before any session exists
		if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
			t.Fatalf("open = %+v", r)
		}
		fr.arm(3) // every attempt Close's release makes
		if r := call(t, sock, Request{Op: "shutdown"}); !r.OK {
			t.Fatalf("shutdown op = %+v", r)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		check(t, srv, srv.WaitShutdown(ctx))
	})

	t.Run("a close opClose started", func(t *testing.T) {
		srv, w := newServer(t)
		fr := &failReleases{Backend: w.Store.B}
		w.Store.B = fr
		closed, release := closingSession(t, srv.SocketPath(), "app", "main")
		fr.arm(3)
		waits := watchCloseWaits(t)
		shut := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			shut <- srv.Shutdown(ctx)
		}()
		within(t, waits, "Shutdown to wait on the close opClose started")
		release()
		within(t, closed, "the close") // Shutdown closed its connection; only the server side matters
		check(t, srv, within(t, shut, "Shutdown"))
	})
}

// TestShutdownLeavesTheNextDaemonsSocketAlone: `session shutdown` returns as
// soon as the daemon acknowledges it, and closing the listener removes the
// socket file at once, so an operator can start the next `serve` on the
// same path while this daemon is still closing sessions. Shutdown used to
// end by removing the socket path again. Once serve stayed up until every
// close had finished, that removed the next daemon's socket and left it
// running with no way to reach it.
func TestShutdownLeavesTheNextDaemonsSocketAlone(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	entered, release := holdNextClose(t)
	if r := call(t, sock, Request{Op: "shutdown"}); !r.OK {
		t.Fatalf("shutdown op = %+v", r)
	}
	// Sessions start closing only after the listener has closed, so by now
	// the path is free.
	within(t, entered, "Shutdown to reach the session's release")
	next, err := net.Listen("unix", sock)
	if err != nil {
		release()
		t.Fatalf("listening on the socket path while the old daemon closes its sessions: %v", err)
	}
	defer next.Close()
	release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.WaitShutdown(ctx); err != nil {
		t.Fatalf("WaitShutdown = %v", err)
	}
	if !Running(sock) {
		t.Fatal("the old daemon's shutdown removed the socket the next daemon is listening on")
	}
}

// TestReopenWaitingWhenShutdownBeginsIsRefused: an open waiting on a close
// when Shutdown starts must not reserve after closing is set, and Shutdown
// must not wait on it (it is not counted in openWG). Direct calls are used
// because Shutdown closes every connection.
func TestReopenWaitingWhenShutdownBeginsIsRefused(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	entered, release := holdNextClose(t)
	waits := watchCloseWaits(t)
	closed := make(chan Response, 1)
	go func() { closed <- srv.opClose(Request{Op: "close", DB: "app", Branch: "main"}) }()
	within(t, entered, "the close to reach the release hook")
	reopened := make(chan Response, 1)
	go func() { reopened <- srv.opOpen(Request{Op: "open", DB: "app", Branch: "main"}) }()
	within(t, waits, "the reopen to wait")
	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- srv.Shutdown(ctx)
	}()
	within(t, waits, "Shutdown to wait on the same close")
	release()

	within(t, closed, "the close")
	if r := within(t, reopened, "the reopen"); r.OK || r.Error != "daemon: shutting down" {
		t.Fatalf("reopen = %+v, want daemon: shutting down", r)
	}
	if err := within(t, shut, "Shutdown"); err != nil {
		t.Fatalf("shutdown = %v", err)
	}
	if sl := slotAt(srv, "app@main"); sl != nil {
		t.Fatalf("slot left after shutdown: %+v", sl)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("lease still held by %q", ref.LeaseHolder)
	}
}

func TestBranchesReportsClosing(t *testing.T) {
	t.Run("held close", func(t *testing.T) {
		srv, _ := newServer(t)
		sock := srv.SocketPath()
		closed, release := closingSession(t, sock, "app", "main")
		resp := call(t, sock, Request{Op: "branches", DB: "app"})
		release()
		within(t, closed, "the close")
		if br := branchInfo(t, resp, "main"); br.State != "closing" {
			t.Fatalf("state = %q, want closing", br.State)
		}
	})

	t.Run("closing outranks error", func(t *testing.T) {
		srv, w := newServer(t)
		sock := srv.SocketPath()
		sess, err := session.Open(context.Background(), session.Options{
			WS: w, DB: "app", Branch: "main", Holder: "session-a", LeaseTTL: time.Nanosecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sess.Close() })
		if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
			t.Fatal(err)
		}
		if _, err := sess.Flush("", nil); err == nil || sess.Err() == nil {
			t.Fatalf("session must be fenced: flush err %v, Err %v", err, sess.Err())
		}
		m := &slot{sess: sess, done: make(chan struct{})}
		srv.mu.Lock()
		srv.sessions[key("app", "main")] = m
		srv.mu.Unlock()
		t.Cleanup(func() { // runs before newServer's Shutdown
			srv.mu.Lock()
			delete(srv.sessions, key("app", "main"))
			srv.mu.Unlock()
			close(m.done)
		})
		if br := branchInfo(t, call(t, sock, Request{Op: "branches", DB: "app"}), "main"); br.State != "closing" {
			t.Fatalf("state = %q, want closing over error", br.State)
		}
	})
}

func TestStatusReportsClosing(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if _, err := w.Fork("app", "main", "b", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "b"}); !r.OK {
		t.Fatalf("open b = %+v", r)
	}
	closed, release := closingSession(t, sock, "app", "main")
	mainInfo := getStatus(t, sock, "app", "main")
	bInfo := getStatus(t, sock, "app", "b")
	release()
	within(t, closed, "the close")
	if mainInfo.State != SessionStateClosing || mainInfo.Holder == "" {
		t.Fatalf("closing session = %+v, want state closing with its holder", mainInfo)
	}
	if bInfo.State != SessionStateOpen {
		t.Fatalf("open session = %+v, want state open", bInfo)
	}
}

func TestSessionCountAndGaugesExcludeClosing(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	if _, err := w.Fork("app", "main", "b", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "b"}); !r.OK {
		t.Fatalf("open b = %+v", r)
	}
	closed, release := closingSession(t, sock, "app", "main")
	n := srv.sessionCount()
	var buf bytes.Buffer
	werr := srv.WritePrometheus(&buf)
	release()
	within(t, closed, "the close")
	if werr != nil {
		t.Fatal(werr)
	}
	if n != 1 {
		t.Fatalf("sessionCount = %d with one open and one closing, want 1", n)
	}
	out := buf.String()
	if !strings.Contains(out, "offshoot_sessions_open 1\n") {
		t.Fatalf("want offshoot_sessions_open 1:\n%s", out)
	}
	sawB := false
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "offshoot_capture_lag_bytes{") {
			continue
		}
		if strings.Contains(line, `branch="main"`) {
			t.Fatalf("capture lag reported for the closing session: %s", line)
		}
		sawB = sawB || strings.Contains(line, `branch="b"`)
	}
	if !sawB {
		t.Fatalf("no capture lag for the open session:\n%s", out)
	}
}
