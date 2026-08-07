package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// ---- pure eventBus unit tests (no daemon) --------------------------------

// TestEventBusDropsSlowSubscriberWithTerminalEvent pins eventBus.publish's
// core contract directly, with no daemon/session/socket involved: a
// subscriber whose buffer is full when publish reaches it is removed, sent
// exactly one terminal {type:"dropped_slow_consumer"} event, and then has
// its channel closed — never blocking the publisher.
func TestEventBusDropsSlowSubscriberWithTerminalEvent(t *testing.T) {
	b := newEventBus()
	ch, _ := b.subscribe(2)

	// Fill the buffer without anyone draining it.
	b.publish(newEvent("flushed", "app", "main", nil))
	b.publish(newEvent("flushed", "app", "main", nil))

	// This third publish must not block (the whole point of the test) and
	// must trigger the drop.
	done := make(chan struct{})
	go func() {
		b.publish(newEvent("flushed", "app", "main", nil))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a full subscriber buffer — must never block the caller")
	}

	// Drain everything queued; the LAST thing we ever see must be the
	// terminal event, and the channel must then be closed.
	var events []Event
	for ev := range ch {
		events = append(events, ev)
	}
	if len(events) == 0 {
		t.Fatal("subscriber received nothing before being dropped")
	}
	last := events[len(events)-1]
	if last.Type != "dropped_slow_consumer" {
		t.Fatalf("last event before channel close = %q, want \"dropped_slow_consumer\" (events: %+v)", last.Type, events)
	}
	if last.V != eventSchemaVersion {
		t.Fatalf("terminal event v = %d, want %d", last.V, eventSchemaVersion)
	}

	// A subsequent publish must not panic (the subscriber is gone, not
	// lingering half-removed).
	b.publish(newEvent("flushed", "app", "main", nil))
}

// TestEventBusUnsubscribeIsIdempotentAfterDrop proves unsubscribe (the
// func a real caller's defer always runs) is safe to call even after the
// bus has already dropped-and-closed that same subscriber on its own —
// streamEvents/handleEvents both `defer unsubscribe()` unconditionally.
func TestEventBusUnsubscribeIsIdempotentAfterDrop(t *testing.T) {
	b := newEventBus()
	ch, unsubscribe := b.subscribe(1)
	b.publish(newEvent("flushed", "app", "main", nil))
	b.publish(newEvent("flushed", "app", "main", nil)) // overflow -> dropped
	for range ch {
		// drain to closed
	}
	// Must not panic or double-close.
	unsubscribe()
	unsubscribe()
}

// TestEventBusPublishNeverBlocksManySlowSubscribers is a slightly larger
// version of the drop test: several subscribers never read at all: publish
// must still return promptly for every one of them.
func TestEventBusPublishNeverBlocksManySlowSubscribers(t *testing.T) {
	b := newEventBus()
	for i := 0; i < 20; i++ {
		b.subscribe(1)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			b.publish(newEvent("flushed", "app", "main", nil))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("publish blocked with entirely-unread subscribers")
	}
}

// ---- unix socket "subscribe" op ------------------------------------------

// subscribeSocket dials a fresh, dedicated connection (per the loud
// dedicated-connection requirement in protocol.go/events.go) to sock,
// sends {op:"subscribe"}, and returns a channel of decoded Events read off
// that connection in the background, plus the raw connection (so a test
// can close it to simulate client disconnect). Fails the test if the ack
// isn't ok=true.
func subscribeSocket(t *testing.T, sock string) (net.Conn, <-chan Event) {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(c).Encode(Request{Op: "subscribe"}); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	dec := json.NewDecoder(r)
	var ack Response
	if err := dec.Decode(&ack); err != nil {
		t.Fatalf("decoding subscribe ack: %v", err)
	}
	if !ack.OK {
		t.Fatalf("subscribe ack = %+v, want ok", ack)
	}
	events := make(chan Event, 64)
	go func() {
		defer close(events)
		for {
			var ev Event
			if err := dec.Decode(&ev); err != nil {
				return
			}
			events <- ev
		}
	}()
	return c, events
}

func waitForEventType(t *testing.T, ch <-chan Event, want string, timeout time.Duration) Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("event stream closed before seeing type %q", want)
			}
			if ev.Type == want {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event type %q", want)
		}
	}
}

// TestSubscribeSocketSeesSessionLifecycle is the core acceptance test: a
// subscriber on a dedicated socket connection sees session_opened ->
// flushed -> session_closed, in order, for a real session opened/flushed/
// closed on a SEPARATE connection, matching the daemon's own request/
// response ops exactly as an operator or SDK helper would use them.
func TestSubscribeSocketSeesSessionLifecycle(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	sc, events := subscribeSocket(t, sock)
	defer sc.Close()

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}
	openedEv := waitForEventType(t, events, "session_opened", 10*time.Second)
	if openedEv.DB != "app" || openedEv.Branch != "main" || openedEv.V != eventSchemaVersion {
		t.Fatalf("session_opened event = %+v", openedEv)
	}
	if _, err := parseEventTS(openedEv.TS); err != nil {
		t.Fatalf("session_opened ts %q not RFC3339: %v", openedEv.TS, err)
	}

	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Flush may race capture; retry briefly until the row is durable —
	// same pattern as TestServerOpenFlushStatusClose.
	var txid uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
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
	flushedEv := waitForEventType(t, events, "flushed", 10*time.Second)
	if flushedEv.DB != "app" || flushedEv.Branch != "main" {
		t.Fatalf("flushed event = %+v", flushedEv)
	}
	if kind, _ := flushedEv.Detail["kind"].(string); kind != "manual" {
		t.Fatalf("flushed event detail.kind = %v, want \"manual\" (detail=%+v)", flushedEv.Detail["kind"], flushedEv.Detail)
	}

	cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
	closedEv := waitForEventType(t, events, "session_closed", 10*time.Second)
	if closedEv.DB != "app" || closedEv.Branch != "main" {
		t.Fatalf("session_closed event = %+v", closedEv)
	}
}

func parseEventTS(ts string) (time.Time, error) {
	return time.Parse(time.RFC3339, ts)
}

// TestSubscribeRequiresDedicatedConnection proves the documented contract:
// once "subscribe" is acked, that connection no longer answers ordinary
// ops — a second Request written on the same connection is never decoded
// (the streaming loop never calls dec.Decode again), so a caller that
// tries to reuse the connection just gets silence, never a Response.
func TestSubscribeRequiresDedicatedConnection(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	c, _ := subscribeSocket(t, sock)
	defer c.Close()

	if err := json.NewEncoder(c).Encode(Request{Op: "dbs"}); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var resp Response
	err := json.NewDecoder(c).Decode(&resp)
	if err == nil {
		t.Fatalf("expected no Response on a subscribed connection reused for \"dbs\", got %+v", resp)
	}
}

// TestSubscribeOverHTTPRPCRefused pins dispatch's "subscribe" case: POSTing
// {"op":"subscribe"} to /rpc must be refused with a clear error pointing at
// GET /events, not silently "acked" with nothing ever streamed.
func TestSubscribeOverHTTPRPCRefused(t *testing.T) {
	_, _, base, token := newHTTPServer(t)
	resp := decodeResponse(t, httpRPC(t, base, "Bearer "+token, Request{Op: "subscribe"}))
	if resp.OK {
		t.Fatalf("subscribe over POST /rpc = %+v, want a refusal", resp)
	}
	if !strings.Contains(resp.Error, "GET /events") {
		t.Fatalf("subscribe-over-rpc error = %q, want it to mention GET /events", resp.Error)
	}
}

// ---- slow subscriber: dropped, daemon/session unaffected -----------------

// TestSlowSubscriberDroppedSessionKeepsFlushing is the Global Constraint's
// end-to-end proof: a subscriber that NEVER reads its channel must be
// dropped (with the terminal event) WITHOUT ever slowing or failing a real,
// concurrently write-heavy session's flushes. eventSubscriberBuffer is
// shrunk so the drop is forced quickly and deterministically rather than
// requiring thousands of real events.
func TestSlowSubscriberDroppedSessionKeepsFlushing(t *testing.T) {
	restore := eventSubscriberBuffer
	eventSubscriberBuffer = 2
	t.Cleanup(func() { eventSubscriberBuffer = restore })

	srv, w := newServer(t)
	sock := srv.SocketPath()

	// A subscriber registered directly against the bus and never drained —
	// the slow consumer. (Using the bus directly, rather than a socket
	// connection with a full OS send buffer, is what makes triggering the
	// drop deterministic and fast rather than dependent on OS buffer
	// sizes.)
	slow, _ := srv.events.subscribe(eventSubscriberBuffer)

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout, "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Write-heavy: insert+flush repeatedly. None of this may fail or hang
	// because of the never-drained subscriber above.
	const n = 25
	var lastTXID uint64
	for i := 0; i < n; i++ {
		if out, err := exec.Command("sqlite3", open.Checkout,
			fmt.Sprintf("INSERT INTO t VALUES (%d);", i)).CombinedOutput(); err != nil {
			t.Fatalf("insert %d: %v: %s", i, err, out)
		}
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
		if !resp.OK {
			t.Fatalf("flush %d = %+v (session must keep progressing despite the slow subscriber)", i, resp)
		}
		lastTXID = resp.TXID
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.HeadTXID != lastTXID {
		t.Fatalf("final ref head txid = %d, want %d (last successful flush)", ref.HeadTXID, lastTXID)
	}

	// The slow subscriber must have been dropped along the way: channel
	// closes, with a dropped_slow_consumer terminal event among what it
	// received.
	var gotTerminal bool
	deadline := time.After(5 * time.Second)
drain:
	for {
		select {
		case ev, ok := <-slow:
			if !ok {
				break drain
			}
			if ev.Type == "dropped_slow_consumer" {
				gotTerminal = true
			}
		case <-deadline:
			t.Fatal("slow subscriber's channel never closed — it was never dropped")
		}
	}
	if !gotTerminal {
		t.Fatal("slow subscriber's channel closed without ever receiving the dropped_slow_consumer terminal event")
	}
}

// ---- HTTP: GET /events (SSE) ---------------------------------------------

// sseLines reads resp.Body line by line, sending decoded Events for every
// "data: " line (skipping ": ping" keepalive comments and blank separator
// lines) on the returned channel until the body closes or ctx is done.
func sseEvents(ctx context.Context, resp *http.Response) <-chan Event {
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev Event
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// sseRawLines is like sseEvents but yields every raw line (used by the
// keepalive test, which needs to see ": ping" comment lines that sseEvents
// deliberately filters out).
func sseRawLines(resp *http.Response) <-chan string {
	out := make(chan string, 64)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			out <- sc.Text()
		}
	}()
	return out
}

func getEvents(t *testing.T, base, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET /events status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		resp.Body.Close()
		t.Fatalf("GET /events Content-Type = %q, want text/event-stream", ct)
	}
	return resp
}

// TestEventsRequiresAuth pins Global Constraints: every HTTP request but
// /healthz needs the Bearer token, and /events is no exception.
func TestEventsRequiresAuth(t *testing.T) {
	_, _, base, _ := newHTTPServer(t)
	req, err := http.NewRequest(http.MethodGet, base+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /events with no Authorization header: status = %d, want 401", resp.StatusCode)
	}
}

// TestSSEParityWithSocketSubscribe subscribes BOTH ways (unix socket
// "subscribe" and HTTP GET /events) before driving a real session
// lifecycle, and asserts the two transports observe the identical sequence
// of event types/db/branch — PM Amendment 4's "SSE and socket streams
// carry the SAME event JSON (one encoder)", proven end to end rather than
// just by code inspection.
func TestSSEParityWithSocketSubscribe(t *testing.T) {
	srv, _, base, token := newHTTPServer(t)
	sock := srv.SocketPath()

	sc, socketEvents := subscribeSocket(t, sock)
	defer sc.Close()

	resp := getEvents(t, base, token)
	defer resp.Body.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sseEvs := sseEvents(ctx, resp)

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !cl.OK {
		t.Fatalf("close = %+v", cl)
	}

	wantTypes := []string{"session_opened", "session_closed"}
	for _, want := range wantTypes {
		sockEv := waitForEventType(t, socketEvents, want, 10*time.Second)
		sseEv := waitForEventType(t, sseEvs, want, 10*time.Second)
		if sockEv.DB != sseEv.DB || sockEv.Branch != sseEv.Branch || sockEv.Type != sseEv.Type || sockEv.V != sseEv.V {
			t.Fatalf("socket/SSE parity mismatch for %q: socket=%+v sse=%+v", want, sockEv, sseEv)
		}
	}
}

// TestSSEKeepalivePing proves PM Amendment 12's periodic ": ping" keepalive
// comment is actually emitted, using the test-only sseKeepaliveInterval var
// to shrink the interval instead of waiting out the real 15s default.
func TestSSEKeepalivePing(t *testing.T) {
	restore := sseKeepaliveInterval
	sseKeepaliveInterval = 50 * time.Millisecond
	t.Cleanup(func() { sseKeepaliveInterval = restore })

	_, _, base, token := newHTTPServer(t)
	resp := getEvents(t, base, token)
	defer resp.Body.Close()

	lines := sseRawLines(resp)
	deadline := time.After(5 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("SSE stream closed before a keepalive ping arrived")
			}
			if line == ": ping" {
				return // success
			}
		case <-deadline:
			t.Fatal("timed out waiting for a \": ping\" keepalive comment")
		}
	}
}

// TestSSEStreamSurvivesPastWriteTimeout is Task 4a's required "honest cheap
// proof" that handleEvents actually clears its write deadline: it lowers
// httpWriteTimeout (a test-only var — see http.go) to a small value, keeps
// an SSE stream alive (via keepalive pings, sseKeepaliveInterval also
// shrunk) well past that shrunk timeout, and asserts events published
// AFTER the would-be-cutoff instant are still delivered — proving the
// stream was not hard-cut at httpWriteTimeout, without the test ever
// sleeping anywhere near the real 90s production value.
func TestSSEStreamSurvivesPastWriteTimeout(t *testing.T) {
	restoreWT := httpWriteTimeout
	httpWriteTimeout = 150 * time.Millisecond
	t.Cleanup(func() { httpWriteTimeout = restoreWT })

	restoreKA := sseKeepaliveInterval
	sseKeepaliveInterval = 40 * time.Millisecond
	t.Cleanup(func() { sseKeepaliveInterval = restoreKA })

	srv, _, base, token := newHTTPServer(t)
	sock := srv.SocketPath()

	resp := getEvents(t, base, token)
	defer resp.Body.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := sseEvents(ctx, resp)

	// Stay connected for well over 10x the shrunk WriteTimeout, driven by
	// real events published from a real session lifecycle timed to
	// straddle that window, then assert delivery kept working the whole
	// time — if the deadline were NOT cleared, http.Server would have cut
	// the connection at httpWriteTimeout and this open() (which happens
	// AFTER that instant) would never be observed.
	time.Sleep(500 * time.Millisecond) // >> httpWriteTimeout, proving the connection is still open below
	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	waitForEventType(t, events, "session_opened", 5*time.Second)
}

// TestHTTPPathTrickMatrixCoversEvents extends the existing pprof/rpc path-
// trick coverage (http_test.go's TestHTTPPathTrickMatrixNeverBypassesAuth)
// to /events specifically: no unauthenticated variant of the path should
// ever reach the handler.
func TestHTTPPathTrickMatrixCoversEvents(t *testing.T) {
	_, _, base, _ := newHTTPServer(t)
	for _, p := range []string{"/events", "/events/", "//events", "/Events"} {
		req, err := http.NewRequest(http.MethodGet, base+p, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("unauthenticated GET %s = 200, must never bypass auth", p)
		}
	}
}
