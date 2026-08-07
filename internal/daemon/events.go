// Eventing: Milestone 4 Task 4a. An in-daemon event bus, fed from the
// existing session transition-callback call site (Milestone 2's
// logTransition / Milestone 4 Task 2's OnTransition hook — see NewServer's
// wiring below) and from the janitor's reap pass, streamed to subscribers
// two ways: the unix socket's "subscribe" op (handle()'s special-casing,
// mirroring its existing "shutdown" special-case — see server.go) and
// HTTP's `GET /events` (Server-Sent Events, wired into the mux by
// http.go's StartHTTP). Both transports marshal every Event through the
// SAME encodeEvent function (PM Amendment 4: "SSE and socket streams carry
// the SAME event JSON — one encoder").
//
// Global Constraint: "Eventing must never block the daemon: slow
// subscribers get bounded buffers and are DROPPED with a terminal event,
// never back-pressure a session or the janitor." eventBus.publish enforces
// this directly — see its doc comment.
package daemon

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/offshoot-db/offshoot/internal/session"
)

// eventSchemaVersion is Event's "v" field (PM Amendment 4/8: "event schema
// versioned with a v field from day one"). Bump this, and document the
// change, if Event's shape ever changes incompatibly; additive fields
// (like every other wire-compat addition in this codebase) don't need a
// bump.
const eventSchemaVersion = 1

// Event is offshoot's ONE event JSON schema (PM Amendment 4): every event
// this daemon ever emits, over EITHER transport, is one of these, encoded
// by the ONE shared encodeEvent function below.
//
// Type is one of: "session_opened", "flushed", "flush_failed", "fenced",
// "session_closed" (all six sourced from the existing session
// transition-callback call site — see sessionEventType), "reaped" (sourced
// from the janitor's Reap pass — see Server.janitorTick), "evicted"
// (RESERVED: registered here as a valid schema member, but nothing emits
// it yet — Milestone 4 Task 5's ro-cache LRU eviction path is the intended
// source and will call eventBus.publish("evicted", ...) at its own
// eviction call site once that path exists; Task 4a deliberately does not
// stub a dead call site for a feature that doesn't exist yet), and
// "dropped_slow_consumer" (synthetic: the terminal event a subscriber's
// own bus.publish delivers to IT ALONE, immediately before its channel is
// closed — see eventBus.publish's doc comment. Never published to any
// OTHER subscriber; not one of T4a's six "real" source types, but part of
// the same one schema/one encoder discipline).
type Event struct {
	V      int            `json:"v"`
	TS     string         `json:"ts"` // RFC3339, UTC
	Type   string         `json:"type"`
	DB     string         `json:"db,omitempty"`
	Branch string         `json:"branch,omitempty"`
	Detail map[string]any `json:"detail,omitempty"`
}

// newEvent builds an Event stamped with the current time and the locked
// schema version — the one place "v" and "ts" get set, so every emission
// call site below just supplies (type, db, branch, detail).
func newEvent(typ, db, branch string, detail map[string]any) Event {
	return Event{
		V:      eventSchemaVersion,
		TS:     time.Now().UTC().Format(time.RFC3339),
		Type:   typ,
		DB:     db,
		Branch: branch,
		Detail: detail,
	}
}

// encodeEvent is the ONE encoder PM Amendment 4 requires: both
// Server.streamEvents (the unix socket "subscribe" op) and
// Server.handleEvents (HTTP `GET /events`) marshal every Event they send
// through this exact function, so a socket subscriber and an SSE client
// watching the same daemon see byte-identical JSON for the same event —
// see events_test.go's TestSSEParityWithSocketSubscribe.
func encodeEvent(ev Event) ([]byte, error) {
	return json.Marshal(ev)
}

// eventSubscriberBuffer is the bounded per-subscriber channel size — a
// var, not a const, purely so tests can shrink it (events_test.go lowers
// it to force the slow-consumer-drop path deterministically and quickly,
// instead of publishing thousands of events to fill a production-sized
// buffer). 64 in production is generous headroom for a subscriber that's
// merely momentarily behind (a slow disk write, a GC pause) while still
// bounding memory for one that's genuinely stuck or gone.
var eventSubscriberBuffer = 64

// eventSubscriber is one registered subscription: a bounded channel the
// bus fans events into.
type eventSubscriber struct {
	ch chan Event
}

// eventBus is this daemon's in-memory pub/sub hub. Zero subscribers is the
// common case (no one has run `offshoot session subscribe` or opened
// `GET /events`) and costs nothing beyond an empty map iteration per
// publish.
type eventBus struct {
	mu   sync.Mutex
	subs map[*eventSubscriber]struct{}
}

func newEventBus() *eventBus {
	return &eventBus{subs: map[*eventSubscriber]struct{}{}}
}

// subscribe registers a new subscriber with a channel buffered to bufSize
// and returns that channel (receive-only to the caller) plus an
// unsubscribe function. unsubscribe is idempotent and safe to call even
// after the bus has already dropped this same subscriber (publish's
// slow-consumer path removes-and-closes on its own — see publish's doc
// comment); calling it again is a harmless no-op, checked under the same
// lock that guards every other membership change so the two can never
// double-close a channel.
func (b *eventBus) subscribe(bufSize int) (<-chan Event, func()) {
	sub := &eventSubscriber{ch: make(chan Event, bufSize)}
	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	unsubscribe := func() {
		b.mu.Lock()
		if _, ok := b.subs[sub]; ok {
			delete(b.subs, sub)
			close(sub.ch)
		}
		b.mu.Unlock()
	}
	return sub.ch, unsubscribe
}

// publish fans ev out to every current subscriber and NEVER blocks the
// caller — the Global Constraint this whole file exists to satisfy
// ("Eventing must never block the daemon... never back-pressure a session
// or the janitor"). Every send is a non-blocking channel send
// (`select ... default`); a subscriber whose buffer is already full when
// its turn comes is DROPPED on the spot, not retried and not waited on:
//
//  1. Removed from the subscriber set immediately (under the same lock
//     that serializes every publish/subscribe/unsubscribe call, so no
//     other publish can observe it mid-drop).
//  2. One already-queued event is discarded (a non-blocking receive) to
//     make room — this subscriber is being dropped precisely because it
//     could not keep up, so "deliver everything queued so far, in order"
//     is no longer a goal; "tell it clearly that it was dropped" is.
//  3. A single terminal `{type:"dropped_slow_consumer"}` event (this
//     subscriber's own doc comment on Event.Type) is sent — now
//     guaranteed to fit, since (2) just freed a slot and publish is the
//     ONLY writer to this channel (both this send and every subscribe/
//     unsubscribe mutation are gated by b.mu, so nothing else can have
//     refilled it in between).
//  4. The channel is closed. A subscriber's read loop (Server.streamEvents
//     for the unix socket, Server.handleEvents for SSE) treats a closed
//     channel exactly like "nothing more will ever arrive" and ends the
//     stream — see those functions' doc comments.
//
// The whole per-subscriber operation is O(1) and allocation-light (one
// Event literal on the drop path only), so publish's total cost is
// O(subscriber count) with no I/O and no blocking regardless of how many
// subscribers are slow — see events_test.go's
// TestSlowSubscriberDroppedSessionKeepsFlushing for the end-to-end proof
// that a write-heavy session's flushes are unaffected by a subscriber that
// never reads its channel at all.
func (b *eventBus) publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subs {
		select {
		case sub.ch <- ev:
		default:
			delete(b.subs, sub)
			select {
			case <-sub.ch: // best-effort: free a slot for the terminal event
			default:
			}
			terminal := newEvent("dropped_slow_consumer", "", "", nil)
			select {
			case sub.ch <- terminal:
			default:
				// Unreachable in practice (see doc comment: (2) just freed
				// a slot and b.mu excludes any other writer) — but never
				// block the publisher over it either way.
			}
			close(sub.ch)
		}
	}
}

// --- session-transition source -------------------------------------------

// sessionEventType maps a session.OnTransition event name (Milestone 2's
// logTransition strings — "opened", "flushed", "flush-failed", "fenced",
// "closed", "sidecar-refresh-skipped") to this bus's versioned Event.Type,
// or ok=false for a transition this schema has no event for
// ("sidecar-refresh-skipped": an internal bookkeeping detail, not one of
// T4a's six locked source types).
func sessionEventType(event string) (typ string, ok bool) {
	switch event {
	case "opened":
		return "session_opened", true
	case "flushed":
		return "flushed", true
	case "flush-failed":
		return "flush_failed", true
	case "fenced":
		return "fenced", true
	case "closed":
		return "session_closed", true
	default:
		return "", false
	}
}

// kvToDetail converts session.OnTransition's flat key/value slice (see
// that var's doc comment in internal/session/session.go) into Event's
// Detail map — the exact same (event, kv) tuple internal/daemon's metrics
// observer already reads (see metrics.go's observeFlushTransition/
// kvString/kvFloat), just reshaped for JSON rather than picked apart field
// by field.
func kvToDetail(kv []any) map[string]any {
	if len(kv) == 0 {
		return nil
	}
	detail := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			continue
		}
		detail[k] = kv[i+1]
	}
	return detail
}

// publishTransitionEvent is the event-bus half of session.OnTransition —
// see wireEvents for how this is composed alongside metrics.go's
// observeFlushTransition rather than replacing it.
func (s *Server) publishTransitionEvent(db, branch, event string, kv []any) {
	typ, ok := sessionEventType(event)
	if !ok {
		return
	}
	s.events.publish(newEvent(typ, db, branch, kvToDetail(kv)))
}

// wireEvents composes this daemon's event-bus feed onto session.OnTransition
// — called once from NewServer, AFTER srv.metrics.wireHooks has already
// assigned session.OnTransition = m.observeFlushTransition (Milestone 4
// Task 2). OnTransition is a single package-level func var (see its doc
// comment: "last assignment wins... a single process only ever runs one
// daemon Server"), so Task 4a cannot simply assign its own function without
// clobbering Task 2's metrics observer — it must ride the SAME hook the
// same way Task 2 rode logTransition's existing call site, per this task's
// "do NOT restructure session code, ride the existing callback" brief.
// Composing here (call whatever was already assigned, then also publish to
// the bus) is that ride: internal/session is untouched, and both observers
// see every transition.
func (s *Server) wireEvents() {
	prev := session.OnTransition
	session.OnTransition = func(db, branch, event string, kv []any) {
		if prev != nil {
			prev(db, branch, event, kv)
		}
		s.publishTransitionEvent(db, branch, event, kv)
	}
}

// splitKey reverses key(db, branch) -> db+"@"+branch, used to recover the
// per-branch fields for a "reaped" event from Reap's []string result
// (each entry is exactly a key() value — see ops.Workspace.Reap). Safe and
// unambiguous: store.ValidateName restricts db/branch names to
// [a-z0-9-_.], which never includes "@", so the FIRST "@" is always the
// one key() itself inserted.
func splitKey(k string) (db, branch string) {
	if i := strings.IndexByte(k, '@'); i >= 0 {
		return k[:i], k[i+1:]
	}
	return k, ""
}

// --- unix socket: the "subscribe" op --------------------------------------

// handleSubscribeOp is handle()'s special-case for the "subscribe" op —
// mirroring handle()'s existing special-case for "shutdown" (see
// server.go's handle and dispatch's "shutdown" case doc comment for the
// precedent this follows). Like shutdown, subscribe cannot be answered by
// dispatch()'s ordinary one-Request-in-one-Response-out shape: this
// connection is about to stop being a request/response connection
// entirely, and the subscription must be registered in the bus BEFORE the
// ack is sent — otherwise a client that received "ok" could still race a
// same-daemon transition that fires in the window between the ack and the
// subscription actually landing in eventBus.subs, and silently miss it
// (see events_test.go's ordering-sensitive tests, which rely on exactly
// this "subscribed strictly before acked" guarantee to make their
// assertions deterministic rather than racy).
//
// *** DEDICATED CONNECTION REQUIRED ***: once this function sends the ack,
// this connection LEAVES request/response mode for good — see
// streamEvents. Any request/response op sent on this same connection after
// "subscribe" is never read (the streaming loop below never calls
// dec.Decode again). SDKs and any other caller MUST open a fresh, dedicated
// connection for "subscribe" and keep using their original connection (or
// another fresh one) for ordinary ops — exactly like this file's own
// events_test.go tests do, dialing a second connection per test rather
// than reusing the one under test.
func (s *Server) handleSubscribeOp(c net.Conn, enc *json.Encoder) {
	ch, unsubscribe := s.events.subscribe(eventSubscriberBuffer)
	if err := enc.Encode(Response{OK: true}); err != nil {
		unsubscribe()
		return
	}
	s.streamEvents(c, ch, unsubscribe)
}

// streamEvents is the loop handleSubscribeOp hands this connection off to:
// line-per-event JSON (encodeEvent's output, followed by "\n" — this
// package's existing newline-delimited-JSON wire shape, see protocol.go's
// package doc comment) written to c until either the bus drops/closes this
// subscriber (ch is closed — see eventBus.publish) or the client
// disconnects.
//
// Detecting client disconnect needs a small trick: per this op's contract
// (see handleSubscribeOp's doc comment) the client sends nothing more on
// this connection, so there is no Decode call left to notice a hangup via.
// The background goroutine's blocking Read exists purely to observe that:
// it returns (with an error) the instant c is closed, whether that's the
// client hanging up or Server.Shutdown's own close-every-live-connection
// pass (c is still tracked in s.conns for exactly that reason — see
// handle's conns bookkeeping, unchanged by this op).
func (s *Server) streamEvents(c net.Conn, ch <-chan Event, unsubscribe func()) {
	defer unsubscribe()

	gone := make(chan struct{})
	go func() {
		var buf [1]byte
		c.Read(buf[:]) //nolint:errcheck // only used to detect close; error/n both ignored
		close(gone)
	}()

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return // dropped by the bus (terminal event already sent) or unsubscribed
			}
			data, err := encodeEvent(ev)
			if err != nil {
				// Event is a plain JSON-safe struct (string/int/map[string]any)
				// — Marshal failing here would be a logic bug, not a reachable
				// runtime condition (unlike the write below, which fails
				// whenever the client is simply gone).
				return
			}
			data = append(data, '\n')
			if _, err := c.Write(data); err != nil {
				return
			}
		case <-gone:
			return
		}
	}
}

// --- HTTP: GET /events (Server-Sent Events) -------------------------------

// sseKeepaliveInterval is how often GET /events writes a ": ping" SSE
// comment line to keep an otherwise-silent stream alive across whatever
// sits in front of this daemon (PM Amendment 12: "SSE emits periodic
// `: ping` keepalive comments — proxies/kubelets kill silent streams"). A
// var, not a const, so a test can shrink it and observe a ping within the
// test's own short timeout instead of waiting out 15 real seconds — see
// events_test.go's TestSSEKeepalivePing.
var sseKeepaliveInterval = 15 * time.Second

// handleEvents is `GET /events`: Server-Sent Events streaming of the SAME
// Event JSON (via encodeEvent) the unix socket's "subscribe" op streams —
// PM Amendment 4's one-encoder requirement; see
// events_test.go's TestSSEParityWithSocketSubscribe. Wired behind
// requireAuth by http.go's StartHTTP, exactly like /rpc and /metrics
// (Global Constraints: everything but /healthz needs the Bearer token).
//
// *** CLEARS THE PER-REQUEST WRITE DEADLINE ***: http.go's httpWriteTimeout
// doc comment (see the "*** MILESTONE 4 TASK 4a WARNING ***" block there)
// spells out why this is mandatory, not optional polish: http.Server's
// WriteTimeout covers a handler's ENTIRE wall-clock run, with no concept of
// "a stream that's still legitimately sending, just slowly" — every OTHER
// handler on this same http.Server (/rpc, /metrics, /debug/pprof/*) still
// wants that 90s bound, so this handler opts ITSELF out via
// http.ResponseController.SetWriteDeadline(time.Time{}) rather than the
// server-wide constant being raised (which would un-bound every handler,
// including the ones that should stay bounded). See
// TestSSEStreamSurvivesPastWriteTimeout for the structural proof (Task 4a's
// brief: don't actually sleep past 90s in a test — lower httpWriteTimeout
// via its test-only var and stream past THAT instead).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		// Unreachable with net/http's real ResponseWriter (which always
		// implements Flusher for an HTTP/1.1 or HTTP/2 connection) — guards
		// only a hypothetical non-flushing ResponseWriter a future refactor
		// (or an unusual test double) might introduce.
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	// See doc comment above: clear the write deadline BEFORE the first
	// write, and before subscribing, so no event can arrive and be written
	// under the still-armed 90s deadline. A failure here (some
	// ResponseWriter/environment that doesn't support per-connection
	// deadline control) is not fatal: this stream simply stays subject to
	// the ordinary httpWriteTimeout, same as before Task 4a, rather than
	// this handler refusing to serve at all.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})

	// Subscribe BEFORE writing headers, for the identical reason
	// handleSubscribeOp subscribes before acking (see its doc comment): by
	// the time a client observes response headers (its http.Client.Do call
	// returns), the subscription must already be live in the bus, or a
	// same-daemon transition firing in that window would be silently missed
	// — events_test.go's TestSSEParityWithSocketSubscribe relies on this
	// ordering to make its assertions deterministic.
	ch, unsubscribe := s.events.subscribe(eventSubscriberBuffer)
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // nginx: don't buffer an SSE response
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(sseKeepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return // dropped by the bus, or unsubscribed
			}
			data, err := encodeEvent(ev)
			if err != nil {
				return // see streamEvents's identical comment: not reachable in practice
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			// The idiomatic HTTP-side disconnect signal: net/http cancels a
			// request's Context when the client connection goes away. No
			// background-Read trick needed here the way streamEvents needs
			// one for the unix socket (that connection has no equivalent
			// context).
			return
		}
	}
}
