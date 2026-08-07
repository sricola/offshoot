// Package daemon serves offshoot sessions over a unix socket: a long-running
// process holds branch leases, captures WAL continuously, and flushes durable
// snapshots while agents write to their checkouts.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/session"
	"github.com/offshoot-db/offshoot/internal/store"
)

type Server struct {
	ws   *ops.Workspace
	ln   net.Listener
	sock string

	mu sync.Mutex
	// sessions maps "db@branch" to its Session. While an open is in
	// progress the key is present with a nil value: a reservation that
	// claims the slot before the (slow, unlocked) session.Open call runs,
	// so no concurrent request can observe the key as free and start a
	// second Open for the same branch. See opOpen's comment for why this
	// replaces a check-then-open-then-recheck pattern.
	sessions map[string]*session.Session
	closing  bool
	// flushEvery is passed as Options.FlushEvery to every session opOpen
	// opens, so every session this daemon serves gets the same background-
	// flush cadence. 0 (the zero value, matching session.Options' own
	// default) means manual only. Set via SetFlushEvery before Serve starts
	// accepting connections; cmd/offshoot's `serve` command sets it from
	// -flush-every (default 30s) — safe-by-default cadence lives at this
	// daemon boundary, not in the session library's own default.
	flushEvery time.Duration
	// openWG counts in-flight opOpen calls: Add(1) happens under mu at the
	// moment a slot is reserved, Done() happens once that call's bookkeeping
	// has fully resolved (map updated and, if it self-closed, the session
	// actually closed). Shutdown sets closing and waits on openWG before it
	// ever reads or drains the sessions map, so it can never observe (or
	// wipe) a reservation that a still-running opOpen believes it owns. See
	// opOpen and Shutdown.
	openWG sync.WaitGroup

	connMu sync.Mutex
	conns  map[net.Conn]struct{} // live accepted connections; Shutdown closes them all

	// janitorStop, closed by Shutdown, tells StartJanitor's loop to exit;
	// janitorWG lets Shutdown wait for that exit before it proceeds. Both
	// are always initialized (in NewServer) even if StartJanitor is never
	// called, so Shutdown's close+Wait is unconditionally safe.
	janitorStop chan struct{}
	janitorWG   sync.WaitGroup

	// metrics is this daemon's metrics registry (Milestone 4 Task 2) —
	// always non-nil once NewServer returns. See metrics.go's Metrics type
	// and newMetrics/wireHooks for what gets registered and how ops/session
	// instrumentation hooks get wired to it without either of those
	// packages importing internal/metrics.
	metrics *Metrics

	// httpSrv is the optional opt-in HTTP listener (Milestone 4 Task 3) —
	// nil unless StartHTTP has been called. Written once, under s.mu, by
	// StartHTTP before its handler goroutine is spawned (see StartHTTP's
	// doc comment); Shutdown reads it under s.mu too, so a Shutdown racing
	// a StartHTTP call sees either nil (nothing to close) or a fully
	// initialized *http.Server, never a partially-constructed one. See
	// http.go.
	httpSrv *http.Server
	// httpToken is the Bearer token every authenticated HTTP request must
	// present (see http.go's requireAuth). Same single-writer-before-
	// serving contract as httpSrv above.
	httpToken string
	// httpAddr is the HTTP listener's actual bound address (ln.Addr().String()),
	// captured so a caller that bound to "host:0" can discover the
	// OS-assigned port via HTTPAddr(). Same single-writer-before-serving
	// contract as httpSrv/httpToken above.
	httpAddr string
	// httpLog is StartHTTP's resolved log writer (HTTPConfig.Log, or
	// os.Stderr if that was nil) — stored so request-time handlers
	// (handleMetrics's error path, in particular) log to the SAME
	// destination StartHTTP's own startup lines went to, rather than a
	// bare os.Stderr that would silently ignore a caller/test's redirected
	// Log. Read under s.mu (unlike httpSrv/httpToken/httpAddr, which rely
	// on the happens-before-Serve guarantee) since handleMetrics runs on a
	// per-request goroutine with no such guarantee relative to a
	// hypothetical concurrent StartHTTP retry; the read is cheap and rare
	// (only on a WritePrometheus error) so this costs nothing in the
	// common path.
	httpLog io.Writer
}

func key(db, branch string) string { return db + "@" + branch }

func NewServer(ws *ops.Workspace, socketPath string) (*Server, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("daemon: listen %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	srv := &Server{ws: ws, ln: ln, sock: socketPath,
		sessions:    map[string]*session.Session{},
		conns:       map[net.Conn]struct{}{},
		janitorStop: make(chan struct{}),
		metrics:     newMetrics(),
	}
	// Wired here, at construction, before Serve can ever accept a
	// connection or StartJanitor can ever tick — see wireHooks/OnTransition/
	// ObserveFork's doc comments for why process-wide assignment this early
	// is safe and sufficient (one daemon Server per process in production).
	srv.metrics.wireHooks(srv)
	return srv, nil
}

func (s *Server) SocketPath() string { return s.sock }

// SetVersion sets the version string reported by offshoot_build_info
// (default "dev" if never called). Call before Serve starts accepting
// connections, mirroring SetFlushEvery's own single-writer-before-Serve
// contract — cmd/offshoot's `serve` command calls this with the same
// -ldflags-embedded version string `offshoot version` prints.
func (s *Server) SetVersion(v string) { s.metrics.setVersion(v) }

// SetFlushEvery sets the background-flush cadence (session.Options.FlushEvery)
// applied to every session opOpen opens from now on. Call before Serve
// starts accepting connections (or accept that only opens that race after
// the call see the new value — reads and writes here are both guarded by
// s.mu, so no torn read is possible, but ordering relative to concurrent
// opens is otherwise the caller's responsibility). 0 disables auto-flush
// (manual only, matching session.Options' own default).
func (s *Server) SetFlushEvery(d time.Duration) {
	s.mu.Lock()
	s.flushEvery = d
	s.mu.Unlock()
}

// StartJanitor reaps expired branches and runs GC every interval until
// Shutdown. grace is passed to GC (tombstone age before deletion); the
// default in cmd is deliberately generous — the spec requires it to exceed
// the longest plausible in-flight fork. every <= 0 disables the janitor
// entirely (no goroutine is started).
//
// Sessions opened by this daemon hold live leases, so the janitor can never
// reap a branch this daemon is actively writing to (see ops.ReapDeadline: a live
// lease only ever pushes a branch's deadline into the future).
//
// Refuses to start once Shutdown has begun (s.closing): Shutdown's own
// close(s.janitorStop)+janitorWG.Wait() sequence assumes every Add it needs
// to wait for has already happened. A janitorWG.Add racing after Shutdown's
// Wait could observe the counter at zero and return early, or — per
// sync.WaitGroup's contract that a positive-delta Add must happen before any
// Wait call it's meant to be counted by — panic outright. Checking and
// adding under s.mu, the same lock Shutdown takes to set closing, makes the
// two calls linearize: either this call observes closing and backs off, or
// it wins the race and its Add is guaranteed visible to Shutdown's Wait.
func (s *Server) StartJanitor(every, grace time.Duration) {
	if every <= 0 {
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.janitorWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.janitorWG.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-s.janitorStop: // closed by Shutdown before it waits on janitorWG
				return
			case <-t.C:
				s.janitorTick(grace)
			}
		}
	}()
}

// janitorTick runs one reap+GC pass and updates every janitor-sourced
// metric (offshoot_reap_total, offshoot_gc_tombstoned_total,
// offshoot_gc_deleted_total, offshoot_gc_backlog,
// offshoot_janitor_runs_total{result}) from its results — split out of
// StartJanitor's ticker loop so a test can drive exactly one tick
// deterministically instead of waiting on a real ticker. Reap/GC results are
// counted even when they also return an error: both ops.Workspace.Reap and
// ops.Workspace.GC keep processing everything they can and report a partial
// result alongside the first error they hit (see their own doc comments),
// so len(reaped)/tombstoned/deleted are real work actually done, not
// discarded just because something else in the same pass failed.
// offshoot_janitor_runs_total{result} is "error" if EITHER step failed,
// "ok" only if both fully succeeded — a single counter per tick, not one
// per step, matching the locked metric's one label (result), not two.
func (s *Server) janitorTick(grace time.Duration) {
	failed := false

	reaped, reapErr := s.ws.Reap(time.Now())
	if reapErr != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: reap: %v\n", reapErr)
		failed = true
	} else if len(reaped) > 0 {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: reaped %v\n", reaped)
	}
	if len(reaped) > 0 {
		s.metrics.ReapTotal.Add(float64(len(reaped)))
	}

	tombstoned, deleted, gcErr := s.ws.GC(grace)
	if gcErr != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: gc: %v\n", gcErr)
		failed = true
	}
	if tombstoned > 0 {
		s.metrics.GCTombstonedTotal.Add(float64(tombstoned))
	}
	if deleted > 0 {
		s.metrics.GCDeletedTotal.Add(float64(deleted))
	}
	if backlog, err := s.ws.TombstoneBacklog(); err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: janitor: gc backlog: %v\n", err)
	} else {
		s.metrics.GCBacklog.Set(float64(backlog))
	}

	result := "ok"
	if failed {
		result = "error"
	}
	s.metrics.JanitorRunsTotal.WithLabelValues(result).Inc()
}

func (s *Server) Serve() error {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			return err
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	s.connMu.Lock()
	s.conns[c] = struct{}{}
	s.connMu.Unlock()
	defer func() {
		s.connMu.Lock()
		delete(s.conns, c)
		s.connMu.Unlock()
		c.Close()
	}()
	dec := json.NewDecoder(c)
	enc := json.NewEncoder(c)
	for {
		var req Request
		if err := dec.Decode(&req); err != nil {
			return // client hung up or sent garbage
		}
		resp := s.dispatch(req)
		if req.Op == "shutdown" && shutdownRespondDelay != nil {
			shutdownRespondDelay() // test hook; nil (a no-op) in production
		}
		// encErr is deliberately not checked before triggering Shutdown
		// below: a "shutdown" request must shut the daemon down whether or
		// not we could tell the requester so (e.g. it already hung up).
		// What matters is ORDER, not the outcome of the write: Shutdown's
		// connection-close pass must never start until the attempt to
		// write this very response has already finished. See dispatch's
		// "shutdown" case for why that attempt is no longer allowed to
		// race Shutdown itself.
		encErr := enc.Encode(resp)
		if req.Op == "shutdown" {
			go s.Shutdown(context.Background())
			return
		}
		if encErr != nil {
			return
		}
	}
}

// shutdownRespondDelay, when non-nil, is invoked by handle after
// dispatching a "shutdown" request but before writing that request's
// response. It exists purely for tests: forcing a delay here lets a test
// prove (or, against the pre-fix code, disprove) that Shutdown's
// connection-close pass cannot run until after the shutdown response has
// actually been written -- see TestShutdownRespondsBeforeClosingRequestingConn.
// Nil (the default) is a no-op and imposes no cost in production.
var shutdownRespondDelay func()

func (s *Server) dispatch(req Request) Response {
	switch req.Op {
	case "open":
		return s.opOpen(req)
	case "flush":
		return s.opFlush(req)
	case "status":
		return s.opStatus()
	case "close":
		return s.opClose(req)
	case "shutdown":
		// Do NOT trigger Shutdown here. It used to be `go s.Shutdown(...)`
		// followed by returning this response, which raced Shutdown's
		// connection-close pass (it force-closes every live connection,
		// including this request's own) against handle's write of this
		// very response on that same connection. Under normal scheduling
		// the write usually won, so the bug only showed up as an
		// occasional CI flake on a loaded runner: the CLI would see
		// "daemon: reading response: EOF" instead of {OK: true}, because
		// Shutdown closed the socket before (or during) the write. handle
		// now triggers Shutdown itself, strictly after attempting to write
		// this response — see handle's shutdown handling and
		// TestShutdownRespondsBeforeClosingRequestingConn.
		return Response{OK: true}
	case "create":
		return s.opCreate(req)
	case "checkout":
		return s.opCheckoutAtRest(req)
	case "fork":
		return s.opFork(req)
	case "destroy":
		return s.opDestroy(req)
	case "rollback":
		return s.opRollback(req)
	case "promote":
		return s.opPromote(req)
	case "touch":
		return s.opTouch(req)
	case "branches":
		return s.opBranches(req)
	case "dbs":
		return s.opDbs()
	case "export":
		return s.opExport(req)
	case "checkout-at":
		return s.opCheckoutAt(req)
	default:
		return errResp(fmt.Errorf("daemon: unknown op %q", req.Op))
	}
}

func errResp(err error) Response { return Response{Error: err.Error()} }

// opOpen claims db@branch, or refuses if it is already open here (or leased
// elsewhere, surfaced as session.Open's error).
//
// Race structure: a naive "check the map, unlock, call session.Open, lock,
// recheck the map" would let two concurrent opens for the SAME key both pass
// the first (unlocked) check and both call session.Open concurrently. That
// is not merely wasteful here: session.Open's checkout path is a single
// fixed file per db@branch (ops.Workspace.CheckoutPath), so two concurrent
// Opens would materialize over the same file and then run two independent
// capture engines against it at once — a real on-disk race, not just a
// duplicate lease. It also isn't fenced by the lease itself: both Opens use
// the same holder identity (ops.LocalHolder(), constant for this whole
// process), and store.AcquireLease treats a re-acquisition by the same live
// holder as an idempotent self-renew rather than a conflict (see
// internal/store/lease.go), so the second Open's AcquireLease call can
// succeed too instead of failing fast.
//
// This function avoids the race by reserving the map slot (writing a nil
// value under s.mu) before releasing the lock and calling session.Open. A
// concurrent opOpen for the same key then sees the key already present and
// is refused immediately, with no second session.Open ever starting for
// that branch. The lock is not held across session.Open itself (which does
// real I/O — materializing a checkout — and can be slow), so unrelated keys'
// opens/flushes/status/close calls are not blocked while one open is in
// flight.
//
// This same reservation is also what Shutdown must not clobber. opOpen
// checks s.closing (under s.mu) before it reserves anything, so once
// Shutdown has set closing no new reservation can be created — and it counts
// every reservation it does create in s.openWG, released only once that
// open's outcome (map bookkeeping, and any self-close below) is fully
// resolved, so Shutdown can wait for exactly the in-flight opens that could
// still touch the map. See Shutdown.
func (s *Server) opOpen(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	k := key(req.DB, branch)

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errResp(fmt.Errorf("daemon: shutting down"))
	}
	if _, exists := s.sessions[k]; exists {
		s.mu.Unlock()
		return errResp(fmt.Errorf("daemon: %s is already open here", k))
	}
	s.sessions[k] = nil // reserve the slot
	s.openWG.Add(1)
	flushEvery := s.flushEvery
	s.mu.Unlock()

	if openDelay != nil {
		openDelay() // test hook; nil (a no-op) in production
	}

	sess, err := session.Open(context.Background(), session.Options{
		WS: s.ws, DB: req.DB, Branch: branch, FlushEvery: flushEvery,
	})

	s.mu.Lock()
	if err != nil {
		delete(s.sessions, k) // release the reservation
		s.openWG.Done()
		s.mu.Unlock()
		return errResp(err)
	}
	if s.closing {
		// Shutdown started while this open was in flight and is, right now,
		// blocked on s.openWG waiting for exactly this call to resolve
		// before it drains the map. Don't leave the newly-acquired lease
		// orphaned: self-close, and only mark this open done (openWG.Done)
		// once that close has actually completed, so Shutdown cannot return
		// — and cannot decide there is nothing left to close — until the
		// lease this open just took is released.
		delete(s.sessions, k)
		s.mu.Unlock()
		sess.Close()
		s.openWG.Done()
		return errResp(fmt.Errorf("daemon: shutting down"))
	}
	s.sessions[k] = sess
	s.openWG.Done()
	s.mu.Unlock()
	return Response{OK: true, Checkout: sess.CheckoutPath()}
}

// openDelay, when non-nil, is invoked by opOpen after it reserves a slot and
// before it calls the slow session.Open. It exists purely for tests: setting
// it lets a test hold an open in flight so Shutdown's handling of in-flight
// opens can be exercised deterministically instead of relying on timing. Nil
// (the default) is a no-op and imposes no cost in production.
var openDelay func()

// lookup returns the live session for db@branch, or an error if it is not
// open (including if an open is still in flight — a reserved-but-nil slot
// counts as not yet open).
func (s *Server) lookup(db, branch string) (*session.Session, error) {
	if branch == "" {
		branch = "main"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[key(db, branch)]
	if !ok || sess == nil {
		return nil, fmt.Errorf("daemon: %s is not open", key(db, branch))
	}
	return sess, nil
}

// opFlush ships the session's current state, optionally as a named
// checkpoint (req.Name), optionally carrying req.Meta on that checkpoint
// (Session.Flush rejects a non-empty Meta with an empty Name — there's no
// checkpoint for it to attach to). There is deliberately no separate
// "checkpoint" daemon op: a live session's checkpoint IS a named flush (see
// ops.Workspace.Checkpoint's own doc comment on why its raw checkout access
// is unsafe against a live session) — this is also why req.Meta rides here
// rather than a dedicated op, giving the daemon-session path parity with
// ops.Workspace.Checkpoint's own meta param.
func (s *Server) opFlush(req Request) Response {
	sess, err := s.lookup(req.DB, req.Branch)
	if err != nil {
		return errResp(err)
	}
	txid, err := sess.Flush(req.Name, req.Meta)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, TXID: txid}
}

// opStatus snapshots the session pointers under s.mu, then builds each
// SessionInfo OUTSIDE the lock. sess.CaptureLag() stats the WAL file (real
// disk I/O — see capture.Engine.Lag), and s.mu is the single lock every
// other RPC (open/flush/close/create/...) also needs to touch the sessions
// map; building status responses while holding it would serialize every
// concurrent request in this daemon behind however long that I/O (repeated
// once per open session) happens to take. See review finding IMPORTANT-2 on
// task 7.
//
// A session snapshotted here can concurrently Close() while its SessionInfo
// is still being built below — that is fine and deliberate, not a race left
// unfixed: every Session accessor used below (DB/Branch/CheckoutPath/
// Lease/DurableTXID/CaptureLag/LastFlush/LastFlushErr/Err) reads the
// session's own internal, still-valid mutex-protected state and remains
// safe to call after Close (CaptureLag's WAL stat simply fails closed to 0
// once the scratch dir is gone — "nothing outstanding", exactly Lag's
// ordinary missing-file case). The cost is ordinary snapshot semantics: this
// response can list a session that finished closing a moment ago, the same
// staleness any status endpoint already has the instant its answer is sent
// over the wire — not a new correctness gap this introduces.
func (s *Server) opStatus() Response {
	s.mu.Lock()
	keys := make([]string, 0, len(s.sessions))
	for k, sess := range s.sessions {
		if sess == nil {
			continue // open still in flight; nothing to report yet
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sessList := make([]*session.Session, 0, len(keys))
	for _, k := range keys {
		sessList = append(sessList, s.sessions[k])
	}
	s.mu.Unlock()

	infos := make([]SessionInfo, 0, len(sessList))
	for _, sess := range sessList {
		info := SessionInfo{
			DB: sess.DB(), Branch: sess.Branch(), Checkout: sess.CheckoutPath(),
			Holder: sess.Lease().Holder, Epoch: sess.Lease().Epoch,
			DurableTXID: sess.DurableTXID(),
			CaptureLag:  sess.CaptureLag(),
		}
		if t, _, ok := sess.LastFlush(); ok {
			info.LastFlushAt = t.Format(time.RFC3339)
			info.DurableAge = time.Since(t).Round(time.Second).String()
		}
		if err := sess.LastFlushErr(); err != nil {
			info.FlushError = err.Error()
		}
		if err := sess.Err(); err != nil {
			info.Error = err.Error()
		}
		infos = append(infos, info)
	}
	return Response{OK: true, Sessions: infos}
}

// sessionCount returns the number of FULLY OPEN sessions (a reserved-but-
// nil in-flight-open slot does not count, matching opStatus's own
// treatment) — backs GET /healthz's `sessions` field.
func (s *Server) sessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sess := range s.sessions {
		if sess != nil {
			n++
		}
	}
	return n
}

func (s *Server) opClose(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	k := key(req.DB, branch)
	s.mu.Lock()
	sess, ok := s.sessions[k]
	if ok && sess != nil {
		delete(s.sessions, k)
	}
	s.mu.Unlock()
	if !ok || sess == nil {
		return errResp(fmt.Errorf("daemon: %s is not open", k))
	}
	if err := sess.Close(); err != nil {
		return errResp(err)
	}
	return Response{OK: true}
}

// sessionState is what this daemon currently has, if anything, for a given
// db@branch key in s.sessions.
type sessionState int

const (
	// sessionAbsent: no entry — nothing here has any claim on this branch.
	sessionAbsent sessionState = iota
	// sessionReserved: the key is present but the value is nil — opOpen has
	// reserved the slot and is still inside its (slow, unlocked)
	// session.Open call materializing the checkout. There is no live
	// *session.Session to hand out yet, but the branch is very much
	// spoken-for: session.Open is, right now, writing the exact checkout
	// file a rollback/promote/checkout-at-rest would also write.
	sessionReserved
	// sessionOpen: a live session is open here.
	sessionOpen
)

// lookupSessionState returns the current sessionState for db@branch and, iff
// that state is sessionOpen, the live *session.Session (nil otherwise). It
// does not default branch to "main" — callers that want that default apply
// it themselves before calling, exactly as opOpen/lookup/opClose already do;
// opPromote deliberately does NOT default its target, so it must not be
// defaulted here either (see opPromote).
func (s *Server) lookupSessionState(db, branch string) (sessionState, *session.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[key(db, branch)]
	if !ok {
		return sessionAbsent, nil
	}
	if sess == nil {
		return sessionReserved, nil
	}
	return sessionOpen, sess
}

// branchState computes db@branch's full state for opBranches: this is the
// daemon half of the split ops.BranchStateAt's doc comment documents — ops
// computes everything derivable from ref+sidecar alone (active/dirty/
// detached/idle); only a daemon knows its own in-memory session map, so
// only a daemon can additionally say "pending" (a slot is reserved, mid-
// Open) or "error" (an open session's Err() has gone non-nil). ref is the
// caller's already-fetched GetRef (opBranches always has one in hand) so
// this never issues a second store read of its own.
//
// Precedence: error and pending are session-derived and, when either
// applies, win outright over whatever ops.BranchStateAt would have said —
// see BranchStateAt's doc comment for the full six-state precedence list.
// The two can never both apply to the SAME db@branch at once: s.sessions
// holds at most one entry per key, either a nil (reserved) or non-nil
// (open) value, never both — so this is a simple switch, not a priority
// comparison between the two. An OPEN, HEALTHY session needs no daemon-side
// casing at all: its own live lease is exactly what already makes
// ops.BranchStateAt itself report "active".
func (s *Server) branchState(db, branch string, ref store.Ref) string {
	switch state, sess := s.lookupSessionState(db, branch); state {
	case sessionOpen:
		if sess.Err() != nil {
			return "error"
		}
	case sessionReserved:
		return "pending"
	}
	return ops.BranchStateAt(ref, s.ws.CheckoutPath(db, branch), time.Now())
}

// refuseIfClaimed refuses (as a "close the session first" error) if this
// daemon has any claim on db@branch — a live session OR an in-flight
// reservation. Guard for ops (checkout/destroy/rollback/promote-target)
// that must not run concurrently with anything that could still be
// materializing or holding that branch's checkout file: treating a bare
// reservation as "not open" here would let a rollback/promote/checkout race
// session.Open's unlocked materialize step and corrupt the same on-disk
// file two ways at once (see lookupSessionState's sessionReserved case).
func (s *Server) refuseIfClaimed(db, branch string) error {
	if st, _ := s.lookupSessionState(db, branch); st != sessionAbsent {
		return fmt.Errorf("daemon: %s is open here; close the session first", key(db, branch))
	}
	return nil
}

// flushIfOpen is fork/promote's "flush the source first" step. If db@branch
// is fully open here, it flushes (unnamed) and returns any flush error. If
// it is only reserved (an open is still in flight — no live session exists
// to flush, and proceeding at-rest would race that in-flight session.Open
// the same way refuseIfClaimed's callers must not), it refuses with the
// same "close the session first"-class error rather than silently falling
// through to an at-rest fork/promote. If absent, it does nothing and the
// caller proceeds at-rest.
func (s *Server) flushIfOpen(db, branch, opName string) error {
	switch st, sess := s.lookupSessionState(db, branch); st {
	case sessionOpen:
		if _, err := sess.Flush("", nil); err != nil {
			return fmt.Errorf("daemon: flush %s before %s: %w", key(db, branch), opName, err)
		}
	case sessionReserved:
		return fmt.Errorf("daemon: %s is open here; close the session first", key(db, branch))
	}
	return nil
}

// opCreate creates a fresh db (main branch at TXID 1). Validation
// (name shape) lives in ops.Create; this handler stays thin.
func (s *Server) opCreate(req Request) Response {
	if err := s.ws.Create(req.DB); err != nil {
		return errResp(err)
	}
	return Response{OK: true}
}

// opCheckoutAtRest materializes db@branch's head snapshot to its fixed
// checkout path (ops.Checkout) and returns that path. Refuses if this
// daemon already has an open session on that branch — the session already
// owns that checkout path and is the one place writes to it should go
// through.
func (s *Server) opCheckoutAtRest(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	if err := s.refuseIfClaimed(req.DB, branch); err != nil {
		return errResp(err)
	}
	path, err := s.ws.Checkout(req.DB, branch)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, Checkout: path}
}

// opFork creates req.Name as a new branch forked from req.Branch (source)
// at checkpoint req.From ("" = source's head), with TTL req.TTL ("" = no
// TTL; else a Go duration), and req.Meta (nil = none) stored on the new
// branch's Ref.Meta (see ops.Workspace.Fork's doc comment for the cap). A
// non-positive duration (<= 0) is refused: fork has no "none" sentinel the
// way touch does (a brand-new branch has no
// existing TTL to explicitly clear), so a caller that means "no TTL" omits
// req.TTL entirely rather than sending a zero or negative one — sending one
// anyway is almost certainly a mistake (e.g. an unintended negative
// duration string), and Fork itself would otherwise silently treat it as no
// TTL, the exact silent-swallow this check exists to prevent. If this
// daemon has an open session on the source, that session is flushed
// (unnamed) first so the fork point includes writes the caller never
// explicitly flushed — matching Fork's semantics for a session that owns
// the source's checkout. req.Meta is validated (ops.ValidateMeta) BEFORE
// that flush: an over-cap meta is a caller mistake that Fork would reject
// anyway, and rejecting it before flushIfOpen means an over-cap fork
// against an open session never triggers store I/O (and never advances the
// session's flushed head) for a call that was only going to unwind on the
// next line regardless. Returns the fork point's txid.
func (s *Server) opFork(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	var ttl time.Duration
	if req.TTL != "" {
		d, err := time.ParseDuration(req.TTL)
		if err != nil {
			return errResp(fmt.Errorf("daemon: invalid ttl %q: %w", req.TTL, err))
		}
		if d <= 0 {
			return errResp(fmt.Errorf(
				"daemon: fork ttl %q must be positive; fork has no \"none\" concept, omit ttl for no TTL", req.TTL))
		}
		ttl = d
	}
	if err := ops.ValidateMeta(req.Meta); err != nil {
		return errResp(err)
	}
	if err := s.flushIfOpen(req.DB, branch, "fork"); err != nil {
		return errResp(err)
	}
	txid, err := s.ws.Fork(req.DB, branch, req.Name, req.From, ttl, req.Meta)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, TXID: txid}
}

// opDestroy deletes db@branch (ops.Destroy; req.Force overrides the
// protected-branch and live-lease refusals ops.Destroy itself enforces).
// Refuses if this daemon has an open session on the branch — closing it
// first, not force, is the way to destroy a branch you're writing to here.
func (s *Server) opDestroy(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	if err := s.refuseIfClaimed(req.DB, branch); err != nil {
		return errResp(err)
	}
	if err := s.ws.Destroy(req.DB, branch, req.Force); err != nil {
		return errResp(err)
	}
	return Response{OK: true}
}

// opRollback repoints db@branch at checkpoint req.Name (ops.Rollback) and
// returns the refreshed checkout path. Refuses if this daemon has an open
// session on the branch, for the same reason as opDestroy: the session
// owns the checkout that a rollback would repoint out from under it.
func (s *Server) opRollback(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	if err := s.refuseIfClaimed(req.DB, branch); err != nil {
		return errResp(err)
	}
	path, err := s.ws.Rollback(req.DB, branch, req.Name)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, Checkout: path}
}

// opPromote repoints db@req.Name (target) at db@req.Branch's (source) head
// (ops.Promote; req.Force overrides the protected-target refusal). Refuses
// if this daemon has any claim — open session or in-flight reservation —
// on the TARGET, matching opDestroy and opRollback's guard. If the SOURCE
// has an open session here, it is flushed (unnamed) first, matching
// opFork, so an un-flushed write on the source still lands; a SOURCE that
// is merely reserved (open in flight) is refused the same way, since there
// is no live session yet to flush. Returns the promoted txid.
//
// target is NOT defaulted to "main" the way Branch (source) is elsewhere:
// an omitted req.Name must fail closed via ops.Promote's own
// store.ValidateName("") rather than silently promoting onto "main" — a
// dropped field in a client bug must not become a destructive default,
// especially once req.Force can override main's protected-branch guard.
func (s *Server) opPromote(req Request) Response {
	source := req.Branch
	if source == "" {
		source = "main"
	}
	target := req.Name
	if err := s.refuseIfClaimed(req.DB, target); err != nil {
		return errResp(err)
	}
	if err := s.flushIfOpen(req.DB, source, "promote"); err != nil {
		return errResp(err)
	}
	txid, err := s.ws.Promote(req.DB, source, target, req.Force)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, TXID: txid}
}

// opTouch resets db@branch's activity clock and optionally sets/clears its
// TTL (ops.Touch): req.TTL == "" keeps the current TTL, "none" clears it,
// anything else is parsed as a Go duration and becomes the new TTL. A
// parse failure, or a parsed duration <= 0, is reported as ok=false rather
// than silently ignored or silently aliased to "none" — a zero or negative
// duration is not a real TTL, and a caller who means "clear it" should say
// "none" and get exactly that, not an implicit alias. Returns nothing
// beyond ok.
func (s *Server) opTouch(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	var ttl *time.Duration
	switch req.TTL {
	case "":
		ttl = nil
	case "none":
		var zero time.Duration
		ttl = &zero
	default:
		d, err := time.ParseDuration(req.TTL)
		if err != nil {
			return errResp(fmt.Errorf("daemon: invalid ttl %q: %w", req.TTL, err))
		}
		if d <= 0 {
			return errResp(fmt.Errorf("daemon: ttl %q must be positive; use \"none\" to clear the ttl", req.TTL))
		}
		ttl = &d
	}
	if _, err := s.ws.Touch(req.DB, branch, ttl, time.Now()); err != nil {
		return errResp(err)
	}
	return Response{OK: true}
}

// opBranches lists every branch of req.DB as BranchInfo, sorted by branch
// name. TTLRemaining is computed with ops.FormatTTLRemaining — the same
// helper Status uses — so a client sees exactly how long it has before a
// branch becomes reap-eligible, computed identically to the janitor's own
// Reap. Errors (rather than reporting ok=true with an empty list) if req.DB
// has no branches at all: ListRefs only ever has a key for a db that has
// (or had) at least one ref, so an absent key means the db was never
// created, or every branch has already been destroyed — either way, not a
// db a client asking for its branches should be quietly told "empty".
//
// A branch that ListRefs saw but is gone by the time its own GetRef runs is
// skipped rather than failing the whole listing: with the janitor reaping
// branches on its own schedule, that window is a real, recurring race, not
// a one-off — see reapOne's identical ErrNotFound-is-benign handling for the
// same race on the reap side.
func (s *Server) opBranches(req Request) Response {
	refs, err := s.ws.Store.ListRefs()
	if err != nil {
		return errResp(err)
	}
	branches, ok := refs[req.DB]
	if !ok {
		return errResp(fmt.Errorf("daemon: no such db %q", req.DB))
	}
	now := time.Now()
	infos := make([]BranchInfo, 0, len(branches))
	for _, br := range branches {
		ref, _, err := s.ws.Store.GetRef(req.DB, br)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			return errResp(err)
		}
		cps := make([]string, 0, len(ref.Checkpoints))
		for name := range ref.Checkpoints {
			cps = append(cps, name)
		}
		sort.Strings(cps)
		// CheckpointsV2 carries the same names, in the same sorted order, as
		// Checkpoints above, plus each one's txid/created_at — built from the
		// same cps slice so the two can never disagree on membership or order.
		cpsV2 := make([]CheckpointInfo, 0, len(cps))
		for _, name := range cps {
			cp := ref.Checkpoints[name]
			cpsV2 = append(cpsV2, CheckpointInfo{Name: name, TXID: cp.TXID, CreatedAt: cp.CreatedAt})
		}
		infos = append(infos, BranchInfo{
			Branch: br, HeadTXID: ref.HeadTXID, Protected: ref.Protected,
			TTL: ref.TTL, TTLRemaining: ops.FormatTTLRemaining(ref, now), LeaseHolder: ref.LeaseHolder,
			Checkpoints: cps, TouchedAt: ref.TouchedAt, CheckpointsV2: cpsV2,
			State: s.branchState(req.DB, br, ref),
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Branch < infos[j].Branch })
	return Response{OK: true, Branches: infos}
}

// opDbs lists every database this store has at least one ref for —
// store.Store.ListRefs's keys, sorted. Unlike opBranches, an empty result is
// not an error: a fresh store with nothing created yet legitimately has no
// databases, and a cleanup job enumerating what exists (the motivating case
// for this op — see the design spec's list-databases note) needs to be able
// to tell "nothing here" from a failure.
func (s *Server) opDbs() Response {
	refs, err := s.ws.Store.ListRefs()
	if err != nil {
		return errResp(err)
	}
	dbs := make([]string, 0, len(refs))
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	return Response{OK: true, Databases: dbs}
}

// opExport materializes db@branch's state at checkpoint req.Name ("" =
// head) to a plain SQLite file at req.Path (ops.Workspace.Export — refuses
// to overwrite an existing file there unless req.Force; writes atomically,
// no sidecar, no lease — see Export's own doc comment for the exact
// semantics). req.Path must be ABSOLUTE (see Request.Path's doc comment for
// the trust model this enforces); a relative path is refused outright
// rather than resolved against the daemon's own working directory.
//
// Deliberately UNGUARDED by refuseIfClaimed, unlike opCheckoutAtRest/
// opRollback/opPromote: Export reads the STORE (the branch's last durable
// chain), never the live checkout, so an open session on db@branch runs
// concurrently with an export of that same branch with no conflict — but
// also with a real consequence worth stating plainly: that session's
// UNFLUSHED writes are invisible to this export. Export always reflects
// the branch's last DURABLE state; a caller that needs in-flight session
// writes included must flush (or checkpoint) first.
func (s *Server) opExport(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	if !filepath.IsAbs(req.Path) {
		return errResp(fmt.Errorf("daemon: export path %q must be absolute", req.Path))
	}
	if err := s.ws.Export(req.DB, branch, req.Name, req.Path, req.Force); err != nil {
		return errResp(err)
	}
	return Response{OK: true}
}

// opCheckoutAt materializes db@branch's state at checkpoint req.Name into
// its dedicated read-only cache file (ops.Workspace.CheckoutAt) — a path
// distinct from, and never touching, the writable checkout a live session
// or opCheckoutAtRest owns. That separation is exactly why this handler,
// unlike opCheckoutAtRest/opRollback/opPromote, needs no refuseIfClaimed
// guard: it is safe to run alongside an open session on the SAME branch.
// req.Force re-materializes an already-cached file (and re-reads the
// store to do it); otherwise a cache hit is returned as-is with no store
// access at all — see CheckoutAt's own doc comment.
func (s *Server) opCheckoutAt(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	path, err := s.ws.CheckoutAt(req.DB, branch, req.Name, req.Force)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, Checkout: path}
}

// Shutdown stops the janitor, stops accepting, refuses any further opens,
// waits out every open already in flight, closes every live session (so no
// lease is orphaned) and every live connection (so no handle goroutine
// outlives it), and removes the socket. It is safe to call twice.
//
// Ordering here is load-bearing:
//
//  0. The janitor's stop SIGNAL goes out first (close(s.janitorStop),
//     immediately after closing is set), but Shutdown does not block
//     waiting for it to actually finish until after the listener and live
//     connections are closed below — a slow in-flight Reap/GC cycle must
//     not delay shedding new connections. It still must be fully stopped
//     before sessions start closing (near the end of this function), since a
//     session close releases that branch's lease, and a reap racing that
//     release could observe a lease in a transient state it was never meant
//     to see. The wait for it is ordered right after the connection-close
//     block and before anything session-related, which is early enough to
//     guarantee that.
//  1. closing is set first, under s.mu, before anything else. opOpen checks
//     closing under the same lock before it ever reserves a slot or calls
//     s.openWG.Add, so once this store completes no new Add can race with
//     the Wait in step 3 below — every Add that will ever happen for this
//     shutdown has already happened, ordered by s.mu before this one.
//  2. s.mu is released before Shutdown does anything that can block. An
//     opOpen already in flight needs s.mu to finish its own bookkeeping (and
//     so call openWG.Done); a Shutdown that kept holding s.mu while waiting
//     would deadlock against it.
//  3. Shutdown waits for openWG (bounded by ctx) before it ever reads the
//     sessions map. By the time that wait returns, every in-flight open has
//     resolved into either a live session sitting in the map (because it
//     finished before closing was set, or observed closing not yet set) or a
//     fully released reservation (open failed, or it observed closing set
//     and self-closed the session it had just opened) — so the map, once
//     locked again, contains exactly what Shutdown must close and nothing an
//     in-flight open could still be about to touch. Without this wait,
//     draining the map here could delete another goroutine's in-flight nil
//     reservation out from under it, and a second opOpen for the same branch
//     could then reuse the now-free key and start a second, concurrent
//     session.Open against the same checkout file.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()

	// Signal the janitor to stop now — cheap and non-blocking, so it starts
	// winding down immediately. The actual wait for it to finish (which can
	// block for as long as its in-flight Reap/GC cycle takes) happens below,
	// AFTER the listener and live connections are closed, so a slow janitor
	// cycle never delays the daemon from shedding the listener and existing
	// connections. It still must finish before sessions start closing (see
	// item 0 above), so the wait stays ahead of the openWG/session-close
	// sequence that follows.
	close(s.janitorStop)

	s.ln.Close()

	// Close every live connection so a handle goroutine blocked in Decode
	// (or about to Encode a response on a connection nobody will read again)
	// unblocks and returns instead of leaking until its client disconnects.
	s.connMu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.connMu.Unlock()

	// The optional HTTP listener (Milestone 4 Task 3), if StartHTTP was ever
	// called, is shed at this same point, right alongside the unix
	// listener/conns above: http.Server.Close() immediately closes its
	// listener AND every connection it currently holds, matching the unix
	// side's philosophy of shedding fast rather than draining gracefully
	// (http.Server.Shutdown would wait out in-flight handlers, which this
	// daemon does not need — see below). This is safe for a request that is
	// ITSELF the "shutdown" op arriving over HTTP: the HTTP rpc handler
	// writes and flushes that request's response, on its own connection,
	// strictly BEFORE it ever triggers this Shutdown call (see http.go's
	// handleRPC, the HTTP analog of handle's own respond-then-shutdown
	// ordering fix) — by the time Close() runs here, those bytes are
	// already past the handler and into the kernel's send buffer, so
	// Close() tearing down the connection immediately afterward cannot
	// erase a response that was already written. Any OTHER HTTP request
	// truly in flight when an unrelated Shutdown (e.g. SIGINT) fires simply
	// sees its connection close — no panic, no hang; dispatch()'s own
	// per-op s.closing checks (opOpen, etc.) already make a request that
	// slips through in the narrow window before Close() takes effect fail
	// safely with "daemon: shutting down" rather than corrupting state,
	// exactly as they do for the unix socket. s.httpSrv is read under s.mu
	// since StartHTTP writes it under the same lock (see its doc comment).
	s.mu.Lock()
	httpSrv := s.httpSrv
	s.mu.Unlock()
	if httpSrv != nil {
		httpSrv.Close()
	}

	janitorWait := make(chan struct{})
	go func() {
		s.janitorWG.Wait()
		close(janitorWait)
	}()
	select {
	case <-janitorWait:
	case <-ctx.Done():
		// Note: closing stays true, so a later call to Shutdown will no-op
		// rather than retry a stop that never completed.
		return fmt.Errorf("daemon: shutdown: timed out waiting for the janitor: %w", ctx.Err())
	}

	openWait := make(chan struct{})
	go func() {
		s.openWG.Wait()
		close(openWait)
	}()
	select {
	case <-openWait:
	case <-ctx.Done():
		// Note: closing stays true, so a later call to Shutdown will no-op
		// (see the check above) rather than retry the drain that never ran.
		// A caller that times out here should treat the daemon as stuck, not
		// cleanly stopped.
		return fmt.Errorf("daemon: shutdown: timed out waiting for in-flight opens: %w", ctx.Err())
	}

	s.mu.Lock()
	sessions := make([]*session.Session, 0, len(s.sessions))
	for k, sess := range s.sessions {
		if sess != nil {
			sessions = append(sessions, sess)
		}
		delete(s.sessions, k)
	}
	s.mu.Unlock()

	closeDone := make(chan error, 1)
	go func() {
		var firstErr error
		for _, sess := range sessions {
			if err := sess.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		closeDone <- firstErr
	}()

	var firstErr error
	select {
	case firstErr = <-closeDone:
	case <-ctx.Done():
		return fmt.Errorf("daemon: shutdown: timed out closing sessions: %w", ctx.Err())
	}

	if err := os.Remove(s.sock); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
