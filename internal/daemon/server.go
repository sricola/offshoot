// Package daemon serves offshoot sessions over a unix socket: a long-running
// process holds branch leases, captures WAL continuously, and flushes durable
// snapshots while agents write to their checkouts.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/session"
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
	return &Server{ws: ws, ln: ln, sock: socketPath,
		sessions:    map[string]*session.Session{},
		conns:       map[net.Conn]struct{}{},
		janitorStop: make(chan struct{}),
	}, nil
}

func (s *Server) SocketPath() string { return s.sock }

// StartJanitor reaps expired branches and runs GC every interval until
// Shutdown. grace is passed to GC (tombstone age before deletion); the
// default in cmd is deliberately generous — the spec requires it to exceed
// the longest plausible in-flight fork. every <= 0 disables the janitor
// entirely (no goroutine is started).
//
// Sessions opened by this daemon hold live leases, so the janitor can never
// reap a branch this daemon is actively writing to (see ttlDeadline: a live
// lease only ever pushes a branch's deadline into the future).
func (s *Server) StartJanitor(every, grace time.Duration) {
	if every <= 0 {
		return
	}
	s.janitorWG.Add(1)
	go func() {
		defer s.janitorWG.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-s.janitorStop: // closed by Shutdown before it waits on janitorWG
				return
			case <-t.C:
				if reaped, err := s.ws.Reap(time.Now()); err != nil {
					fmt.Fprintf(os.Stderr, "offshoot: janitor: reap: %v\n", err)
				} else if len(reaped) > 0 {
					fmt.Fprintf(os.Stderr, "offshoot: janitor: reaped %v\n", reaped)
				}
				if _, _, err := s.ws.GC(grace); err != nil {
					fmt.Fprintf(os.Stderr, "offshoot: janitor: gc: %v\n", err)
				}
			}
		}
	}()
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
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

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
		go s.Shutdown(context.Background())
		return Response{OK: true}
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
	s.mu.Unlock()

	if openDelay != nil {
		openDelay() // test hook; nil (a no-op) in production
	}

	sess, err := session.Open(context.Background(), session.Options{
		WS: s.ws, DB: req.DB, Branch: branch,
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

func (s *Server) opFlush(req Request) Response {
	sess, err := s.lookup(req.DB, req.Branch)
	if err != nil {
		return errResp(err)
	}
	txid, err := sess.Flush(req.Name)
	if err != nil {
		return errResp(err)
	}
	return Response{OK: true, TXID: txid}
}

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
	infos := make([]SessionInfo, 0, len(keys))
	for _, k := range keys {
		sess := s.sessions[k]
		info := SessionInfo{
			DB: sess.DB(), Branch: sess.Branch(), Checkout: sess.CheckoutPath(),
			Holder: sess.Lease().Holder, Epoch: sess.Lease().Epoch,
			DurableTXID: sess.DurableTXID(),
		}
		if err := sess.Err(); err != nil {
			info.Error = err.Error()
		}
		infos = append(infos, info)
	}
	s.mu.Unlock()
	return Response{OK: true, Sessions: infos}
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

// Shutdown stops the janitor, stops accepting, refuses any further opens,
// waits out every open already in flight, closes every live session (so no
// lease is orphaned) and every live connection (so no handle goroutine
// outlives it), and removes the socket. It is safe to call twice.
//
// Ordering here is load-bearing:
//
//  0. The janitor is stopped FIRST, before anything else below: it runs
//     Reap/GC independently of the sessions map and s.mu, so nothing else in
//     this ordering waits for or depends on it — but it must not still be
//     running once sessions start closing (near the end of this function),
//     since a session close releases that branch's lease, and a reap racing
//     that release could observe a lease in a transient state it was never
//     meant to see. Stopping it first, before the listener even closes,
//     keeps that window from ever opening.
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

	close(s.janitorStop)
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

	s.ln.Close()

	// Close every live connection so a handle goroutine blocked in Decode
	// (or about to Encode a response on a connection nobody will read again)
	// unblocks and returns instead of leaking until its client disconnects.
	s.connMu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.connMu.Unlock()

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
