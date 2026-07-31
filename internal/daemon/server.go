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
		sessions: map[string]*session.Session{}}, nil
}

func (s *Server) SocketPath() string { return s.sock }

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
	defer c.Close()
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
func (s *Server) opOpen(req Request) Response {
	branch := req.Branch
	if branch == "" {
		branch = "main"
	}
	k := key(req.DB, branch)

	s.mu.Lock()
	if _, exists := s.sessions[k]; exists {
		s.mu.Unlock()
		return errResp(fmt.Errorf("daemon: %s is already open here", k))
	}
	s.sessions[k] = nil // reserve the slot
	s.mu.Unlock()

	sess, err := session.Open(context.Background(), session.Options{
		WS: s.ws, DB: req.DB, Branch: branch,
	})

	s.mu.Lock()
	if err != nil {
		delete(s.sessions, k) // release the reservation
		s.mu.Unlock()
		return errResp(err)
	}
	if s.closing {
		// Shutdown ran while this open was in flight and didn't know to
		// wait for it. Don't leave the newly-acquired lease orphaned.
		delete(s.sessions, k)
		s.mu.Unlock()
		sess.Close()
		return errResp(fmt.Errorf("daemon: shutting down"))
	}
	s.sessions[k] = sess
	s.mu.Unlock()
	return Response{OK: true, Checkout: sess.CheckoutPath()}
}

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

// Shutdown stops accepting, closes every session so no lease is orphaned, and
// removes the socket. It is safe to call twice.
//
// A session whose open is still in flight (a reserved nil slot) has nothing
// to close here: opOpen itself checks s.closing after session.Open returns
// and closes the session then if Shutdown got there first, so no lease is
// left orphaned either way.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	sessions := make([]*session.Session, 0, len(s.sessions))
	for k, sess := range s.sessions {
		if sess != nil {
			sessions = append(sessions, sess)
		}
		delete(s.sessions, k)
	}
	s.mu.Unlock()

	s.ln.Close()
	var firstErr error
	for _, sess := range sessions {
		if err := sess.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := os.Remove(s.sock); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
