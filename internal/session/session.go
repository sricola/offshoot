// Package session binds a leased branch to a live checkout, a shadow replica
// kept in lockstep by the WAL capture engine, and the lease that authorizes
// the daemon's writes. The agent writing to the checkout is never paused.
package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/offshoot-db/offshoot/internal/capture"
	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/replay"
	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/wal"
)

type Options struct {
	WS         *ops.Workspace
	DB, Branch string
	Holder     string        // defaults to ops.LocalHolder()
	LeaseTTL   time.Duration // defaults to ops.DefaultLeaseTTL
	Dir        string        // scratch dir for the replica and capture state; defaults to a temp dir
}

// replicaSink adapts replay.Replica to the capture engine's Sink. The
// engine seeds the replica itself: on startup (no eligible resume state in
// a fresh scratch dir) it always performs an initial rebase, which snapshots
// the checkout's current content and hands it to Sink.Rebase before any
// Apply call — so Open does not need to seed the replica separately.
type replicaSink struct{ r *replay.Replica }

func (s replicaSink) Rebase(path string) error             { return s.r.Rebase(path) }
func (s replicaSink) Apply(ps uint32, f []wal.Frame) error { return s.r.Apply(ps, f) }

// Session binds a leased branch to a live checkout, a shadow replica kept in
// lockstep by the capture engine, and the lease that authorizes its writes.
type Session struct {
	ws           *ops.Workspace
	db, branch   string
	dir          string
	ownsDir      bool
	checkoutPath string
	replica      *replay.Replica

	cancel context.CancelFunc
	// engDone is closed exactly once, by the single goroutine that runs the
	// capture engine, after Run returns and any failure has been recorded via
	// fail(). It carries no value (a value-carrying channel here would be a
	// single-item mailbox with two independent readers — Close and a
	// watcher — racing to consume the one send; only one would ever see it
	// and the other would block forever). close() broadcasts to every
	// receiver, so Close() and any other waiter can both observe completion
	// safely.
	engDone  chan struct{}
	captured *capture.Engine

	mu     sync.Mutex
	lease  store.Lease
	err    error
	closed bool
}

// Open acquires the lease, materializes the checkout, seeds the replica from
// the branch head, and starts capturing. The returned Session runs until
// Close or until it loses its lease.
func Open(ctx context.Context, o Options) (*Session, error) {
	if o.WS == nil {
		return nil, errors.New("session: workspace is required")
	}
	if o.Holder == "" {
		o.Holder = ops.LocalHolder()
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = ops.DefaultLeaseTTL
	}
	dir, ownsDir := o.Dir, false
	if dir == "" {
		d, err := os.MkdirTemp("", "offshoot-session-*")
		if err != nil {
			return nil, err
		}
		dir, ownsDir = d, true
	}
	cleanup := func() {
		if ownsDir {
			os.RemoveAll(dir)
		}
	}

	lease, err := o.WS.AcquireLease(o.DB, o.Branch, o.Holder, o.LeaseTTL)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("session: acquire %s@%s: %w", o.DB, o.Branch, err)
	}

	checkoutPath, err := o.WS.Checkout(o.DB, o.Branch)
	if err != nil {
		o.WS.ReleaseLease(lease)
		cleanup()
		return nil, err
	}

	s := &Session{
		ws: o.WS, db: o.DB, branch: o.Branch, dir: dir, ownsDir: ownsDir,
		checkoutPath: checkoutPath,
		replica:      replay.New(filepath.Join(dir, "replica.db")),
		lease:        lease,
	}

	cctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.captured = capture.NewEngine(capture.Options{
		DBPath: checkoutPath, StateDir: dir, Sink: replicaSink{s.replica},
	})
	s.engDone = make(chan struct{})
	go s.runEngine(cctx)
	return s, nil
}

// runEngine runs the capture engine to completion, records a terminal
// failure (a clean shutdown via Close cancels the context and Run returns
// nil, which is not a failure), then closes engDone so every waiter —
// Close(), possibly others — observes completion exactly once.
func (s *Session) runEngine(ctx context.Context) {
	defer close(s.engDone)
	if err := s.captured.Run(ctx); err != nil {
		s.fail(fmt.Errorf("session: capture stopped: %w", err))
	}
}

// fail records the first terminal error and stops the session's work.
func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	s.cancel()
}

func (s *Session) CheckoutPath() string { return s.checkoutPath }
func (s *Session) ReplicaPath() string  { return s.replica.Path() }
func (s *Session) DB() string           { return s.db }
func (s *Session) Branch() string       { return s.branch }

func (s *Session) Lease() store.Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lease
}

// Err returns the terminal error that ended the session (lease loss, capture
// failure), or nil while it is healthy.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops capture, releases the lease, and removes the scratch dir. It is
// safe to call twice.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	lease := s.lease
	s.mu.Unlock()

	s.cancel()
	<-s.engDone

	var relErr error
	if err := s.ws.ReleaseLease(lease); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		relErr = err
	}
	if s.ownsDir {
		os.RemoveAll(s.dir)
	}
	return relErr
}
