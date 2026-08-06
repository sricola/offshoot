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
	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/replay"
	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/wal"
)

// defaultSnapshotEvery is Options.SnapshotEvery's default: a full snapshot
// every 16 flushes, with incremental segments in between.
const defaultSnapshotEvery = 16

type Options struct {
	WS         *ops.Workspace
	DB, Branch string
	Holder     string        // defaults to ops.LocalHolder()
	LeaseTTL   time.Duration // defaults to ops.DefaultLeaseTTL
	RenewEvery time.Duration // defaults to LeaseTTL/3
	Dir        string        // scratch dir for the replica and capture state; defaults to a temp dir

	// SnapshotEvery is how often Flush writes a full snapshot instead of an
	// incremental segment: every Nth flush snapshots, the rest write
	// segments. Defaults to 16. 1 means every flush snapshots, restoring the
	// pre-segment behavior. Flush may also snapshot before the cadence is
	// reached — the first flush ever always does (a segment cannot start a
	// chain), and one following a large enough batch of changed pages does
	// too (see largeSegmentFraction).
	SnapshotEvery int

	// FlushEvery, if > 0, starts a background goroutine that calls
	// Flush("") on this cadence for as long as the session is open, so a
	// caller that never calls Flush manually still gets bounded data loss
	// on crash. 0 (the default) means manual only — the library stays
	// conservative by default; safe-by-default cadence lives at the daemon
	// boundary instead (cmd/offshoot serve defaults its own -flush-every to
	// 30s), not baked into this primitive. See flushLoop's doc comment for
	// the idle short-circuit and failure-handling contract.
	FlushEvery time.Duration
}

// replicaSink adapts replay.Replica to the capture engine's Sink. The
// engine seeds the replica itself: on startup (no eligible resume state in
// a fresh scratch dir) it always performs an initial rebase, which snapshots
// the checkout's current content and hands it to Sink.Rebase before any
// Apply call — so Open does not need to seed the replica separately.
//
// Both Rebase and Apply mutate the replica file in place (Rebase can recur
// mid-session on divergence, not just at startup — see capture.Engine's
// rebase-on-divergence path), while Flush concurrently reads that same file
// page-by-page via ltxio.EncodeSnapshot with no locking of its own. Apply
// writes pages with separate WriteAt calls and then Truncates, and Rebase
// truncates-and-rewrites the whole file via os.Create; neither is atomic
// with respect to a concurrent reader, so an Encode running at the same
// time could observe a torn mix of pre- and post-mutation bytes (or a
// nPages read from the header that no longer matches the pages on disk
// after a concurrent Truncate). replicaSink holds sess.replicaMu, shared
// with Session.Flush: it serializes replica-file mutation against
// replica-file encoding so Flush always reads a replica frozen at a single
// transaction boundary.
//
// replicaSink also folds every applied transaction into sess's pending
// pageSet and running checksum (Session.recordApply), and re-baselines both
// on every Rebase (Session.rebaseline) — see those methods' doc comments.
// Both happen here, under the same replicaMu Apply/Rebase already need for
// the replica file itself, so "a page recorded" and "a page applied" can
// never diverge.
type replicaSink struct {
	sess *Session
}

func (s replicaSink) Rebase(path string) error {
	s.sess.replicaMu.Lock()
	defer s.sess.replicaMu.Unlock()
	if err := s.sess.replica.Rebase(path); err != nil {
		return err
	}
	return s.sess.rebaseline()
}

func (s replicaSink) Apply(ps uint32, f []wal.Frame) error {
	s.sess.replicaMu.Lock()
	defer s.sess.replicaMu.Unlock()
	// recordApply must run BEFORE the replica file is mutated: it reads each
	// touched page's prior content from the still-unmodified file to fold
	// out its old checksum contribution (see recordApply's doc comment).
	if err := s.sess.recordApply(ps, f); err != nil {
		return err
	}
	return s.sess.replica.Apply(ps, f)
}

// Session binds a leased branch to a live checkout, a shadow replica kept in
// lockstep by the capture engine, and the lease that authorizes its writes.
// Cancelling the parent context stops capture but does not release the lease
// or remove the scratch directory — callers must call Close to clean up.
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

	// renewDone mirrors engDone: closed exactly once, by renewLoop itself
	// after its loop exits (ctx cancelled, or a terminal renewal failure
	// called fail(), which itself cancels ctx). Close joins this before
	// releasing the lease so a straggler renewal — one that had already
	// fired its ticker case in the same instant Close cancelled ctx — cannot
	// run after the lease is released, observe the resulting holder
	// mismatch, and mark a cleanly-closed session as fenced. See Close's
	// comment for the required shutdown order.
	renewDone chan struct{}

	// flushDone mirrors renewDone, for flushLoop: nil if Options.FlushEvery
	// was 0 (no loop was ever started, so there is nothing to join), else
	// closed exactly once by flushLoop itself after its loop exits (ctx
	// cancelled). Close joins this, alongside engDone/renewDone, before it
	// proceeds to release the lease and remove the scratch dir.
	flushDone chan struct{}

	// replicaMu serializes writes to the replica file (capture's Rebase and
	// Apply, via replicaSink) against Flush's read of that same file, so
	// Flush never encodes a torn mix of pre- and post-mutation bytes. See
	// replicaSink's doc comment for why the race is real.
	//
	// It also guards every field below that derives from the replica's
	// content — pages, checksum, flushChecksum, pageSize, commit,
	// forceSnapshot, rebaseGen, appliedGen, flushedGen — for the same
	// reason: they are maintained incrementally exactly where Apply/Rebase
	// already touch the file (recordApply, rebaseline), and Flush reads
	// them from within its own replicaMu section, so they can never be
	// observed out of step with the file they describe.
	replicaMu sync.Mutex

	// pages accumulates the latest content of every page recordApply has
	// seen since the last flush — the pending segment's payload.
	pages *pageSet
	// checksum is the LTX rolling checksum (ltxio.ChecksumDatabase) of the
	// replica's current on-disk content: kept exactly current by recordApply
	// (O(changed pages), via ltxio.UpdateChecksum) and re-established by a
	// full rescan in rebaseline whenever the replica is rebuilt wholesale.
	checksum uint64
	// flushChecksum is checksum's value as of the last successful flush: the
	// preApplyChecksum the next segment must declare, since a segment
	// continues the chain from exactly that state. Only updated once a
	// flush is confirmed to have succeeded — see Flush.
	flushChecksum uint64
	pageSize      uint32
	commit        uint32 // current database size, in pages
	// forceSnapshot means the next Flush must write a full snapshot rather
	// than trust an incremental segment diff. Set whenever that trust would
	// be misplaced: after a mid-session rebase (rebaseline — the replica was
	// rebuilt from a fresh copy of the checkout, possibly folding in changes
	// recordApply never saw, see capture.Sink's doc comment on the
	// Rebase/Apply overlap) and, pessimistically, the moment a flush attempt
	// drains pages/checksum state for a segment it hasn't yet confirmed
	// uploaded (a failed attempt must not let a retry trust an already-
	// drained pageSet — see Flush). Cleared only once a flush actually
	// succeeds.
	forceSnapshot bool
	// rebaseGen counts rebaseline calls, so Flush's post-success bookkeeping
	// can detect a rebase that raced its upload/PutRef (which runs without
	// replicaMu held) and, if one happened, leave rebaseline's own — more
	// current — baseline alone instead of silently stomping it with stale
	// pre-rebase values. See Flush.
	rebaseGen int

	// appliedGen counts recordApply calls that folded in at least one real
	// frame — i.e. every time the capture engine actually observed a
	// committed transaction, not merely a rebase. flushedGen is the value
	// appliedGen held at the moment the most recently CONFIRMED-successful
	// Flush drained its pageSet (captured as appliedGenAtEncode in Flush,
	// stored here only once PutRef confirms). appliedGen != flushedGen is
	// therefore exactly "at least one transaction has committed since the
	// last successful flush" — the idle short-circuit's pending signal (see
	// flushLoop). Using a monotonic counter rather than a bool sidesteps a
	// race a bool would need extra bookkeeping to avoid: a new transaction
	// applied while a Flush's upload/PutRef is in flight (both run without
	// replicaMu held) must not be silently cleared by that Flush's own
	// post-success bookkeeping — with a counter, flushedGen is simply set to
	// the pre-encode snapshot value, so any appliedGen increment racing the
	// upload is untouched and still shows up as pending afterward.
	//
	// This does not, and cannot without the new engine query the brief
	// explicitly rules out, catch a rebase-on-divergence that folds real
	// content changes into the replica without ever going through Apply
	// (see capture.Sink's doc comment on the Rebase/Apply overlap) — such a
	// rebase alone does not advance appliedGen. In practice this is a
	// narrow window: any agent still writing to the checkout keeps
	// generating ordinary Apply calls that do advance it.
	appliedGen, flushedGen int

	// snapshotEvery and flushesSinceSnapshot are read and written only from
	// within Flush, which already holds flushMu for its entire body — no
	// separate lock needed.
	snapshotEvery        int
	flushesSinceSnapshot int

	// flushMu serializes the entire body of Flush: two goroutines calling
	// Flush concurrently would otherwise both read the same ref (same etag,
	// same HeadTXID), compute the identical next txid, and race to write it —
	// one loses the ref CAS and, worse, both raced encoding the replica out
	// from under each other's assumptions about what txid they were
	// producing. Holding flushMu for the whole call makes one Flush's read
	// (GetRef) through write (PutRef) atomic with respect to any other Flush
	// on this same Session.
	//
	// Lock ordering across this package, when more than one of these is held
	// at once, is flushMu -> replicaMu -> mu, and never the reverse: flushMu
	// nests around both replicaMu (Flush holds flushMu across its replicaMu
	// section that encodes the replica) and mu (Flush's Lease() call and its
	// final durable-txid update both happen while flushMu is held; Close
	// likewise takes flushMu, after already having released mu, before it
	// removes the scratch dir — see Close and Flush). replicaMu and mu,
	// though, are never held at the same time by anything in this package.
	// Violating the flushMu-outermost rule — e.g. taking flushMu while
	// already holding mu — is a deadlock waiting for a caller that holds
	// them in the other order.
	flushMu sync.Mutex

	mu      sync.Mutex
	lease   store.Lease
	err     error
	closed  bool
	durable uint64

	// lastFlushAt, lastFlushTXID, lastFlushOK describe the most recent
	// SUCCESSFUL flush, manual or automatic (see LastFlush). Stamped by
	// Flush itself, at its single ref-write confirmation site, alongside
	// durable above — see Flush.
	lastFlushAt   time.Time
	lastFlushTXID uint64
	lastFlushOK   bool
	// lastFlushErr is the most recent AUTOMATIC-flush failure (see
	// LastFlushErr): set by flushLoop when its own call to Flush returns an
	// error, and cleared by Flush itself on any later success — manual or
	// automatic — alongside lastFlushAt/lastFlushTXID/lastFlushOK above. A
	// manual Flush failure (e.g. from a direct caller, or the daemon's
	// "flush" op) does NOT set this: it already returns its error directly
	// to that caller, and lastFlushErr exists specifically to surface a
	// failure nobody was synchronously waiting on.
	lastFlushErr error
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
	if o.RenewEvery == 0 {
		o.RenewEvery = o.LeaseTTL / 3
		if o.RenewEvery <= 0 {
			o.RenewEvery = time.Second
		}
	}
	if o.SnapshotEvery <= 0 {
		o.SnapshotEvery = defaultSnapshotEvery
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
		relErr := o.WS.ReleaseLease(lease)
		cleanup()
		if relErr != nil {
			return nil, errors.Join(err, fmt.Errorf("session: also failed to release the lease after checkout failed: %w", relErr))
		}
		return nil, err
	}

	s := &Session{
		ws: o.WS, db: o.DB, branch: o.Branch, dir: dir, ownsDir: ownsDir,
		checkoutPath:  checkoutPath,
		replica:       replay.New(filepath.Join(dir, "replica.db")),
		lease:         lease,
		pages:         newPageSet(),
		snapshotEvery: o.SnapshotEvery,
	}

	cctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.captured = capture.NewEngine(capture.Options{
		DBPath: checkoutPath, StateDir: dir, Sink: replicaSink{s},
	})
	s.engDone = make(chan struct{})
	s.renewDone = make(chan struct{})
	go s.runEngine(cctx)

	// A clean resume (see capture.Engine.Resumed's doc comment — reused
	// Options.Dir, clean prior shutdown, main file unchanged) makes the
	// engine skip its startup rebase entirely, which means
	// replicaSink.Rebase — and therefore Session.rebaseline, the only thing
	// that ever seeds s.checksum/s.flushChecksum/s.forceSnapshot — never
	// runs. Left alone, this session would start with all three at their
	// zero values while the replica file it's continuing to trust already
	// holds real content: the first flush's segment would declare a bogus
	// preApplyChecksum and every read of the resulting chain would hard-fail
	// checksum verification. WaitReady blocks only until the engine's
	// resume-or-rebase verdict has settled (see its doc comment) — not the
	// full startup rebase when one runs, which stays exactly as
	// asynchronous as it always was — so this adds no latency to the
	// ordinary (fresh Dir, always-rebase) path.
	if s.captured.WaitReady(ctx) == nil && s.captured.Resumed() {
		s.replicaMu.Lock()
		rerr := s.rebaseline()
		s.replicaMu.Unlock()
		if rerr != nil {
			cancel()
			<-s.engDone
			relErr := o.WS.ReleaseLease(lease)
			cleanup()
			err := fmt.Errorf("session: establish checksum baseline after clean resume: %w", rerr)
			if relErr != nil {
				return nil, errors.Join(err,
					fmt.Errorf("session: also failed to release the lease: %w", relErr))
			}
			return nil, err
		}
	}

	go s.renewLoop(cctx, o.RenewEvery, o.LeaseTTL)

	if o.FlushEvery > 0 {
		s.flushDone = make(chan struct{})
		go s.flushLoop(cctx, o.FlushEvery)
	}
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

// Resumed reports whether this Session's capture engine skipped its startup
// rebase because it proved continuity with a prior clean shutdown of a
// session that used the same Options.Dir — see capture.Engine.Resumed's doc
// comment for exactly what that proves. Ordinary callers do not need this;
// it exists mainly so tests can assert which startup path Open actually
// took (see TestCleanResumeRebaselinesBeforeFirstSegment).
func (s *Session) Resumed() bool       { return s.captured.Resumed() }
func (s *Session) ReplicaPath() string { return s.replica.Path() }
func (s *Session) DB() string          { return s.db }
func (s *Session) Branch() string      { return s.branch }

func (s *Session) Lease() store.Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lease
}

// Err returns the terminal error that ended the session (lease loss, capture
// failure), or nil while it is healthy. Lease-loss detection is wired in the
// renewal loop and surfaces here as errors are recorded.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// LastFlush reports the time and txid of the most recent SUCCESSFUL flush —
// manual (an explicit Flush call) or automatic (flushLoop) — and ok=false if
// no flush has ever succeeded on this Session.
func (s *Session) LastFlush() (t time.Time, txid uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastFlushAt, s.lastFlushTXID, s.lastFlushOK
}

// LastFlushErr reports the most recent AUTOMATIC-flush failure recorded by
// flushLoop, or nil if none has happened since the last success (manual or
// automatic). A manual Flush call's own error is returned directly to its
// caller and never touches this — see the field's doc comment.
func (s *Session) LastFlushErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastFlushErr
}

// flushLoop calls Flush("") on a fixed cadence for as long as the session is
// open, so a caller that never calls Flush manually still gets bounded data
// loss on crash. Mirrors renewLoop's shutdown discipline exactly: it exits
// when ctx is cancelled (Close, or a terminal fail() elsewhere — a fenced or
// otherwise fatal session already stops itself via Err(), so this loop needs
// no special handling for that case beyond exiting on its next ctx.Done()
// check) and closes flushDone exactly once so Close can join it.
//
// Idle short-circuit (PM-review blocking amendment): a tick with nothing
// pending — appliedGen == flushedGen (no transaction has committed since the
// last successful flush) AND no outstanding failure to retry — calls Flush
// not at all: no object write, no ref write, no flushesSinceSnapshot
// advance. Without this, an idle session would still upload a full snapshot
// every SnapshotEvery ticks forever: Flush unconditionally advances txid and
// writes something once actually called, whether or not anything changed.
//
// A failing tick records lastFlushErr and is retried on the next tick (the
// pending check above is satisfied unconditionally while lastFlushErr != nil,
// regardless of appliedGen/flushedGen, so a failed attempt keeps retrying
// even if nothing NEW has committed since); it never terminates the session.
// lastFlushErr is cleared by Flush itself on any later success, so once a
// retry lands this loop has nothing further to do.
//
// Uses a time.Ticker, which drops ticks it cannot deliver in time rather
// than queuing them — so a tick delayed behind a slow manual Flush holding
// flushMu is simply skipped, never piled up.
func (s *Session) flushLoop(ctx context.Context, every time.Duration) {
	defer close(s.flushDone)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		s.replicaMu.Lock()
		pending := s.appliedGen != s.flushedGen
		s.replicaMu.Unlock()
		if !pending && s.LastFlushErr() == nil {
			continue // idle short-circuit: nothing to do this tick.
		}

		if _, err := s.Flush(""); err != nil {
			s.mu.Lock()
			s.lastFlushErr = err
			s.mu.Unlock()
		}
	}
}

// ErrClosed reports that Flush was called on a session that Close has
// already started closing (or finished closing). Close sets the closed flag
// as the very first thing it does, before any of its own slow work (joining
// goroutines, releasing the lease, removing the scratch dir); Flush checks
// it immediately after acquiring flushMu, so a Flush that observes it false
// is guaranteed to run to completion — including its encode of the replica
// file — before Close can reach the flushMu section that removes that file's
// directory. See Close and flushMu's doc comment.
var ErrClosed = errors.New("session: closed")

// Close stops capture, releases the lease, and removes the scratch dir. It is
// safe to call twice.
//
// Shutdown order matters: cancel the context, join the capture goroutine,
// THEN join the renewal goroutine, THEN release the lease, THEN remove the
// scratch dir. Joining renewLoop before ReleaseLease is what closes the race
// where a renewal tick fires in the same instant Close cancels ctx: without
// the join, that straggler could run after the lease is released, see the
// holder cleared out from under it, and call fail(ErrFenced) — marking a
// cleanly-closed session as fenced even though nothing actually went wrong.
//
// Removing the scratch dir is additionally serialized against flushMu: Flush
// holds flushMu for its whole body, including the point where it reads the
// replica file (under this same dir) via ltxio.EncodeSnapshot. Without that
// serialization, RemoveAll here could run concurrently with that read —
// pulling the directory out from under a Flush that is mid-encode. Taking
// flushMu only around the RemoveAll itself, rather than around this entire
// function, keeps Close from ever holding flushMu while blocked on the
// engDone/renewDone joins above: those joins do not depend on flushMu being
// free, so nesting them the other way would gain nothing and would make a
// future change that adds a flushMu dependency to shutdown much easier to
// deadlock by accident. A Flush that hasn't yet acquired flushMu when Close
// begins observes s.closed (set below, first) once it does and fails fast
// with ErrClosed instead of racing this removal at all.
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
	<-s.renewDone
	if s.flushDone != nil {
		<-s.flushDone
	}

	var relErr error
	if err := s.ws.ReleaseLease(lease); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		relErr = err
	}
	if s.ownsDir {
		s.flushMu.Lock()
		os.RemoveAll(s.dir)
		s.flushMu.Unlock()
	}
	return relErr
}

// recordApply folds one captured transaction's frames into the session's
// running checksum and pending page set. Called by replicaSink.Apply while
// holding replicaMu, BEFORE those frames are written to the replica file: it
// needs each touched page's content as it stood immediately before this
// transaction, read from the still-unmodified file, to fold that old
// contribution out of the running checksum (ltxio.UpdateChecksum) before
// folding the new one in — the same incremental technique
// ltxio.MaterializeChain uses to replay a segment (see its doc comment),
// applied here per-transaction instead of per-segment so the checksum never
// falls behind what's actually on disk.
//
// This mutates running state (s.checksum, s.commit, s.pageSize, s.pages)
// before replica.Apply below has validated the commit frame; that's safe
// because it relies on the same contract Apply itself requires (the last
// frame in a well-formed transaction is a commit frame — see
// replay.Replica.Apply), and because any Apply failure surfaces to the
// capture engine as fatal, which forces a rebase before capture resumes —
// rebaseline() re-establishes all of this state from scratch rather than
// leaving a partially-folded update in place.
func (s *Session) recordApply(pageSize uint32, frames []wal.Frame) error {
	if len(frames) == 0 {
		return nil
	}
	f, err := os.Open(s.replica.Path())
	if err != nil {
		return fmt.Errorf("session: open replica for checksum update: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("session: stat replica for checksum update: %w", err)
	}
	prevCommit := uint32(fi.Size() / int64(pageSize))
	newCommit := frames[len(frames)-1].Header.CommitSize
	// The lock page carries no real data — SQLite never stores page content
	// there — and every other checksum path in this codebase (
	// ltxio.ChecksumDatabase, ltxio.EncodeSnapshot, ltxio.MaterializeChain)
	// skips it for exactly that reason. It matters here specifically for a
	// shrinking commit that crosses the lock page's page number (the
	// ~1GB PENDING_BYTE boundary): without this skip, the shrink loop below
	// would fold OUT a contribution that was never folded IN, silently
	// diverging s.checksum from the value ltxio.MaterializeChain computes on
	// read — a checksum mismatch that hard-fails materialization.
	lockPgno := ltxio.LockPgno(pageSize)

	// Last write per pgno wins, matching pageSet's own latest-write
	// semantics (record below re-derives this independently; touched here
	// only decides what "old" content the checksum needs to fold out).
	latest := make(map[uint32][]byte, len(frames))
	touched := make([]uint32, 0, len(frames))
	for _, fr := range frames {
		if _, ok := latest[fr.Header.Pgno]; !ok {
			touched = append(touched, fr.Header.Pgno)
		}
		latest[fr.Header.Pgno] = fr.Data
	}

	running := s.checksum
	buf := make([]byte, pageSize)
	seen := make(map[uint32]bool, len(touched))
	for _, pgno := range touched {
		seen[pgno] = true
		if pgno == lockPgno {
			continue
		}
		var oldData []byte
		if pgno <= prevCommit {
			if _, err := f.ReadAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
				return fmt.Errorf("session: read old page %d: %w", pgno, err)
			}
			oldData = buf
		}
		var newData []byte
		if pgno <= newCommit {
			newData = latest[pgno]
		}
		running = ltxio.UpdateChecksum(running, pgno, oldData, newData)
	}
	// A shrinking commit drops trailing pages this transaction had no reason
	// to write explicitly; their old contribution must still be removed.
	// pageSet's own dropAbove(newCommit) call below (after record) clears any
	// of those same page numbers pageSet may still be holding from an
	// earlier, not-yet-flushed transaction — see its doc comment for why
	// leaving them would corrupt the next segment.
	if newCommit < prevCommit {
		for pgno := newCommit + 1; pgno <= prevCommit; pgno++ {
			if seen[pgno] || pgno == lockPgno {
				continue
			}
			if _, err := f.ReadAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
				return fmt.Errorf("session: read dropped page %d: %w", pgno, err)
			}
			running = ltxio.UpdateChecksum(running, pgno, buf, nil)
		}
	}
	// A growing commit can, in principle, extend past what this transaction
	// explicitly wrote — Truncate zero-extends the file, so any such page's
	// "new" content is all-zero. Every real SQLite commit writes every page
	// it introduces, so in practice this never runs; kept for the same
	// reason ltxio.MaterializeChain keeps its analogous loop.
	if newCommit > prevCommit {
		zero := make([]byte, pageSize)
		for pgno := prevCommit + 1; pgno <= newCommit; pgno++ {
			if seen[pgno] || pgno == lockPgno {
				continue
			}
			running = ltxio.UpdateChecksum(running, pgno, nil, zero)
		}
	}

	s.checksum = running
	s.commit = newCommit
	s.pageSize = pageSize
	// record must run BEFORE dropAbove, unconditionally, in that order: this
	// transaction's own frames are folded into pageSet first, and only then
	// is anything numbered beyond the (possibly just-shrunk) commit size
	// evicted. Running dropAbove only inside the shrink branch above, before
	// record, let a stale pgno > newCommit recorded by THIS transaction (or
	// left over from pageSet's own map iteration order relative to record)
	// survive into drain() — which ltxio.EncodeSegment then rejects as a
	// page number beyond the segment's declared commit size, a spurious
	// flush failure that only self-heals via forceSnapshot on retry. Calling
	// dropAbove unconditionally here, after record, guarantees pageSet never
	// holds anything past the current commit regardless of whether this
	// transaction grew or shrank the database.
	s.pages.record(frames)
	s.pages.dropAbove(newCommit)
	// A real transaction was folded in — see appliedGen's doc comment for
	// why this is the idle short-circuit's pending signal.
	s.appliedGen++
	return nil
}

// rebaseline re-establishes checksum from scratch by fully rescanning the
// replica file, and discards any pending page set. Called by
// replicaSink.Rebase while holding replicaMu, whenever the replica is
// rebuilt wholesale — the capture engine's initial startup rebase, and any
// later rebase-on-divergence.
//
// A rebase can fold pages into the replica's new base content without ever
// passing them through Apply (capture.Sink's doc comment describes the
// Rebase/Apply overlap this guards against), so a checksum or pageSet
// accumulated before this point cannot be trusted to describe the diff from
// here forward: continuing to trust them could produce a segment whose
// preApplyChecksum doesn't actually match the chain, or whose pages don't
// actually reflect what changed. A full rescan is normally too expensive to
// run on the flush hot path (see ltxio.ChecksumDatabase's doc comment for
// why), but establishing a baseline after the replica's content changed out
// from under incremental tracking is exactly the case that comment carves
// out as acceptable.
//
// forceSnapshot is set so the next Flush writes a full snapshot rather than
// a segment: a snapshot's checksum is self-contained (computed fresh over
// its own content, no preApplyChecksum required), so it needs no continuity
// with whatever the session tracked before the rebase — sidestepping the
// exact problem the rebase just created. pageSize and commit are left as
// they were (possibly still their zero values, if this is the very first,
// startup rebase): forceSnapshot guarantees the next flush is a snapshot,
// which needs neither, and the next recordApply call — which always runs
// before pageSet can hold anything a segment would need them for —
// re-establishes both from the frames it receives.
func (s *Session) rebaseline() error {
	sum, err := ltxio.ChecksumDatabase(s.replica.Path())
	if err != nil {
		return fmt.Errorf("session: checksum replica after rebase: %w", err)
	}
	s.checksum = sum
	s.flushChecksum = sum
	s.pages = newPageSet()
	s.forceSnapshot = true
	s.rebaseGen++
	return nil
}
