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
	// forceSnapshot, rebaseGen, appliedGen, flushedGen, flushedRebaseGen —
	// for the same reason: they are maintained incrementally exactly where
	// Apply/Rebase already touch the file (recordApply, rebaseline), and
	// Flush reads them from within its own replicaMu section, so they can
	// never be observed out of step with the file they describe.
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
	// committed transaction via Apply, not merely a rebase. flushedGen is
	// the value appliedGen held at the moment the most recently
	// CONFIRMED-successful Flush drained its pageSet (captured as
	// appliedGenAtEncode in Flush, stored here only once PutRef confirms).
	// appliedGen != flushedGen means "at least one transaction has been
	// applied since the last successful flush."
	//
	// That alone is NOT a sufficient idle-short-circuit signal for
	// flushLoop: capture.Engine.rebase's checkpoint(TRUNCATE) folds whatever
	// is currently sitting in the foreign WAL into the main file BEFORE
	// Sink.Rebase (and therefore rebaseline) ever runs — see engine.go's
	// rebase doc comment. Any transaction that had already committed but
	// that this session's WAL reader had not yet consumed at that instant is
	// absorbed into the fresh baseline snapshot WITHOUT ever passing through
	// Apply: appliedGen never advances for it, even though real,
	// previously-unflushed content is now sitting in the replica file. This
	// is reachable on a session's very first write — Open returns once
	// WaitReady's resume-or-rebase VERDICT has settled, not once a
	// fresh-Dir session's startup rebase has actually finished running (see
	// Open's comment) — and on any later rebase-on-divergence too.
	//
	// flushedRebaseGen closes this using state Flush already tracks for its
	// own forceSnapshot bookkeeping — rebaseGen — rather than a new
	// engine-side query: it is the value rebaseGen held at the drain point
	// of the most recently CONFIRMED-successful flush (mirroring
	// flushedGen/appliedGen exactly), and is gated the same way
	// forceSnapshot's own clear already is (see Flush) — a rebase racing the
	// upload/PutRef window must leave flushedRebaseGen at its pre-race
	// value, so the pending signal below stays true and the next tick
	// retries rather than believing the raced rebase's content is durable.
	// rebaseGen != flushedRebaseGen means "a rebase has happened since the
	// last successful flush." flushLoop's full pending check is
	// `appliedGen != flushedGen || rebaseGen != flushedRebaseGen`.
	//
	// Consequence: every session performs one settling flush shortly after
	// its startup rebase lands (rebaseGen ticks 0→1; flushedRebaseGen is
	// still 0 until the first flush confirms), even if the agent never
	// writes anything. That is correct — it is exactly what closes the
	// startup-write race described above — and costs one PUT per session
	// lifetime, nothing like the PM amendment's actual target (an idle
	// session re-uploading a full snapshot every SnapshotEvery ticks
	// forever).
	//
	// Using monotonic counters rather than bools for both pairs sidesteps a
	// race bools would need extra bookkeeping to avoid: state that changes
	// while a Flush's upload/PutRef is in flight (both run without
	// replicaMu held) must not be silently cleared by that Flush's own
	// post-success bookkeeping — with a counter, the "flushed" value is
	// simply set to the pre-encode snapshot, so any increment racing the
	// upload is untouched and still shows up as pending afterward.
	appliedGen, flushedGen, flushedRebaseGen int

	// cleanAtOpen, headPostApplyChecksum, and headPostApplyValid are set once
	// by Open and never written again — see Open's own comment at the
	// fetch site for exactly how each is derived. Together they are the
	// settling-flush suppression's proof (M2 follow-up, ledgered — see
	// appliedGen's doc comment above for what the settling flush exists to
	// close, and rebaseline's doc comment for the full gate and the CRITICAL
	// race this pair of fields exists to catch).
	//
	// cleanAtOpen alone is NOT sufficient proof, and must never be treated
	// as such: it means the checkout Open's synchronous CheckoutProven call
	// received was already proven byte-identical to the branch's head AT
	// THAT MOMENT (T0) — but the engine's own startup rebase (the checkpoint
	// that actually seeds the replica) runs LATER, asynchronously, in the
	// engine goroutine, and Open returns to its caller before that rebase
	// necessarily finishes (see the comment on the WaitReady/Resumed block
	// below for exactly why). A write landing in that window — after
	// CheckoutProven's read, before the startup rebase's own
	// checkpoint(TRUNCATE) — is folded into the checkout's main file by that
	// checkpoint and DOES show up in what rebaseline hashes (T1), even
	// though cleanAtOpen, describing only T0, has no way to know that
	// happened.
	//
	// headPostApplyChecksum closes that gap: the durable head's TRUE
	// content checksum, fetched from the store once, synchronously, in Open
	// (ops.Workspace.HeadPostApplyChecksum) — independent of, and not
	// derived from, the local checkout at all. rebaseline compares its own
	// freshly-computed checksum (necessarily reflecting T1, whatever
	// actually ended up in the checkout by the time the real rebase ran)
	// against this value: if a race-window write folded in, the two differ
	// and the settle correctly proceeds; only an exact match proves nothing
	// happened in the gap. headPostApplyValid is false whenever that fetch
	// (or the ref-stability check guarding it — see Open) didn't succeed,
	// which conservatively disables the suppression rather than trusting an
	// incomplete proof.
	//
	// Read-only after Open; no lock needed for the reads in rebaseline,
	// which already holds replicaMu for everything else it touches. Safe
	// without their own synchronization because Open sets all three fields
	// before either of rebaseline's two possible first callers can run: the
	// engine goroutine (started via `go s.runEngine` after these fields are
	// assigned in the struct literal, so the `go` statement's
	// happens-before guarantee covers it) and Open's own direct call on the
	// clean-resume path (same goroutine, strictly later).
	cleanAtOpen           bool
	headPostApplyChecksum uint64
	headPostApplyValid    bool

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

	checkoutRes, err := o.WS.CheckoutProven(o.DB, o.Branch)
	if err != nil {
		relErr := o.WS.ReleaseLease(lease)
		cleanup()
		if relErr != nil {
			return nil, errors.Join(err, fmt.Errorf("session: also failed to release the lease after checkout failed: %w", relErr))
		}
		return nil, err
	}

	// Settling-flush suppression's proof (M2 follow-up, ledgered — see
	// rebaseline's doc comment for the full gate this feeds and exactly why
	// checkoutRes.Clean ALONE is not sufficient). Fetched only when
	// checkoutRes.Clean (no point otherwise: the suppression can never
	// fire), and only here — once, synchronously, in Open's own goroutine,
	// strictly before the capture engine's startup rebase can run.
	//
	// A SECOND, independent GetRef — not a reuse of checkoutRes.Ref — is
	// deliberate: it confirms the ref genuinely had not moved between
	// CheckoutProven's own read and this one, closing the (narrow, but
	// real, since these are two separate store calls) gap between them.
	// Any mismatch, or any error along the way, simply leaves
	// headPostApplyValid false rather than failing Open — this checksum is
	// a proof for an optimization, never something Open's own correctness
	// depends on.
	var headPostApplyChecksum uint64
	var headPostApplyValid bool
	if checkoutRes.Clean {
		if ref2, _, rerr := o.WS.Store.GetRef(o.DB, o.Branch); rerr == nil &&
			ref2.Lineage == checkoutRes.Ref.Lineage &&
			ref2.HeadEpoch == checkoutRes.Ref.HeadEpoch &&
			ref2.HeadTXID == checkoutRes.Ref.HeadTXID {
			if sum, herr := o.WS.HeadPostApplyChecksum(ref2); herr == nil {
				headPostApplyChecksum, headPostApplyValid = sum, true
			}
		}
	}

	s := &Session{
		ws: o.WS, db: o.DB, branch: o.Branch, dir: dir, ownsDir: ownsDir,
		checkoutPath:          checkoutRes.Path,
		replica:               replay.New(filepath.Join(dir, "replica.db")),
		lease:                 lease,
		pages:                 newPageSet(),
		snapshotEvery:         o.SnapshotEvery,
		cleanAtOpen:           checkoutRes.Clean,
		headPostApplyChecksum: headPostApplyChecksum,
		headPostApplyValid:    headPostApplyValid,
	}

	cctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.captured = capture.NewEngine(capture.Options{
		DBPath: checkoutRes.Path, StateDir: dir, Sink: replicaSink{s},
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
	// ordinary (fresh Dir, always-rebase) path. A consequence: on that
	// ordinary path, Open can return to its caller before the startup
	// rebase has actually finished running, so a write the caller makes
	// immediately after Open returns can land in the checkout's WAL before
	// that rebase's checkpoint(TRUNCATE) runs and get folded into its
	// snapshot without ever passing through Apply. See
	// appliedGen/flushedRebaseGen's doc comment on Session for why
	// flushLoop's idle short-circuit does not lose track of that write.
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
	s.logTransition("opened", "holder", lease.Holder, "epoch", lease.Epoch)
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
	first := s.err == nil
	if first {
		s.err = err
	}
	s.mu.Unlock()
	if first {
		// "fenced" names this session's one terminal-failure transition,
		// whatever its cause — lease loss (the literal ErrFenced case),
		// the branch being destroyed out from under it, or a capture-engine
		// failure (see runEngine and renewLoop, this function's only
		// callers). All three end the session the same way from an
		// observer's point of view: it stops serving and Err() starts
		// returning non-nil. Logged once, matching the "if s.err == nil"
		// gate above exactly, so a session that fails twice (e.g. a second
		// caller racing to report the same underlying failure) reports the
		// transition exactly once, not once per caller.
		s.logTransition("fenced", "cause", err.Error())
	}
	s.cancel()
}

// logTransition writes one structured, key=value line to stderr for a
// session state transition (opened, flushed, flush-failed, fenced, closed —
// see Open, flush, fail, and Close). It deliberately matches the daemon
// janitor's existing "offshoot: janitor: ..." prefix family (see
// internal/daemon/server.go's StartJanitor) — "offshoot: session: db@branch:
// event key=value ..." — so an operator grepping stderr for "offshoot:"
// finds one consistent line shape rather than two competing ones.
//
// kv is a flat key/value list (kv[0], kv[1], kv[2], kv[3], ...); an odd
// trailing element with no paired value is silently dropped rather than
// panicking — this is a logging path, not something that should ever be
// able to take a session down over a caller's own bug. String values are
// %q-quoted (so an error message containing spaces stays a single token for
// anything parsing key=value pairs); everything else uses %v.
func (s *Session) logTransition(event string, kv ...any) {
	line := fmt.Sprintf("offshoot: session: %s@%s: %s", s.db, s.branch, event)
	for i := 0; i+1 < len(kv); i += 2 {
		if sv, ok := kv[i+1].(string); ok {
			line += fmt.Sprintf(" %v=%q", kv[i], sv)
		} else {
			line += fmt.Sprintf(" %v=%v", kv[i], kv[i+1])
		}
	}
	fmt.Fprintln(os.Stderr, line)
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

// CaptureLag reports WAL bytes committed by writers but not yet applied to
// the replica — see capture.Engine.Lag's doc comment. Safe to call at any
// time, including concurrently with the capture engine's own goroutine (see
// Lag's doc comment for why).
func (s *Session) CaptureLag() int64 { return s.captured.Lag() }

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

// autoFlushPending reports flushLoop's idle-short-circuit pending signal:
// true when EITHER appliedGen != flushedGen (a real transaction has been
// applied since the last successful flush) OR rebaseGen != flushedRebaseGen
// (a rebase — startup, or on-divergence — has happened since the last
// successful flush). See appliedGen/flushedRebaseGen's doc comment on
// Session for exactly why the rebaseGen half is load-bearing and not
// redundant with the appliedGen half. A single method, rather than the
// two-line expression inlined at each call site, so flushLoop's own check
// and tests that need to know "has the session settled yet" (see
// TestIdleAutoFlushWritesNothing and the rebase tests in flush_test.go)
// can never drift apart from what flushLoop itself actually evaluates.
func (s *Session) autoFlushPending() bool {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	return s.appliedGen != s.flushedGen || s.rebaseGen != s.flushedRebaseGen
}

// flushLoop calls flush("", true) — the auto-tagged path Flush("") itself is
// a thin wrapper around, see flush's doc comment — on a fixed cadence for as
// long as the session is open, so a caller that never calls Flush manually
// still gets bounded data loss on crash. Mirrors renewLoop's shutdown
// discipline exactly: it exits when ctx is cancelled (Close, or a terminal
// fail() elsewhere — a fenced or
// otherwise fatal session already stops itself via Err(), so this loop needs
// no special handling for that case beyond exiting on its next ctx.Done()
// check) and closes flushDone exactly once so Close can join it.
//
// Idle short-circuit (PM-review blocking amendment) — pending signal: a tick
// calls Flush only when EITHER appliedGen != flushedGen (a real transaction
// has been applied since the last successful flush) OR rebaseGen !=
// flushedRebaseGen (a rebase — startup, or on-divergence — has happened
// since the last successful flush). See appliedGen/flushedRebaseGen's doc
// comment on Session for exactly why the rebaseGen half is load-bearing and
// not redundant with the appliedGen half — in short, a rebase's
// checkpoint(TRUNCATE) can fold a real, previously-uncaptured commit into
// the baseline without ever calling Apply, so appliedGen alone would miss
// it. When neither signal is pending, this tick calls Flush not at all: no
// DrainNow, no GetRef, no object write, no ref write, no
// flushesSinceSnapshot advance. Without this, an idle session would still
// upload a full snapshot every SnapshotEvery ticks forever: Flush
// unconditionally advances txid and writes something once actually called,
// whether or not anything changed.
//
// Consequence: every session performs one settling flush shortly after its
// startup rebase lands (see appliedGen/flushedRebaseGen's doc comment for
// why), even if the agent never writes anything — one PUT per session
// lifetime, nothing like the idle-forever-snapshotting bug the short-circuit
// exists to prevent.
//
// A failing tick records lastFlushErr and is retried on the next tick (the
// pending check above is satisfied unconditionally while lastFlushErr !=
// nil, so a failed attempt keeps retrying even if nothing NEW is pending);
// it never terminates the session. lastFlushErr is cleared by Flush itself
// on any later success, manual or automatic. Because a manual Flush can run
// concurrently with this loop's own (flushMu only serializes their bodies,
// not their outcomes' visibility), a failing tick only stamps lastFlushErr
// if lastFlushAt/lastFlushTXID are still exactly what they were when this
// tick's own attempt began — otherwise a fresher success already landed
// (from this tick's own call racing a concurrent manual Flush, or from a
// manual Flush that ran between this tick's Flush call returning and this
// loop regaining s.mu below) and stamping the stale error would incorrectly
// resurrect a failure that no longer reflects reality.
//
// Flush returning ErrClosed means Close has already started (or finished)
// concurrently with this tick — session shutdown racing the ticker, not an
// operational failure worth surfacing: a caller polling status mid-shutdown
// must not see "session: closed" reported as a flush error. ctx.Done() ends
// this loop on its own next iteration regardless.
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

		if !s.autoFlushPending() && s.LastFlushErr() == nil {
			continue // idle short-circuit: nothing to do this tick.
		}

		s.mu.Lock()
		beforeAt, beforeTXID := s.lastFlushAt, s.lastFlushTXID
		s.mu.Unlock()

		if _, err := s.flush("", true); err != nil {
			if errors.Is(err, ErrClosed) {
				continue
			}
			s.mu.Lock()
			if s.lastFlushAt.Equal(beforeAt) && s.lastFlushTXID == beforeTXID {
				s.lastFlushErr = err
			}
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

	// Sidecar refresh on clean close (M2 follow-up, ledgered), split around
	// the engine shutdown join below — see prepareSidecarRefresh's doc
	// comment for why stamping needs to straddle it rather than run
	// entirely on one side.
	refresh := s.prepareSidecarRefresh()

	s.cancel()
	<-s.engDone
	<-s.renewDone
	if s.flushDone != nil {
		<-s.flushDone
	}

	if refresh {
		s.commitSidecarRefresh()
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
	if relErr != nil {
		s.logTransition("closed", "error", relErr.Error())
	} else {
		s.logTransition("closed")
	}
	return relErr
}

// prepareSidecarRefresh and commitSidecarRefresh together re-stamp the
// checkout's .sum sidecar (via ops.StampSum) on a CLEAN Close, so the next
// Open/Checkout against this db@branch clean-skips re-materializing instead
// of paying a full chain replay — restoring, for the daemon-reopen pattern,
// the win Milestone 2 Task 1 already established for a Checkout call
// landing on an already-current checkout. This pairs directly with
// rebaseline's settling-flush suppression (Commit A, cleanAtOpen): a session
// that reopens against a sidecar these functions stamped both clean-skips
// the materialization AND suppresses its own startup settle.
//
// Split in two, straddling Close's engine-shutdown join
// (s.cancel(); <-s.engDone), because of a hazard specific to same-process
// SQLite locking. Stamping the sidecar needs a hash of the checkout's
// content with its WAL ALREADY checkpointed into the main file — SQLite in
// WAL mode can leave a just-committed write sitting in the -wal file with
// the main file's bytes still reflecting the state before it, and a hash
// taken before that checkpoint would describe stale bytes. But this
// session's OWN capture engine may still hold live SQLite state on this
// exact path while it's running, and opening a second, independent
// connection here to checkpoint and hash it ourselves would risk the POSIX
// (process, inode) lock-drop hazard internal/dbfile's package comment warns
// against (closing ANY fd this process holds on an inode drops EVERY
// advisory lock this process holds on it, including the engine's).
//
// commitSidecarRefresh does NOT open a second connection to solve this.
// Instead it reuses a fingerprint the engine's OWN shutdown already
// computed, from ITS own long-lived connection: capture.Engine.shutdown
// (called from Run's ctx.Done() branch, i.e. exactly what s.cancel()
// triggers) drains, checkpoints(RESTART then TRUNCATE), and — ONLY if every
// one of those steps verified clean — hashes the checkout's main file and
// persists {Clean: true, MainHash: <that hash>} to capture.State (see that
// type's doc comment). Critically, shutdown has FOUR early-return paths
// (a failed final drain, checkpoint(RESTART) failing or reporting more
// frames folded than drain() had consumed, walRacedSinceRestart detecting a
// foreign write in the checkpoint gap, or checkpoint(TRUNCATE) itself
// failing) that leave the persisted State at whatever it was before —
// Clean=false, or a stale Clean=true from an earlier session entirely — NOT
// a corrected one. An earlier version of this function assumed shutdown's
// checkpoint always ran and always succeeded and independently re-quiesced
// + re-hashed the checkout itself; that is wrong on exactly this path, and
// worse than merely redundant: a fresh quiesce()+hash after one of
// shutdown's own early returns would fold in, and then stamp, content
// shutdown itself had just refused to vouch for. commitSidecarRefresh must
// therefore load capture.State (capture.LoadState(capture.StatePath(s.dir)))
// and require Clean == true before trusting MainHash for anything — see the
// body below.
//
// So: everything that needs the engine ALIVE (a final DrainNow, to make the
// pending check below trustworthy — DrainNow folds content into the
// replica/appliedGen bookkeeping but does not itself checkpoint the
// checkout's WAL, and does not touch capture.State at all) runs in
// prepareSidecarRefresh, called BEFORE s.cancel(); everything that needs
// the engine's own shutdown to have already run (successfully or not — its
// verdict is exactly what gates this) and its connection already gone runs
// in commitSidecarRefresh, called AFTER <-s.engDone (Run's own deferred
// e.conn.Close/e.db.Close run before the deferred close(e.done) that
// unblocks <-s.engDone — see Run's defer order — so no engine-owned
// connection is still open by the time this runs, even though this
// function opens none of its own either way now). Both phases re-check
// sidecarRefreshEligible() independently rather than trusting phase one's
// verdict blindly: shutdown's own best-effort drain can itself apply one
// more transaction that raced between phase one's DrainNow and s.cancel(),
// which would make autoFlushPending() flip from false to true in between —
// exactly what commitSidecarRefresh's re-check exists to catch.
//
// Both are best-effort: any failure along the way (drain timeout, no
// verified-clean state, GetRef error, sidecar write error) simply skips the
// refresh — Close's job is shutting the session down cleanly, not
// guaranteeing this optimization landed, and the worst case of skipping it
// is only that the next Open pays a materialize it could have avoided,
// never incorrect data. There remains an inherent, unclosable TOCTOU
// window around the whole sequence (an external process could commit a
// write to the checkout at almost any point in it); that is the same class
// of race every other checkoutState consumer in this codebase already
// accepts (see ops.Checkout's own quiesce-then-check flow), not something
// new introduced here.
func (s *Session) prepareSidecarRefresh() bool {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if s.Err() != nil {
		return false
	}
	// 30s matches Flush's own DrainNow budget (see its doc comment for the
	// full worst-case-latency reasoning this borrows: the capture engine's
	// takeover/retry budgets can legitimately run into the tens of
	// seconds under contention) rather than a shorter one specific to
	// Close: a tighter budget here would risk spuriously abandoning this
	// best-effort optimization under exactly the same contention DrainNow
	// already has to ride out elsewhere, for no real benefit — Close still
	// blocks the caller either way, and skipping the refresh on a false
	// timeout only costs a future reopen's materialize, never correctness.
	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := s.captured.DrainNow(dctx)
	cancel()
	if err != nil {
		return false
	}
	return s.sidecarRefreshEligible()
}

// commitSidecarRefresh is prepareSidecarRefresh's other half — see its doc
// comment for why this must run only after Close has joined engDone, and
// why it stamps from the engine's own verified-clean State.MainHash rather
// than independently re-hashing the checkout.
func (s *Session) commitSidecarRefresh() {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if !s.sidecarRefreshEligible() {
		return
	}
	st, ok, err := capture.LoadState(capture.StatePath(s.dir))
	if err != nil || !ok || !st.Clean || st.MainHash == "" {
		// Not a verified-clean shutdown (one of shutdown's four early
		// returns fired — see the doc comment above) — nothing safe to
		// stamp from. This is the expected outcome whenever a write raced
		// the engine's own final checkpoint, not a bug; log it at
		// warn-to-stderr, matching this package's other non-fatal
		// transition logging, so an operator investigating "why did this
		// reopen re-materialize" has a signal without this being surfaced
		// as an error to any caller.
		s.logTransition("sidecar-refresh-skipped", "reason", "shutdown state not verified clean")
		return
	}
	s.mu.Lock()
	durable := s.durable
	s.mu.Unlock()
	lease := s.Lease()
	ref, _, err := s.ws.Store.GetRef(s.db, s.branch)
	if err != nil {
		return
	}
	if ref.LeaseHolder != lease.Holder || ref.Epoch != lease.Epoch {
		return // fenced or reclaimed out from under us
	}
	if ref.HeadTXID != durable || ref.HeadEpoch != lease.Epoch {
		return // a concurrent writer elsewhere moved the head past what we flushed
	}

	if err := ops.StampSum(s.checkoutPath, st.MainHash, ref.Lineage, ref.HeadEpoch, ref.HeadTXID); err != nil {
		s.logTransition("sidecar-refresh-skipped", "reason", err.Error())
	}
}

// sidecarRefreshEligible is the pending/success check both
// prepareSidecarRefresh and commitSidecarRefresh run — see the caller
// closest to each call for why each needs its own independent evaluation
// rather than sharing one verdict. Callers hold flushMu already; this reads
// s.mu itself (briefly) for lastFlushOK/durable, same as every other
// Session accessor that touches those fields.
//
//   - s.Err() == nil: no fencing, no capture-engine failure. A session that
//     ended abnormally has no business asserting anything about what its
//     checkout currently contains.
//   - !s.autoFlushPending(): everything ever applied or rebased-in is
//     already reflected in the last successful flush — the exact signal
//     flushLoop's own idle short-circuit uses (see autoFlushPending's doc
//     comment). This is the load-bearing check: a session with ANY
//     unflushed content (a write that was never flushed, or a flush attempt
//     that failed and left autoFlushPending() true — the failed-flush case
//     this must refuse to stamp for) must not have its checkout stamped as
//     durable, or a later reopen would clean-skip re-materializing content
//     that was never actually saved to the store — silently losing it,
//     since the settling-flush suppression would then also treat that
//     reopen as needing no settle either.
//   - s.lastFlushOK and s.durable > 0: at least one flush has actually
//     succeeded on this session. A session that never flushed anything
//     (pure read-only, nothing written) has nothing fresher to record than
//     whatever sidecar Open's own Checkout call already left in place —
//     nothing to refresh.
//   - singleStartupRebase(): rebaseGen is still exactly 1 — nothing beyond
//     this session's one mandatory startup rebase ever rebuilt the replica.
//     A later rebase-on-divergence rebuilds it from whatever snapshot the
//     engine's divergence recovery produced at that moment; in real
//     operation that snapshot is always itself derived from a checkpoint of
//     the ACTUAL checkout (see capture.Engine.rebase's doc comment), so the
//     mirror invariant this whole function otherwise leans on — "the
//     replica's flushed content already equals the live checkout's actual
//     bytes" — still holds even then. But nothing here can re-verify that
//     cheaply (doing so would mean re-hashing the checkout wholesale,
//     defeating the point of this optimization), so this stays deliberately
//     conservative and refuses to stamp for any session that ever took that
//     path — see singleStartupRebase's own doc comment for the test that
//     exists specifically to pin this boundary.
func (s *Session) sidecarRefreshEligible() bool {
	if s.Err() != nil {
		return false
	}
	if s.autoFlushPending() {
		return false
	}
	if !s.singleStartupRebase() {
		return false
	}
	s.mu.Lock()
	okFlush, durable := s.lastFlushOK, s.durable
	s.mu.Unlock()
	return okFlush && durable > 0
}

// singleStartupRebase reports whether rebaseGen is still exactly 1 — see the
// gate's use in sidecarRefreshEligible for the full reasoning, and
// TestSidecarNotStampedAfterMidSessionRebase for a test pinning it directly.
// Without this gate, a session whose replica was rebuilt via
// replicaSink.Rebase with content unrelated to its actual checkout —
// exactly what TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle and
// TestFlushLoopRetriesAfterRebaseDuringUpload also drive directly, to
// exercise flushedRebaseGen's bookkeeping in isolation from a real WAL race
// — would have its checkout sidecar confidently stamped on Close, claiming
// the checkout matches durable content the checkout's own bytes never
// actually received; both tests' later `w.Checkout` call to read that
// content back would then silently clean-skip instead of re-materializing
// it, and fail.
func (s *Session) singleStartupRebase() bool {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	return s.rebaseGen == 1
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
//
// Settling-flush suppression (M2 follow-up, ledgered). This is the FIRST
// call of rebaseline's life (rebaseGen 0 -> 1 — either call site: the
// engine's mandatory startup Rebase, or Open's own direct call on the
// clean-resume path, see cleanAtOpen's doc comment) AND s.cleanAtOpen is
// true AND s.headPostApplyValid is true AND sum (just computed, above)
// equals s.headPostApplyChecksum EXACTLY suppresses the settling flush that
// appliedGen/flushedRebaseGen's doc comment describes: it marks THIS rebase
// as already durable (flushedRebaseGen = rebaseGen) instead of leaving it
// pending, and clears forceSnapshot, so flushLoop's next tick finds nothing
// pending and performs no upload at all.
//
// Proof obligation, and why cleanAtOpen ALONE is not sufficient (this was a
// real, shipped bug in an earlier version of this suppression, caught by
// code review before it merged — kept here so it is never reintroduced):
// cleanAtOpen only proves the checkout's content AT THE MOMENT Open's
// synchronous CheckoutProven call ran (T0). The engine's actual startup
// rebase — the one that seeds the replica this function is about to
// checksum — runs LATER and asynchronously, in the engine's own goroutine;
// Open returns to its caller before that rebase necessarily finishes (see
// the WaitReady/Resumed comment in Open). A write landing in the window
// between T0 and the real rebase's own checkpoint(TRUNCATE) (T1) is folded
// into the checkout's main file by that checkpoint — and therefore INTO sum
// below — without ever passing through Apply, so appliedGen never counts
// it either. cleanAtOpen, describing only T0, cannot see that this
// happened; a suppression gated on cleanAtOpen alone would silently believe
// the settle unnecessary and that write would never become durable, AND
// s.flushChecksum would be left describing content one write ahead of what
// was ever actually uploaded, so the next real segment's declared
// preApplyChecksum would no longer match the chain — a hard read failure
// for everyone materializing this branch from then on.
//
// The `sum == s.headPostApplyChecksum` comparison is what actually closes
// this: headPostApplyChecksum is the durable head's TRUE content checksum,
// fetched from the store independently of the local checkout (Open calls
// ops.Workspace.HeadPostApplyChecksum once, synchronously, before the
// engine's startup rebase can run — see Open's own comment at that call
// site). sum, by contrast, necessarily reflects T1 — whatever the checkout
// actually contained by the time the real rebase ran, race-window write or
// not. If one landed, the two values differ (folding in even one changed
// page changes the rolling checksum — see ltxio.UpdateChecksum) and this
// gate correctly leaves forceSnapshot true, so the settle proceeds and
// ships that write for real. Only an exact match is proof nothing happened
// in the gap. headPostApplyValid guards the case where that store fetch (or
// the ref-stability re-check guarding it, in Open) didn't succeed: no
// number to compare against means no suppression, full stop — conservative
// by construction, never a false "nothing to do."
//
// No store read is added here, in the engine goroutine: headPostApplyChecksum
// was already fetched, once, synchronously, inside Open — see Open's own
// comment at that call site for why a SECOND independent GetRef there,
// rather than reusing CheckoutProven's ref, is itself part of this same
// proof.
//
// When cleanAtOpen is false (the checkout was freshly (re)materialized —
// stale, locally modified, or simply didn't exist yet), this suppression
// does NOT apply, even though a freshly materialized checkout ALSO exactly
// equals the head afterward: proving that would need capturing a checksum
// as a byproduct of the chain replay materializeChainAt just did, which
// this function deliberately does not attempt, matching the suppression's
// actual target — the reopen-after-clean-close workload (Session.Close's
// sidecar refresh pairs directly with this) — rather than trying to also
// optimize the cold-materialize path. The settle proceeds exactly as
// before M2's original settling-flush behavior.
//
// This gate applies ONLY to rebaseGen's 0 -> 1 transition, never to a later
// rebase-on-divergence (rebaseGen 1 -> 2, 2 -> 3, ...): those have no
// cleanAtOpen/headPostApplyChecksum-shaped proof available (both describe
// Open's ONE-TIME checkout materialization, not anything about mid-session
// state), so `first` below is false for them and this whole block is
// skipped — the settle for a rebase that DID fold content (M2's critical
// fix, see TestFlushLoopFlushesRebaseFoldedContentWhenOtherwiseIdle and
// TestFlushLoopRetriesAfterRebaseDuringUpload) is completely untouched by
// this change. TestSettlingSuppressionCatchesRaceWindowFold pins the T0/T1
// race directly, by white-box-rewinding rebaseGen back to 0 after a real
// (unraced) startup rebase has already landed and re-driving that "first"
// transition with content headPostApplyChecksum knows nothing about.
func (s *Session) rebaseline() error {
	sum, err := ltxio.ChecksumDatabase(s.replica.Path())
	if err != nil {
		return fmt.Errorf("session: checksum replica after rebase: %w", err)
	}
	first := s.rebaseGen == 0
	s.checksum = sum
	s.flushChecksum = sum
	s.pages = newPageSet()
	s.forceSnapshot = true
	s.rebaseGen++
	if first && s.cleanAtOpen && s.headPostApplyValid && sum == s.headPostApplyChecksum {
		s.forceSnapshot = false
		s.flushedRebaseGen = s.rebaseGen
	}
	return nil
}
