// Package capture implements the foreign-connection WAL capture engine:
// the read-lock dance, incremental frame capture, checkpoint takeover, and
// rebase-on-divergence. This is offshoot's risk-spike core.
package capture

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/dbfile"
	"github.com/offshoot-db/offshoot/internal/wal"
)

// Sink receives capture output. Implementations: replay.Replica satisfies
// this via a thin adapter in the tests; plan 2 adds an LTX sink.
//
// Apply is called at most once per transaction, ever: the engine guarantees
// every call carries frames for a transaction that has not previously been
// passed to Apply, across rebases, takeovers, and process restarts alike
// (see shutdown()/tryResume()'s TRUNCATE-based clean-resume proof and
// rebase()'s snapshot-based re-establishment). Sink implementations are NOT
// required to be idempotent. replay.Replica happens to be idempotent, which
// masked an earlier bug where a resumed engine re-applied an already-folded
// WAL generation; the LTX sink must not rely on that — it should treat a
// duplicate Apply as a bug, not a no-op.
//
// A Rebase snapshot is not guaranteed to partition cleanly against the
// Apply calls around it, though. The new base a Rebase call hands the sink
// can already physically reflect transactions whose frames will still
// arrive via Apply afterward — rebase()'s post-TRUNCATE snapshot copy runs
// without a read lock held, so a foreign write can land and get folded into
// the copied file while the WAL, independently, still carries (and the
// reader still re-delivers) those same frames as well; see rebase()'s doc
// comment for why replaying them again heals rather than corrupts. The
// no-duplicate-Apply guarantee above still holds unconditionally — but the
// snapshot a Rebase hands over and the Apply stream that follows it may
// overlap in what they each already reflect. Plan 2's LTX sink must handle
// that overlap explicitly (e.g. by checking its own cumulative state before
// trusting a rebase snapshot at face value), not assume Rebase output and
// Apply calls together partition the transaction history exactly once each.
type Sink interface {
	Rebase(snapshotPath string) error
	Apply(pageSize uint32, frames []wal.Frame) error
}

type Options struct {
	DBPath   string
	StateDir string // sidecar state + snapshots live here
	Sink     Sink
	Poll     time.Duration // default 10ms
}

type Engine struct {
	o        Options
	db       *sql.DB
	conn     *sql.Conn
	inTx     bool
	reader   *wal.Reader
	pageSize uint32

	// rebased and resumed are read from outside the engine goroutine (see
	// Rebased()/Resumed()) — by test code polling for progress, and by
	// cmd/torture's round loop, which reads them concurrently with Run()
	// still executing. They are atomic so those reads are race-safe from any
	// goroutine at any time, not just after an established happens-before.
	rebased atomic.Int64

	captured int // txns since last takeover

	// resumed is set true exactly once, in tryResume's success branch, when
	// this Engine instance skipped the initial rebase because it proved
	// continuity across a restart. See Resumed().
	resumed atomic.Bool

	// expectRestart is true immediately after a verified-clean takeover
	// (checkpoint RESTART folded exactly what we'd already consumed, no
	// more). It means the WAL's lazy header/salt reset — which lands on the
	// *next* writer's commit, not at RESTART time itself — is anticipated:
	// when it lands, our replica state already equals the main DB content at
	// the restart point, so the new WAL generation is a continuation, not a
	// continuity loss. See takeover() and Run()'s wal.ErrWALRestarted
	// handling.
	expectRestart bool

	// reqs carries DrainNow requests into Run's select loop, so an immediate
	// catch-up poll is serviced by the same goroutine that owns e.reader and
	// e.conn — never concurrently with the ticker-driven path. See DrainNow
	// and pollOnce.
	reqs chan drainRequest
	// done is closed once, when Run returns, so a DrainNow call racing the
	// engine's own shutdown does not block forever with nobody left to
	// service e.reqs.
	done chan struct{}
	// ready is closed exactly once, by Run's own goroutine, the moment its
	// startup tryResume/rebase decision has settled — specifically, right
	// after tryResume returns (successfully or not) and BEFORE the optional
	// startup rebase() call that follows when it did not resume. At that
	// point Resumed() already holds its final value (tryResume sets it,
	// synchronously, before returning) even though the slower rebase() may
	// still be running. See WaitReady.
	ready chan struct{}
}

// drainRequest is one DrainNow call's handoff to Run's loop: done carries
// back pollOnce's result (nil on an ordinary successful catch-up, or the
// same fatal error Run itself returns and stops on — Run's reqs case sends
// the result to done and THEN returns it, exactly as the ticker path below
// does, so a DrainNow-triggered failure stops the engine just as promptly
// as one the ticker happened to hit first).
type drainRequest struct{ done chan error }

func NewEngine(o Options) *Engine {
	if o.Poll == 0 {
		o.Poll = 10 * time.Millisecond
	}
	return &Engine{o: o, reqs: make(chan drainRequest), done: make(chan struct{}), ready: make(chan struct{})}
}

// Rebased reports how many times this Engine instance rebased. This
// predates resume support, when a startup rebase was unconditional and 1
// meant "just the initial one, nothing else went wrong" — that's no longer
// the baseline: a cleanly resumed session (see Resumed()) skips the startup
// rebase entirely and reports 0, while a session that rebases at startup
// (no eligible resume) still reports 1 with nothing else having gone wrong.
// Any count beyond what startup alone accounts for — above 0 after a clean
// resume, or above 1 after a startup rebase — means continuity was lost and
// re-established later in the session: detected, never silent.
func (e *Engine) Rebased() int { return int(e.rebased.Load()) }

// Resumed reports whether this Engine instance skipped the initial rebase
// on startup because tryResume proved continuity across a restart (clean
// marker + empty WAL + matching main-file content hash — see tryResume's
// doc comment). It is set exactly once, in tryResume's success branch, and
// never cleared afterward — it answers "did THIS session's startup resume
// cleanly," not "has this session ever rebased since." A later in-session
// rebase (e.g. a takeover fold-race) does not un-set it.
func (e *Engine) Resumed() bool { return e.resumed.Load() }

// WaitReady blocks until Run's startup tryResume/rebase decision has
// settled — Resumed() is guaranteed to hold its final value once this
// returns nil — or until the engine stops before getting there (Run
// returned, e.done closed), or ctx is cancelled. See the ready field's doc
// comment for exactly which moment this is (deliberately before, not after,
// an ordinary startup rebase() finishes — that stays a slow, asynchronous
// step exactly as it always has been; only the tryResume verdict itself is
// worth blocking a caller on).
//
// A nil return does not by itself mean the engine started up successfully —
// the e.done case returns nil too, exactly like DrainNow's identical
// tradeoff (see its doc comment): a caller that needs to know about a
// terminal engine failure must check that some other way (Session.Err).
func (e *Engine) WaitReady(ctx context.Context) error {
	select {
	case <-e.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return nil
	}
}

func (e *Engine) statePath() string    { return filepath.Join(e.o.StateDir, "capture-state.json") }
func (e *Engine) snapshotPath() string { return filepath.Join(e.o.StateDir, "snapshot.db") }

// DrainNow requests an immediate, out-of-band poll and blocks until it has
// been serviced: every transaction already committed to the checkout's WAL
// as of the moment this call is issued is captured (applied to the Sink)
// before it returns, instead of waiting for the next tick of Engine.Poll
// (default 10ms). This is the guarantee Session.Flush needs: without it,
// Flush would encode whatever the replica happens to hold at that instant,
// which can lag a write a caller just committed to the checkout by up to
// one poll interval — Flush would then advance the branch head to
// reference a snapshot that silently omits it, while still reporting
// success.
//
// That guarantee is enforced by draining to a real target (the WAL's
// on-disk size at the moment the request is serviced), not by a wall-clock
// budget — see drainUntil's doc comment. A nil return from DrainNow is
// therefore proof the target was reached, never a "ran out of time" guess;
// if the target genuinely cannot be reached in a sane window (see
// drainSafetyDeadline), DrainNow returns ErrDrainIncomplete instead of a
// misleading nil, and the caller (Flush) must treat that as a failed flush
// rather than commit a possibly-short snapshot.
//
// The request is handed to Run's own select loop (see pollOnce/pollOnceTo),
// so it is serviced by the single goroutine that owns e.reader/e.conn —
// never concurrently with the regular ticker path. If ctx is cancelled
// first, or the engine has already stopped (Run returned, e.done closed)
// with nothing left to service the request, DrainNow returns promptly
// rather than blocking forever; a caller that needs to know about a
// terminal engine failure should check that some other way (Session.Err),
// not through DrainNow's return value in that case.
func (e *Engine) DrainNow(ctx context.Context) error {
	req := drainRequest{done: make(chan error, 1)}
	select {
	case e.reqs <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return nil
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		// Run's reqs case sends to req.done (buffered, cap 1) strictly
		// before it can return and trigger the deferred close(e.done) — but
		// that program-order guarantee inside Run doesn't make select
		// prefer one ready case over another here: if both req.done and
		// e.done are ready by the time this select runs, Go picks between
		// them pseudo-randomly. Since close(e.done) happens-after the send
		// (same goroutine, sequenced before the return that unwinds into
		// it), a pending value on req.done is always already visible by the
		// time e.done's close is observed — so check it once more,
		// non-blockingly, rather than risk reporting a spurious nil success
		// for what was actually a fatal DrainNow-triggered error.
		select {
		case err := <-req.done:
			return err
		default:
			return nil
		}
	}
}

// Run blocks, capturing until ctx is cancelled. It performs the initial
// rebase (checkpoint + snapshot copy), holds the read-lock dance, polls for
// committed transactions, and periodically performs checkpoint takeover.
//
// Every fallible step between here and the first point where ctx.Done() is
// actually selected on (the main loop below) is guarded with the same
// ctx.Err() check the startup rebase, pollOnce/pollOnceTo's rebase branches,
// and afterDrain's takeover call already use: Close() cancels ctx, and
// Session.Open starts this goroutine asynchronously and returns immediately
// (see session.go's Open), so a Close() landing before — or while — Run's
// own DB setup (sql.Open/Conn/PRAGMAs) or tryResume runs is a completely
// ordinary, if early, race, not a special case. database/sql surfaces that
// as ctx.Err() (typically context.Canceled, unwrapped) from whichever
// ExecContext/QueryRowContext/Conn call happened to be in flight — and
// without a guard, that flows straight out as Run's own fatal return value,
// which runEngine (session.go) records as "session: capture stopped:
// context canceled" on what was actually a clean Close. Nothing has been
// set up yet at any of these points for shutdown()'s final drain/checkpoint
// dance to safely run against, so — exactly like the startup rebase guard
// below — the right response is simply to stop, not to attempt a graceful
// shutdown() that has nothing valid to act on. Reproduced empirically
// (TestCloseDoesNotMarkSessionFenced failing under whole-suite contention,
// pinned via temporary logging to the PRAGMA page_size QueryRowContext call
// specifically, though Conn and the wal_autocheckpoint PRAGMA are exactly
// as capable of losing the same race and are guarded for the same reason,
// not because either was separately observed to fail).
func (e *Engine) Run(ctx context.Context) error {
	defer close(e.done)
	var err error
	// NOTE: this function deliberately opens NO descriptor of its own on the
	// main database file, and in particular closes none at teardown. Raw
	// reads of that file go through internal/dbfile, whose descriptors are
	// never closed — see copySrc/hashSrc below and dbfile's package comment
	// for the POSIX (process, inode) lock semantics that make an engine-owned
	// descriptor unsafe no matter how carefully its close is ordered.
	e.db, err = sql.Open("sqlite3",
		e.o.DBPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return err
	}
	defer e.db.Close()
	e.conn, err = e.db.Conn(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer e.conn.Close()
	if _, err := e.conn.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if err := e.conn.QueryRowContext(ctx, "PRAGMA page_size").Scan(&e.pageSize); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	resumed, err := e.tryResume(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	// Resumed() already holds its final value at this point (tryResume sets
	// it synchronously before returning) even though the rebase() call just
	// below, when it runs, can still take a while (checkpoint + full-file
	// copy + full-file hash — see rebase's and hashSrc's doc comments).
	// Closing ready here, rather than after that optional call, is what lets
	// WaitReady's caller (Session.Open, see its doc comment there) learn the
	// resume verdict promptly without also having to wait out an ordinary
	// startup rebase that was always asynchronous before this existed.
	close(e.ready)
	if !resumed {
		if err := e.rebase(ctx); err != nil {
			if ctx.Err() != nil {
				// Close() landed while the startup rebase was still
				// running: an ordinary, if early, shutdown — not a real
				// capture failure. Nothing has been set up yet for
				// shutdown()'s final drain/checkpoint dance to safely run
				// against (e.reader isn't even bound on every failure path
				// through rebase()), so there is nothing more to do than
				// stop cleanly; the next start simply rebases again. See
				// the identical guard on pollOnce's own rebase call below,
				// and the existing guard around the takeover() call, for
				// the same reasoning applied at the other two rebase call
				// sites reachable from Run's goroutine.
				return nil
			}
			return err
		}
	}

	idle := time.Now()
	tick := time.NewTicker(e.o.Poll)
	defer tick.Stop()
	for {
		// Check for a pending DrainNow request non-blockingly, before ever
		// entering the blocking select below. Without this, a request that
		// is already waiting on e.reqs by the time this loop comes back
		// around competes on equal footing with an already-fired tick — Go's
		// select picks pseudo-randomly between two simultaneously-ready
		// cases — so under sustained write load (drain's per-call time
		// budget, see drain's doc comment, forces this loop back here
		// roughly every drainBudget) a DrainNow caller could keep losing
		// that coin flip. Session.Flush's DrainNow call is exactly the
		// caller this would starve. Servicing e.reqs first, unconditionally,
		// whenever it's ready guarantees a queued request is serviced on the
		// very next iteration rather than merely being more likely to be.
		select {
		case req := <-e.reqs:
			if err := e.serviceReq(ctx, req, &idle); err != nil {
				return err
			}
			continue
		default:
		}
		select {
		case <-ctx.Done():
			e.shutdown()
			return nil
		case req := <-e.reqs:
			if err := e.serviceReq(ctx, req, &idle); err != nil {
				return err
			}
			continue
		case <-tick.C:
		}
		if err := e.pollOnce(ctx, &idle); err != nil {
			return err
		}
	}
}

// serviceReq runs one DrainNow caller's poll and reports the result back to
// it. Send the result to the requester FIRST, then — on a fatal error — stop
// the loop exactly as the ticker path does. A poll triggered by DrainNow is
// not a lesser failure than one triggered by the ticker: if it were treated
// as one, Run would keep looping looking healthy while capture is actually
// broken, and nothing would ever call session.runEngine's failure path.
// Sending before returning means the requester still gets the error even
// though Run is about to exit and close e.done right after.
//
// The drain target — the WAL's on-disk size as of right now — is read here,
// in Run's own goroutine at the moment the request is actually serviced, not
// inside DrainNow itself (which runs on the caller's goroutine and must not
// touch e.reader/e.conn/the WAL file's stat outside this goroutine's
// ownership). See drainUntil's doc comment for why a fixed target, decided
// once up front, is what makes DrainNow's contract satisfiable at all under
// a nonstop writer. A failure to even establish the target (e.g. a stat
// error other than the WAL not existing yet) is reported the same way any
// other fatal poll failure is: sent to the requester and then treated as
// fatal for Run itself, never silently skipped.
func (e *Engine) serviceReq(ctx context.Context, req drainRequest, idle *time.Time) error {
	target, terr := e.drainTarget()
	if terr != nil {
		req.done <- terr
		return terr
	}
	err := e.pollOnceTo(ctx, idle, target, time.Now().Add(drainSafetyDeadline))
	req.done <- err
	return err
}

// drainTarget reads the WAL's current on-disk size — the completion target
// a DrainNow-triggered drain must reach (see drainUntil). A missing WAL file
// (nothing has ever been written to it, or it was just truncated) means
// there is nothing to catch up to, so target 0 is trivially already met by
// any reader offset.
func (e *Engine) drainTarget() (int64, error) {
	fi, err := os.Stat(e.o.DBPath + "-wal")
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("drain target: stat WAL: %w", err)
	}
	return fi.Size(), nil
}

// pollOnce runs one capture pass using the ticker's own drain(), which is
// bounded by drainBudget and may return having only partially caught up
// (see drain's doc comment) — harmless here since Run's next tick simply
// continues. It is Run's regular, liveness-only poll path; DrainNow uses
// pollOnceTo instead, which drains to a real completion target rather than a
// clock. idle is Run's loop-local idle timestamp, threaded through by
// pointer so pollOnce can update it exactly as the inline code used to.
//
// On a WAL restart, this returns after handling (at most) one restart —
// exactly the original inline behavior this was factored out of. That is
// deliberately weaker than pollOnceTo's guarantee (see its doc comment for
// why the two must differ here): it is fine for the ticker path because
// Run's loop comes back around every e.o.Poll regardless, so anything left
// after a restart is simply picked up by the next tick.
func (e *Engine) pollOnce(ctx context.Context, idle *time.Time) error {
	n, err := e.drain(ctx)
	if err == wal.ErrWALRestarted {
		if e.expectRestart {
			// The WAL's lazy reset (armed by our own verified-clean
			// takeover) has now physically landed: a new WAL generation
			// with fresh salts. Our replica already reflects every frame
			// up to the restart point, so the new generation's frames
			// apply cleanly on top — this is a continuation, not lost
			// continuity. No rebase, no snapshot, not counted.
			e.expectRestart = false
			e.reader = wal.NewReader(e.o.DBPath + "-wal")
			e.captured = 0
			return nil
		}
		// Our lock lapsed (or an external RESTART happened) without a
		// preceding verified-clean takeover: continuity lost —
		// detected. Re-establish from a fresh snapshot.
		if err := e.rebase(ctx); err != nil {
			if ctx.Err() != nil {
				// Close() landed mid-rebase: an ordinary shutdown, not a
				// real capture failure — let Run's ctx.Done() branch
				// perform the final drain/endRead and return nil. See the
				// identical guard on Run's own startup rebase, and the
				// guard on the takeover() call below, for the same
				// reasoning at the other two rebase call sites.
				return nil
			}
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	return e.afterDrain(ctx, idle, n)
}

// pollOnceTo runs one capture pass servicing a DrainNow request: it drains
// to target (see drainUntil) instead of drain()'s time budget, so — unlike
// pollOnce — it either reaches everything committed as of the request or
// returns a loud error (ErrDrainIncomplete), never a silent partial catch-up.
// deadline is the single, absolute cutoff for the WHOLE call — computed once
// by serviceReq — not just one drainUntil attempt; see the loop below for
// why that distinction matters.
//
// Unlike pollOnce, this MUST NOT return after handling just one WAL
// restart the way the original inline code (and pollOnce, above) did: a
// restart's expectRestart-continuation branch only proves the reader had
// consumed everything in the OLD generation as of the takeover that armed
// expectRestart — which can be earlier than, and so weaker than, "as of
// target" when target was computed by a LATER DrainNow request (e.g. one
// whose caller committed new data, in the old generation, after that
// takeover ran but before the lazy reset physically landed). Returning nil
// right there — as an earlier version of this function did — would report
// success while silently leaving that newer data, now sitting in the fresh
// generation, completely undrained. Reproduced directly: with a foreign
// reader pinned open (blocking every checkpoint, so takeover's own
// safety-net rebase can never mask this), a marker row committed
// immediately before Flush was absent from the flushed snapshot even
// though DrainNow reported success — see the task-7 hardening report for
// the captured run.
//
// The fix is to treat target as scoped to "whatever WAL generation is
// current" rather than a fixed byte count: after a clean-continuation
// restart, re-derive target fresh (via drainTarget, against the NOW-current
// generation) and keep draining toward it, rather than trusting the
// pre-restart number in a generation it no longer describes. A rebase, by
// contrast, needs no such re-derivation and no loop — rebase()'s snapshot
// copy happens strictly after any target was computed, so it already
// supersedes it by construction (see the rebase branch below).
func (e *Engine) pollOnceTo(ctx context.Context, idle *time.Time, target int64, deadline time.Time) error {
	for {
		n, err := e.drainUntil(ctx, target, deadline)
		if err == wal.ErrWALRestarted {
			if e.expectRestart {
				e.expectRestart = false
				e.reader = wal.NewReader(e.o.DBPath + "-wal")
				e.captured = 0
				newTarget, terr := e.drainTarget()
				if terr != nil {
					return terr
				}
				target = newTarget
				continue
			}
			// Continuity lost — detected. rebase() copies the live main DB
			// file, which reflects everything checkpointed as of that
			// copy — strictly later than target — so this alone already
			// satisfies the DrainNow request in flight; no need to keep
			// draining toward target afterward.
			if err := e.rebase(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		return e.afterDrain(ctx, idle, n)
	}
}

// afterDrain runs the bookkeeping shared by pollOnce and pollOnceTo once a
// drain phase has completed without error: update the idle timestamp and
// cumulative captured count, and perform checkpoint takeover once enough has
// accumulated.
func (e *Engine) afterDrain(ctx context.Context, idle *time.Time, n int) error {
	if n > 0 {
		*idle = time.Now()
		e.captured += n
	}
	if e.captured >= 64 || (e.captured > 0 && time.Since(*idle) > 5*time.Second) {
		if err := e.takeover(ctx); err != nil {
			if ctx.Err() != nil {
				// Shutting down: let Run's ctx.Done() branch perform the
				// final drain/endRead and return nil.
				return nil
			}
			return err
		}
	}
	return nil
}

// shutdown runs on the ctx.Done() path. It drains whatever's still pending,
// then attempts a final verified-clean checkpoint(RESTART) — the exact
// log==consumed proof takeover() uses — and, only if that succeeds,
// immediately follows with checkpoint(TRUNCATE) to physically zero the -wal
// file. Only if BOTH succeed does it persist State{Clean: true} — see
// state.go's doc comment for exactly what that marker means and why
// tryResume requires it.
//
// Why two checkpoints instead of one TRUNCATE (as rebase() uses): verified
// empirically (see task-7 fix report) that PRAGMA wal_checkpoint(TRUNCATE)'s
// own `log`/`checkpointed` result columns are populated AFTER the physical
// reset, so on success they read back 0 regardless of how many frames were
// actually folded — unlike RESTART, whose reset is lazy (the on-disk
// header/salts aren't rewritten until the next writer's commit — see
// takeover()'s doc comment), so its `log` column still reflects the
// pre-reset frame count at the instant of that same atomic call. TRUNCATE's
// eagerly-zeroed result makes it useless for the log==consumed comparison
// that catches a write landing in the endRead()→checkpoint() gap — the
// exact race takeover() is built to detect. So: RESTART first, to get a
// trustworthy verified-clean proof from a mechanism already proven correct;
// TRUNCATE second (running against the now-quiescent, already-emptied-in-
// SQLite's-own-bookkeeping WAL), purely to make the on-disk file physically
// match what RESTART already established logically. rebase() doesn't need
// this two-step dance because it always re-copies a fresh snapshot of the
// main DB file afterward regardless of what got folded — it never relies on
// an incrementally-tracked `consumed` count the way this verified-clean path
// does.
//
// ============================================================================
// KNOWN RESIDUAL RISK (task-7 hardening pass, Finding 2) — read before
// touching the RESTART/TRUNCATE sequence below.
//
// A foreign write that lands between the verified-clean checkpoint(RESTART)
// above and checkpoint(TRUNCATE) below gets folded by TRUNCATE exactly like
// any other frame, becomes part of the main file's content, and enters the
// hash baseline SaveState records at the end of this function. The replica
// never saw those frames — TRUNCATE's own result columns are eagerly zeroed
// (see the "why two checkpoints" paragraph above), so there is nothing in
// SQLite's own return values that distinguishes "TRUNCATE folded nothing
// new" from "TRUNCATE folded a write that landed in the gap." Neither a
// content hash nor any other metadata check performed from OUTSIDE SQLite
// can close this: by the time we can observe the main file again, the write
// is already indistinguishable from "was there before RESTART."
//
// walRacedSinceRestart() below narrows this window as far as is achievable
// through SQLite's pragma interface: it parses the on-disk -wal file
// directly, immediately before TRUNCATE is issued, and aborts the
// clean-state save if the header salts have moved past the generation
// checkpoint(RESTART) just verified, or if more frames are present than
// `consumed` accounts for. That shrinks the exposed window from "the full
// RESTART call plus the TRUNCATE call" down to "the microseconds between
// that parse returning and TRUNCATE acquiring its own locks" — a single Go
// function call with no intervening I/O. It does NOT close the window to
// zero. This is accepted as a residual risk for this spike, not claimed
// airtight: Plan 2's LTX sink closes it structurally, by comparing
// cumulative checksums at shutdown and resume rather than reasoning about a
// gap between two separate checkpoint calls at all.
// ============================================================================
//
// It uses a fresh, short-lived context rather than the engine's own ctx
// (already cancelled here by definition) — checkpoint() and endRead() both
// bail out immediately on a done context and would otherwise never run.
//
// Every failure path here is swallowed on purpose: it only forgoes the
// resume optimization for the next start (the last state drain() wrote
// stays on disk with Clean=false, which tryResume already rejects), never
// correctness. Silently skipping an optimization is fine; silently resuming
// across a gap is exactly the failure this task exists to kill.
//
// A foreign connection held open across our shutdown (see
// TestEngineResumesCleanly) does not by itself defeat RESTART/TRUNCATE:
// both only require that no OTHER connection hold a read or write lock on
// the WAL at the instant they run, which a merely-open idle connection does
// not. If one does race us with an actual lock, checkpoint()'s busy retry
// (up to 5s per call) absorbs ordinary contention; persistent busy still
// falls through to the swallowed-failure path above, which is always safe.
func (e *Engine) shutdown() {
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := e.drain(sctx); err != nil {
		// Reader.Next() advances the reader's own offset the moment it
		// returns a transaction's frames — BEFORE Sink.Apply ever runs (see
		// drain() and wal.Reader.Next()). So a failed Apply right here, in
		// the final drain, would otherwise leave that transaction counted as
		// consumed while the sink never actually received it: silently
		// excluding it from the replica forever, at the exact moment we're
		// about to persist a Clean=true marker asserting nothing is pending.
		// Abort the entire clean-state save — end the read tx and return,
		// leaving the last successfully drain()-written state (Clean=false)
		// on disk, so tryResume rejects it and the next start rebases
		// instead of silently dropping this transaction.
		e.endRead(sctx)
		return
	}
	off, _, _ := e.reader.Offset()
	var consumed int64
	if off != 0 {
		frameSize := int64(wal.FrameHeaderSize) + int64(e.pageSize)
		consumed = (off - wal.HeaderSize) / frameSize
	}
	e.endRead(sctx)

	logN, err := e.checkpoint(sctx, "RESTART")
	if err != nil || int64(logN) != consumed {
		// Not verified clean — either the checkpoint itself failed (busy,
		// timeout, DB gone) or frames were folded that we hadn't consumed.
		// Leave the last drain()-written state as-is (Clean=false), so
		// tryResume will reject it and the next start rebases. Safe, just
		// not optimal.
		return
	}

	// Narrow the checkpoint-window residual documented above: parse the
	// on-disk WAL directly, immediately before the TRUNCATE call that will
	// fold whatever's present into the main file. If a foreign write has
	// already landed a new generation, or appended frames beyond what
	// checkpoint(RESTART) just verified as `consumed`, abort here rather
	// than let TRUNCATE silently absorb it into the clean-state baseline.
	if raced, err := e.walRacedSinceRestart(consumed); raced || err != nil {
		return
	}

	if _, err := e.checkpoint(sctx, "TRUNCATE"); err != nil {
		// Verified clean logically (via RESTART above) but couldn't
		// physically empty the file — same fallback as above: leave state
		// as-is, next start rebases.
		return
	}

	// Fingerprint the main DB file (content hash, not mtime+size — see
	// state.go's doc comment for why) right now, immediately after the
	// checkpoint that folded everything we've captured into it. This is
	// tryResume's condition (2) — see its doc comment for why WAL-emptiness
	// alone isn't sufficient proof that nothing happened while we were down.
	hash, err := e.hashSrc()
	if err != nil {
		// Can't fingerprint ⇒ can't offer a safe resume point. Same
		// fallback: leave state as-is, next start rebases.
		return
	}

	SaveState(e.statePath(), State{
		Clean:    true,
		MainHash: hash,
	})
}

// walRacedSinceRestart parses the on-disk -wal file directly and reports
// whether it shows evidence of a write that landed after the
// checkpoint(RESTART) call whose `consumed` count the caller just verified
// as clean. See shutdown()'s "KNOWN RESIDUAL RISK" comment block for why
// this check exists and exactly how much of the window it does (and does
// not) close.
//
// Two signals, either one sufficient to report a race:
//
//   - The on-disk header's salts no longer match the generation
//     e.reader was bound to when drain() last consumed from it. RESTART's
//     reset is lazy (see takeover()'s doc comment) — salts only change once
//     a writer actually commits into the new generation, so a salt change
//     observed here means a writer both landed AND committed since our read
//     lock was released.
//   - The header's salts still match, but the file holds more complete
//     frames than `consumed` accounts for: a writer appended to the same
//     generation before its reset physically landed.
//
// A missing or sub-header-length WAL file is treated as "no race": TRUNCATE
// hasn't run yet at this point in shutdown(), so an absent/empty WAL here
// would only occur if something external already truncated it out from
// under us — indistinguishable from "nothing happened" by this check alone,
// and no more or less trustworthy than proceeding to TRUNCATE normally.
func (e *Engine) walRacedSinceRestart(consumed int64) (bool, error) {
	_, salt1, salt2 := e.reader.Offset()
	b, err := os.ReadFile(e.o.DBPath + "-wal")
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if len(b) < wal.HeaderSize {
		return false, nil
	}
	hdr, err := wal.ParseHeader(b[:wal.HeaderSize])
	if err != nil {
		// Torn header: something is actively writing it right now. Treat as
		// a race rather than guess — safe fallback either way.
		return true, nil
	}
	if hdr.Salt1 != salt1 || hdr.Salt2 != salt2 {
		return true, nil
	}
	frameSize := wal.FrameHeaderSize + int(hdr.PageSize)
	if frameSize <= 0 {
		return true, nil
	}
	available := int64(len(b)-wal.HeaderSize) / int64(frameSize)
	return available > consumed, nil
}

// drainBudget bounds how long a single ticker-driven drain call may keep
// consuming before it yields control back to Run's loop, even if more
// committed transactions are still arriving. See drain's doc comment for why
// this exists and why a partial catch-up here is harmless; the value is
// generous relative to everything it needs to absorb (see the comment).
const drainBudget = 2 * time.Second

// ErrDrainIncomplete is returned by drainUntil — and therefore can surface
// from DrainNow and, in turn, Session.Flush — when a DrainNow-triggered
// drain could not reach its target offset (see drainUntil) within
// drainSafetyDeadline. This is a loud, terminal-for-the-call signal that
// something is deeply wrong (e.g. sustained I/O stalls, or a target that
// somehow never becomes reachable) — never a silent "close enough". Flush
// must fail on this, not durably commit a snapshot that might be short.
var ErrDrainIncomplete = errors.New("capture: drain did not reach its target before the safety deadline")

// drainSafetyDeadline backstops drainUntil against pathological cases (see
// its doc comment) — deliberately generous, well above any observed real
// drain latency, because exceeding it is a loud failure (ErrDrainIncomplete)
// rather than a silent partial return. Session.Flush's own 30s DrainNow
// context (see flush.go) is the bound that actually governs day-to-day
// behavior; this is only the backstop that turns "ran out of time" into an
// observable error instead of a snapshot that silently omits committed work.
//
// 15s, not something closer to the full 30s Flush budget: a single
// DrainNow-triggered poll can spend this deadline on drainUntil AND,
// afterward, up to ~10s more on takeover's own checkpoint/beginReadRetry
// busy-retries (see poll and flush.go's comment on its 30s timeout) —
// 15+10=25s leaves Flush's context a real margin instead of racing its own
// backstop to expiry.
const drainSafetyDeadline = 15 * time.Second

// drainStep reads and applies exactly one committed transaction, if one is
// immediately available: it is the unit of work shared by drain (ticker,
// time-budgeted) and drainUntil (DrainNow, target-budgeted). Returns
// frames == nil, err == nil when nothing further is immediately available
// (including wal.ErrWALRestarted passed through unmodified, for the callers'
// shared restart handling in poll).
func (e *Engine) drainStep() ([]wal.Frame, error) {
	frames, err := e.reader.Next()
	if err != nil {
		return nil, err
	}
	if frames == nil {
		return nil, nil
	}
	// SQLite cannot change a database's page size while in WAL mode, so
	// e.pageSize (captured once via PRAGMA page_size at startup — see Run())
	// is assumed to hold for the process's entire lifetime and is what every
	// consumed-offset/frame-size computation in this file relies on. Make
	// that assumption explicit and fail loudly rather than silently mis-slice
	// frame data if it's ever violated.
	if ps := e.reader.PageSize(); ps != e.pageSize {
		return nil, fmt.Errorf("drain: WAL page size %d does not match engine page size %d recorded at startup", ps, e.pageSize)
	}
	if err := e.o.Sink.Apply(e.pageSize, frames); err != nil {
		return nil, err
	}
	off, s1, s2 := e.reader.Offset()
	if err := SaveState(e.statePath(), State{Off: off, Salt1: s1, Salt2: s2}); err != nil {
		return nil, err
	}
	return frames, nil
}

// drain consumes committed transactions until none are immediately available
// or drainBudget elapses, whichever comes first — it chases newly-landed
// transactions within that budget (matching a plain "loop until nothing's
// left" loop for any ordinary, finite burst) but is guaranteed to return
// control to its caller within bounded time regardless of how long the
// writer keeps writing.
//
// Ticker-driven only — see pollOnce. A budget-exhausted return here is (n,
// nil), indistinguishable from a return caused by genuinely catching up, and
// that is fine for this caller specifically: pollOnce's caller is Run's own
// ticker loop, which comes back around every e.o.Poll (default 10ms)
// regardless, so a partial catch-up this call is simply completed by the
// next one — liveness, not a promise that every committed transaction has
// been reached. This must NEVER be reused to service a DrainNow request:
// DrainNow's contract requires a real proof of completion, which is exactly
// what drainUntil (with a fixed target and a loud ErrDrainIncomplete on
// failure, rather than a silent nil) provides instead. See drainUntil's doc
// comment for the full contrast, including the review finding that led to
// splitting these two into separate functions.
//
// The budget is load-bearing, not cosmetic. e.reader.Next() re-reads the WAL
// file from disk on every call; under a foreign writer that never leaves a
// gap (see TestFlushesUnderContinuousWritesAreConsistent, which drives
// exactly this via a tight loop of single-row inserts with no pause), a
// plain "loop until Next() returns nil" never actually sees nil — a new
// commit is always landing just as the previous one finishes. That would
// starve Run's own ticker loop of a chance to notice anything else pending
// (a ctx cancellation, a DrainNow request) — empirically reproduced before
// this budget existed: drain() consumed 7500+ transactions over 60+ seconds
// without ever returning, until the writer stopped.
//
// A per-call time budget was chosen over bounding by the WAL's on-disk size
// snapshotted at entry (the first fix attempted here, now what drainUntil
// does instead — but only for the DrainNow path, where the resulting
// precision is load-bearing, not merely nice-to-have): a size snapshot stops
// drain from absorbing anything that lands after it starts, even when doing
// so would be perfectly safe — which measurably slowed down ordinary,
// finite-burst catch-up in TestEngineTakeoverExpectedRestartIsNotRebase and
// TestEngineTakeoverUnderConcurrentWrites (each extra chunk boundary costs a
// wait for Run's next 10ms tick; under those tests' per-transaction fsync
// cost in SaveState, that was enough to blow through the tests' own timing
// margins and turn a benign fold-then-resync race into an observed extra
// rebase). A time budget instead keeps this ticker path's old
// chase-everything-currently-available behavior intact for any burst that
// finishes within the budget — which every legitimate burst does, by a wide
// margin — and only kicks in as a backstop against a writer that truly never
// stops. 2 seconds leaves ample headroom above the slowest observed real
// burst (~400ms for ~75 fsync'd transactions).
//
// Deliberately does NOT also check ctx.Err() and bail out early: an earlier
// version of this fix did, and it broke clean shutdown. Run's ticker-path
// case treats any non-nil pollOnce error as fatal and returns it directly —
// correct for a real capture failure, but ctx cancellation via Close() is
// not one; it is the ordinary shutdown signal, meant to be observed only at
// Run's own top-level select and handled via shutdown() (which drains one
// last time on a fresh, uncancelled context — see its doc comment). If
// Close() cancels ctx while a ticker-triggered drain() happens to be
// mid-flight, an early ctx-aware return would surface context.Canceled as
// pollOnce's error, which Run's ticker branch would then return as ITS OWN
// result — skipping shutdown() entirely: no final checkpoint, no clean-state
// marker, and runEngine (see session.go) records it as a genuine failure
// ("session: capture stopped: context canceled") even though nothing went
// wrong. Empirically reproduced via TestCloseDoesNotMarkSessionFenced and
// TestEngineResumesCleanly failing intermittently under load (contention
// makes the cancel-lands-mid-drain race far more likely to be hit) once this
// check was added, and gone once it was removed. The drainBudget deadline
// above is sufficient on its own to bound this call; ctx cancellation is
// still honored, just one loop iteration later than it could be in the
// absolute worst case, at Run's own select.
func (e *Engine) drain(ctx context.Context) (int, error) {
	deadline := time.Now().Add(drainBudget)
	n := 0
	for {
		if time.Now().After(deadline) {
			return n, nil
		}
		frames, err := e.drainStep()
		if err != nil {
			return n, err
		}
		if frames == nil {
			return n, nil
		}
		n++
	}
}

// drainUntil services one attempt within a DrainNow request: it drains until
// the reader's consumed offset reaches or passes target, or until nothing
// further is immediately available, whichever comes first. target is the
// WAL's on-disk size read once, up front, by serviceReq/drainTarget at the
// moment Run's goroutine actually picks up the request — see drainTarget's
// doc comment for why it must be read there and not inside DrainNow itself.
// deadline is an absolute cutoff, not a duration relative to this call:
// pollOnceTo computes it once and threads it through every attempt of a
// single DrainNow request, including any re-attempts after a WAL restart
// forces target to be re-derived (see pollOnceTo's doc comment) — so a
// request that hits several restarts in a row still cannot run past a
// single drainSafetyDeadline in aggregate, only per restart.
//
// Why this — a fixed target — and not drain()'s time budget, for DrainNow:
// DrainNow's documented contract is "every transaction already committed to
// the checkout's WAL as of the moment this call is issued is captured
// before it returns" — Session.Flush depends on that unconditionally (see
// flush.go). drain()'s wall-clock budget cannot make that promise: under
// sustained writes it can (and, per the review that motivated this
// function, empirically does — hitting its deadline mid-stream on nearly
// every call) return having only partially caught up, and a
// budget-exhausted drain() returns (n, nil) — indistinguishable, from the
// return value alone, from a drain that genuinely reached everything
// available. A caller cannot tell "reached the target" from "ran out of
// time" from that, which reopens exactly the hazard a prior task fixed:
// Flush durably committing a snapshot that omits a write already committed
// to the checkout before Flush was even called.
//
// A target — a fixed offset decided once, up front — rather than a moving
// deadline is what makes this terminate under a nonstop writer while still
// honoring the contract: new transactions committed AFTER the request was
// made are explicitly not required, and the target does not move to chase
// them. The loop below stops as soon as either becomes true:
//
//   - the reader's consumed offset has reached or passed target: everything
//     that was on disk at request time has been read and applied (and
//     possibly a bit more, if the transaction straddling target extended
//     past it — harmless, that's still committed data), or
//   - drainStep reports nothing further is immediately available. Since the
//     WAL only ever grows (never shrinks except via checkpoint/rebase,
//     handled separately — see below) and target was read from its on-disk
//     size, any gap between the current offset and target at this point can
//     only be the tail of a transaction that had not yet finished
//     committing as of the moment target was read — which means it had not
//     committed as of the request either, and so is outside DrainNow's
//     contract by definition, not a shortfall.
//
// A wal.ErrWALRestarted from drainStep is returned as-is to pollOnceTo's
// restart handling — see its doc comment for why a restart requires
// re-deriving target and looping (expectRestart) or is already subsumed by
// a rebase, rather than being handled by comparing offsets against the
// stale target directly.
//
// deadline backstops this against pathological cases (e.g. an I/O stall or
// a target that somehow never becomes reachable within a sane window) — but
// exceeding it surfaces as ErrDrainIncomplete, a loud error, never a silent
// nil: see drainSafetyDeadline's doc comment.
//
// CRITICAL ordering, found the hard way: drainStep MUST run at least once
// per iteration BEFORE off is ever compared against target — never check
// off >= target first and only call drainStep if it's false. An earlier
// version did check first, on the reasoning that off only ever grows and
// target was read from the same file, so off >= target should mean "already
// there." That reasoning silently breaks across a WAL restart the reader
// hasn't observed yet: e.reader.Offset() is a byte count WITHIN WHATEVER
// GENERATION the reader last bound to, but a checkpoint(RESTART) that lands
// between one DrainNow call and the next causes the WAL file to be reused
// from HeaderSize, not grown — so the file's on-disk size after the new
// generation accumulates its own commits is in no way guaranteed to exceed
// (or even relate to) the OLD generation's final consumed offset. Reproduced
// directly: two rounds of ~301 identically-shaped transactions each,
// separated by a clean takeover+restart (armed by round N-1, landing on
// round N's first write) — round N's fresh generation happened to reach
// almost exactly the same on-disk size as round N-1's old generation had,
// so off (stale, still positioned in the old generation) >= target (the new
// generation's current size) was true from the very first loop iteration,
// short-circuiting before drainStep — and therefore Next()'s own salt
// check, the only thing that actually notices a restart happened — ever
// ran. drainUntil returned (0, nil) claiming completion while an entire
// round's backlog and marker row, already sitting in the new generation,
// went completely undrained. Calling drainStep unconditionally first
// forces Next() to validate the reader's binding against whatever's
// CURRENTLY on disk before off is ever trusted for a target comparison: on
// a restart it returns wal.ErrWALRestarted (caught below, handled by
// pollOnceTo), never a stale offset masquerading as "caught up."
func (e *Engine) drainUntil(ctx context.Context, target int64, deadline time.Time) (int, error) {
	n := 0
	for {
		if time.Now().After(deadline) {
			off, _, _ := e.reader.Offset()
			return n, fmt.Errorf("%w: consumed offset %d has not reached target %d after %s",
				ErrDrainIncomplete, off, target, drainSafetyDeadline)
		}
		frames, err := e.drainStep()
		if err != nil {
			return n, err
		}
		if frames != nil {
			n++
		}
		off, _, _ := e.reader.Offset()
		if off >= target {
			return n, nil
		}
		if frames == nil {
			return n, nil
		}
	}
}

func (e *Engine) beginRead(ctx context.Context) error {
	if e.inTx {
		return nil
	}
	if _, err := e.conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	if _, err := e.conn.ExecContext(ctx, "SELECT count(*) FROM sqlite_master"); err != nil {
		e.conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	e.inTx = true
	return nil
}

func (e *Engine) endRead(ctx context.Context) {
	if e.inTx {
		e.conn.ExecContext(ctx, "COMMIT")
		e.inTx = false
	}
}

// tryResume resumes capture without a rebase when — and only when — it can
// prove nothing was missed while the process was down. Two independent
// conditions must both hold; either one failing means rebase:
//
//  1. A saved state exists with Clean=true (see state.go's doc comment;
//     produced only by shutdown()'s verified-clean RESTART+TRUNCATE
//     sequence, which physically zeroes the -wal file at the moment we went
//     down) AND the on-disk -wal file confirms it is still physically empty
//     right now: absent, zero-length, or shorter than a complete header.
//
//  2. The main DB file's (not the WAL's) content — a SHA-256 hash over its
//     entire bytes — is identical to what shutdown() recorded immediately
//     after its final TRUNCATE. This closes a gap condition (1) alone
//     cannot: TRUNCATE physically erases the WAL, so an empty WAL is
//     necessary but NOT sufficient proof that nothing happened — it is
//     equally what you'd see if writes landed while we were down AND got
//     fully folded into the main file by someone else (a third-party
//     checkpoint, or — empirically confirmed, see
//     TestEngineDetectsMissedWritesAfterCrash — SQLite's own automatic
//     checkpoint-and-WAL-deletion when the last connection to a WAL-mode
//     database closes). That folding writes real bytes to the main file,
//     which condition (2) catches even though condition (1) alone cannot:
//     it can't tell "empty because nothing happened" from "empty because
//     something happened and got absorbed." (The SQLite file change-counter
//     header field was tried and rejected for this: verified empirically
//     that WAL-mode checkpoints do not reliably bump it, unlike
//     rollback-journal-mode commits — see the task-7 fix report.)
//
//     Content hash, not mtime+size (task-7 hardening pass, Finding 1):
//     mtime+size was the original implementation and is insufficient on its
//     own. An in-place UPDATE (same row count, fixed-width columns) folds
//     into the main file without changing its size, and mtime granularity
//     is filesystem-dependent (1s on HFS+/FAT/NFS with client-side caching)
//     — a resume cycle fast enough to land in that window, combined with
//     such an UPDATE landing while the engine was down, would silently pass
//     an mtime+size check even though the file's content, and therefore the
//     replica's correctness, has diverged. See
//     TestEngineDetectsInPlaceUpdateAfterCleanShutdown for the regression
//     test. A hash has no such blind spot: any byte difference anywhere in
//     the file changes it, independent of size or timing. Any read/hash
//     error on either end of the comparison (here or in shutdown()) is
//     treated as "can't prove continuity" ⇒ rebase, the same conservative
//     fallback as every other failure path in this function.
//
// Condition (1)'s WAL-emptiness check is still necessary on its own even
// with (2) added: without ever-physically-truncating (i.e. under the old,
// buggy RESTART-only scheme), the main file's content would legitimately
// match (RESTART doesn't touch the main file beyond what it already
// checkpointed) while OLD, already-applied frames remained physically
// present in the WAL for a fresh reader to wrongly re-consume — exactly
// Finding 1's original bug. Condition (2) alone would not have caught that.
//
// Unlike a RESTART-based marker, TRUNCATE leaves nothing to compare salts
// against: the file is gone or empty, full stop. So there's no old
// generation to pin the reader to and no drift to detect by comparison —
// there is only "still empty" (safe to resume, pending condition 2) or "not
// still empty" (something wrote since shutdown; rebase). Since TRUNCATE
// means the physical file itself is the proof, a torn/partial header found
// here is necessarily a write in progress racing us on the way up, not a
// leftover from before we went down — treated the same as "not provably
// empty" ⇒ rebase, never trusted.
//
// The reader is left UNBOUND (not Bound at a remembered offset/salt): there
// is no prior generation's salts to pin against, so it will simply self-bind
// to whatever header the next writer creates, treating that as the start of
// a new generation — which is exactly what it is. Consequently no
// expectRestart arming is needed on this path (contrast takeover(), which
// DOES arm it): an unbound reader's first Next() call binds rather than
// compares, so there is no "expected restart" for it to ever raise.
//
// Lock ordering: the read lock (beginRead) is acquired FIRST, before either
// condition is checked, not after both pass. Checking-then-locking leaves a
// TOCTOU gap — a foreign commit+checkpoint(TRUNCATE) landing between "checks
// passed" and "lock acquired" would satisfy both conditions as observed yet
// still be exactly the missed-write scenario they exist to catch. Locking
// first closes that gap structurally: our view of the database is
// established before either check runs, so nothing can land in between.
func (e *Engine) tryResume(ctx context.Context) (bool, error) {
	st, ok, err := LoadState(e.statePath())
	if err != nil {
		return false, err
	}
	if !ok || !st.Clean {
		return false, nil
	}

	// Acquire the read lock BEFORE either verification check below, not
	// after. Checking first and locking second leaves a TOCTOU window: a
	// foreign commit followed by a checkpoint(TRUNCATE) landing between "WAL
	// confirmed empty"/"hash confirmed match" and "lock acquired" would
	// pass both checks yet still represent exactly the missed-write scenario
	// they exist to catch. Locking first closes it — beginRead's BEGIN
	// (deferred, but the SELECT immediately after actually opens the read
	// transaction — see beginRead) establishes our view of the database
	// before either check runs, so nothing can land in the gap the checks
	// are trying to detect. Any verification failure past this point ends
	// the read transaction before returning, so the caller's subsequent
	// rebase() (which starts with its own endRead()) begins from the
	// engine's normal not-in-a-transaction state — no double-BEGIN.
	if err := e.beginRead(ctx); err != nil {
		return false, err
	}

	fi, statErr := os.Stat(e.o.DBPath + "-wal")
	switch {
	case statErr == nil && fi.Size() >= int64(wal.HeaderSize):
		// A WAL with at least a full header present means a write landed
		// since our clean shutdown (TRUNCATE leaves nothing behind) ⇒
		// can't prove continuity ⇒ rebase.
		e.endRead(ctx)
		return false, nil
	case statErr != nil && !os.IsNotExist(statErr):
		// Unreadable for some other reason ⇒ can't prove continuity ⇒ rebase.
		e.endRead(ctx)
		return false, nil
	}
	// Absent, zero-length, or a torn partial header: still provably empty
	// (or empty enough that any bytes present can't be a completed write).
	// Condition (2): the main DB file's content hash must be untouched since
	// our shutdown's fingerprint (see doc comment above for why this is
	// needed in addition to WAL-emptiness, and why a hash rather than
	// mtime+size).
	hash, err := e.hashSrc()
	if err != nil {
		e.endRead(ctx)
		return false, nil // can't hash the main file ⇒ can't prove continuity ⇒ rebase
	}
	if hash != st.MainHash {
		e.endRead(ctx)
		return false, nil // main file content changed since our shutdown ⇒ rebase
	}
	e.reader = wal.NewReader(e.o.DBPath + "-wal")
	e.captured = 0
	e.rebased.Store(0) // a true resume is never a rebase
	e.expectRestart = false
	e.resumed.Store(true)
	return true, nil
}

// rebase: checkpoint, snapshot the main file, reset reader + sink + state.
func (e *Engine) rebase(ctx context.Context) error {
	// Any rebase supersedes a prior takeover's expectation of a clean
	// continuation — full re-establishment from a snapshot is happening
	// instead, so the eventual WAL restart (if any) must not be treated as
	// an already-accounted-for continuation.
	e.expectRestart = false
	e.endRead(ctx)
	if _, err := e.checkpoint(ctx, "TRUNCATE"); err != nil {
		// Loud failure, not a fallback. This used to fall back to VACUUM
		// INTO (which needs no exclusive checkpoint lock) on the theory that
		// some snapshot beats none. It doesn't: VACUUM re-pages the
		// database from scratch, so its output is physically incompatible
		// with raw frame replay at fixed page numbers — frames captured
		// afterward would be applied at pgnos that no longer mean what they
		// used to in the snapshot. Worse, the un-truncated WAL (TRUNCATE
		// having failed) would then be replayed on top of that
		// already-incompatible snapshot. Returning the error instead is
		// exactly the loud-failure-as-detection posture this spike is built
		// around: Run's caller already treats any rebase error as fatal, so
		// this surfaces immediately instead of silently producing a replica
		// that can never converge.
		return fmt.Errorf("rebase: checkpoint TRUNCATE: %w", err)
	}
	// Copy-window note: from here until copySrc below returns, no read lock
	// is held — endRead() already ran above, and beginRead() doesn't run
	// again until after the copy. A foreign writer can commit in that
	// window, and a passive checkpoint (its own, or SQLite's automatic one)
	// can fold those new frames into the main DB file while copySrc is
	// mid-read, so the snapshot produced here may include bytes this
	// session's WAL reader never saw pass through Apply.
	//
	// This is convergent, not a correctness gap. Folded frames stay
	// physically present in the WAL regardless of whether they made it into
	// the copied snapshot — the fresh reader bound just below picks up the
	// new generation from its start, and Sink.Apply re-applies those same
	// frames as full-page images at their original page numbers. Writing
	// the same bytes to the same pgno a second time heals any torn or
	// half-copied page the race could have produced, rather than
	// compounding it. See the Sink interface doc comment above for the
	// consequence this has for a sink's Apply/Rebase overlap contract.
	//
	// One further wrinkle now that the copy reads through an io.SectionReader
	// (see srcReader): the section's length is the file's size as of the
	// moment the reader is obtained, so a foreign writer that GROWS the main
	// file mid-copy — a checkpoint folding frames that extend the database —
	// has its extra pages truncated out of the snapshot rather than
	// half-copied into it. That is strictly the safer of the two outcomes and
	// needs no extra handling for the same reason the torn-page case above
	// needs none: every frame in the current WAL generation is re-applied on
	// top of this snapshot by the fresh reader bound just below, so pages the
	// snapshot is missing or has stale are written again at their original
	// pgno. A short snapshot heals; it does not diverge.
	if err := e.copySrc(e.snapshotPath()); err != nil {
		return err
	}
	if err := e.beginRead(ctx); err != nil {
		return err
	}
	if err := e.o.Sink.Rebase(e.snapshotPath()); err != nil {
		return err
	}
	e.reader = wal.NewReader(e.o.DBPath + "-wal")
	e.rebased.Add(1)
	e.captured = 0
	return SaveState(e.statePath(), State{})
}

// takeover: restart the WAL under our control. Only safe when fully drained.
//
// There is an inherent gap between endRead() (releasing our read lock, which
// is what stops foreign checkpoints from restarting the WAL) and a
// successful checkpoint(RESTART) actually resetting it. A foreign writer can
// commit frames in that gap; RESTART folds *everything* present in the WAL —
// including those unseen frames — into the main DB file and then resets it.
// If we blindly rebound a fresh reader at that point, those frames would
// never be captured: silent divergence.
//
// We detect this by comparing frames consumed (from our reader's offset,
// recorded before endRead) against `log`, the total frame count RESTART
// reports for the WAL at checkpoint time. If they match, nothing was folded
// unseen and a fresh reader is correct. If they don't, we rebase: the
// snapshot copy happens after the fold, so it already contains those frames
// — continuity is detected and re-established, never silently lost.
func (e *Engine) takeover(ctx context.Context) error {
	off, _, _ := e.reader.Offset()
	var consumed int64
	if off != 0 {
		frameSize := int64(wal.FrameHeaderSize) + int64(e.pageSize)
		consumed = (off - wal.HeaderSize) / frameSize
	}

	// Default to "no expectation" for every path except the verified-clean
	// one below. A second takeover firing while a prior one's expectation
	// is still pending (the lazy reset hasn't physically landed yet) will
	// recompute log==consumed against an unchanged, already-checkpointed
	// WAL and land back on the clean path, re-arming it — so the flag stays
	// effectively true across repeated clean takeovers until the restart
	// actually lands or a fold forces a rebase.
	e.expectRestart = false

	e.endRead(ctx)
	logN, err := e.checkpoint(ctx, "RESTART")
	if err != nil {
		// The WAL was NOT restarted (busy/timeout/ctx cancelled): our
		// existing reader is still correctly positioned. Do not reset it —
		// just reacquire the read lock so foreign checkpoints stay blocked.
		return e.beginReadRetry(ctx, 5*time.Second)
	}

	if int64(logN) != consumed {
		// Frames were folded into the main DB that we never saw: detected
		// continuity loss, counted via rebase — not silent.
		return e.rebase(ctx)
	}

	// Cheap path: everything folded by RESTART is exactly everything we'd
	// already consumed — nothing lost, no rebase needed.
	//
	// Deliberately do NOT replace e.reader here. SQLite's RESTART/TRUNCATE
	// reset is lazy: a successful (busy=0) checkpoint only *arms* the reset;
	// the WAL file's header/salts on disk are not rewritten until the next
	// writer commits (verified empirically — see task-5 fix report). A fresh
	// unbound reader created at this instant would simply re-bind to the
	// still-unchanged file and re-read the same already-consumed frames,
	// spiking `captured` straight back over the takeover threshold and
	// spinning takeover in a tight loop that starves the writer. The
	// existing reader is already correctly positioned at `off` with valid
	// running-checksum state: it will either keep consuming new frames from
	// the same generation, or — once the physical reset actually lands —
	// detect the salt change itself and return wal.ErrWALRestarted.
	//
	// That eventual restart is expected, not a loss: at this instant replica
	// state == base snapshot + all applied frames == main DB content at the
	// restart point (that's exactly what log==consumed just verified), so
	// the next WAL generation's frames apply cleanly on top. Mark it so
	// Run()'s poll loop treats the eventual ErrWALRestarted as a
	// continuation (fresh reader, no rebase, not counted) rather than a
	// detected loss.
	e.captured = 0
	if err := e.beginReadRetry(ctx, 5*time.Second); err != nil {
		return err
	}
	e.expectRestart = true
	return nil
}

// beginReadRetry retries beginRead with backoff until it succeeds, ctx is
// cancelled, or maxWait elapses. The read lock is what prevents foreign
// checkpoints from restarting the WAL out from under us; running without it
// must never happen silently, so persistent failure is returned to the
// caller rather than swallowed.
func (e *Engine) beginReadRetry(ctx context.Context, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	backoff := 10 * time.Millisecond
	for {
		err := e.beginRead(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("beginRead: giving up after %s: %w", maxWait, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

// checkpoint runs PRAGMA wal_checkpoint(mode), retrying on busy up to 5s.
// It returns `log`, the total number of valid frames in the WAL at
// checkpoint time (needed by takeover to detect folded-but-unseen frames).
func (e *Engine) checkpoint(ctx context.Context, mode string) (log int, err error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		var busy, logN, ckptN int
		qerr := e.conn.QueryRowContext(ctx,
			"PRAGMA wal_checkpoint("+mode+")").Scan(&busy, &logN, &ckptN)
		if qerr == nil && busy == 0 {
			return logN, nil
		}
		if time.Now().After(deadline) {
			if qerr != nil {
				return 0, fmt.Errorf("checkpoint %s: %w", mode, qerr)
			}
			return 0, fmt.Errorf("checkpoint %s: busy", mode)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// srcReader returns a reader over the whole main database file as it exists
// right now, obtained from internal/dbfile rather than by opening a
// descriptor of this engine's own.
//
// The indirection is the entire fix for a silent capture failure, so it is
// worth being precise about why an engine-owned descriptor cannot work here,
// even one carefully closed last at teardown. POSIX advisory locks are keyed
// by (process, inode): closing any descriptor drops every lock the PROCESS
// holds on that file, including locks belonging to SQLite connections this
// engine does not own and cannot see — another session's engine on the same
// checkout, or an application connection a caller is holding open across
// this engine's lifetime. An engine-scoped descriptor is therefore safe only
// if this engine is the last thing in the process touching that file, which
// is precisely what an engine cannot know. dbfile's descriptors are never
// closed at all, which is the only formulation that is unconditionally safe.
// See dbfile's package comment; TestEngineResumesCleanly and
// TestEngineResumeAppliesNothingBeforeNewWrite are the regression tests
// (both hold a foreign connection open across an engine bounce).
func (e *Engine) srcReader() (*io.SectionReader, error) {
	return dbfile.Reader(e.o.DBPath)
}

// hashSrc returns the lowercase-hex SHA-256 of the main database file,
// computed by streaming its full contents (io.Copy into the hash, not a full
// in-memory read).
//
// Plan-2 note: this re-hashes the entire main DB file on every clean
// shutdown and every resume attempt — an O(file size) cost at exactly two
// process-lifetime boundaries, not a hot-path cost. For this spike's target
// session sizes (MBs to low-single-digit GBs) that's acceptable, but it does
// not scale indefinitely: a much larger DB would make this a noticeable
// startup/shutdown stall. Plan 2's LTX sink carries cumulative checksums
// that already cover the full committed history incrementally as writes
// happen; once that lands, this whole-file re-hash should be replaced by
// comparing against LTX's own running checksum instead of re-deriving one
// from scratch here.
func (e *Engine) hashSrc() (string, error) {
	r, err := e.srcReader()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// copySrc copies the main database file to `to`. Only the destination
// descriptor is opened and closed here; the source is read through
// srcReader. Closing the destination is safe precisely because SQLite holds
// no locks on it — it is a fresh snapshot path, not the database this engine
// has open.
func (e *Engine) copySrc(to string) error {
	r, err := e.srcReader()
	if err != nil {
		return err
	}
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, r); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
