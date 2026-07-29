// Package capture implements the foreign-connection WAL capture engine:
// the read-lock dance, incremental frame capture, checkpoint takeover, and
// rebase-on-divergence. This is offshoot's risk-spike core.
package capture

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
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
	rebased  int
	captured int // txns since last takeover

	// expectRestart is true immediately after a verified-clean takeover
	// (checkpoint RESTART folded exactly what we'd already consumed, no
	// more). It means the WAL's lazy header/salt reset — which lands on the
	// *next* writer's commit, not at RESTART time itself — is anticipated:
	// when it lands, our replica state already equals the main DB content at
	// the restart point, so the new WAL generation is a continuation, not a
	// continuity loss. See takeover() and Run()'s wal.ErrWALRestarted
	// handling.
	expectRestart bool
}

func NewEngine(o Options) *Engine {
	if o.Poll == 0 {
		o.Poll = 10 * time.Millisecond
	}
	return &Engine{o: o}
}

// Rebased reports how many times the engine rebased (1 = initial only).
// Every rebase beyond the first means continuity was lost and re-established
// — detected, never silent.
func (e *Engine) Rebased() int { return e.rebased }

func (e *Engine) statePath() string    { return filepath.Join(e.o.StateDir, "capture-state.json") }
func (e *Engine) snapshotPath() string { return filepath.Join(e.o.StateDir, "snapshot.db") }

// Run blocks, capturing until ctx is cancelled. It performs the initial
// rebase (checkpoint + snapshot copy), holds the read-lock dance, polls for
// committed transactions, and periodically performs checkpoint takeover.
func (e *Engine) Run(ctx context.Context) error {
	var err error
	e.db, err = sql.Open("sqlite3",
		e.o.DBPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return err
	}
	defer e.db.Close()
	e.conn, err = e.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer e.conn.Close()
	if _, err := e.conn.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		return err
	}
	if err := e.conn.QueryRowContext(ctx, "PRAGMA page_size").Scan(&e.pageSize); err != nil {
		return err
	}

	if resumed, err := e.tryResume(ctx); err != nil {
		return err
	} else if !resumed {
		if err := e.rebase(ctx); err != nil {
			return err
		}
	}

	idle := time.Now()
	tick := time.NewTicker(e.o.Poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			e.shutdown()
			return nil
		case <-tick.C:
		}
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
				continue
			}
			// Our lock lapsed (or an external RESTART happened) without a
			// preceding verified-clean takeover: continuity lost —
			// detected. Re-establish from a fresh snapshot.
			if err := e.rebase(ctx); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if n > 0 {
			idle = time.Now()
			e.captured += n
		}
		if e.captured >= 64 || (e.captured > 0 && time.Since(idle) > 5*time.Second) {
			if err := e.takeover(ctx); err != nil {
				if ctx.Err() != nil {
					// Shutting down: let the ctx.Done() branch above
					// perform the final drain/endRead and return nil.
					continue
				}
				return err
			}
		}
	}
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
// The gap between the two calls is a single Go conditional with no
// intervening I/O — about as tight as achievable through SQLite's pragma
// interface. A writer landing in exactly that gap would get folded by the
// TRUNCATE call the same as anything else, with no way for us to detect it
// from TRUNCATE's zeroed return values; this residual risk is accepted as
// negligible relative to the two-call round-trip's overall latency.
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

	e.drain(sctx)
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

	if _, err := e.checkpoint(sctx, "TRUNCATE"); err != nil {
		// Verified clean logically (via RESTART above) but couldn't
		// physically empty the file — same fallback as above: leave state
		// as-is, next start rebases.
		return
	}

	// Fingerprint the main DB file (mtime+size) right now, immediately after
	// the checkpoint that folded everything we've captured into it. This is
	// tryResume's condition (2) — see its doc comment for why WAL-emptiness
	// alone isn't sufficient proof that nothing happened while we were down.
	mfi, err := os.Stat(e.o.DBPath)
	if err != nil {
		// Can't fingerprint ⇒ can't offer a safe resume point. Same
		// fallback: leave state as-is, next start rebases.
		return
	}

	SaveState(e.statePath(), State{
		Clean:       true,
		MainMTimeNS: mfi.ModTime().UnixNano(),
		MainSize:    mfi.Size(),
	})
}

// drain consumes all currently available committed transactions.
func (e *Engine) drain(ctx context.Context) (int, error) {
	n := 0
	for {
		frames, err := e.reader.Next()
		if err != nil {
			return n, err
		}
		if frames == nil {
			return n, nil
		}
		if err := e.o.Sink.Apply(e.pageSize, frames); err != nil {
			return n, err
		}
		off, s1, s2 := e.reader.Offset()
		if err := SaveState(e.statePath(), State{Off: off, Salt1: s1, Salt2: s2}); err != nil {
			return n, err
		}
		n++
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
//  2. The main DB file's (not the WAL's) mtime and size are byte-for-byte
//     identical to what shutdown() recorded immediately after its final
//     TRUNCATE. This closes a gap condition (1) alone cannot: TRUNCATE
//     physically erases the WAL, so an empty WAL is necessary but NOT
//     sufficient proof that nothing happened — it is equally what you'd see
//     if writes landed while we were down AND got fully folded into the main
//     file by someone else (a third-party checkpoint, or — empirically
//     confirmed, see TestEngineDetectsMissedWritesAfterCrash — SQLite's own
//     automatic checkpoint-and-WAL-deletion when the last connection to a
//     WAL-mode database closes). That folding writes real bytes to the main
//     file, which condition (2) catches even though condition (1) alone
//     cannot: it can't tell "empty because nothing happened" from "empty
//     because something happened and got absorbed." (The SQLite file
//     change-counter header field was tried and rejected for this: verified
//     empirically that WAL-mode checkpoints do not reliably bump it, unlike
//     rollback-journal-mode commits — see the task-7 fix report.)
//
// Condition (1)'s WAL-emptiness check is still necessary on its own even
// with (2) added: without ever-physically-truncating (i.e. under the old,
// buggy RESTART-only scheme), the main file's mtime/size would legitimately
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
func (e *Engine) tryResume(ctx context.Context) (bool, error) {
	st, ok, err := LoadState(e.statePath())
	if err != nil {
		return false, err
	}
	if !ok || !st.Clean {
		return false, nil
	}
	fi, statErr := os.Stat(e.o.DBPath + "-wal")
	switch {
	case statErr == nil && fi.Size() >= int64(wal.HeaderSize):
		// A WAL with at least a full header present means a write landed
		// since our clean shutdown (TRUNCATE leaves nothing behind) ⇒
		// can't prove continuity ⇒ rebase.
		return false, nil
	case statErr != nil && !os.IsNotExist(statErr):
		// Unreadable for some other reason ⇒ can't prove continuity ⇒ rebase.
		return false, nil
	}
	// Absent, zero-length, or a torn partial header: still provably empty
	// (or empty enough that any bytes present can't be a completed write).
	// Condition (2): the main DB file itself must be untouched since our
	// shutdown's fingerprint (see doc comment above for why this is needed
	// in addition to WAL-emptiness).
	mfi, err := os.Stat(e.o.DBPath)
	if err != nil {
		return false, nil // can't read the main file ⇒ can't prove continuity ⇒ rebase
	}
	if mfi.ModTime().UnixNano() != st.MainMTimeNS || mfi.Size() != st.MainSize {
		return false, nil // main file changed since our shutdown ⇒ rebase
	}
	if err := e.beginRead(ctx); err != nil {
		return false, err
	}
	e.reader = wal.NewReader(e.o.DBPath + "-wal")
	e.captured = 0
	e.rebased = 0 // a true resume is never a rebase
	e.expectRestart = false
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
		// Fall back to VACUUM INTO — needs no exclusive checkpoint lock.
		os.Remove(e.snapshotPath())
		if _, verr := e.conn.ExecContext(ctx,
			fmt.Sprintf("VACUUM INTO %q", e.snapshotPath())); verr != nil {
			return fmt.Errorf("rebase: checkpoint %v; vacuum %v", err, verr)
		}
	} else {
		if err := copyFile(e.o.DBPath, e.snapshotPath()); err != nil {
			return err
		}
	}
	if err := e.beginRead(ctx); err != nil {
		return err
	}
	if err := e.o.Sink.Rebase(e.snapshotPath()); err != nil {
		return err
	}
	e.reader = wal.NewReader(e.o.DBPath + "-wal")
	e.rebased++
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

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
