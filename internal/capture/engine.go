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

	if err := e.rebase(ctx); err != nil {
		return err
	}

	idle := time.Now()
	tick := time.NewTicker(e.o.Poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			e.drain(ctx)
			e.endRead(ctx)
			return nil
		case <-tick.C:
		}
		n, err := e.drain(ctx)
		if err == wal.ErrWALRestarted {
			// Our lock lapsed (or an external RESTART happened): continuity
			// lost — detected. Re-establish from a fresh snapshot.
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

// rebase: checkpoint, snapshot the main file, reset reader + sink + state.
func (e *Engine) rebase(ctx context.Context) error {
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
	// detect the salt change itself and return wal.ErrWALRestarted, which
	// the poll loop already handles via a counted rebase.
	e.captured = 0
	return e.beginReadRetry(ctx, 5*time.Second)
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
