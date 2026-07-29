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
			e.takeover(ctx)
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
	if err := e.checkpoint(ctx, "TRUNCATE"); err != nil {
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
func (e *Engine) takeover(ctx context.Context) {
	e.endRead(ctx)
	if err := e.checkpoint(ctx, "RESTART"); err == nil {
		// Restart succeeded: new salts. All prior frames were captured
		// (drain precedes takeover), so a fresh reader is correct, not lossy.
		e.reader = wal.NewReader(e.o.DBPath + "-wal")
		e.captured = 0
	}
	e.beginRead(ctx) // best effort; next drain surfaces any problem
}

func (e *Engine) checkpoint(ctx context.Context, mode string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		var busy, logN, ckptN int
		err := e.conn.QueryRowContext(ctx,
			"PRAGMA wal_checkpoint("+mode+")").Scan(&busy, &logN, &ckptN)
		if err == nil && busy == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("checkpoint %s: busy", mode)
		}
		time.Sleep(50 * time.Millisecond)
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
