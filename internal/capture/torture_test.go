//go:build torture

package capture

import (
	"context"
	"math/rand"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/replay"
	"github.com/offshoot-db/offshoot/internal/wal"
)

const writerSQL = `PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);
BEGIN; INSERT INTO t (v, n) VALUES (randomblob(200), 1); COMMIT;
BEGIN; UPDATE t SET n = n + 1 WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 5); COMMIT;
BEGIN; INSERT INTO t (v, n) SELECT randomblob(100), 0 FROM t LIMIT 3; COMMIT;
BEGIN; DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 1); COMMIT;`

type tortureSink struct{ r *replay.Replica }

func (s tortureSink) Rebase(p string) error                { return s.r.Rebase(p) }
func (s tortureSink) Apply(ps uint32, f []wal.Frame) error { return s.r.Apply(ps, f) }

func TestTortureWriterKill(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: src, StateDir: dir, Sink: tortureSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	engDone := make(chan error, 1)
	go func() { engDone <- e.Run(ctx) }()
	defer cancel()

	deadline := time.Now().Add(5 * time.Minute)
	round := 0
	for time.Now().Before(deadline) {
		round++
		cmd := exec.Command("sqlite3", src, writerSQL)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if rand.Intn(2) == 0 {
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			cmd.Process.Signal(syscall.SIGKILL)
		}
		cmd.Wait() // reap either way; exit status irrelevant

		// every 10th round: bounce the capturer itself mid-traffic — cancel
		// its context (a capturer crash/restart, distinct from the
		// foreign-writer kills above), wait for Run to return, then start a
		// fresh Engine on the same StateDir/replica and keep going. This is
		// Task 7's tryResume/rebase decision exercised under real
		// torture-level concurrency: a bounce can land the capturer in a
		// dirty (resume-ineligible ⇒ rebase) or clean (resume-eligible)
		// state depending on what raced it, and both must converge with
		// zero silent divergence.
		if round%10 == 0 {
			cancel()
			if err := <-engDone; err != nil {
				t.Fatalf("round %d: engine.Run returned error on capturer bounce: %v", round, err)
			}
			e = NewEngine(Options{DBPath: src, StateDir: dir, Sink: tortureSink{rep}})
			ctx, cancel = context.WithCancel(context.Background())
			engDone = make(chan error, 1)
			go func() { engDone <- e.Run(ctx) }()
		}

		// The engine's Run can fail loudly (e.g. it could not reacquire its
		// read lock) — surface that immediately rather than let the round
		// loop spin against a dead engine and report a misleading divergence.
		select {
		case err := <-engDone:
			t.Fatalf("round %d: engine.Run returned early: %v", round, err)
		default:
		}

		// Quiesce: wait for the replica to converge with the live source.
		if !converged(t, src, rep, 15*time.Second) {
			t.Fatalf("round %d: replica diverged and did not converge (rebases=%d)",
				round, e.Rebased())
		}
	}
	cancel()
	if err := <-engDone; err != nil {
		t.Fatalf("engine.Run returned error on shutdown: %v", err)
	}
	t.Logf("torture complete: %d rounds, %d rebases", round, e.Rebased())
}

func converged(t *testing.T, src string, rep *replay.Replica, d time.Duration) bool {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		sd, e1 := replay.Dump(src)
		rd, e2 := replay.Dump(rep.Path())
		if e1 == nil && e2 == nil && sd == rd {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
