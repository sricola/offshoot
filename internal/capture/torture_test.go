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

	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/testutil"
	"github.com/sricola/offshoot/internal/wal"
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
	testutil.RequireSQLite3(t)
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
	// totalRebases accumulates Rebased() across every engine instance this
	// test creates (the initial one plus one per bounce below) — Finding 2
	// of the task-7 review: a bare `e = NewEngine(...)` discards the outgoing
	// engine's Rebased() count, so the torture run never actually measured
	// its own resume claim. bounceCount tracks how many capturer bounces
	// (round%10==0 below) occurred, so the aggregate can be checked against
	// it after the loop.
	//
	// resumedCount is a direct proof of resume, added in the task-7
	// hardening pass (Finding 3): totalRebases < 1+bounceCount only *infers*
	// that some bounce resumed, by process of elimination against the
	// aggregate rebase count — it can't distinguish "resumed cleanly" from
	// other ways a bounce could avoid contributing a rebase, and conflates
	// mid-session takeover rebases with resume/no-resume at startup.
	// resumedCount instead reads Engine.Resumed() directly off each engine
	// instance right after it's known to have started (the same
	// happens-before points totalRebases already relies on: a channel
	// receive from engDone, or — for the still-live final engine — after
	// the loop's cancel()/<-engDone below), so it reports exactly how many
	// of the 1+bounceCount session-starts actually took the tryResume
	// success path, independent of whatever rebases (if any) that session
	// went on to accumulate later in its own lifetime.
	var totalRebases int
	var resumedCount int
	bounceCount := 0
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
		// its context, which exercises Run's graceful ctx.Done()/shutdown()
		// path (final drain, verified-clean checkpoint attempt), NOT a
		// process SIGKILL of the capturer — distinct from the real
		// foreign-writer SIGKILLs above. A true capturer SIGKILL is
		// argued-safe (see engine.go's shutdown()/tryResume() doc comments)
		// but untested by this harness; see the spike report's "What was NOT
		// proven" section. Wait for Run to return, then start a fresh Engine
		// on the same StateDir/replica and keep going. This is Task 7's
		// tryResume/rebase decision exercised under real torture-level
		// concurrency: a bounce can land the capturer in a dirty
		// (resume-ineligible ⇒ rebase) or clean (resume-eligible) state
		// depending on what raced it, and both must converge with zero
		// silent divergence.
		if round%10 == 0 {
			cancel()
			if err := <-engDone; err != nil {
				t.Fatalf("round %d: engine.Run returned error on capturer bounce: %v", round, err)
			}
			// Accumulate BEFORE reassigning e — this is the value Finding 2
			// said was being silently discarded.
			totalRebases += e.Rebased()
			bounceCount++
			// The channel receive above (<-engDone) is a real happens-before
			// with the engine goroutine's tryResume() call, so reading
			// Resumed() here is race-safe — same reasoning as Rebased().
			if e.Resumed() {
				resumedCount++
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
	// Add the final engine's count — the same accumulation step applied at
	// every bounce above, just for the session that's still live when the
	// deadline hits.
	totalRebases += e.Rebased()
	if e.Resumed() {
		resumedCount++
	}

	t.Logf("torture complete: %d rounds, %d bounces, %d aggregate rebases across %d session-starts, %d resumed cleanly (resumed/bounce ratio: %.2f)",
		round, bounceCount, totalRebases, 1+bounceCount, resumedCount, resumedRatio(resumedCount, bounceCount))

	// The very first session always rebases once (no prior state to resume
	// from), so the aggregate can never be zero.
	if totalRebases < 1 {
		t.Fatalf("aggregate rebases = %d, want >= 1 (the initial session's startup rebase)", totalRebases)
	}
	// There is deliberately NO assertion of the shape "totalRebases >=
	// 1+bounceCount means no bounce ever resumed" here. One existed (the
	// Finding-2 inference check) and was unsound in that direction:
	// Rebased() also counts MID-SESSION rebases — takeover fold-races
	// (logN != consumed in takeover()) and unexpected WAL restarts — which
	// are the engine's designed, detected fallback and which legitimately
	// proliferate on a slow/contended machine, where takeover's
	// endRead→checkpoint(RESTART) window stretches enough for foreign
	// writer commits to land inside it. The first nightly on the public
	// runner pool (2026-08-14, run 31790331162: 158 rounds, 51 rebases
	// across 16 session-starts) tripped it while 10 of 15 bounces HAD
	// resumed cleanly — a false red, reproduced locally without any code
	// change under a 0.06-CPU cgroup quota (367 rounds, 47 rebases across
	// 37 starts, 31/36 bounces resumed). Only the inference's other
	// direction is valid (a strictly smaller aggregate proves some
	// session-start contributed 0 rebases), and everything that direction
	// can prove is subsumed by the direct resumedCount check below: if
	// resume truly never works, every session-start rebases at startup, so
	// resumedCount is 0 and that check fails loudly at any machine speed.

	// Direct resume proof (task-7 hardening pass, Finding 3): resumedCount
	// is read straight off Engine.Resumed() in each session's success
	// branch, so — unlike any aggregate-rebase inference — it cannot
	// conflate a mid-session takeover rebase with the startup resume/rebase
	// decision. Skip when no bounce occurred (short/slow run): nothing
	// could have resumed if the capturer was never bounced.
	if bounceCount > 0 && resumedCount < 1 {
		t.Fatalf("resumedCount = %d, want >= 1: not one of the %d capturer bounces resumed cleanly (direct proof via Engine.Resumed())",
			resumedCount, bounceCount)
	}
}

// resumedRatio returns resumedCount/bounceCount, or 0 when there were no
// bounces to divide by (avoids a NaN in the log line for short/skipped runs).
func resumedRatio(resumedCount, bounceCount int) float64 {
	if bounceCount == 0 {
		return 0
	}
	return float64(resumedCount) / float64(bounceCount)
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
