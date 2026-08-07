package capture

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/replay"
	"github.com/offshoot-db/offshoot/internal/wal"
)

type replicaSink struct{ r *replay.Replica }

func (s replicaSink) Rebase(p string) error                 { return s.r.Rebase(p) }
func (s replicaSink) Apply(ps uint32, fr []wal.Frame) error { return s.r.Apply(ps, fr) }

// countingSink wraps replicaSink and counts Apply calls, independent of
// replay.Replica's (accidental) idempotency — see
// TestEngineResumeAppliesNothingBeforeNewWrite, the regression test for
// Finding 1 of the task-7 review.
type countingSink struct {
	replicaSink
	applyCount *int32
}

func (s countingSink) Apply(ps uint32, fr []wal.Frame) error {
	atomic.AddInt32(s.applyCount, 1)
	return s.replicaSink.Apply(ps, fr)
}

// applyErrSink always fails Apply with a fixed error while leaving Rebase
// intact (delegated to the embedded replicaSink), so the engine's initial
// startup rebase still succeeds and only the drain/Apply path fails — used
// by TestDrainNowFatalErrorStopsRun to force pollOnce into its fatal-error
// return.
type applyErrSink struct {
	replicaSink
	err error
}

func (s applyErrSink) Apply(ps uint32, fr []wal.Frame) error { return s.err }

func startEngine(t *testing.T, dbPath string) (*Engine, *replay.Replica, context.CancelFunc, chan error) {
	t.Helper()
	dir := t.TempDir()
	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: dbPath, StateDir: dir, Sink: replicaSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	return e, rep, cancel, done
}

// waitEqual polls until the replica's dump matches the (quiesced) source.
func waitEqual(t *testing.T, src string, rep *replay.Replica, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var sd, rd string
	for time.Now().Before(end) {
		var err1, err2 error
		sd, err1 = replay.Dump(src)
		rd, err2 = replay.Dump(rep.Path())
		if err1 == nil && err2 == nil && sd == rd {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica never converged.\n--- source ---\n%.2000s\n--- replica ---\n%.2000s", sd, rd)
}

func TestEngineCapturesForeignGoWriter(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}

	_, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	for i := 0; i < 50; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(128))"); err != nil {
			t.Fatal(err)
		}
	}
	waitEqual(t, src, rep, 10*time.Second)
}

func TestEngineCapturesStockCLIWriter(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	// Create + WAL mode via the CLI itself — the engine never owns the writer.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3: %v: %s", err, out)
	}

	_, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	for i := 0; i < 20; i++ {
		if out, err := exec.Command("sqlite3", src,
			"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(128));").CombinedOutput(); err != nil {
			t.Fatalf("sqlite3 insert: %v: %s", err, out)
		}
	}
	waitEqual(t, src, rep, 10*time.Second)
}

// sqlite3UnlinksWALOnExit reports whether the sqlite3 CLI on PATH performs
// the standard close-time checkpoint-and-unlink of -wal/-shm when the last
// connection to a WAL-mode database goes away. Stock builds do; Apple's
// system build (persistent WAL) does not, and on it the absence of a read
// lock has no observable consequence at all. Probed rather than assumed from
// a version string or GOOS, since which sqlite3 is first on PATH is what
// actually decides it.
func sqlite3UnlinksWALOnExit(t *testing.T) bool {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "probe.db")
	if out, err := exec.Command("sqlite3", probe,
		"PRAGMA journal_mode=WAL; CREATE TABLE p (v BLOB); INSERT INTO p VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3 probe: %v: %s", err, out)
	}
	// The CLI has exited and nothing else in this process ever opened the
	// probe database, so it was unambiguously the last connection.
	_, err := os.Stat(probe + "-wal")
	return os.IsNotExist(err)
}

// TestEngineHoldsReadLockAcrossSnapshotCopy pins the invariant that the whole
// capture design rests on: once the engine's startup rebase has taken its read
// lock, a foreign writer that commits and then exits must NOT be able to
// checkpoint the WAL into the main database and delete it. The read lock is
// the only thing standing between the engine and that close-time cleanup, and
// losing it is completely silent — the engine keeps reporting healthy while
// its WAL reader sees an empty file forever and the replica never receives
// another frame.
//
// The regression this guards is subtle and was invisible on the development
// machine for thousands of runs: rebase() used to read the main database file
// for its snapshot copy through a second, short-lived os.File. POSIX advisory
// locks (fcntl), which is what SQLite uses on unix, are keyed by (process,
// inode) rather than by descriptor, so closing that second descriptor dropped
// every lock this process held on the database file — including the SHARED
// lock SQLite holds for a WAL-mode connection's entire lifetime, which SQLite
// tracks in memory and therefore never re-acquires. See internal/dbfile's
// package comment for the full mechanism and copySrc/hashSrc for the fix.
//
// Note that this test only covers the engine's STARTUP snapshot copy. The
// same hazard at engine teardown — where the closing descriptor drops the
// locks of an unrelated, still-open SQLite connection elsewhere in the
// process — is covered by TestEngineResumesCleanly and
// TestEngineResumeAppliesNothingBeforeNewWrite, both of which hold a foreign
// connection open across an engine bounce. Those two are the reason the
// descriptor is owned by dbfile and never closed at all, rather than owned by
// the engine and closed last.
//
// Written as a WAL-survival assertion rather than a lock-introspection one
// because that is the observable consequence that actually breaks capture.
// That makes the assertion meaningful only where the platform's sqlite3 build
// actually performs the close-time checkpoint-and-delete: Apple's system
// sqlite3 ships with persistent WAL enabled and leaves -wal/-shm in place
// regardless of locking, so on that build this test would pass whether or not
// the lock is held. Rather than let it stand as a green check that proves
// nothing — exactly how the original bug reached CI — it probes for the
// behaviour first and skips LOUDLY, naming the reason, when the platform
// cannot observe the failure. It runs for real against any stock sqlite3
// build (Linux distributions, Homebrew, and the GitHub-hosted runners).
func TestEngineHoldsReadLockAcrossSnapshotCopy(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	if !sqlite3UnlinksWALOnExit(t) {
		v, _ := exec.Command("sqlite3", "--version").Output()
		t.Skipf("this platform's sqlite3 does not unlink the WAL when the last "+
			"connection closes (persistent WAL), so a dropped read lock is not "+
			"observable here and this test cannot fail — it proves nothing on "+
			"this machine. Run it against a stock sqlite3 build. CLI: %s",
			strings.TrimSpace(string(v)))
	}
	src := filepath.Join(t.TempDir(), "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3: %v: %s", err, out)
	}

	e, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	// Wait for the startup rebase — which is what performs the snapshot copy
	// and then takes the read lock — to have completed, so this test is
	// asserting about an engine that is genuinely holding the lock rather
	// than one that simply hasn't got there yet.
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	if err := e.WaitReady(rctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for e.Rebased() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if e.Rebased() < 1 {
		t.Fatal("engine never completed its startup rebase")
	}

	// A whole foreign writer lifecycle: connect, commit, disconnect. The exit
	// is the dangerous part — that is when SQLite tries to checkpoint and
	// unlink the WAL if it believes it is the last connection.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(128));").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3 insert: %v: %s", err, out)
	}

	if _, err := os.Stat(src + "-wal"); err != nil {
		t.Fatalf("engine's read lock did not survive the startup snapshot copy: "+
			"the foreign writer's close-time checkpoint folded and deleted the WAL "+
			"(stat %s-wal: %v). Every frame committed from here on is lost silently.", src, err)
	}
	// The lock surviving is necessary but not sufficient — the frames must
	// actually reach the sink.
	waitEqual(t, src, rep, 10*time.Second)
}

// TestEngineTakeoverUnderConcurrentWrites hammers the engine with a foreign
// writer committing continuously while the engine repeatedly crosses the
// 64-txn takeover threshold, and — critically — keeps writing *after* a
// takeover fires. Writes must survive the deferred WAL restart that follows
// a verified-clean takeover: SQLite's RESTART reset is lazy and only lands
// on the next writer's commit, so any writer activity after takeover exists
// specifically to exercise that path.
//
// Phase 1 approaches the 64-txn threshold and then pauses so the takeover
// runs against a quiet WAL: this makes the checkpoint's `log`-vs-consumed
// comparison come back clean (log == consumed) with overwhelming
// likelihood, so the takeover should take the verified-clean path and arm
// the expected-restart continuation rather than rebase. Phase 2 then keeps
// writing — below the next takeover threshold, so no second takeover
// attempt (clean or fold) is triggered — specifically to force the WAL's
// deferred restart while the engine is mid-stream, and to prove the
// resulting wal.ErrWALRestarted is absorbed as a continuation (fresh reader,
// no rebase) rather than a full rebase. Under the pre-fix behavior every
// such takeover was followed one write later by a full (counted) rebase;
// under the fix, Rebased() must stay at 1 (the initial rebase only).
//
// This test's exact-rebase assertion relies on timing (the quiet pause
// before the threshold crossing) rather than an unconditional guarantee — a
// genuine foreign commit landing in the endRead()→checkpoint(RESTART) gap
// is a legitimate fold race that still correctly forces a counted rebase.
// It is verified for stability with `-count=15`; if it ever flakes, that is
// itself informative (a fold race actually occurred) rather than a bug.
func TestEngineTakeoverUnderConcurrentWrites(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite3", src+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}

	e, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	insert := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(128))"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Phase 1: approach the 64-txn threshold (60, then 8 more — 68 total),
	// then pause so the engine fully drains and the takeover it triggers
	// runs against a quiet WAL (verified-clean path, near-certain).
	insert(60)
	time.Sleep(300 * time.Millisecond)
	insert(8)
	// Let the takeover fire and settle. Generous relative to the ~10ms poll
	// interval: under concurrent system load (e.g. the full suite's other
	// packages running in parallel, or this repo's continuous-write stress
	// test churning CPU/subprocesses elsewhere), a per-transaction fsync
	// inside drain's SaveState can push a single poll's catch-up well past a
	// tight margin — empirically, 300ms was occasionally too little,
	// producing a benign (safe, just non-optimal) extra rebase when phase 2's
	// writes below landed before takeover's checkpoint got a chance to run;
	// see drain's doc comment in engine.go for the full mechanism.
	time.Sleep(2 * time.Second)

	// Phase 2: writes CONTINUE after takeover — this is what forces the
	// WAL's deferred (lazy) restart to actually land. Stay under the next
	// 64-txn threshold so this phase can't itself trigger another takeover
	// (clean or fold), keeping the Rebased() assertion below meaningful.
	insert(40)

	waitEqual(t, src, rep, 15*time.Second)
	t.Logf("rebased=%d", e.Rebased())
	if got := e.Rebased(); got != 1 {
		t.Errorf("clean takeover + continued writes should not rebase; Rebased() = %d, want 1", got)
	}
}

// TestEngineTakeoverExpectedRestartIsNotRebase is a deterministic companion
// to TestEngineTakeoverUnderConcurrentWrites: transactions are written
// serially, each with a minimal pacing sleep, crossing the 64-txn takeover
// threshold partway through. A few more writes at the end force SQLite's
// deferred WAL-header rewrite (new salts) to actually land, driving the
// engine through the wal.ErrWALRestarted path this fix changes: it must be
// absorbed as an expected continuation, not counted as a rebase.
//
// (The idle-takeover trigger — >5s with no writes — would exercise the same
// code path but is too slow for a test; crossing the 64-txn threshold is
// equivalent for this purpose and fast.)
//
// Getting the writer provably out of the way before the 64-txn threshold is
// crossed matters more than it looks. takeover()'s clean-vs-rebase decision
// hinges on whether a foreign write lands in the gap between endRead() and
// checkpoint(RESTART) — landing there is exactly what a straddling write
// looks like, and rebasing in response is the CORRECT, data-preserving
// reaction to that race (see takeover()'s doc comment). afterDrain fires
// takeover() synchronously the instant `captured` reaches 64 — there is no
// extra tick, no settling time to rely on. An earlier version of this test
// ran with the engine's default 10ms poll ticker: it wrote 70 txns in one
// paced loop, then just slept, hoping the threshold would happen to be
// crossed only after the loop returned. But the free-running ticker can
// notice captured>=64 and fire takeover() on ANY tick, including one that
// lands while the test's own loop still has commits left to issue — the
// loop's own remaining writes are then indistinguishable from a genuinely
// foreign writer straddling the gap. On a fast, idle machine the
// endRead()->checkpoint() gap is a few microseconds wide and rarely catches
// one; under GitHub Actions ubuntu's CI load (scheduler delays widen the gap,
// or delay the ticker itself) it can and did — producing a legitimate extra
// rebase (Rebased()==2) that this test misread as a bug.
//
// The fix has two parts, both needed together:
//
//  1. Poll is set to an hour, so the free-running ticker never fires within
//     the test's lifetime — the ONLY thing that can ever invoke drain /
//     afterDrain / takeover is an explicit DrainNow call this test makes
//     itself (see TestDrainNowCapturesPendingTransaction for the same
//     pattern). This removes the race at its root: there is no longer a
//     background goroutine that can decide, unprompted, to run takeover()
//     while this test's writer loop is still mid-flight.
//  2. The writes are split into a below-threshold phase and an
//     over-threshold phase, with a DrainNow — Run's own synchronous
//     catch-up-to-a-real-target request (see its doc comment) — forced
//     between them and after the second phase. DrainNow is serviced by
//     Run's single goroutine and only returns once that service completes,
//     so a DrainNow call made after this test's writer loop has already
//     returned is proof — not a probabilistic bet — that every commit
//     issued so far is captured and that nothing else is being written
//     concurrently. The final DrainNow call is therefore the one and only
//     place takeover() can run: `captured` has just crossed 64, and the
//     writer is provably idle, so there is no foreign write left anywhere
//     to possibly land in the endRead()->checkpoint(RESTART) gap. The
//     verified-clean (log==consumed) path is not just likely but
//     guaranteed.
func TestEngineTakeoverExpectedRestartIsNotRebase(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	db, err := sql.Open("sqlite3", src+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{
		DBPath:   src,
		StateDir: dir,
		Sink:     replicaSink{rep},
		Poll:     time.Hour, // long enough that only DrainNow can trigger a poll here
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Let the engine's own initial rebase (checkpoint TRUNCATE + snapshot
	// copy + reader bind) finish before hammering writes. Otherwise the
	// write burst below races the initial rebase itself: writes that land
	// before the reader is bound get folded into the initial snapshot
	// instead of captured incrementally via the WAL, so `captured` never
	// reaches the 64-txn threshold and takeover — the very thing this test
	// exists to exercise — never fires.
	time.Sleep(300 * time.Millisecond)

	insertTxns := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(128))"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	drainNow := func() {
		t.Helper()
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		if err := e.DrainNow(dctx); err != nil {
			t.Fatalf("DrainNow() = %v, want nil", err)
		}
	}

	// Phase 1: 60 txns, serial and paced — safely under the 64-txn
	// threshold regardless of scheduling, since with Poll disabled nothing
	// observes `captured` until the DrainNow below is called, and 60 < 64
	// even then.
	insertTxns(60)

	// Force a full synchronous catch-up and wait for it before writing
	// another byte: by the time this returns, all 60 txns above are
	// captured (captured == 60, still under threshold, so no takeover fires
	// here) and this goroutine — the only writer, and the only thing that
	// can trigger a drain at all with Poll disabled — is provably doing
	// nothing else.
	drainNow()

	// Phase 2: 10 more txns, serial and paced, crossing the 64-txn
	// threshold. With Poll disabled, nothing services these until the
	// DrainNow below is explicitly called — no background ticker can fire
	// takeover() mid-loop the way it could before this fix.
	insertTxns(10)

	// Drain again, synchronously, with the writer loop already fully
	// returned — every one of phase 2's commits is durable before this call
	// is even issued. This is the one and only place `captured` can cross
	// 64 and takeover() can run, and it does so with zero concurrent writer
	// activity anywhere in the process: no foreign write can land in the
	// endRead()->checkpoint(RESTART) gap.
	drainNow()

	// A few more commits force SQLite to actually rewrite the WAL header
	// with new salts (the lazy part of RESTART), which is what makes the
	// engine's reader observe wal.ErrWALRestarted on its next drain.
	insertTxns(5)
	drainNow()

	waitEqual(t, src, rep, 10*time.Second)
	t.Logf("rebased=%d", e.Rebased())
	if got := e.Rebased(); got != 1 {
		t.Errorf("expected-restart continuation after a clean takeover must not rebase; Rebased() = %d, want 1", got)
	}
}

// TestEngineDetectsMissedWritesAfterCrash is Task 7's core safety test:
// after the engine is stopped (context cancelled, which exercises Run's
// graceful ctx.Done()/shutdown() path — drain, lock release — strictly
// gentler than an actual SIGKILL of the capturer process; see the spike
// report's "What was NOT proven" section), writes land AND get
// checkpointed+restarted (new WAL salts) while it's down, so frames pass
// through unseen. On restart, the engine must NOT silently resume: it must
// detect the divergence and rebase, and Rebased() must report it.
func TestEngineDetectsMissedWritesAfterCrash(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e1 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- e1.Run(ctx1) }()

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)

	// Stop the engine via context cancellation. This exercises Run's
	// graceful ctx.Done()/shutdown() path (final drain, lock release), NOT a
	// process SIGKILL — a real capturer-SIGKILL harness is still open work,
	// see the spike report.
	cancel1()
	<-done1

	// While the engine is dead: write AND checkpoint+restart the WAL, so
	// frames pass through the WAL unseen and the salts change.
	if out, err := exec.Command("sqlite3", src, "PRAGMA busy_timeout=5000;"+
		"INSERT INTO t (v) VALUES (randomblob(64));"+
		"PRAGMA wal_checkpoint(RESTART);"+
		"INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Restart the engine on the same StateDir.
	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()

	// It must converge (via rebase), and it must KNOW it rebased.
	waitEqual(t, src, rep, 10*time.Second)
	if e2.Rebased() < 1 {
		t.Fatal("engine resumed silently across missed writes — undetected divergence")
	}
}

// TestEngineResumesCleanly is the positive companion: a clean stop (no
// writes while dead) must let the restarted engine resume without a rebase.
// A clean stop relies on Engine.shutdown()'s final verified-clean
// checkpoint(RESTART), which leaves Off==HeaderSize + matching salts in the
// saved state — the only state tryResume ever accepts.
//
// Adaptation vs the brief: a foreign *sql.DB connection to src is opened
// before e1 starts and kept open across the whole test (closed via defer at
// the very end). This models the real topology — the application holds its
// own long-lived connection to the SQLite file; the capturer is a separate
// side process that can be bounced independently. Without it, e1's own
// connection is the *only* connection to src when it closes at the end of
// Run(), and SQLite's documented last-connection-close behavior in WAL mode
// (checkpoint + delete the -wal/-shm files once it can get an exclusive
// lock) wipes out the very salts tryResume needs to compare against —
// deterministically forcing a rebase and failing this test, independent of
// any capture-engine logic. That deletion is real SQLite behavior, not a
// test artifact: verified directly (WAL file present immediately after
// Engine.shutdown() saves state, gone by the time Run() returns) before
// adding the foreign connection fixed it.
func TestEngineResumesCleanly(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	// Foreign application connection: stays open across the capturer bounce
	// so the capturer's own close is never the *last* connection to src.
	fdb, err := sql.Open("sqlite3", src+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer fdb.Close()
	if _, err := fdb.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if err := fdb.Ping(); err != nil {
		t.Fatal(err)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))

	// First engine: capture, then a clean stop (drain + verified-clean
	// checkpoint RESTART happens inside Run()'s ctx.Done() shutdown path).
	e1 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- e1.Run(ctx1) }()
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	cancel1()
	<-done1

	// No writes while dead. Second engine must resume without rebase.
	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	if e2.Rebased() != 0 {
		t.Errorf("clean restart should resume, not rebase (rebased=%d)", e2.Rebased())
	}
}

// TestEngineResumeAppliesNothingBeforeNewWrite is the regression test for
// Finding 1 of the task-7 review: "clean resume silently re-applies the
// whole prior session". It uses countingSink so the assertion is on the raw
// number of Sink.Apply calls, not on replica content — replay.Replica
// happens to be idempotent, which is exactly what let the bug through
// undetected before (empirically: 3 re-applies before any new write, per
// the review). The Sink doc comment in engine.go now states Apply must never
// be called twice for the same transaction; this test enforces the stronger,
// directly-observable form of that: zero Apply calls between a clean resume
// and the first genuinely new write.
//
// Against the pre-fix code (shutdown() checkpointing RESTART instead of
// TRUNCATE, tryResume Bind-ing at wal.HeaderSize with matching salts and
// re-parsing whatever checksum-valid frames are still physically present in
// the -wal file) this test fails: session 2's Apply count is >0 —
// empirically 1, matching the single transaction captured in session 1 —
// before any new write ever lands. Verified by running this test against a
// stashed pre-fix engine.go/state.go: it failed with exactly that shape
// ("session 2 resumed but re-applied 1 already-captured transaction(s)
// before any new write").
func TestEngineResumeAppliesNothingBeforeNewWrite(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	// Foreign application connection kept open across the whole test — see
	// TestEngineResumesCleanly's comment for why: without it, the
	// capturer's own close would be the last connection to src, and
	// SQLite's last-close WAL deletion in WAL mode would make this test
	// pass for the wrong reason (no WAL left to re-apply from, rather than
	// the fix actually preventing re-application).
	fdb, err := sql.Open("sqlite3", src+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer fdb.Close()
	if _, err := fdb.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if err := fdb.Ping(); err != nil {
		t.Fatal(err)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))

	var count1 int32
	e1 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: countingSink{replicaSink{rep}, &count1}})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- e1.Run(ctx1) }()
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	if got := atomic.LoadInt32(&count1); got != 1 {
		t.Fatalf("session 1: expected exactly 1 Apply call for the one write, got %d", got)
	}
	cancel1()
	<-done1

	// No writes while dead. Second engine must resume cleanly.
	var count2 int32
	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: countingSink{replicaSink{rep}, &count2}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()

	// Give the engine time to run tryResume/rebase and settle into its poll
	// loop, well before any new write arrives. This is the crux of the
	// regression: on the pre-fix code, still-present WAL frames from
	// session 1 get re-parsed and re-applied right here, with no new write
	// having happened at all.
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&count2); got != 0 {
		t.Fatalf("session 2 resumed but re-applied %d already-captured transaction(s) before any new write — Finding 1 regression", got)
	}

	// Now a genuinely new write arrives; exactly it must be applied.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	if got := atomic.LoadInt32(&count2); got != 1 {
		t.Fatalf("session 2: expected exactly 1 Apply call for the new write, got %d", got)
	}
	// Engine.rebased/resumed are atomic, so reading Rebased() is race-safe
	// from any goroutine at any time — but it's still checked here, after
	// waitEqual() has established a happens-before with the engine
	// goroutine's tryResume()/rebase(), rather than right after the earlier
	// 300ms sleep: a sleep is not a real happens-before, so reading here
	// guarantees the value reflects this session's actual startup decision
	// rather than a not-yet-updated snapshot.
	if e2.Rebased() != 0 {
		t.Errorf("clean restart should resume, not rebase (rebased=%d)", e2.Rebased())
	}
}

// TestEngineDetectsInPlaceUpdateAfterCleanShutdown is the regression test
// for Finding 1 of the task-7 hardening-pass review: "replace mtime+size
// with a content hash." mtime+size cannot see an in-place UPDATE that keeps
// the row count and blob width fixed — the main file's size never changes,
// and a fast enough bounce can land inside a single filesystem mtime tick.
// Under the pre-fix (mtime+size) fingerprint this exact scenario is a
// silent false match: tryResume would trust a main file whose content had
// actually diverged, and the replica would go permanently stale with no
// detectable signal. Forging the mtime back isn't needed to prove this with
// a content hash — the test simply asserts the hash catches a content
// change even when size is identical, which mtime+size structurally cannot.
//
// Sequence: e1 captures one row, shuts down cleanly (verified-clean
// RESTART+TRUNCATE, hash fingerprint saved). While the engine is down, a
// foreign connection performs an in-place UPDATE — same blob width as the
// original insert, so the main file's size is unchanged — and its own
// close (it is the sole connection at that point) triggers SQLite's
// last-connection-close auto-checkpoint, which folds the UPDATE into the
// main file and deletes the WAL. e2 must detect this (hash mismatch, WAL
// still provably empty) and rebase rather than silently resume, and the
// replica must converge to include the UPDATE.
func TestEngineDetectsInPlaceUpdateAfterCleanShutdown(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	// Foreign application connection kept open across e1's lifetime, same
	// pattern as TestEngineResumesCleanly: without it, e1's own close would
	// be the last connection to src, entangling SQLite's last-close
	// auto-checkpoint with e1's own shutdown instead of the foreign UPDATE
	// below, which is what this test needs to isolate and control.
	fdb, err := sql.Open("sqlite3", src+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fdb.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if err := fdb.Ping(); err != nil {
		t.Fatal(err)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))

	e1 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- e1.Run(ctx1) }()
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	cancel1()
	<-done1 // clean shutdown: verified-clean RESTART+TRUNCATE, hash fingerprint saved

	// Close the foreign connection now, as a controlled step of its own:
	// this is the last connection at this instant, so it triggers SQLite's
	// last-close auto-checkpoint over the already-TRUNCATE-emptied WAL —
	// nothing to fold, harmless to the fingerprint we just saved.
	if err := fdb.Close(); err != nil {
		t.Fatal(err)
	}

	// While the engine is down: a fresh single connection performs an
	// in-place UPDATE (same blob width as the original INSERT, so the main
	// file's size is unchanged) and then closes. That close is the sole/last
	// connection close, which triggers SQLite's own checkpoint-and-delete:
	// the UPDATE gets folded into the main file and the -wal file is
	// removed — exactly the "close-checkpoint" attack this test targets.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; UPDATE t SET v = randomblob(64) WHERE id = 1;").CombinedOutput(); err != nil {
		t.Fatalf("update: %v: %s", err, out)
	}

	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()

	waitEqual(t, src, rep, 10*time.Second)
	if got := e2.Rebased(); got < 1 {
		t.Fatalf("engine resumed silently across an in-place UPDATE that changed content but not size — Finding 1 regression; Rebased() = %d, want >= 1", got)
	}
}

// TestEngineShutdownLeavesUnverifiedStateAfterRacedRestart pins
// ShutdownRaceHook and, through it, the `int64(logN) != consumed` early
// return in shutdown() (see the "KNOWN RESIDUAL RISK" block above it): a
// foreign write landing in the window between shutdown's final endRead and
// its checkpoint(RESTART) call makes RESTART fold (and report) a frame
// drain() never consumed, so shutdown must leave the persisted State at
// Clean=false rather than record a clean marker over content the replica
// never captured. internal/session's own TestCloseDoesNotStampSidecarAfterUnverifiedShutdown
// reuses this exact hook to verify Session.commitSidecarRefresh's gate on
// this same State.
func TestEngineShutdownLeavesUnverifiedStateAfterRacedRestart(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)

	ShutdownRaceHook = func() {
		if out, err := exec.Command("sqlite3", src,
			"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
			t.Errorf("hook write: %v: %s", err, out)
		}
	}
	defer func() { ShutdownRaceHook = nil }()

	cancel()
	<-done

	st, ok, err := LoadState(StatePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected a persisted state file after shutdown")
	}
	if st.Clean {
		t.Fatal("a write racing shutdown's RESTART checkpoint must leave State.Clean false, not record a clean marker over unverified content")
	}

	// Confirms the consequence end to end: a fresh engine reusing the SAME
	// StateDir (and replica) must rebase against this Clean=false state, not
	// silently resume — same dir/rep as the first engine, not startEngine's
	// own fresh t.TempDir(), which would trivially have no state file at all
	// rather than exercising the Clean=false verdict specifically.
	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()
	waitEqual(t, src, rep, 10*time.Second)
	if e2.Rebased() == 0 {
		t.Fatal("expected the next engine to rebase after an unverified (Clean=false) prior shutdown, not resume")
	}
}

func TestEngineSurvivesForeignPassiveCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3: %v: %s", err, out)
	}
	e, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	for i := 0; i < 10; i++ {
		if out, err := exec.Command("sqlite3", src, "PRAGMA busy_timeout=5000;"+
			"INSERT INTO t (v) VALUES (randomblob(128)); PRAGMA wal_checkpoint(PASSIVE);").CombinedOutput(); err != nil {
			t.Fatalf("sqlite3: %v: %s", err, out)
		}
	}
	waitEqual(t, src, rep, 10*time.Second)
	if e.Rebased() != 1 {
		t.Errorf("passive checkpoints must not force rebase; rebased = %d", e.Rebased())
	}
}

// TestDrainNowCapturesPendingTransaction is the DrainNow happy-path test:
// with Poll set long enough that background ticking cannot be what captures
// the write, a single DrainNow call must itself drive the poll that applies
// a pending transaction to the Sink, and a second DrainNow with nothing left
// pending must return quickly (it should not block for anywhere near Poll).
func TestDrainNowCapturesPendingTransaction(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	var applyCount int32
	e := NewEngine(Options{
		DBPath:   src,
		StateDir: dir,
		Sink:     countingSink{replicaSink{rep}, &applyCount},
		Poll:     time.Hour, // long enough that only DrainNow can trigger a poll here
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Let the engine's initial rebase (checkpoint + snapshot + reader bind)
	// finish before writing — same ordering every other test in this file
	// relies on.
	time.Sleep(300 * time.Millisecond)

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	if err := e.DrainNow(dctx); err != nil {
		t.Fatalf("DrainNow() = %v, want nil", err)
	}
	if got := atomic.LoadInt32(&applyCount); got != 1 {
		t.Fatalf("DrainNow did not capture the pending transaction: Apply called %d time(s), want 1", got)
	}
	waitEqual(t, src, rep, 2*time.Second)

	// A second DrainNow with nothing pending must return quickly — nowhere
	// near the 1h Poll interval — since there's no ticker to fall back on
	// within this test's timeframe.
	start := time.Now()
	dctx2, dcancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel2()
	if err := e.DrainNow(dctx2); err != nil {
		t.Fatalf("second DrainNow() = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("second DrainNow (nothing pending) took %s, want quick", elapsed)
	}
	if got := atomic.LoadInt32(&applyCount); got != 1 {
		t.Fatalf("second DrainNow applied something with nothing pending: Apply called %d time(s), want still 1", got)
	}
}

// TestDrainNowFatalErrorStopsRun is the regression test for the review
// finding that Run's reqs branch swallowed a fatal pollOnce error: DrainNow
// got it back, but Run just kept looping as if nothing had happened, so
// Session.Err() would keep reporting healthy while capture was actually
// dead. Poll is set to an hour so only DrainNow can trigger a poll within
// this test, and the Sink's Apply always fails, so the write below can only
// be discovered — and only fail — via the DrainNow call.
//
// Verified to fail against the pre-fix engine.go: DrainNow returned the
// injected error correctly (that part was never broken), but Run's done
// channel did not deliver anything within the 5s wait — Run kept running
// the ticker loop instead of stopping, exactly the swallowed-error bug this
// test exists to catch. See task-6-report.md for the captured failure
// output.
func TestDrainNowFatalErrorStopsRun(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	wantErr := errors.New("applyErrSink: refusing to apply")
	e := NewEngine(Options{
		DBPath:   src,
		StateDir: dir,
		Sink:     applyErrSink{replicaSink{rep}, wantErr},
		Poll:     time.Hour, // long enough that only DrainNow can trigger a poll here
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	// Let the engine's initial rebase finish (Rebase succeeds; only Apply is
	// rigged to fail) before writing.
	time.Sleep(300 * time.Millisecond)

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	gotErr := e.DrainNow(dctx)
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("DrainNow() = %v, want %v", gotErr, wantErr)
	}

	// Run must stop promptly on its own, carrying the same fatal error —
	// not just eventually via the deferred ctx cancel below.
	select {
	case runErr := <-done:
		if !errors.Is(runErr, wantErr) {
			t.Fatalf("Run() returned %v, want %v", runErr, wantErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after DrainNow's fatal error — fatal error was swallowed")
	}
}

// TestEngineLagReportsPendingBacklogThenZero pins task 7's Engine.Lag()
// contract: WAL bytes committed by writers but not yet applied to the
// replica. Poll is set to an hour, the same shape the DrainNow tests above
// use to construct a pending backlog — with no ticker able to drain it
// within this test's timeframe, a committed write sits undrained until an
// explicit DrainNow call, so Lag() has a real, deterministic backlog to
// report in between.
func TestEngineLagReportsPendingBacklogThenZero(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{
		DBPath:   src,
		StateDir: dir,
		Sink:     replicaSink{rep},
		Poll:     time.Hour, // long enough that only DrainNow can trigger a poll here
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Let the engine's initial rebase (checkpoint + snapshot + reader bind)
	// finish before checking or writing — same ordering every other test in
	// this file relies on.
	time.Sleep(300 * time.Millisecond)

	if got := e.Lag(); got != 0 {
		t.Fatalf("Lag() before any write = %d, want 0", got)
	}

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(4096));").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}
	// Give the commit a moment to land on disk. The ticker cannot drain it
	// (Poll is an hour) and nothing else in this test calls DrainNow yet, so
	// this is not a race against the engine's own catch-up.
	time.Sleep(100 * time.Millisecond)

	if got := e.Lag(); got <= 0 {
		t.Fatalf("Lag() with an undrained committed write = %d, want > 0", got)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	if err := e.DrainNow(dctx); err != nil {
		t.Fatalf("DrainNow() = %v, want nil", err)
	}
	if got := e.Lag(); got != 0 {
		t.Fatalf("Lag() after DrainNow = %d, want 0", got)
	}
}

// TestEngineLagOrderingUndercountsAcrossARestartRace is the regression test
// for the review finding that Lag()'s two independent reads (e.consumed,
// then the WAL file's on-disk size) can race a WAL restart landing in the
// gap between them — and that which read runs first decides which direction
// the resulting skew errs in. See Lag's doc comment for the full algebra;
// this pins the consequence directly, without needing a real Run goroutine
// or an actual WAL restart: lagRaceHook fires in the exact gap between
// Lag()'s e.consumed.Load() and its os.Stat call, and mutates both exactly
// the way a real restart landing there would (a fresh, small WAL file; a
// freshly-reset e.consumed) — reproducing the race deterministically on
// every run instead of leaving it to chance.
//
// e.consumed starts at 5000, matching a WAL file that is ALSO 5000 bytes —
// i.e. fully caught up, Lag() should read 0 if nothing races it. The hook
// then simulates a restart: it shrinks the on-disk WAL to 100 bytes (a fresh
// generation) and resets e.consumed to 0 (Run's own
// expectRestart-continuation branches do exactly this). Lag()'s consumed
// local was already captured (5000) before the hook ran, so the subtraction
// becomes 100 - 5000 = -4900, clamped to 0 — an undercount of a real
// pre-restart backlog, never a false-positive one. Had the read order been
// reversed (stat first, consumed second — the ordering Lag's doc comment
// explicitly rejects), the same hook landing between those two reads would
// instead produce 5000 - 0 = 5000: a large false-positive backlog with no
// real write behind it. This test only passes under the documented,
// safe-direction ordering.
func TestEngineLagOrderingUndercountsAcrossARestartRace(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	walPath := src + "-wal"
	if err := os.WriteFile(walPath, make([]byte, 5000), 0o644); err != nil {
		t.Fatal(err)
	}

	// Lag() only ever touches e.o.DBPath and e.consumed — no Sink, no Run
	// goroutine, no sqlite3 CLI needed to exercise it in isolation.
	e := NewEngine(Options{DBPath: src, StateDir: dir})
	e.consumed.Store(5000)

	lagRaceHook = func() {
		if err := os.WriteFile(walPath, make([]byte, 100), 0o644); err != nil {
			t.Fatal(err)
		}
		e.consumed.Store(0)
	}
	defer func() { lagRaceHook = nil }()

	if got := e.Lag(); got != 0 {
		t.Fatalf("Lag() racing a restart landing mid-call = %d, want 0 (undercount, never a false-positive backlog)", got)
	}
}

// TestEngineLagRetiresPhantomAfterIdleTakeover is the regression test for the
// whole-branch review finding that Lag() reports a persistent phantom
// backlog after any mid-session WAL takeover: takeover() uses
// checkpoint(RESTART), which — unlike TRUNCATE (see takeover()'s and
// e.deadTail's doc comments) — never shrinks the -wal file on disk. Once the
// lazy reset lands and the expectRestart continuation stores consumed=0, the
// OLD generation's now-inert dead tail is still physically present in the
// file, and an unfixed Lag() (fi.Size() - consumed) reads it as outstanding
// backlog forever, with no real write behind it (reviewer repro: drained,
// WAL 32768 bytes, consumed 4152, Lag()=28616).
//
// This exercises the exact trigger path a running daemon hits routinely: a
// write, then idle past afterDrain's takeover threshold (captured > 0 &&
// idle > 5s — the >=64-txn threshold is exercised instead by
// TestEngineTakeoverExpectedRestartIsNotRebase, but that path never reaches
// this bug because 64 rapid txns don't leave time for the file to look
// "quiet" the way a real idle daemon session does; the idle threshold is
// what settling-flush/daemon-reopen sessions actually hit — see the
// README/dbfile.go doc fixes in this same review pass), then a further small
// write to actually land the lazy reset, then DrainNow to fully catch up.
// Lag() must read exactly 0 once fully drained — never the dead tail.
//
// Fail-verified against the pre-fix code (temporary revert of e.deadTail and
// Lag()'s floor, confirming this test fails with a large nonzero Lag()
// before the fix is restored) — see the task-8 report's "Whole-branch fix
// wave" section for that run.
func TestEngineLagRetiresPhantomAfterIdleTakeover(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Let the engine's initial rebase finish before writing — same ordering
	// every other test in this file relies on.
	time.Sleep(300 * time.Millisecond)

	// The big write: gives afterDrain's captured>0 half of the idle-takeover
	// condition, and resets idle to roughly now.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(4096));").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}
	waitEqual(t, src, rep, 5*time.Second)

	// Idle past afterDrain's 5s threshold — the smallest honest margin above
	// it, since the poll loop only checks time.Since(idle) > 5*time.Second on
	// its own 10ms ticks. Once crossed, the engine's own ticker drives
	// takeover() (checkpoint RESTART, verified clean, arms expectRestart)
	// with no test-side call needed.
	time.Sleep(5300 * time.Millisecond)

	// The small write: what actually lands SQLite's deferred WAL-header
	// rewrite (new salts) — takeover() only ARMS the reset, this is what
	// makes it land. This is also the write whose commit leaves the OLD
	// generation's on-disk length behind as a dead tail once the reset
	// physically happens, exactly the reviewer's repro shape.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(16));").CombinedOutput(); err != nil {
		t.Fatalf("insert: %v: %s", err, out)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	if err := e.DrainNow(dctx); err != nil {
		t.Fatalf("DrainNow() = %v, want nil", err)
	}

	if got := e.Lag(); got != 0 {
		t.Fatalf("Lag() after a fully-drained mid-session takeover = %d, want 0 (phantom dead-tail backlog, not a real one)", got)
	}
	if got := e.Rebased(); got != 1 {
		t.Errorf("a clean idle-triggered takeover must not rebase; Rebased() = %d, want 1 (the startup rebase only)", got)
	}
}
