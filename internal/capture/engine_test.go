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
// to TestEngineTakeoverUnderConcurrentWrites: 70 transactions are written
// serially, each with a minimal pacing sleep so the engine's 10ms poll loop
// can interleave and capture them incrementally (a true no-pacing burst
// races the engine's own initial rebase and checkpoint machinery — writes
// contend with checkpoint locks and land in one clump instead, which starves
// the poll loop of the chance to drain mid-stream and never exercises the
// path this test exists for). A brief pause after crossing the 64-txn
// threshold lets a takeover fire and complete against an already-quiet WAL.
// A few more writes then force SQLite's deferred WAL-header rewrite (new
// salts) to actually land, driving the engine through the
// wal.ErrWALRestarted path this fix changes: it must be absorbed as an
// expected continuation, not counted as a rebase.
//
// (The idle-takeover trigger — >5s with no writes — would exercise the same
// code path but is too slow for a test; crossing the 64-txn threshold is
// equivalent for this purpose and fast.)
func TestEngineTakeoverExpectedRestartIsNotRebase(t *testing.T) {
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

	// Let the engine's own initial rebase (checkpoint TRUNCATE + snapshot
	// copy + reader bind) finish before hammering writes. Otherwise the
	// write burst below races the initial rebase itself: writes that land
	// before the reader is bound get folded into the initial snapshot
	// instead of captured incrementally via the WAL, so `captured` never
	// reaches the 64-txn threshold and takeover — the very thing this test
	// exists to exercise — never fires.
	time.Sleep(300 * time.Millisecond)

	// 70 txns, serial, crossing the 64-txn threshold.
	for i := 0; i < 70; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(128))"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}

	// Let the poll loop notice captured >= 64 and run takeover against the
	// now-quiet WAL: checkpoint(RESTART)'s log count should equal exactly
	// what we'd already consumed, taking the verified-clean path. Generous
	// relative to the ~10ms poll interval for the same reason as the
	// analogous wait in TestEngineTakeoverUnderConcurrentWrites above — see
	// that comment.
	time.Sleep(2 * time.Second)

	// A few more commits force SQLite to actually rewrite the WAL header
	// with new salts (the lazy part of RESTART), which is what makes the
	// engine's reader observe wal.ErrWALRestarted on its next drain.
	for i := 0; i < 5; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(128))"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}

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
