package capture

import (
	"context"
	"database/sql"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/replay"
	"github.com/offshoot-db/offshoot/internal/wal"
)

type replicaSink struct{ r *replay.Replica }

func (s replicaSink) Rebase(p string) error                 { return s.r.Rebase(p) }
func (s replicaSink) Apply(ps uint32, fr []wal.Frame) error { return s.r.Apply(ps, fr) }

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
	time.Sleep(300 * time.Millisecond) // let the takeover fire and settle

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
	// what we'd already consumed, taking the verified-clean path.
	time.Sleep(300 * time.Millisecond)

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
// after the engine is "crashed" (context cancelled — its lock and in-memory
// state vanish, like kill -9), writes land AND get checkpointed+restarted
// (new WAL salts) while it's dead, so frames pass through unseen. On
// restart, the engine must NOT silently resume: it must detect the
// divergence and rebase, and Rebased() must report it.
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

	// "Crash" the engine (cancel = its lock vanishes, like kill -9 would).
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
