package capture

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/replay"
)

// seedWideDB creates a WAL-mode database with rows pages wide: every row's
// blob fills most of a 4 KiB page, so an UPDATE of n rows writes about n
// frames and a bulk update trips a foreign connection's default
// wal_autocheckpoint=1000. Returns the path and a long-lived connection that
// plays the agent (autocheckpoint applies to it).
func seedWideDB(t *testing.T, rows int) (string, *sql.DB, *sql.Conn) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA page_size=4096; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := tx.Exec("INSERT INTO t (id, v) VALUES (?, zeroblob(3900))", i+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	fc, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fc.ExecContext(context.Background(), "PRAGMA busy_timeout=5000"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fc.Close(); db.Close() })
	return src, db, fc
}

// stalePreT1Rows counts rows whose blob is still the seed zeroblob among
// ids <= n, i.e. rows a bulk UPDATE of ids <= n has NOT reached.
func stalePreT1Rows(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, n int) int {
	t.Helper()
	var stale int
	if err := q.QueryRowContext(context.Background(), "SELECT count(*) FROM t WHERE id <= ? AND v = zeroblob(3900)", n).Scan(&stale); err != nil {
		t.Fatalf("query (replica corrupt?): %v", err)
	}
	return stale
}

// TestRebaseCopyHoldsReadLockAgainstFoldedTransaction: during rebase's
// snapshot copy a foreign writer commits T1 (1200 rows, >1000 frames, so the
// agent connection's autocheckpoint folds it into the main file mid-copy)
// and then T2 (a small commit which, with the WAL fully backfilled and no
// reader pinning it, RESTARTS the WAL under new salts). Without a read lock
// held across the copy, the fresh reader bound after it binds to T2's
// generation: T1's frames are gone from the WAL and only the pages the copy
// had not yet passed carry T1 — the replica silently diverges while the
// engine reports rebased=1 and no error. The read lock taken right after
// checkpoint(TRUNCATE) holds READ_LOCK(0), which every checkpoint needs
// exclusively to backfill, so nothing can be folded or restarted until the
// copy is done and the reader is bound.
func TestRebaseCopyHoldsReadLockAgainstFoldedTransaction(t *testing.T) {
	src, _, fc := seedWideDB(t, 3000)

	fired := make(chan struct{}, 1)
	copySrcMidHook = func() {
		copySrcMidHook = nil
		// T1 touches ids 1..1200: the lowest pages, which the half-done copy
		// has already read. T2 restarts the WAL once T1 is backfilled.
		if _, err := fc.ExecContext(context.Background(), "UPDATE t SET v = randomblob(3900) WHERE id <= 1200"); err != nil {
			t.Errorf("T1: %v", err)
		}
		if _, err := fc.ExecContext(context.Background(), "INSERT INTO t (id, v) VALUES (999999, zeroblob(10))"); err != nil {
			t.Errorf("T2: %v", err)
		}
		fired <- struct{}{}
	}
	defer func() { copySrcMidHook = nil }()

	dir := t.TempDir()
	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case <-fired:
	case <-time.After(20 * time.Second):
		t.Fatal("copySrcMidHook never fired")
	}
	waitRebased(t, e, 1, 20*time.Second)
	dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer dcancel()
	for i := 0; i < 2; i++ {
		if err := e.DrainNow(dctx); err != nil {
			t.Fatalf("DrainNow: %v", err)
		}
	}

	rdb, err := sql.Open("sqlite3", "file:"+rep.Path()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	var marker int
	if err := rdb.QueryRow("SELECT count(*) FROM t WHERE id = 999999").Scan(&marker); err != nil {
		t.Fatalf("replica query (corrupt?): %v", err)
	}
	if marker != 1 {
		t.Fatalf("T2 never reached the replica (marker=%d, rebased=%d, lag=%d)", marker, e.Rebased(), e.Lag())
	}
	stale := stalePreT1Rows(t, rdb, 1200)
	srcStale := stalePreT1Rows(t, fc, 1200)
	if stale != srcStale {
		t.Fatalf("DIVERGENCE: replica has %d of T1's 1200 rows still pre-T1, checkout has %d; engine reports rebased=%d lag=%d and no error", stale, srcStale, e.Rebased(), e.Lag())
	}
	if lag := e.Lag(); lag != 0 {
		t.Fatalf("drained engine reports lag=%d bytes: frames the reader cannot reach", lag)
	}
}

// TestShutdownRefusesCleanStateWhenAWriteLandsBeforeTheHash: shutdown runs
// checkpoint(TRUNCATE) and then hashes the main file for the clean-resume
// baseline. A foreign commit big enough to trip the agent's autocheckpoint
// that lands between the two is folded into the file BEFORE the hash, so a
// State{Clean:true, MainHash} would describe content the replica never
// saw; once the agent's last connection closes (SQLite checkpoints and
// unlinks the WAL), the next start would find a matching hash and RESUME
// over the missing write. shutdown now takes the read lock after TRUNCATE
// and re-checks that the WAL is still empty before hashing: any write that
// landed leaves frames the lock keeps from being folded, and the clean
// save is refused.
func TestShutdownRefusesCleanStateWhenAWriteLandsBeforeTheHash(t *testing.T) {
	src, db, fc := seedWideDB(t, 1500)

	dir := t.TempDir()
	rep := replay.New(filepath.Join(dir, "replica.db"))
	start := func() (*Engine, context.CancelFunc, chan error) {
		e := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		return e, cancel, done
	}
	e, cancel, done := start()
	waitRebased(t, e, 1, 20*time.Second)
	if _, err := fc.ExecContext(context.Background(), "INSERT INTO t (id, v) VALUES (500000, zeroblob(10))"); err != nil {
		t.Fatal(err)
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer dcancel()
	if err := e.DrainNow(dctx); err != nil {
		t.Fatal(err)
	}

	shutdownPreHashHook = func() {
		shutdownPreHashHook = nil
		if _, err := fc.ExecContext(context.Background(), "UPDATE t SET v = randomblob(3900) WHERE id <= 1100"); err != nil {
			t.Errorf("window write: %v", err)
		}
	}
	defer func() { shutdownPreHashHook = nil }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	st, ok, err := LoadState(StatePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if ok && st.Clean {
		t.Fatalf("shutdown saved Clean=true although a 1100-row write landed between TRUNCATE and the hash (MainHash=%s)", st.MainHash)
	}

	// Belt and braces: the resume path itself. The agent exits (last
	// connection closes → SQLite checkpoints and unlinks the WAL); a second
	// start must rebase, not resume, and the replica must carry the write.
	fc.Close()
	db.Close()
	if _, err := os.Stat(src + "-wal"); err == nil {
		return // persistent-WAL build: the resume path is unreachable here; the state assertion above already holds
	}
	e2, cancel2, done2 := start()
	defer func() { cancel2(); <-done2 }()
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	if err := e2.WaitReady(rctx); err != nil {
		t.Fatal(err)
	}
	if e2.Resumed() {
		t.Fatal("second start resumed over a Clean=false state")
	}
	waitRebased(t, e2, 1, 20*time.Second)
	if err := e2.DrainNow(dctx); err != nil {
		t.Fatal(err)
	}
	rdb, err := sql.Open("sqlite3", "file:"+rep.Path()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	if stale := stalePreT1Rows(t, rdb, 1100); stale > 0 {
		t.Fatalf("replica is missing %d of the 1100 rows the shutdown-window write updated", stale)
	}
}
