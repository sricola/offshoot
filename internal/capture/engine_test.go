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
