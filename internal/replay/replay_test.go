package replay

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/wal"
)

func TestReplayMatchesSource(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")

	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0"} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	// Checkpoint so the base snapshot contains the schema, then copy it.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "base.db")
	copyFile(t, src, base)

	// Write 20 transactions into the WAL (not checkpointed).
	for i := 0; i < 20; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(256))"); err != nil {
			t.Fatal(err)
		}
	}

	// Capture all committed txns from the WAL and apply to a replica.
	rep := New(filepath.Join(dir, "replica.db"))
	if err := rep.Rebase(base); err != nil {
		t.Fatal(err)
	}
	r := wal.NewReader(src + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
		if err := rep.Apply(4096, tx); err != nil {
			t.Fatal(err)
		}
	}

	// Quiesce source and compare dumps.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	sd, err := Dump(src)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := Dump(rep.Path())
	if err != nil {
		t.Fatal(err)
	}
	if sd != rd {
		t.Fatalf("dump mismatch:\n--- source ---\n%.2000s\n--- replica ---\n%.2000s", sd, rd)
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
