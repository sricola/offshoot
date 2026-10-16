package testutil

import (
	"bytes"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestSQLite3CLIWaitsOutTransientLocks pins the busy timeout SQLite3CLI
// exists for: a write that meets a briefly held write lock — the shape of a
// live session's startup checkpoint — must wait it out and land, not fail
// instantly with "database is locked (5)".
func TestSQLite3CLIWaitsOutTransientLocks(t *testing.T) {
	RequireSQLite3(t)
	path := filepath.Join(t.TempDir(), "t.db")
	if out, err := SQLite3CLI(path, "PRAGMA journal_mode=WAL; CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	holder := exec.Command("sqlite3", path)
	in, err := holder.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := holder.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	defer holder.Wait()
	defer in.Close()
	if _, err := io.WriteString(in, "BEGIN IMMEDIATE;\nSELECT 'lock-held';\n"); err != nil {
		t.Fatal(err)
	}
	// Reading the marker back proves BEGIN IMMEDIATE has executed and the
	// write lock is genuinely held before the write under test starts.
	var seen []byte
	tmp := make([]byte, 64)
	for !bytes.Contains(seen, []byte("lock-held")) {
		n, err := out.Read(tmp)
		if err != nil {
			t.Fatalf("reading lock holder's output: %v (got %q)", err, seen)
		}
		seen = append(seen, tmp[:n]...)
	}

	release := time.AfterFunc(300*time.Millisecond, func() {
		io.WriteString(in, "COMMIT;\n")
		in.Close()
	})
	defer release.Stop()

	if out, err := SQLite3CLI(path, "INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("write under a transient lock: %v: %s", err, out)
	}
	// Stdout stays the caller's own: .timeout prints nothing.
	if got, err := SQLite3CLI(path, "SELECT count(*) FROM t;").Output(); err != nil || string(got) != "1\n" {
		t.Fatalf("count = %q, %v; want \"1\\n\"", got, err)
	}
}
