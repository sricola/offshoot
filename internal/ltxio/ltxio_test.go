package ltxio

import (
	"bytes"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/sricola/offshoot/internal/testutil"
)

func makeDB(t *testing.T, rows int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(300))"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func dump(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", path, ".dump").Output()
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestSnapshotRoundTrip(t *testing.T) {
	testutil.RequireSQLite3(t)
	src := makeDB(t, 50)
	var buf bytes.Buffer
	if _, err := EncodeSnapshot(src, 7, &buf); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "restored.db")
	txid, err := Materialize(bytes.NewReader(buf.Bytes()), dst)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 7 {
		t.Errorf("txid = %d, want 7", txid)
	}
	if dump(t, src) != dump(t, dst) {
		t.Fatal("dump mismatch after round trip")
	}
}

func TestEncodeRefusesDirtyWAL(t *testing.T) {
	src := makeDB(t, 3)
	// Recreate a non-empty WAL: write without checkpointing.
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; INSERT INTO t (v) VALUES (randomblob(10))"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := EncodeSnapshot(src, 2, &buf); err == nil {
		t.Fatal("want error on non-empty WAL")
	}
}

func TestMaterializeFailsClosedOnCorruption(t *testing.T) {
	src := makeDB(t, 20)
	var buf bytes.Buffer
	if _, err := EncodeSnapshot(src, 3, &buf); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	b[len(b)/2] ^= 0xFF // flip a bit mid-file
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := Materialize(bytes.NewReader(b), dst); err == nil {
		t.Fatal("want checksum error")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("corrupt materialize must leave no destination file")
	}
}
