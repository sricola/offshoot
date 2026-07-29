package wal

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openWALDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, p := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestReaderSeesEachCommit(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")

	drain := func() int {
		n := 0
		for {
			tx, err := r.Next()
			if err != nil {
				t.Fatal(err)
			}
			if tx == nil {
				return n
			}
			if tx[len(tx)-1].Header.CommitSize == 0 {
				t.Fatal("returned txn does not end in commit frame")
			}
			n++
		}
	}
	drain() // consume CREATE TABLE

	for i := 0; i < 5; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
			t.Fatal(err)
		}
	}
	if got := drain(); got != 5 {
		t.Errorf("committed txns = %d, want 5", got)
	}
}

func TestReaderIgnoresTornTail(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}
	// Simulate a torn write: append half a frame of garbage to the WAL.
	f, err := os.OpenFile(path+"-wal", os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	tx, err := r.Next()
	if err != nil {
		t.Fatalf("torn tail must not error, got %v", err)
	}
	if tx != nil {
		t.Fatal("torn tail must not produce a transaction")
	}
	_ = db
}

func TestReaderDetectsRestart(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}
	// RESTART the WAL (new salts), then write again.
	if _, err := db.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != ErrWALRestarted {
		t.Fatalf("want ErrWALRestarted, got %v", err)
	}
}
