package dbfile

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHoldSandwichDetectsRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.db")
	newWALDB(t, path)
	ctx := context.Background()

	t.Run("unchanged path verifies", func(t *testing.T) {
		release, ino, err := Hold(path)
		if err != nil {
			t.Fatal(err)
		}
		defer release() // registered first: runs after both closes below
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := Verify(path, ino); err != nil {
			t.Fatalf("Verify on an unchanged path = %v", err)
		}
	})

	t.Run("rename between Hold and Conn", func(t *testing.T) {
		release, ino, err := Hold(path)
		if err != nil {
			t.Fatal(err)
		}
		replaceWithWALDB(t, path) // the re-materialize lands before the lazy open
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = Verify(path, ino)
		conn.Close()
		db.Close()
		release()
		if !errors.Is(err, ErrReplaced) {
			t.Fatalf("Verify after a rename = %v, want ErrReplaced", err)
		}
		if n := pinsOf(ino); n != 0 {
			t.Fatalf("pins left on the held inode = %d, want 0", n)
		}
	})
}

// TestHoldReleaseIsIdempotent: a deferred release plus an explicit one must
// not consume a pin another holder took on the same inode.
func TestHoldReleaseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	writeFile(t, path, "x")
	r1, _, err := Hold(path)
	if err != nil {
		t.Fatal(err)
	}
	r2, _, _ := Hold(path)
	r1()
	r1()
	if n, _ := PinsAt(path); n != 1 {
		t.Fatalf("pins = %d, want 1 (the second holder's)", n)
	}
	r2()
	if n, _ := PinsAt(path); n != 0 {
		t.Fatalf("pins = %d, want 0", n)
	}
}

func TestHoldMissingPathPinsNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gone.db")
	if _, _, err := Hold(path); !os.IsNotExist(err) {
		t.Fatalf("Hold on a missing path = %v, want IsNotExist", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("Hold created the missing file")
	}
}

func TestHoldTouchesTheCachedEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	writeFile(t, path, "x")
	s, _ := Reader(path)
	s.Close()
	e := lookupLive(t, path)
	mu.Lock()
	before := e.lastUse
	mu.Unlock()
	release, _, _ := Hold(path)
	defer release()
	mu.Lock()
	after := e.lastUse
	mu.Unlock()
	if after <= before {
		t.Fatal("Hold did not touch the cached entry for Evict's LRU order")
	}
}

// TestNoCreateDSN: a held open must neither create a missing file (a
// checkout removed between Hold's stat and the lazy connection would come
// back empty) nor misread a path that URI syntax gives meaning to.
func TestNoCreateDSN(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T, path string) (*sql.DB, *sql.Conn, error) {
		t.Helper()
		db, err := sql.Open("sqlite3", NoCreateDSN(path, "_busy_timeout=1000&_journal_mode=WAL"))
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return db, conn, nil
	}
	names := func(t *testing.T, dir string) []string {
		t.Helper()
		es, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range es {
			out = append(out, e.Name())
		}
		return out
	}

	t.Run("missing file is not created", func(t *testing.T) {
		dir := t.TempDir()
		if _, _, err := open(t, filepath.Join(dir, "gone.db")); err == nil {
			t.Fatal("opening a missing file succeeded")
		}
		if got := names(t, dir); len(got) != 0 {
			t.Fatalf("files created by a failed open: %q", got)
		}
	})

	t.Run("URI-significant characters name the file itself", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "a b#c?d%41e.db")
		writeFile(t, path, "") // an empty file is an empty database
		db, conn, err := open(t, path)
		if err != nil {
			t.Fatal(err)
		}
		var mode string
		if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE t (v)"); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		db.Close()
		if mode != "wal" {
			t.Fatalf("journal_mode = %q, want wal: go-sqlite3's own parameters were lost", mode)
		}
		for _, n := range names(t, dir) {
			if n != "a b#c?d%41e.db" && n != "a b#c?d%41e.db-wal" && n != "a b#c?d%41e.db-shm" {
				t.Fatalf("the open touched another file: %q", n)
			}
		}
		if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
			t.Fatalf("nothing was written to %s (size %v, err %v)", path, fi, err)
		}
	})
}
