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
