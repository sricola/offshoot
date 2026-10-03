package ops

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sricola/offshoot/internal/dbfile"
)

func newWALFile(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS t (v)"); err != nil {
		t.Fatal(err)
	}
}

// setHoldHook installs dbfile.HoldHookForTest for one test. It is a plain
// package variable, so no test that sets it runs a session or daemon whose
// background Hold could read it concurrently.
func setHoldHook(t *testing.T, f func(path string)) {
	dbfile.HoldHookForTest = f
	t.Cleanup(func() { dbfile.HoldHookForTest = nil })
}

func TestQuiesceHoldsTheCheckout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	newWALFile(t, path)
	abs, _ := filepath.Abs(path)
	var seen []int
	setHoldHook(t, func(p string) {
		if p == abs {
			n, _ := dbfile.PinsAt(p)
			seen = append(seen, n)
		}
	})
	if err := quiesce(path); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != 1 {
		t.Fatalf("holds seen during quiesce = %v, want exactly one, pinned", seen)
	}
	if n, _ := dbfile.PinsAt(path); n != 0 {
		t.Fatalf("pins after quiesce = %d, want 0", n)
	}
}

func TestQuiesceRefusesAReplacedCheckout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	newWALFile(t, path)
	old := path + ".old-inode"
	if err := os.Link(path, old); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(path)
	setHoldHook(t, func(p string) {
		if p == abs {
			tmp := path + ".tmp"
			newWALFile(t, tmp)
			if err := os.Rename(tmp, path); err != nil {
				t.Error(err)
			}
		}
	})
	if err := quiesce(path); !errors.Is(err, dbfile.ErrReplaced) {
		t.Fatalf("quiesce = %v, want dbfile.ErrReplaced", err)
	}
	if n, _ := dbfile.PinsAt(old); n != 0 {
		t.Fatalf("pins left on the replaced inode = %d", n)
	}
}

// TestQuiesceDoesNotCreateAMissingCheckout: a checkout removed between a
// caller's stat and quiesce (destroy racing branches) used to come back as
// an empty database, because SQLite creates a missing file on open. Both
// windows are covered: the path already gone at the Hold, and the path
// removed after the Hold but before the lazy open.
func TestQuiesceDoesNotCreateAMissingCheckout(t *testing.T) {
	t.Run("gone before the hold", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "gone.db")
		err := quiesce(path)
		if err == nil || errors.Is(err, errQuiesceBusy) {
			t.Fatalf("quiesce on a missing path = %v, want a not-busy error", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("quiesce created a database at a missing checkout path")
		}
	})
	t.Run("removed between Hold and Conn", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "c.db")
		newWALFile(t, path)
		abs, _ := filepath.Abs(path)
		setHoldHook(t, func(p string) {
			if p == abs {
				if err := os.Remove(path); err != nil {
					t.Error(err)
				}
			}
		})
		err := quiesce(path)
		if err == nil || errors.Is(err, errQuiesceBusy) {
			t.Fatalf("quiesce on a path removed after its hold = %v, want a not-busy error", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("quiesce recreated a removed checkout as an empty database")
		}
	})
}

// TestCreateFromHoldsItsImportSource: the daemon's create op imports any
// absolute path, including a checkout of this same daemon, so the source
// read is pinned like any checkout open.
func TestCreateFromHoldsItsImportSource(t *testing.T) {
	w := newWS(t)
	src := filepath.Join(t.TempDir(), "import.db")
	newWALFile(t, src)
	abs, _ := filepath.Abs(src)
	var held []string
	setHoldHook(t, func(p string) { held = append(held, p) })
	if err := w.CreateFrom("imp", src); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(held, abs) {
		t.Fatalf("holds = %v, want one on the import source %s", held, abs)
	}
	if n, _ := dbfile.PinsAt(src); n != 0 {
		t.Fatalf("pins on the source after import = %d", n)
	}
}
