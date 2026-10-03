package dbfile

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// under scopes an evictor to one test's directory, so a test never closes
// a descriptor some other test in this binary still relies on.
func under(dir string) func(string) bool {
	abs, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}
	prefix := abs + string(filepath.Separator)
	return func(p string) bool { return strings.HasPrefix(p, prefix) }
}

func lookupLive(t *testing.T, path string) *entry {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	e := live[abs]
	if e == nil {
		t.Fatalf("no cached descriptor for %s", abs)
	}
	return e
}

func isLive(e *entry) bool {
	mu.Lock()
	defer mu.Unlock()
	return live[e.path] == e
}

func isOrphan(e *entry) bool {
	mu.Lock()
	defer mu.Unlock()
	for _, o := range orphans[e.ino] {
		if o == e {
			return true
		}
	}
	return false
}

func pinsOf(ino Inode) int {
	mu.Lock()
	defer mu.Unlock()
	return pins[ino]
}

// isOpen reports whether e's descriptor is still open WITHOUT closing it:
// closing a descriptor a test expects open would drop the very POSIX locks
// some of these tests check.
func isOpen(e *entry) bool {
	_, err := e.f.Stat()
	return !errors.Is(err, os.ErrClosed)
}

// assertClosed is the spec's closure criterion: gone from the registry, and
// a second Close reports os.ErrClosed.
func assertClosed(t *testing.T, e *entry) {
	t.Helper()
	if isLive(e) || isOrphan(e) {
		t.Fatalf("%s: closed descriptor is still registered", e.path)
	}
	if err := e.f.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("%s: Close() = %v, want os.ErrClosed", e.path, err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// replaceFile renames a fresh file over path, the way every checkout
// re-materialization does.
func replaceFile(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	writeFile(t, tmp, content)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// newWALDB creates a WAL-mode database with go-sqlite3, so nothing here
// depends on the sqlite3 CLI's build (Apple's keeps a persistent WAL).
func newWALDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
}

// replaceWithWALDB renames a fresh WAL database over path.
func replaceWithWALDB(t *testing.T, path string) {
	t.Helper()
	tmp := path + ".tmp"
	newWALDB(t, tmp)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}
