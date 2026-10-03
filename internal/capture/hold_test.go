package capture

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/testutil"
)

func newHoldTestSrc(t *testing.T, path string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}
}

func holdTestEngine(t *testing.T, src string) *Engine {
	dir := t.TempDir()
	return NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{replay.New(filepath.Join(dir, "replica.db"))}})
}

// runBounded runs an engine that is expected to fail at startup. A Run that
// does not fail starts capturing and only returns once its ctx is done, so
// the deadline turns that regression into a failure here instead of a hang
// that go test's own timeout would end for the whole package.
func runBounded(t *testing.T, e *Engine) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := e.Run(ctx)
	if ctx.Err() != nil {
		t.Fatalf("Run did not fail at startup; it was still running at its deadline (err=%v)", err)
	}
	return err
}

func pinsAt(t *testing.T, path string) int {
	t.Helper()
	n, err := dbfile.PinsAt(path)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// setHoldHook installs dbfile.HoldHookForTest for one subtest. Nothing else
// in this binary is running an engine while it is set: the hook is a plain
// package variable, and -race would flag a leftover engine's Hold reading it.
func setHoldHook(t *testing.T, f func(abs string)) {
	t.Helper()
	dbfile.HoldHookForTest = f
	t.Cleanup(func() { dbfile.HoldHookForTest = nil })
}

// TestEngineReleasesItsHold: the engine's SQLite connection holds POSIX
// locks on the checkout for its whole life, so the engine pins the inode
// for exactly that long, and releases it on every way out of Run.
// Eviction here is scoped to each subtest's directory (dbfile.EvictUnder),
// so other tests' hold-less test connections are never touched.
func TestEngineReleasesItsHold(t *testing.T) {
	testutil.RequireSQLite3(t)

	t.Run("normal shutdown", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "src.db")
		newHoldTestSrc(t, src)
		e, _, cancel, done := startEngine(t, src)
		// Stop the engine even when an assertion fails first, so no engine
		// outlives this subtest (see setHoldHook).
		var stopped bool
		var runErr error
		stop := func() error {
			if !stopped {
				cancel()
				runErr, stopped = <-done, true
			}
			return runErr
		}
		t.Cleanup(func() { stop() })
		waitRebased(t, e, 1, 10*time.Second)
		if n := pinsAt(t, src); n != 1 {
			t.Fatalf("pins on a running engine's checkout = %d, want 1 (its hold)", n)
		}
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if n := pinsAt(t, src); n != 0 {
			t.Fatalf("pins after Run returned = %d, want 0", n)
		}
		if dbfile.EvictUnder(filepath.Dir(src)) == 0 {
			t.Fatal("no descriptor on the checkout became evictable after Run returned")
		}
	})

	t.Run("cancelled before Conn", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "src.db")
		newHoldTestSrc(t, src)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := holdTestEngine(t, src).Run(ctx); err != nil {
			t.Fatalf("Run on a cancelled ctx = %v, want nil", err)
		}
		if n := pinsAt(t, src); n != 0 {
			t.Fatalf("pins = %d, want 0", n)
		}
	})

	t.Run("first connection fails", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "src.db")
		if err := os.WriteFile(src, bytes.Repeat([]byte("not a database "), 512), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runBounded(t, holdTestEngine(t, src)); err == nil {
			t.Fatal("Run on a non-database file succeeded")
		}
		if n := pinsAt(t, src); n != 0 {
			t.Fatalf("pins = %d, want 0", n)
		}
	})

	t.Run("missing checkout", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "missing.db")
		err := runBounded(t, holdTestEngine(t, src))
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Run on a missing checkout = %v, want ErrNotExist", err)
		}
		if _, err := os.Stat(src); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("Run created the missing checkout (SQLite's create-on-open)")
		}
	})

	t.Run("replaced between Hold and Conn", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "src.db")
		newHoldTestSrc(t, src)
		old := src + ".old-inode"
		if err := os.Link(src, old); err != nil {
			t.Fatal(err)
		}
		abs, _ := filepath.Abs(src)
		setHoldHook(t, func(p string) {
			if p != abs {
				return
			}
			tmp := src + ".tmp"
			newHoldTestSrc(t, tmp)
			if err := os.Rename(tmp, src); err != nil {
				t.Error(err)
			}
		})
		err := runBounded(t, holdTestEngine(t, src))
		if !errors.Is(err, dbfile.ErrReplaced) {
			t.Fatalf("Run = %v, want dbfile.ErrReplaced", err)
		}
		if n := pinsAt(t, old); n != 0 {
			t.Fatalf("pins left on the replaced inode = %d, want 0", n)
		}
	})

	// A checkout destroyed after the hold's stat but before the lazy open
	// must not come back as an empty database at its path: the open is
	// read-write but never creates (dbfile.NoCreateDSN).
	t.Run("removed between Hold and Conn", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "src.db")
		newHoldTestSrc(t, src)
		old := src + ".old-inode"
		if err := os.Link(src, old); err != nil {
			t.Fatal(err)
		}
		abs, _ := filepath.Abs(src)
		setHoldHook(t, func(p string) {
			if p != abs {
				return
			}
			for _, f := range []string{src, src + "-wal", src + "-shm"} {
				if err := os.Remove(f); err != nil && !errors.Is(err, fs.ErrNotExist) {
					t.Error(err)
				}
			}
		})
		if err := runBounded(t, holdTestEngine(t, src)); err == nil {
			t.Fatal("Run on a checkout removed before its connection opened succeeded")
		}
		if _, err := os.Stat(src); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("Run recreated the removed checkout (SQLite's create-on-open)")
		}
		if n := pinsAt(t, old); n != 0 {
			t.Fatalf("pins left on the removed inode = %d, want 0", n)
		}
	})
}
