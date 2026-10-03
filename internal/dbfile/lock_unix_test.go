//go:build unix

package dbfile

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// TestHelperLockProbe is not a test: probeSharedLock re-runs this binary
// with only it selected, as a separate process that reads the lock.
func TestHelperLockProbe(t *testing.T) { testutil.LockProbeHelper() }

// probeSharedLock reports "held" or "free" for SQLite's SHARED byte range of
// path, read from another process (F_GETLK never reports the caller's own
// locks). Unlike lockSurvives in dbfile_test.go, this does not depend on the
// sqlite3 CLI's build.
func probeSharedLock(t *testing.T, path string) string {
	t.Helper()
	return testutil.SQLiteSharedLock(t, path)
}

func TestEvictNeverClosesAHeldInode(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	path := filepath.Join(dir, "held.db")
	newWALDB(t, path)
	ctx := context.Background()

	release, ino, err := Hold(path)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
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
		t.Fatal(err)
	}
	// The long-lived read lock, taken the way the capture engine takes it.
	for _, q := range []string{"BEGIN", "SELECT count(*) FROM sqlite_master"} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	// Cache an unpinned descriptor on the held inode, as fileSum would.
	s, _ := Reader(path)
	io.Copy(io.Discard, s)
	s.Close()
	e := lookupLive(t, path)

	if got := probeSharedLock(t, path); got != "held" {
		t.Fatalf("before eviction: probe = %q, want held", got)
	}
	if n := evict(0, in); n != 0 {
		t.Fatalf("evict(0) closed %d descriptor(s) on a held inode", n)
	}
	if n := evictStranded(in); n != 0 {
		t.Fatalf("evictStranded closed %d descriptor(s) on a held inode", n)
	}
	if !isLive(e) || !isOpen(e) {
		t.Fatal("the held inode's cached descriptor was closed")
	}
	if got := probeSharedLock(t, path); got != "held" {
		t.Fatalf("after eviction: probe = %q, want held", got)
	}

	// Orphan it while held, the way a re-materialize does. A hard link keeps
	// the old inode reachable for the probe.
	keep := path + ".old-inode"
	if err := os.Link(path, keep); err != nil {
		t.Fatal(err)
	}
	replaceWithWALDB(t, path)
	if n := evictStranded(in); n != 0 {
		t.Fatalf("evictStranded closed %d pinned orphan(s)", n)
	}
	if !isOrphan(e) || !isOpen(e) {
		t.Fatal("pinned orphan was closed or lost")
	}
	if got := probeSharedLock(t, keep); got != "held" {
		t.Fatalf("orphaned: probe = %q, want held", got)
	}

	// Control: an ordinary open and close of the same inode DOES drop the
	// lock, so "held" above is evidence, not an artifact of the probe.
	f, err := os.Open(keep)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if got := probeSharedLock(t, keep); got != "free" {
		t.Fatalf("control: probe = %q after os.Open+Close, want free (the probe cannot see a dropped lock)", got)
	}

	conn.ExecContext(ctx, "COMMIT")
	conn.Close()
	db.Close()
	release()
	released = true
	if n := evictStranded(in); n != 1 {
		t.Fatalf("evictStranded after release = %d, want 1", n)
	}
	assertClosed(t, e)
}

// TestHoldProtectsEveryInodeItsPathNames: a held open connects lazily, after
// Hold has pinned the inode the path named then, and the connect itself
// takes SHARED (go-sqlite3 runs a schema-reading PRAGMA on every new
// connection, and a WAL-mode connection keeps that lock). A re-materialize
// that renames a fresh inode X over the path between the Hold and the
// connect leaves the connection locking X, which the Hold's pin does not
// cover. If an eviction then closed a descriptor cached on X, the kernel
// would drop that lock while SQLite, which tracks lock state per inode for
// the whole process, still counts it: the next connection here on X takes
// SHARED by bumping that count, with no fcntl, and runs pinned, verified and
// unlocked. So nothing cached under a held path may be closed, live (evict)
// or orphaned by a later re-materialize (EvictStranded).
func TestHoldProtectsEveryInodeItsPathNames(t *testing.T) {
	for _, tc := range []struct {
		name   string
		orphan bool // a second re-materialize orphans X before the eviction pass
	}{
		{"live", false},
		{"orphaned", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			in := under(dir)
			path := filepath.Join(dir, "c.db")
			keepX := filepath.Join(dir, "x.db") // a second name for X, for the probe
			newWALDB(t, path)
			ctx := context.Background()

			// T, shaped like quiesce: the Hold pins the original inode, and a
			// re-materialize lands before the lazy connect. StampSum's Reader
			// then caches a descriptor on X that nothing pins.
			HoldHookForTest = func(string) {
				replaceWithWALDB(t, path)
				if err := os.Link(path, keepX); err != nil {
					t.Fatal(err)
				}
				s, err := Reader(path)
				if err != nil {
					t.Fatal(err)
				}
				s.Close()
			}
			releaseT, inoT, err := Hold(path)
			HoldHookForTest = nil
			if err != nil {
				t.Fatal(err)
			}
			tReleased := false
			defer func() {
				if !tReleased {
					releaseT()
				}
			}()
			dbT, err := sql.Open("sqlite3", NoCreateDSN(path, "_busy_timeout=3000"))
			if err != nil {
				t.Fatal(err)
			}
			defer dbT.Close()
			connT, err := dbT.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer connT.Close()
			if got := probeSharedLock(t, keepX); got != "held" {
				t.Fatalf("precondition: T's connect took no SHARED lock on X (probe = %q)", got)
			}
			if err := Verify(path, inoT); !errors.Is(err, ErrReplaced) {
				t.Fatalf("Verify = %v, want ErrReplaced: T's connection is on X", err)
			}

			if tc.orphan {
				replaceWithWALDB(t, path)
				s, err := Reader(path) // orphans X's descriptor under path
				if err != nil {
					t.Fatal(err)
				}
				s.Close()
			}
			evictStranded(in)
			evict(0, in)
			if got := probeSharedLock(t, keepX); got != "held" {
				t.Fatalf("an eviction while T held the path closed a descriptor on X and dropped T's lock (probe = %q)", got)
			}
			if tc.orphan {
				return
			}

			// Q, shaped like the capture engine, joins X while T is open.
			releaseQ, inoQ, err := Hold(path)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseQ()
			dbQ, err := sql.Open("sqlite3", NoCreateDSN(path, "_busy_timeout=3000&_journal_mode=WAL"))
			if err != nil {
				t.Fatal(err)
			}
			defer dbQ.Close()
			connQ, err := dbQ.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer connQ.Close()
			if err := Verify(path, inoQ); err != nil {
				t.Fatal(err)
			}
			for _, q := range []string{"BEGIN", "SELECT count(*) FROM sqlite_master"} {
				if _, err := connQ.ExecContext(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			defer connQ.ExecContext(ctx, "COMMIT")
			connT.Close()
			dbT.Close()
			releaseT()
			tReleased = true
			if got := probeSharedLock(t, keepX); got != "held" {
				t.Fatalf("Q is pinned and verified on X but holds no SHARED lock (probe = %q)", got)
			}
		})
	}
}
