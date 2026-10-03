//go:build unix

package dbfile

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// SQLite's unix VFS takes a SHARED lock as an F_RDLCK over these bytes of
// the main database file (os_unix.c: PENDING_BYTE, SHARED_FIRST, SHARED_SIZE).
const (
	sqlitePendingByte = 0x40000000
	sqliteSharedFirst = sqlitePendingByte + 2
	sqliteSharedSize  = 510
	lockProbeEnv      = "DBFILE_LOCK_PROBE"
)

// TestHelperProcess is not a test. Run by probeSharedLock as a separate
// process, it reports whether any process holds a lock that would block a
// write lock over SQLite's SHARED range of the file named in its env.
func TestHelperProcess(t *testing.T) {
	path := os.Getenv(lockProbeEnv)
	if path == "" {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		fmt.Print("error: ", err)
		os.Exit(2)
	}
	lk := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart, Start: sqliteSharedFirst, Len: sqliteSharedSize}
	if err := unix.FcntlFlock(f.Fd(), unix.F_GETLK, &lk); err != nil {
		fmt.Print("error: ", err)
		os.Exit(2)
	}
	if lk.Type == unix.F_UNLCK {
		fmt.Print("free")
	} else {
		fmt.Print("held")
	}
	os.Exit(0)
}

// probeSharedLock asks another process, because F_GETLK never reports the
// caller's own locks. Unlike lockSurvives in dbfile_test.go, this does not
// depend on the sqlite3 CLI's build.
func probeSharedLock(t *testing.T, path string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), lockProbeEnv+"="+path)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("lock probe: %v: %s", err, out)
	}
	return string(out)
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
