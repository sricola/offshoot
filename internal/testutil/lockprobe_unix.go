//go:build unix

package testutil

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

// SQLite's unix VFS takes a SHARED lock as an F_RDLCK over these bytes of
// the main database file (os_unix.c: PENDING_BYTE, SHARED_FIRST,
// SHARED_SIZE).
const (
	sqlitePendingByte = 0x40000000
	sqliteSharedFirst = sqlitePendingByte + 2
	sqliteSharedSize  = 510
	lockProbeEnv      = "OFFSHOOT_TEST_LOCK_PROBE"
)

// SQLiteSharedLock reports, as "held" or "free", whether any process holds a
// lock over SQLite's SHARED byte range of path that would block a write
// lock there. It asks a separate process, because F_GETLK never reports the
// caller's own locks: that process is the test binary itself, re-run with
// only TestHelperLockProbe selected, so a package that calls this declares
//
//	func TestHelperLockProbe(t *testing.T) { testutil.LockProbeHelper() }
//
// Unlike a foreign sqlite3 writer, the probe does not depend on how the
// sqlite3 CLI on PATH was built.
func SQLiteSharedLock(t *testing.T, path string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperLockProbe$")
	cmd.Env = append(os.Environ(), lockProbeEnv+"="+path)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("lock probe: %v: %s", err, out)
	}
	if s := string(out); s != "held" && s != "free" {
		t.Fatalf("lock probe printed %q; is TestHelperLockProbe declared in this package?", s)
	}
	return string(out)
}

// LockProbeHelper is TestHelperLockProbe's body. In an ordinary test run it
// returns at once. Run by SQLiteSharedLock, it prints "held" or "free" and
// exits the process.
func LockProbeHelper() {
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
