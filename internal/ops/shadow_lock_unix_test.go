//go:build unix

package ops

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestHelperLockProbe is not a test: testutil.SQLiteSharedLock re-runs this
// binary with only it selected, as a separate process that reads the lock.
func TestHelperLockProbe(t *testing.T) { testutil.LockProbeHelper() }

// TestRefreshShadowKeepsInProcessLocks: refreshShadow runs in the daemon
// after a checkout, a rollback, promote or compact refresh, and a session's
// clean Close, any of which another request's session or quiesce can
// overlap on the same checkout. Cloning by path opened and closed the
// checkout on Linux (FICLONE takes descriptors), even where the clone then
// failed, and that close dropped every SQLite lock this process held on it:
// a capture engine kept running unlocked. The clone must go through
// dbfile's pinned descriptor instead.
func TestRefreshShadowKeepsInProcessLocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "main.db")
	newWALFile(t, path)
	ctx := context.Background()

	// A connection that holds SHARED the way the capture engine does.
	release, ino, err := dbfile.Hold(path)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	db, err := sql.Open("sqlite3", dbfile.NoCreateDSN(path, "_busy_timeout=3000&_journal_mode=WAL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := dbfile.Verify(path, ino); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"BEGIN", "SELECT count(*) FROM sqlite_master"} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	defer conn.ExecContext(ctx, "COMMIT")
	if got := testutil.SQLiteSharedLock(t, path); got != "held" {
		t.Fatalf("precondition: probe = %q, want held", got)
	}

	refreshShadow(path)
	if got := testutil.SQLiteSharedLock(t, path); got != "held" {
		t.Fatalf("refreshShadow dropped the in-process connection's SHARED lock (probe = %q)", got)
	}

	// Control: an ordinary open and close of the checkout DOES drop it, so
	// "held" above is evidence, not an artifact of the probe.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if got := testutil.SQLiteSharedLock(t, path); got != "free" {
		t.Fatalf("control: probe = %q after os.Open+Close, want free", got)
	}
}
