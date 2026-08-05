package dbfile_test

import (
	"context"
	"database/sql"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/dbfile"
)

// sqlite3UnlinksWALOnExit reports whether the sqlite3 CLI on PATH does the
// standard close-time checkpoint-and-unlink of -wal/-shm. Apple's system
// build (persistent WAL) does not, and on it a dropped lock has no observable
// consequence, so the lock tests below cannot fail there and say so loudly
// rather than passing vacuously.
func sqlite3UnlinksWALOnExit(t *testing.T) bool {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "probe.db")
	if out, err := exec.Command("sqlite3", probe,
		"PRAGMA journal_mode=WAL; CREATE TABLE p (v BLOB); INSERT INTO p VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3 probe: %v: %s", err, out)
	}
	_, err := os.Stat(probe + "-wal")
	return os.IsNotExist(err)
}

func newWALDB(t *testing.T) string {
	t.Helper()
	src := filepath.Join(t.TempDir(), "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}
	return src
}

// lockSurvives holds a SQLite read lock on src, runs `read` (the raw-read
// strategy under test), then runs a full foreign writer lifecycle and reports
// whether the WAL survived — i.e. whether the read lock was still held.
func lockSurvives(t *testing.T, src string, read func(t *testing.T, path string)) bool {
	t.Helper()
	d, err := sql.Open("sqlite3", src+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	c, err := d.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	// Take the long-lived read lock exactly as the capture engine does.
	if _, err := c.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "SELECT count(*) FROM sqlite_master"); err != nil {
		t.Fatal(err)
	}
	defer c.ExecContext(ctx, "COMMIT")

	read(t, src)

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("foreign insert: %v: %s", err, out)
	}
	_, err = os.Stat(src + "-wal")
	return err == nil
}

// TestReaderPreservesSQLiteLocks is the unit-level statement of the whole
// reason this package exists: reading a live database file through
// dbfile.Reader must not disturb the locks any SQLite connection in this
// process holds on it. The companion sub-test pins the hazard itself — an
// ordinary os.Open/Close of the same file DOES drop those locks — so that the
// guarantee is demonstrated against a control rather than merely asserted.
func TestReaderPreservesSQLiteLocks(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	if !sqlite3UnlinksWALOnExit(t) {
		t.Skip("this platform's sqlite3 keeps the WAL on last close (persistent " +
			"WAL), so a dropped lock is not observable and these tests cannot " +
			"fail here — run against a stock sqlite3 build")
	}

	t.Run("dbfile.Reader keeps the lock", func(t *testing.T) {
		src := newWALDB(t)
		if !lockSurvives(t, src, func(t *testing.T, path string) {
			// Several times, as rebase()/hashSrc()/fileSum() would over a
			// session's lifetime.
			for i := 0; i < 3; i++ {
				r, err := dbfile.Reader(path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, r); err != nil {
					t.Fatal(err)
				}
			}
		}) {
			t.Fatal("dbfile.Reader dropped the SQLite connection's read lock: " +
				"the foreign writer's close-time checkpoint unlinked the WAL")
		}
	})

	t.Run("control: os.Open+Close drops the lock", func(t *testing.T) {
		src := newWALDB(t)
		if lockSurvives(t, src, func(t *testing.T, path string) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, f)
			f.Close()
		}) {
			t.Skip("this platform does not exhibit the POSIX (process, inode) " +
				"lock drop, so dbfile's guarantee is untestable here by contrast")
		}
	})
}

// TestReaderFollowsAReplacedFile pins the revalidation behaviour: because the
// descriptor is cached for the life of the process and never closed, a path
// that is replaced by a different inode — how checkouts are re-materialized,
// write-temp-then-rename — must not keep answering from the old, unlinked
// file.
func TestReaderFollowsAReplacedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.db")
	if err := os.WriteFile(path, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := dbfile.Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	if string(got) != "first" {
		t.Fatalf("got %q, want %q", got, "first")
	}

	// Replace by rename, exactly as a re-materialized checkout would.
	tmp := filepath.Join(dir, "tmp.db")
	if err := os.WriteFile(tmp, []byte("second-and-longer"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}

	r, err = dbfile.Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(r)
	if string(got) != "second-and-longer" {
		t.Fatalf("stale descriptor: got %q, want %q", got, "second-and-longer")
	}
}

// TestReaderSeesGrowth checks that each Reader call is bound to the file's
// size at that moment (so a snapshot copy cannot read past what it measured)
// while a later call observes an extended file.
func TestReaderSeesGrowth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "g.db")
	if err := os.WriteFile(path, []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	early, err := dbfile.Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("aaaabbbb"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := io.ReadAll(early); string(got) != "aaaa" {
		t.Fatalf("reader was not bound to its size at creation: got %q", got)
	}
	late, err := dbfile.Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := io.ReadAll(late); string(got) != "aaaabbbb" {
		t.Fatalf("later reader did not see the extended file: got %q", got)
	}
}

func TestReaderMissingFile(t *testing.T) {
	_, err := dbfile.Reader(filepath.Join(t.TempDir(), "nope.db"))
	if !os.IsNotExist(err) {
		t.Fatalf("Reader on a missing file = %v, want an IsNotExist error", err)
	}
}
