package ops

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/dbfile"
	"github.com/sricola/offshoot/internal/testutil"
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

// watchHeldOpen checks the one held open of path an operation makes,
// through dbfile's test hooks, at the two edges of the pin's job. At
// Verify, with the open's connection up, the pin must still be in place
// and an eviction under path's directory must close nothing, though an
// unpinned descriptor on the inode is cached there; a closed descriptor
// would drop the connection's locks. At release, the connection must
// already be closed: SQLite unlinks a WAL database's -wal when its last
// connection closes, so a -wal still there means the pin is being dropped
// under a live connection (a release deferred after the Close, or called
// early). The returned func reports how many Verifies and releases of path
// the hooks saw.
func watchHeldOpen(t *testing.T, path string) (seen func() (verifies, releases int)) {
	t.Helper()
	abs, _ := filepath.Abs(path)
	// The unpinned descriptor a checkout's stamp leaves behind.
	s, err := dbfile.Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	var verifies, releases int
	dbfile.VerifyHookForTest = func(p string) {
		if p != abs {
			return
		}
		verifies++
		if n, _ := dbfile.PinsAt(p); n != 1 {
			t.Errorf("pins on %s with its connection open = %d, want 1", p, n)
		}
		if n := dbfile.EvictUnder(filepath.Dir(p)); n != 0 {
			t.Errorf("an eviction with the connection open closed %d descriptor(s)", n)
		}
	}
	dbfile.ReleaseHookForTest = func(p string) {
		if p != abs {
			return
		}
		releases++
		if _, err := os.Stat(p + "-wal"); !os.IsNotExist(err) {
			t.Errorf("the pin on %s was released with its connection still open (-wal stat: %v)", p, err)
		}
	}
	t.Cleanup(func() {
		dbfile.VerifyHookForTest = nil
		dbfile.ReleaseHookForTest = nil
	})
	return func() (int, int) { return verifies, releases }
}

// TestQuiesceHoldsTheCheckout: quiesce's pin covers its connection from
// open to close, so a janitor eviction running alongside branches,
// checkout, rollback, reap or destroy cannot drop its locks partway
// through the TRUNCATE checkpoint.
func TestQuiesceHoldsTheCheckout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	newWALFile(t, path)
	seen := watchHeldOpen(t, path)
	if err := quiesce(path); err != nil {
		t.Fatal(err)
	}
	if v, r := seen(); v != 1 || r != 1 {
		t.Fatalf("quiesce verified %d and released %d hold(s) on the checkout, want 1 and 1", v, r)
	}
	if n, _ := dbfile.PinsAt(path); n != 0 {
		t.Fatalf("pins after quiesce = %d, want 0", n)
	}
	if live, _ := descriptorsAt(t, path); !live {
		t.Fatal("the cached descriptor on the checkout was closed while quiesce held it")
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

// TestQuiesceRaceErrorsSayRetry: a checkout replaced or removed while
// quiesce opens it lost a race with another operation, and no caller calls
// such a checkout busy or in use. Only an operation that has committed
// nothing yet (checkout, checkpoint, destroy, and session open through
// checkout) says "(retry)", and retrying it works. Promote, rollback and
// compact refresh the checkout after their repoint has committed, so they
// say so and name 'offshoot checkout' instead: retrying a promote or
// rollback would move its rolling safety fork onto the already-repointed
// head, leaving the original head on no branch.
func TestQuiesceRaceErrorsSayRetry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		race  func(t *testing.T, path string)
		cause error // also wrapped, when not nil
	}{
		{"replaced", func(t *testing.T, path string) {
			tmp := path + ".tmp"
			newWALFile(t, tmp)
			if err := os.Rename(tmp, path); err != nil {
				t.Error(err)
			}
		}, dbfile.ErrReplaced},
		{"removed", func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Error(err)
			}
		}, nil},
	} {
		t.Run(tc.name+"/quiesce", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "c.db")
			newWALFile(t, path)
			abs, _ := filepath.Abs(path)
			setHoldHook(t, func(p string) {
				if p == abs {
					tc.race(t, path)
				}
			})
			err := quiesce(path)
			if !errors.Is(err, errCheckoutReplaced) || (tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("quiesce = %v, want errCheckoutReplaced", err)
			}
			if msg := err.Error(); strings.Contains(msg, "retry") || strings.Contains(msg, "checkpoint") {
				t.Fatalf("quiesce = %q: its callers add any retry hint, and it is no checkpoint", msg)
			}
		})

		// Each op races the first Hold on the branch's checkout after arm
		// runs; a nil arm arms it before the op starts.
		for _, op := range []struct {
			name      string
			branch    string
			committed bool // the op has repointed the branch when it refreshes
			arm       func(t *testing.T, w *Workspace, arm func())
			run       func(t *testing.T, w *Workspace) error
		}{
			{"checkout", "f", false, nil, func(t *testing.T, w *Workspace) error {
				_, err := w.Checkout("app", "f")
				return err
			}},
			{"checkpoint", "f", false, nil, func(t *testing.T, w *Workspace) error {
				_, err := w.Checkpoint("app", "f", "cp", nil)
				return err
			}},
			{"destroy", "f", false, nil, func(t *testing.T, w *Workspace) error {
				return w.Destroy("app", "f", false)
			}},
			{"promote", "main", true, func(t *testing.T, w *Workspace, arm func()) {
				ObservePromote = func(bool) { arm() }
				t.Cleanup(func() { ObservePromote = nil })
			}, func(t *testing.T, w *Workspace) error {
				res, err := w.PromoteWith("app", "f", "main", PromoteOptions{Force: true})
				if err != nil && res.Backup == "" {
					t.Errorf("the committed promote's result does not name its safety fork: %+v", res)
				}
				return err
			}},
			{"rollback", "main", true, func(t *testing.T, w *Workspace, arm func()) {
				ObserveRollback = func(bool) { arm() }
				t.Cleanup(func() { ObserveRollback = nil })
			}, func(t *testing.T, w *Workspace) error {
				_, err := w.Rollback("app", "main", "init")
				return err
			}},
			{"compact", "f", true, func(t *testing.T, w *Workspace, arm func()) {
				compactBeforeCASForTest = arm
				t.Cleanup(func() { compactBeforeCASForTest = nil })
			}, func(t *testing.T, w *Workspace) error {
				_, err := w.Compact("app", "f")
				return err
			}},
		} {
			t.Run(tc.name+"/"+op.name, func(t *testing.T) {
				w := newWS(t)
				if err := w.Create("app"); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
					t.Fatal(err)
				}
				path, err := w.Checkout("app", op.branch)
				if err != nil {
					t.Fatal(err)
				}
				abs, _ := filepath.Abs(path)
				armed, raced := op.arm == nil, false
				if op.arm != nil {
					op.arm(t, w, func() { armed = true })
				}
				setHoldHook(t, func(p string) {
					if p == abs && armed && !raced {
						raced = true
						tc.race(t, path)
					}
				})
				err = op.run(t, w)
				if !raced {
					t.Fatalf("%s never held its checkout after arming (err = %v)", op.name, err)
				}
				if !errors.Is(err, errCheckoutReplaced) {
					t.Fatalf("%s = %v, want errCheckoutReplaced", op.name, err)
				}
				msg := err.Error()
				if strings.Contains(msg, "in use") || strings.Contains(msg, "busy") {
					t.Fatalf("%s = %q: a replaced checkout is neither busy nor in use", op.name, msg)
				}
				if !op.committed {
					if !strings.Contains(msg, "(retry)") {
						t.Fatalf("%s = %q: nothing committed, want a retry hint", op.name, msg)
					}
					err := op.run(t, w)
					if op.name == "checkpoint" && tc.name == "removed" {
						if err == nil || !strings.Contains(err.Error(), "no checkout") {
							t.Fatalf("retried checkpoint of a removed checkout = %v, want 'no checkout'", err)
						}
					} else if err != nil {
						t.Fatalf("retried %s = %v", op.name, err)
					}
					return
				}
				if strings.Contains(msg, "retry") {
					t.Fatalf("%s = %q: it committed, so a retry is wrong", op.name, msg)
				}
				if !strings.Contains(msg, "offshoot checkout") {
					t.Fatalf("%s = %q: want the 'offshoot checkout' next step", op.name, msg)
				}
			})
		}
	}
}

// holdBusy makes path's checkout busy the way a live app does: a reader
// holds a snapshot while a write lands after it, so quiesce's TRUNCATE
// checkpoint cannot finish (see TestBranchStateBusyCheckoutIsDirty). The
// returned func closes both connections; cleanup closes them too.
func holdBusy(t *testing.T, path string) (closeConns func()) {
	t.Helper()
	var dbs []*sql.DB
	var once sync.Once
	closeConns = func() {
		once.Do(func() {
			for _, db := range dbs {
				db.Close()
			}
		})
	}
	t.Cleanup(closeConns)
	for _, stmt := range []string{
		"BEGIN; SELECT count(*) FROM sqlite_master;",
		"CREATE TABLE busy (v); INSERT INTO busy VALUES (1);",
	} {
		db, err := sql.Open("sqlite3", path+"?_busy_timeout=3000")
		if err != nil {
			t.Fatal(err)
		}
		dbs = append(dbs, db)
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return closeConns
}

// TestQuiesceBusyErrorsSayRetryOnlyBeforeACommit: a checkout another
// connection keeps busy fails quiesce with errQuiesceBusy, whatever the
// caller. Checkout, checkpoint and destroy have committed nothing, so they
// say to close connections and retry (destroy: before destroying), and
// doing that works. Promote, rollback and compact find it busy in the
// refresh after their repoint has committed, which is the common case: no
// busy check refuses them first. They say the operation stands and name
// 'offshoot checkout <db>@<branch>' after closing the connections, and
// never "retry": a second promote or rollback would move its rolling
// safety fork onto the repointed head, leaving the original head on no
// branch. Following their hint brings the checkout back to idle.
func TestQuiesceBusyErrorsSayRetryOnlyBeforeACommit(t *testing.T) {
	for _, op := range []struct {
		name   string
		branch string
		// arm installs a hook that calls busy once the op has committed
		// (or is about to, with nothing left to refuse it); nil makes the
		// checkout busy before the op starts.
		arm  func(t *testing.T, busy func())
		run  func(t *testing.T, w *Workspace) error
		hint string // the pre-commit op's next step; "" for a committed one
	}{
		{"checkout", "f", nil, func(t *testing.T, w *Workspace) error {
			_, err := w.Checkout("app", "f")
			return err
		}, "close connections and retry"},
		{"checkpoint", "f", nil, func(t *testing.T, w *Workspace) error {
			_, err := w.Checkpoint("app", "f", "cp", nil)
			return err
		}, "close connections and retry"},
		{"destroy", "f", nil, func(t *testing.T, w *Workspace) error {
			return w.Destroy("app", "f", false)
		}, "close connections before destroy"},
		{"promote", "main", func(t *testing.T, busy func()) {
			ObservePromote = func(bool) { busy() }
			t.Cleanup(func() { ObservePromote = nil })
		}, func(t *testing.T, w *Workspace) error {
			res, err := w.PromoteWith("app", "f", "main", PromoteOptions{Force: true})
			if err != nil && res.Backup == "" {
				t.Errorf("the committed promote's result does not name its safety fork: %+v", res)
			}
			return err
		}, ""},
		{"rollback", "main", func(t *testing.T, busy func()) {
			ObserveRollback = func(bool) { busy() }
			t.Cleanup(func() { ObserveRollback = nil })
		}, func(t *testing.T, w *Workspace) error {
			_, err := w.Rollback("app", "main", "init")
			return err
		}, ""},
		{"compact", "f", func(t *testing.T, busy func()) {
			compactBeforeCASForTest = busy
			t.Cleanup(func() { compactBeforeCASForTest = nil })
		}, func(t *testing.T, w *Workspace) error {
			_, err := w.Compact("app", "f")
			return err
		}, ""},
	} {
		t.Run(op.name, func(t *testing.T) {
			// Each busy quiesce waits out its 3s busy timeout. The hooks
			// above are distinct package variables, one per subtest, and
			// no other test runs alongside these.
			t.Parallel()
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			path, err := w.Checkout("app", op.branch)
			if err != nil {
				t.Fatal(err)
			}
			var closeConns func()
			busy := func() {
				if closeConns == nil {
					closeConns = holdBusy(t, path)
				}
			}
			if op.arm == nil {
				busy()
			} else {
				op.arm(t, busy)
			}
			err = op.run(t, w)
			if closeConns == nil {
				t.Fatalf("%s never reached its busy point (err = %v)", op.name, err)
			}
			if !errors.Is(err, errQuiesceBusy) {
				t.Fatalf("%s = %v, want errQuiesceBusy", op.name, err)
			}
			msg := err.Error()
			closeConns()
			if op.hint != "" {
				if !strings.Contains(msg, op.hint) {
					t.Fatalf("%s = %q: nothing committed, want %q", op.name, msg, op.hint)
				}
				if err := op.run(t, w); err != nil {
					t.Fatalf("%s after closing the connections = %v", op.name, err)
				}
				return
			}
			if strings.Contains(msg, "retry") {
				t.Fatalf("%s = %q: it committed, so a retry is wrong", op.name, msg)
			}
			next := "offshoot checkout app@" + op.branch
			if !strings.Contains(msg, "close its connections") || !strings.Contains(msg, next) {
				t.Fatalf("%s = %q: want 'close its connections' and the %q next step", op.name, msg, next)
			}
			if _, err := w.Checkout("app", op.branch); err != nil {
				t.Fatalf("the named next step, checkout, = %v", err)
			}
			if state, err := w.BranchState("app", op.branch); err != nil || state != "idle" {
				t.Fatalf("state after the named checkout = %q, %v; want idle", state, err)
			}
		})
	}
}

// TestCreateFromHoldsItsImportSource: the daemon's create op imports any
// absolute path, including a checkout of this same daemon, so the source
// read is pinned like any checkout open, for the connection's whole life.
func TestCreateFromHoldsItsImportSource(t *testing.T) {
	w := newWS(t)
	src := filepath.Join(t.TempDir(), "import.db")
	newWALFile(t, src)
	seen := watchHeldOpen(t, src)
	if err := w.CreateFrom("imp", src); err != nil {
		t.Fatal(err)
	}
	if v, r := seen(); v != 1 || r != 1 {
		t.Fatalf("the import verified %d and released %d hold(s) on its source, want 1 and 1", v, r)
	}
	if n, _ := dbfile.PinsAt(src); n != 0 {
		t.Fatalf("pins on the source after import = %d", n)
	}
	if live, _ := descriptorsAt(t, src); !live {
		t.Fatal("the cached descriptor on the import source was closed while the import held it")
	}
}

// descriptorsAt lists dbfile's descriptors for path: live and orphaned.
func descriptorsAt(t *testing.T, path string) (live bool, orphans int) {
	t.Helper()
	abs, _ := filepath.Abs(path)
	for _, e := range dbfile.Entries() {
		if e.Path != abs {
			continue
		}
		if e.Orphan {
			orphans++
		} else {
			live = true
		}
	}
	return live, orphans
}

// strandedUnder lists dbfile's descriptors under root that no path reaches:
// orphans, and cached entries whose path is gone but which no sweep has
// re-checked yet (a by-chain build's stage file, renamed to its entry).
func strandedUnder(t *testing.T, root string) []dbfile.EntryInfo {
	t.Helper()
	abs, _ := filepath.Abs(root)
	var out []dbfile.EntryInfo
	for _, e := range dbfile.Entries() {
		if !strings.HasPrefix(e.Path, abs+string(filepath.Separator)) {
			continue
		}
		if _, err := os.Stat(e.Path); e.Orphan || os.IsNotExist(err) {
			out = append(out, e)
		}
	}
	return out
}

func dirtyCheckout(t *testing.T, path string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestCheckoutReclaimsTheDescriptorItStrands(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if live, _ := descriptorsAt(t, path); !live {
		t.Fatal("precondition: checkout's stamp did not cache a descriptor")
	}
	dirtyCheckout(t, path) // the next checkout re-materializes: a new inode is renamed over path
	before, _ := os.Stat(path)
	captureStderr(t, func() {
		if _, err := w.Checkout("app", "main"); err != nil {
			t.Fatal(err)
		}
	})
	after, _ := os.Stat(path)
	if os.SameFile(before, after) {
		t.Fatal("precondition: checkout did not re-materialize")
	}
	if _, n := descriptorsAt(t, path); n != 0 {
		t.Fatalf("%d stranded descriptor(s) left on the re-materialized checkout", n)
	}
	if o := strandedUnder(t, w.Root); len(o) != 0 {
		t.Fatalf("stranded descriptors left under the store (by-chain build stages included): %+v", o)
	}
}

func TestRollbackRefreshReclaimsTheDescriptorItStrands(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	dirtyCheckout(t, path)
	if _, err := w.Checkpoint("app", "main", "cp1", nil); err != nil {
		t.Fatal(err)
	}
	dirtyCheckout(t, path)
	if _, err := w.Checkpoint("app", "main", "cp2", nil); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	if _, err := w.Rollback("app", "main", "cp1"); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if os.SameFile(before, after) {
		t.Fatal("precondition: rollback did not refresh the checkout")
	}
	if _, n := descriptorsAt(t, path); n != 0 {
		t.Fatalf("%d stranded descriptor(s) left after rollback's refresh", n)
	}
}

func TestDestroyReclaimsTheCheckoutDescriptor(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "f")
	if err != nil {
		t.Fatal(err)
	}
	if live, _ := descriptorsAt(t, path); !live {
		t.Fatal("precondition: no cached descriptor")
	}
	if err := w.Destroy("app", "f", false); err != nil {
		t.Fatal(err)
	}
	if live, n := descriptorsAt(t, path); live || n != 0 {
		t.Fatalf("destroyed checkout still has descriptors: live=%v orphans=%d", live, n)
	}
}

// TestPruneByChainReclaimsTheEntriesItRemoves: pruning runs after the
// materialize that may have grown the by-chain area, so the materialize's
// own reclaim has already run by then; the entries prune deletes are
// reclaimed by prune itself.
func TestPruneByChainReclaimsTheEntriesItRemoves(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 64<<10)
	mustFork(t, w, "app", "main", "b0", "seed")
	mustCheckout(t, w, "app", "b0")
	entry := w.byChainPath("app", headChainID(t, w, "app", "b0"))
	if live, _ := descriptorsAt(t, entry); !live {
		t.Fatal("precondition: building the by-chain entry did not cache a descriptor on it")
	}
	w.pruneByChain("app", 0)
	if _, err := os.Stat(entry); !os.IsNotExist(err) {
		t.Fatalf("precondition: prune kept the entry (%v)", err)
	}
	if live, n := descriptorsAt(t, entry); live || n != 0 {
		t.Fatalf("pruned by-chain entry still has descriptors: live=%v orphans=%d", live, n)
	}
}
