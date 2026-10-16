package ops

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestCreateFromLeavesAWALModeSourceByteIdentical pins docs/reference.md
// "offshoot create" ("the source file itself is never modified or
// truncated") for the case the existing import test misses: a WAL-mode
// source whose committed rows still sit in its -wal file. Closing the last
// read-write connection would checkpoint that WAL into the main file and
// unlink it; the import must open the source read-only so neither file
// changes. Verified against the pre-fix code (main file rewritten, -wal
// gone).
func TestCreateFromLeavesAWALModeSourceByteIdentical(t *testing.T) {
	testutil.RequireSQLite3(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"CREATE TABLE t (v)",
		"INSERT INTO t VALUES (42)",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Snapshot the live pair while the writer is still open, so the copy's
	// committed row lives only in its -wal, exactly like a database whose
	// last writer exited without a clean close.
	live := filepath.Join(dir, "live.db")
	if err := copyFile(src, live); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src+"-wal", live+"-wal"); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	db.Close()
	before, beforeWAL := readFile(t, live), readFile(t, live+"-wal")
	if len(beforeWAL) <= 32 {
		t.Fatalf("precondition: the copied WAL holds no frames (%d bytes)", len(beforeWAL))
	}

	w := newWS(t)
	if err := w.CreateFrom("imp", live); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, live), before) {
		t.Fatal("import rewrote the WAL-mode source's main file")
	}
	afterWAL, err := os.ReadFile(live + "-wal")
	if err != nil {
		t.Fatalf("import removed the source's -wal: %v", err)
	}
	if !bytes.Equal(afterWAL, beforeWAL) {
		t.Fatal("import rewrote the source's -wal")
	}
	path := mustCheckout(t, w, "imp", "main")
	out, err := testutil.SQLite3CLI(path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "42\n" {
		t.Fatalf("imported content must include the row committed in the WAL: %q err=%v", out, err)
	}
}

// TestRefusedCreateMintsNoObject: docs/reference.md "offshoot create"
// says create "refuses if the name exists". A refusal must not leave a
// whole-database snapshot behind in the object store (one full upload per
// typo'd re-run on S3, reclaimable only by gc). Verified against the
// pre-fix code, which uploaded the snapshot before the ref CAS refused.
func TestRefusedCreateMintsNoObject(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	before := dataKeys(t, w)
	if err := w.Create("app"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second create: err = %v, want already exists", err)
	}
	if got := dataKeys(t, w); !reflect.DeepEqual(got, before) {
		t.Fatalf("refused create left objects behind: before %v, after %v", before, got)
	}
	src := filepath.Join(t.TempDir(), "legacy.db")
	if out, err := testutil.SQLite3CLI(src, "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if err := w.CreateFrom("app", src); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create --from over an existing db: err = %v, want already exists", err)
	}
	if got := dataKeys(t, w); !reflect.DeepEqual(got, before) {
		t.Fatalf("refused import left objects behind: before %v, after %v", before, got)
	}
}

// TestForkAtValidatesTheCheckpointName: docs/reference.md "Naming rules"
// are "enforced on every database, branch, and checkpoint name"; fork's
// --at is a checkpoint name and must get the same invalid-name refusal
// instead of an unknown-checkpoint lookup that echoes the raw string.
func TestForkAtValidatesTheCheckpointName(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	_, err := w.Fork("app", "main", "kid", "../x", 0, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid name") {
		t.Fatalf("fork --at ../x: err = %v, want an invalid-name refusal", err)
	}
	if _, _, err := w.Store.GetRef("app", "kid"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a refused fork must not create the branch: %v", err)
	}
}

// listHookBackend runs fn once, right after the first List of exactly
// prefix has been taken, so the caller holds a listing that predates
// whatever fn wrote — the shape of a flush landing on a parent lineage
// between GC's listing of it and GC's read of the parent's own ref.
type listHookBackend struct {
	store.Backend
	prefix string
	once   sync.Once
	fn     func()
}

func (b *listHookBackend) List(prefix string) ([]string, error) {
	keys, err := b.Backend.List(prefix)
	if prefix == b.prefix {
		b.once.Do(b.fn)
	}
	return keys, err
}

// TestGCMarkToleratesACheckpointLandingMidPass: docs/architecture.md says
// the mark phase uses the same store.Chain resolution the read path uses
// and that GC "fails closed: an incomplete mark deletes nothing". Fail-
// closed is right for a real error, but a checkpoint that lands on a
// forked parent while the pass runs is not one: GC resolves the child
// ("kid" sorts before "main") first, which lists and caches the parent's
// lineage, and only then reads the parent's ref — whose head has moved
// past everything in that cached listing. The pass must read every ref
// before it lists any lineage, so a listing is never older than the ref
// it serves. Verified against the pre-fix code, which failed the pass
// with "no contiguous run reaches target".
func TestGCMarkToleratesACheckpointLandingMidPass(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<14)
	mustFork(t, w, "app", "main", "kid", "")
	path := mustCheckout(t, w, "app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(10));")
	mainRef := refOf(t, w, "app", "main")
	other, err := Open(w.Root)
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	w.Store.B = &listHookBackend{Backend: w.Store.B, prefix: store.LineagePrefix(mainRef.Lineage), fn: func() {
		fired = true
		if _, err := other.Checkpoint("app", "main", "mid", nil); err != nil {
			t.Errorf("checkpoint during the pass: %v", err)
		}
	}}
	_, _, err = w.GC(0)
	if !fired {
		t.Fatal("precondition: the pass never listed the parent lineage, so the race was not modelled")
	}
	if err != nil {
		t.Fatalf("gc pass racing a checkpoint on a forked parent must succeed: %v", err)
	}
	if got := refOf(t, w, "app", "main").Checkpoints["mid"].TXID; got <= mainRef.HeadTXID {
		t.Fatalf("precondition: the mid-pass checkpoint did not advance main (%d)", got)
	}
	// The objects that landed mid-pass are live and must survive a
	// following pass too.
	if _, deleted, err := w.GC(0); err != nil || deleted != 0 {
		t.Fatalf("second pass: deleted=%d err=%v, want nothing collected", deleted, err)
	}
	if _, err := w.CheckoutAt("app", "main", "mid", false); err != nil {
		t.Fatalf("the mid-pass checkpoint must still resolve: %v", err)
	}
}

// TestGCMarksLineagesOfClaimedRefs: a ref carrying a Deleting or Reaping
// claim (a destroy or reap that died after claiming) is still a ref; its
// lineage stays in the mark until the claim resolves, so a later sweep
// never collects objects a self-healed branch still reads. Verified
// against a mutation that dropped claimed refs from the mark (two passes
// then swept them).
func TestGCMarksLineagesOfClaimedRefs(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<14)
	mustFork(t, w, "app", "main", "kid", "")
	kpath := mustCheckout(t, w, "app", "kid")
	mustSQL(t, kpath, "INSERT INTO t (v) VALUES (randomblob(10));")
	if _, err := w.Checkpoint("app", "kid", "own", nil); err != nil {
		t.Fatal(err)
	}
	before := dataKeys(t, w)
	for _, claim := range []string{"Deleting", "Reaping"} {
		ref, etag, err := w.Store.GetRef("app", "kid")
		if err != nil {
			t.Fatal(err)
		}
		ref.Deleting, ref.Reaping = claim == "Deleting", claim == "Reaping"
		ref.DeletingAt = nowStamp()
		if _, err := w.Store.PutRef("app", "kid", ref, etag); err != nil {
			t.Fatal(err)
		}
		for pass := 1; pass <= 2; pass++ {
			if _, deleted, err := w.GC(0); err != nil || deleted != 0 {
				t.Fatalf("%s pass %d: deleted=%d err=%v, want nothing collected", claim, pass, deleted, err)
			}
		}
		if got := dataKeys(t, w); !reflect.DeepEqual(got, before) {
			t.Fatalf("%s: GC changed data/ under a claimed ref: before %v, after %v", claim, before, got)
		}
	}
}

// TestCreateRefusesADatabaseWithSurvivingBranches: docs/reference.md
// "offshoot create" refuses an existing database. A database whose main
// was destroyed but whose other branches survive still exists; create
// must not mint a fresh main inside it. Verified against the pre-fix code
// (create succeeded, branches [exp main]).
func TestCreateRefusesADatabaseWithSurvivingBranches(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "exp", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	before := dataKeys(t, w)
	if err := w.Create("app"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create over a database with surviving branches: err = %v, want already exists", err)
	}
	if _, _, err := w.Store.GetRef("app", "main"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refused create minted main: %v", err)
	}
	if got := dataKeys(t, w); !reflect.DeepEqual(got, before) {
		t.Fatalf("refused create left objects behind: before %v, after %v", before, got)
	}
}

// TestGCFailsClosedOnARefWithAnInvalidName: a ref written under an older,
// looser naming rule (or by something else) that ValidateName now
// rejects must stop every listing loudly rather than vanish from it: a
// skipped ref's lineage would be unreachable to the mark and swept while
// the ref still exists. Runs on the fake S3 backend, where such a key can
// exist (the local backend hides .lock files itself). Verified against
// the pre-fix code, which skipped the ref and swept 2 of 2 objects.
func TestGCFailsClosedOnARefWithAnInvalidName(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWSOnFakeS3(t)
	seedDB(t, w, "app", 1<<14)
	mustFork(t, w, "app", "main", "keep", "")
	kpath := mustCheckout(t, w, "app", "keep")
	mustSQL(t, kpath, "INSERT INTO t (v) VALUES (randomblob(10));")
	if _, err := w.Checkpoint("app", "keep", "own", nil); err != nil {
		t.Fatal(err)
	}
	ref := refOf(t, w, "app", "keep")
	own, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil || len(own) == 0 {
		t.Fatalf("precondition: keep has no own objects (%v, %v)", own, err)
	}
	// Rewrite the ref under a name the rule now refuses.
	data, _, err := w.Store.B.Get(store.RefKey("app", "keep"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Store.B.Put("refs/app/keep.lock", data); err != nil {
		t.Fatal(err)
	}
	if err := w.Store.B.Delete(store.RefKey("app", "keep")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.ListRefs(); err == nil || !strings.Contains(err.Error(), "keep.lock") {
		t.Fatalf("ListRefs over an invalid ref name: err = %v, want a refusal naming the key", err)
	}
	for pass := 1; pass <= 2; pass++ {
		if _, deleted, err := w.GC(0); err == nil || deleted != 0 {
			t.Fatalf("pass %d: deleted=%d err=%v, want fail-closed with nothing collected", pass, deleted, err)
		}
	}
	for _, key := range own {
		if ok, err := w.Store.ObjectExists(key); err != nil || !ok {
			t.Fatalf("object %s of the invalidly named ref was collected (exists=%v err=%v)", key, ok, err)
		}
	}
}

// TestCreateFromRefusesAHotRollbackJournal: a rollback-journal source
// copied mid-transaction carries a hot journal. Importing it read-only
// cannot roll that journal back, and importing it read-write would (a
// modification of the source the docs rule out), so the import refuses
// with the recovery step named and leaves both files untouched. Verified
// against the pre-fix code (SQLite's "attempt to write a readonly
// database" with no hint).
func TestCreateFromRefusesAHotRollbackJournal(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"PRAGMA journal_mode=DELETE",
		"PRAGMA cache_size=-8",
		"CREATE TABLE t (v)",
		"INSERT INTO t VALUES (1)",
		"BEGIN",
		"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 2000) INSERT INTO t SELECT randomblob(512) FROM n",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	live := filepath.Join(dir, "live.db")
	if err := copyFile(src, live); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src+"-journal", live+"-journal"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	db.Close()
	before, beforeJournal := readFile(t, live), readFile(t, live+"-journal")
	if len(beforeJournal) < 512 {
		t.Fatalf("precondition: the copied journal is not hot (%d bytes)", len(beforeJournal))
	}
	w := newWS(t)
	err = w.CreateFrom("imp", live)
	if err == nil || !strings.Contains(err.Error(), "hot rollback journal") {
		t.Fatalf("import of a hot-journal source: err = %v, want the hot-journal refusal", err)
	}
	if !bytes.Equal(readFile(t, live), before) || !bytes.Equal(readFile(t, live+"-journal"), beforeJournal) {
		t.Fatal("the refused import modified the source or its journal")
	}
	if _, _, err := w.Store.GetRef("imp", "main"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refused import created the database: %v", err)
	}
}
