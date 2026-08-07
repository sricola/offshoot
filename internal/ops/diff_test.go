package ops

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// --- TableRowCounts / DiffSummary: pure functions over plain SQLite files,
// no Workspace needed ---

func TestTableRowCountsCountsEveryOrdinaryTableExcludingSqliteInternal(t *testing.T) {
	requireSQLite3(t)
	path := filepath.Join(t.TempDir(), "x.db")
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE a (v); INSERT INTO a VALUES (1),(2),(3);"+
			"CREATE TABLE b (v);"+
			"CREATE TABLE c (v INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO c VALUES (1);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	counts, err := TableRowCounts(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"a": 3, "b": 0, "c": 1}
	if len(counts) != len(want) {
		t.Fatalf("counts = %v, want %v (sqlite_sequence must be excluded)", counts, want)
	}
	for k, v := range want {
		if counts[k] != v {
			t.Fatalf("counts[%q] = %d, want %d (full: %v)", k, counts[k], v, counts)
		}
	}
	if _, ok := counts["sqlite_sequence"]; ok {
		t.Fatalf("counts must not include sqlite_sequence, got %v", counts)
	}
}

func TestTableRowCountsOpensReadOnlyEvenOnA0444File(t *testing.T) {
	requireSQLite3(t)
	path := filepath.Join(t.TempDir(), "x.db")
	if out, err := exec.Command("sqlite3", path, "CREATE TABLE t (v);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := TableRowCounts(path); err != nil {
		t.Fatalf("TableRowCounts on a 0444 file: %v", err)
	}
}

func TestDiffSummaryReportsAddedRemovedAndChangedTables(t *testing.T) {
	requireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")

	// left: users(3 rows), gone(2 rows)
	if out, err := exec.Command("sqlite3", left,
		"CREATE TABLE users (v); INSERT INTO users VALUES (1),(2),(3);"+
			"CREATE TABLE gone (v); INSERT INTO gone VALUES (1),(2);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	// right: users(5 rows, +2), new_table(4 rows) — no `gone`.
	if out, err := exec.Command("sqlite3", right,
		"CREATE TABLE users (v); INSERT INTO users VALUES (1),(2),(3),(4),(5);"+
			"CREATE TABLE new_table (v); INSERT INTO new_table VALUES (1),(2),(3),(4);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	byTable := map[string]TableDiff{}
	for _, d := range got {
		byTable[d.Table] = d
	}
	if len(byTable) != 3 {
		t.Fatalf("got %d tables, want 3 (users, gone, new_table): %+v", len(byTable), got)
	}

	users, ok := byTable["users"]
	if !ok || !users.LeftExists || !users.RightExists {
		t.Fatalf("users = %+v, want present both sides", users)
	}
	if users.Left != 3 || users.Right != 5 || users.Delta() != 2 {
		t.Fatalf("users left=%d right=%d delta=%d, want 3/5/2", users.Left, users.Right, users.Delta())
	}

	gone, ok := byTable["gone"]
	if !ok || !gone.LeftExists || gone.RightExists {
		t.Fatalf("gone = %+v, want present left-only", gone)
	}
	if gone.Left != 2 {
		t.Fatalf("gone.Left = %d, want 2", gone.Left)
	}

	nt, ok := byTable["new_table"]
	if !ok || nt.LeftExists || !nt.RightExists {
		t.Fatalf("new_table = %+v, want present right-only", nt)
	}
	if nt.Right != 4 {
		t.Fatalf("new_table.Right = %d, want 4", nt.Right)
	}
}

func TestDiffSummaryAcrossTwoEntirelyDifferentDatabasesIsLegit(t *testing.T) {
	requireSQLite3(t)
	a := filepath.Join(t.TempDir(), "a.db")
	b := filepath.Join(t.TempDir(), "b.db")
	exec.Command("sqlite3", a, "CREATE TABLE only_a (v); INSERT INTO only_a VALUES (1);").Run()
	exec.Command("sqlite3", b, "CREATE TABLE only_b (v);").Run()

	got, err := DiffSummary(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d tables, want 2: %+v", len(got), got)
	}
}

// --- MaterializeForDiff: checkpoint side uses the ro-cache, head side is a
// private, always-fresh export ---

func TestMaterializeForDiffCheckpointSideUsesTheReadOnlyCache(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}

	side, err := w.MaterializeForDiff("app", "main", "v1")
	if err != nil {
		t.Fatal(err)
	}
	defer side.Close()

	if side.Path != w.CheckoutAtPath("app", "main", "v1") {
		t.Fatalf("checkpoint-side path = %q, want the ro-cache path %q", side.Path, w.CheckoutAtPath("app", "main", "v1"))
	}
	fi, err := os.Stat(side.Path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o444 {
		t.Fatalf("checkpoint-side perm = %o, want 0444 (the ro-cache convention)", perm)
	}

	// Close on a checkpoint side must NOT remove the ro-cache file — it's
	// meant to persist and be reused, exactly like every other CheckoutAt
	// caller gets.
	if err := side.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(side.Path); err != nil {
		t.Fatalf("Close on a checkpoint side must leave the ro-cache file in place, got stat err=%v", err)
	}
}

// TestMaterializeForDiffHeadSideAlwaysReflectsANewWrite is the direct test
// for this task's staleness decision (documented in MaterializeForDiff's
// doc comment): a head side is exported fresh on every call, so a write
// made between two diff calls MUST be visible in the second one — nothing
// about it is ever served from a stale cache.
func TestMaterializeForDiffHeadSideAlwaysReflectsANewWrite(t *testing.T) {
	requireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}

	first, err := w.MaterializeForDiff("app", "main", "")
	if err != nil {
		t.Fatal(err)
	}
	firstCounts, err := TableRowCounts(first.Path)
	if err != nil {
		t.Fatal(err)
	}
	if firstCounts["t"] != 1 {
		t.Fatalf("first head materialization: t has %d rows, want 1", firstCounts["t"])
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first.Path); !os.IsNotExist(err) {
		t.Fatalf("Close on a head side must remove its private temp file, stat err=%v", err)
	}

	// Advance head with a new write + checkpoint (so the durable head really
	// moves — MaterializeForDiff's head side reads durable state, matching
	// Export's own semantics).
	exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").Run()
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}

	second, err := w.MaterializeForDiff("app", "main", "")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondCounts, err := TableRowCounts(second.Path)
	if err != nil {
		t.Fatal(err)
	}
	if secondCounts["t"] != 2 {
		t.Fatalf("second head materialization: t has %d rows, want 2 (must not be a stale cache of the first call)", secondCounts["t"])
	}
}

func TestMaterializeForDiffErrorsOnMissingCheckpoint(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.MaterializeForDiff("app", "main", "nope"); err == nil {
		t.Fatal("MaterializeForDiff at a nonexistent checkpoint must error")
	}
}

func TestMaterializeForDiffErrorsOnMissingBranch(t *testing.T) {
	w := newWS(t)
	if _, err := w.MaterializeForDiff("nope", "main", ""); err == nil {
		t.Fatal("MaterializeForDiff on a nonexistent db@branch must error")
	}
}

func TestDiffSideCloseIsSafeOnZeroValueAndTwiceInARow(t *testing.T) {
	var s DiffSide
	if err := s.Close(); err != nil {
		t.Fatalf("Close on a zero DiffSide must be a no-op, got %v", err)
	}

	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	side, err := w.MaterializeForDiff("app", "main", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := side.Close(); err != nil {
		t.Fatal(err)
	}
	if err := side.Close(); err != nil {
		t.Fatalf("a second Close must not error, got %v", err)
	}
}
