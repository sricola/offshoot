package ops

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// --- TableRowCounts / DiffSummary: pure functions over plain SQLite files,
// no Workspace needed ---

func TestTableRowCountsCountsEveryOrdinaryTableExcludingSqliteInternal(t *testing.T) {
	testutil.RequireSQLite3(t)
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
	testutil.RequireSQLite3(t)
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
	testutil.RequireSQLite3(t)
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
	if !users.Comparable || users.Added != 2 || users.Removed != 0 || users.Changed != 0 || users.Status != "changed" {
		t.Fatalf("users = %+v, want comparable added=2 removed=0 changed=0 status=changed", users)
	}

	gone, ok := byTable["gone"]
	if !ok || !gone.LeftExists || gone.RightExists {
		t.Fatalf("gone = %+v, want present left-only", gone)
	}
	if gone.Left != 2 {
		t.Fatalf("gone.Left = %d, want 2", gone.Left)
	}
	if gone.Status != "removed" {
		t.Fatalf("gone.Status = %q, want removed", gone.Status)
	}

	nt, ok := byTable["new_table"]
	if !ok || nt.LeftExists || !nt.RightExists {
		t.Fatalf("new_table = %+v, want present right-only", nt)
	}
	if nt.Right != 4 {
		t.Fatalf("new_table.Right = %d, want 4", nt.Right)
	}
	if nt.Status != "added" {
		t.Fatalf("nt.Status = %q, want added", nt.Status)
	}
}

func TestDiffSummaryAcrossTwoEntirelyDifferentDatabasesIsLegit(t *testing.T) {
	testutil.RequireSQLite3(t)
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

// TestDiffSummaryIsContentAware pins the reason the summary exists for
// agents: two attempts with identical row counts but different VALUES are
// not "same". With a declared primary key, a row whose key is on both sides
// but whose content differs is Changed; a key only on the left is Removed;
// only on the right is Added. Row counts stay reported alongside.
func TestDiffSummaryIsContentAware(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	if out, err := exec.Command("sqlite3", left,
		"CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT);"+
			"INSERT INTO results VALUES (1,1),(2,1),(3,1);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if out, err := exec.Command("sqlite3", right,
		"CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT);"+
			"INSERT INTO results VALUES (1,1),(2,0),(4,1);", // 2 changed, 3 removed, 4 added
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d tables, want 1: %+v", len(got), got)
	}
	r := got[0]
	if !r.Comparable || r.Left != 3 || r.Right != 3 {
		t.Fatalf("results = %+v, want comparable with 3/3 rows", r)
	}
	if r.Added != 1 || r.Removed != 1 || r.Changed != 1 || r.Status != "changed" {
		t.Fatalf("results = %+v, want added=1 removed=1 changed=1 status=changed", r)
	}
	if r.SchemaChanged {
		t.Fatalf("results schema must not be flagged changed: %+v", r)
	}
	if r.Key != "pk" {
		t.Fatalf("results.Key = %q, want %q (declared PK is unique on both sides)", r.Key, "pk")
	}
}

// TestDiffSummaryRowidTablesUseRowidAsKey: a table with no declared primary
// key is keyed by rowid (both sides descend from one seed, so rowids line
// up), and an in-place UPDATE shows as Changed, not Removed+Added.
func TestDiffSummaryRowidTablesUseRowidAsKey(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	exec.Command("sqlite3", left, "CREATE TABLE t (v); INSERT INTO t VALUES ('a'),('b');").Run()
	exec.Command("sqlite3", right, "CREATE TABLE t (v); INSERT INTO t VALUES ('a'),('B');").Run()
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Changed != 1 || got[0].Added != 0 || got[0].Removed != 0 {
		t.Fatalf("t = %+v, want changed=1 only", got[0])
	}
	if got[0].Key != "rowid" {
		t.Fatalf("t.Key = %q, want %q (no declared PK)", got[0].Key, "rowid")
	}
}

// TestDiffSummaryNonUniqueDeclaredPKFallsBackToRowid: SQLite allows a
// declared composite PRIMARY KEY on an ordinary rowid table to contain
// duplicate rows when a key component is NULL (a long-standing legacy
// quirk, not a defect in the caller's schema). Trusting that PK as a row
// identity would undercount via the anti-joins/EXCEPT (both rows collide
// on the same key), so DiffSummary must detect the PK is non-unique and
// fall back to rowid instead: 2 rows on the left, 1 identical-content row
// on the right, must be reported as Removed=1, not "same".
func TestDiffSummaryNonUniqueDeclaredPKFallsBackToRowid(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	if out, err := exec.Command("sqlite3", left,
		"CREATE TABLE t (a, b, v, PRIMARY KEY (a, b));"+
			"INSERT INTO t (a,b,v) VALUES (1,NULL,'x'),(1,NULL,'y');",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if out, err := exec.Command("sqlite3", right,
		"CREATE TABLE t (a, b, v, PRIMARY KEY (a, b));"+
			"INSERT INTO t (a,b,v) VALUES (1,NULL,'x');",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	d := got[0]
	if d.Key != "rowid" {
		t.Fatalf("t.Key = %q, want %q (declared PK is non-unique on the left)", d.Key, "rowid")
	}
	if !d.Comparable || d.Removed != 1 || d.Status != "changed" {
		t.Fatalf("t = %+v, want comparable removed=1 status=changed", d)
	}
}

// TestDiffSummaryKeylessTableWithShadowedRowidNameFallsBackToOid: a keyless
// table whose own column is literally named "rowid" shadows that alias, so
// DiffSummary must fall back further, to "oid" (or "_rowid_"), to reach the
// table's actual internal rowid rather than misreading the user column as
// row identity.
func TestDiffSummaryKeylessTableWithShadowedRowidNameFallsBackToOid(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	if out, err := exec.Command("sqlite3", left,
		`CREATE TABLE t ("rowid", v); INSERT INTO t VALUES ('dup','a'),('dup','b');`,
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if out, err := exec.Command("sqlite3", right,
		`CREATE TABLE t ("rowid", v); INSERT INTO t VALUES ('dup','a');`,
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	d := got[0]
	if d.Key != "rowid" {
		t.Fatalf("t.Key = %q, want %q (the true rowid, reached via oid/_rowid_)", d.Key, "rowid")
	}
	if !d.Comparable || d.Removed != 1 || d.Changed != 0 || d.Status != "changed" {
		t.Fatalf("t = %+v, want comparable removed=1 changed=0 status=changed", d)
	}
}

// TestDiffSummarySchemaMismatchIsReportedNotCompared: when the column list
// differs, row-level counts are impossible to define, so the table is
// reported with row counts, Comparable=false, SchemaChanged=true, and
// Status "changed" — never a SQL error from a malformed EXCEPT.
func TestDiffSummarySchemaMismatchIsReportedNotCompared(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	exec.Command("sqlite3", left, "CREATE TABLE t (a); INSERT INTO t VALUES (1);").Run()
	exec.Command("sqlite3", right, "CREATE TABLE t (a, b); INSERT INTO t VALUES (1, 2);").Run()
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	d := got[0]
	if d.Comparable || !d.SchemaChanged || d.Status != "changed" || d.Left != 1 || d.Right != 1 {
		t.Fatalf("t = %+v, want incomparable schema-changed with 1/1 rows", d)
	}
}

// TestDiffSummaryIdenticalSidesAreSame: same content, same schema → same.
func TestDiffSummaryIdenticalSidesAreSame(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	for _, p := range []string{left, right} {
		exec.Command("sqlite3", p, "CREATE TABLE t (id INTEGER PRIMARY KEY, v); INSERT INTO t VALUES (1,'x');").Run()
	}
	got, err := DiffSummary(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != "same" || got[0].Added+got[0].Removed+got[0].Changed != 0 {
		t.Fatalf("t = %+v, want same", got[0])
	}
}

// TestDiffSummaryOpensBothSidesReadOnlyEvenOn0444Files: the attached right
// side must be opened read-only too (CheckoutAt/Export produce 0444 files).
func TestDiffSummaryOpensBothSidesReadOnlyEvenOn0444Files(t *testing.T) {
	testutil.RequireSQLite3(t)
	left := filepath.Join(t.TempDir(), "left.db")
	right := filepath.Join(t.TempDir(), "right.db")
	for _, p := range []string{left, right} {
		exec.Command("sqlite3", p, "CREATE TABLE t (id INTEGER PRIMARY KEY, v); INSERT INTO t VALUES (1,'x');").Run()
		if err := os.Chmod(p, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := DiffSummary(left, right); err != nil {
		t.Fatalf("read-only sides must be diffable: %v", err)
	}
}

// TestFormatDiffSummaryRendersCountsAndTotals pins the shared renderer the
// CLI and MCP both use.
func TestFormatDiffSummaryRendersCountsAndTotals(t *testing.T) {
	rep := DiffReportOf([]TableDiff{
		{Table: "results", LeftExists: true, RightExists: true, Left: 3, Right: 3, Comparable: true, Added: 1, Removed: 1, Changed: 1, Status: "changed"},
		{Table: "scratch", LeftExists: true, Left: 2, Status: "removed"},
		{Table: "t", LeftExists: true, RightExists: true, Left: 1, Right: 1, Comparable: true, Status: "same"},
	})
	if rep.Totals != (DiffTotals{Same: 1, Changed: 1, Added: 0, Removed: 1}) {
		t.Fatalf("totals = %+v", rep.Totals)
	}
	var b bytes.Buffer
	if err := FormatDiffSummary(&b, rep, "L", "R"); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"TABLE", "ADDED", "REMOVED", "CHANGED", "STATUS", "results", "changed", "scratch", "removed", "3 tables: 1 same, 1 changed, 0 added, 1 removed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

// --- MaterializeForDiff: checkpoint side uses the ro-cache, head side is a
// private, always-fresh export ---

func TestMaterializeForDiffCheckpointSideUsesTheReadOnlyCache(t *testing.T) {
	testutil.RequireSQLite3(t)
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
	testutil.RequireSQLite3(t)
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
