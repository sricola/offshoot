package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// TestDiffCLIRejectsBadTarget pins ParseExportTarget's arity check at the
// diff CLI boundary, mirroring TestExportCLIRejectsBadTarget.
func TestDiffCLIRejectsBadTarget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "diff", "app@br@cp@extra", "app@main"}); err == nil {
		t.Fatal("diff with a 4-component target must be refused")
	}
}

// TestDiffCLIRequiresExactlyTwoTargets pins the arg-count usage check.
func TestDiffCLIRequiresExactlyTwoTargets(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "diff", "app"}); err == nil {
		t.Fatal("diff with only one target must be refused")
	}
	if err := run([]string{"-store", dir, "diff", "app", "app", "app"}); err == nil {
		t.Fatal("diff with three targets must be refused")
	}
}

// seedTwoSidesForDiff builds two forks of `app` with a known, exact
// difference — used by the --summary tests below to assert precise counts
// in the printed output, not just "some output."
//
//   - `users`: 3 rows on the left, 5 on the right (+2, a "changed" table)
//   - `gone`: 2 rows on the left only (a "removed" table, from the right's
//     point of view)
//   - `arrivals`: 4 rows on the right only (an "added" table)
func seedTwoSidesForDiff(t *testing.T, store string) (leftTarget, rightTarget string) {
	t.Helper()
	call(t, store, "init")
	call(t, store, "create", "app")
	leftPath := strings.TrimSpace(call(t, store, "checkout", "app"))
	if out, err := exec.Command("sqlite3", leftPath,
		"CREATE TABLE users (v); INSERT INTO users VALUES (1),(2),(3);"+
			"CREATE TABLE gone (v); INSERT INTO gone VALUES (1),(2);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app", "left")

	call(t, store, "fork", "app", "attempt")
	rightPath := strings.TrimSpace(call(t, store, "checkout", "app@attempt"))
	if out, err := exec.Command("sqlite3", rightPath,
		"DROP TABLE gone;"+
			"INSERT INTO users VALUES (4),(5);"+
			"CREATE TABLE arrivals (v); INSERT INTO arrivals VALUES (1),(2),(3),(4);",
	).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app@attempt", "right")

	return "app@main@left", "app@attempt@right"
}

// TestDiffSummaryCLIReportsExactRowCounts is the load-bearing --summary
// test: it asserts the EXACT counts (not just presence/absence) for a
// changed, an added, and a removed table, plus the trailing totals line.
func TestDiffSummaryCLIReportsExactRowCounts(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	left, right := seedTwoSidesForDiff(t, store)

	out := call(t, store, "diff", left, right, "--summary")

	for _, want := range []string{
		"users", "3", "5", "changed (+2)",
		"gone", "2", "removed",
		"arrivals", "4", "added",
		"3 tables: 0 same, 1 changed, 1 added, 1 removed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("--summary output missing %q; full output:\n%s", want, out)
		}
	}
}

// TestDiffSummaryCLIWorksAcrossTwoDifferentDatabases proves cross-db diff is
// legit (Milestone 3 Task 6): the two targets need not name the same `db`.
func TestDiffSummaryCLIWorksAcrossTwoDifferentDatabases(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")

	call(t, store, "create", "left-db")
	lp := strings.TrimSpace(call(t, store, "checkout", "left-db"))
	exec.Command("sqlite3", lp, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	call(t, store, "checkpoint", "left-db", "v1")

	call(t, store, "create", "right-db")
	rp := strings.TrimSpace(call(t, store, "checkout", "right-db"))
	exec.Command("sqlite3", rp, "CREATE TABLE t (v); INSERT INTO t VALUES (1),(2);").Run()
	call(t, store, "checkpoint", "right-db", "v1")

	out := call(t, store, "diff", "left-db@main@v1", "right-db@main@v1", "--summary")
	if !strings.Contains(out, "changed (+1)") {
		t.Fatalf("cross-db --summary output missing the expected delta; full output:\n%s", out)
	}
}

// TestDiffCLIHeadSideReflectsNewWriteNotStaleCache is the direct CLI-level
// assertion for the staleness decision documented on
// ops.Workspace.MaterializeForDiff: a bare (no-checkpoint) target names the
// branch's HEAD, and running diff --summary again after a new write must
// show the new state — never a cached, stale head from the first call.
func TestDiffCLIHeadSideReflectsNewWriteNotStaleCache(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	call(t, store, "checkpoint", "app", "base")

	first := call(t, store, "diff", "app@main@base", "app@main", "--summary")
	if !strings.Contains(first, "same") {
		t.Fatalf("first diff (head == base) should show no differences; got:\n%s", first)
	}

	if out, err := exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	// No checkpoint here on purpose — MaterializeForDiff's head side reads
	// durable state the same way Export does, so flush the checkout's
	// uncommitted write to the store: `offshoot checkpoint` writes a
	// checkpoint but head diffs read the ref's HeadTXID, which a checkpoint
	// also advances, so checkpoint the new state as "advanced" and diff
	// base against head again.
	call(t, store, "checkpoint", "app", "advanced")

	second := call(t, store, "diff", "app@main@base", "app@main", "--summary")
	if !strings.Contains(second, "changed (+1)") {
		t.Fatalf("second diff (head advanced by 1 row) must reflect the new write, not a stale cache; got:\n%s", second)
	}
}

// TestDiffCLIErrorsClearlyWhenSqldiffMissing forces PATH to a directory with
// no sqldiff (but keeps sqlite3 reachable via an absolute path for the
// test's own setup, done before PATH is cleared) and asserts the default
// (non --summary) diff path names the sqlite3-tools package rather than
// failing with a bare "executable file not found" error.
func TestDiffCLIErrorsClearlyWhenSqldiffMissing(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()
	call(t, store, "checkpoint", "app", "v1")

	t.Setenv("PATH", t.TempDir()) // an empty directory: no sqldiff, no sqlite3
	err := run([]string{"-store", store, "diff", "app@main@v1", "app@main@v1"})
	if err == nil {
		t.Fatal("diff without sqldiff on PATH must fail")
	}
	if !strings.Contains(err.Error(), "sqlite3-tools") && !strings.Contains(err.Error(), "brew install sqldiff") {
		t.Fatalf("error must name an install path for sqldiff, got: %v", err)
	}
	if !strings.Contains(err.Error(), "--summary") {
		t.Fatalf("error must mention --summary as the sqldiff-free alternative, got: %v", err)
	}
}

// TestDiffCLISqldiffPresentStreamsOutput exercises the real sqldiff path —
// skipped when sqldiff isn't installed (probed the same way requireSQLite3
// gates on the sqlite3 CLI elsewhere in this package). CI's ubuntu runner
// installs sqlite3-tools specifically so this test runs there; locally it
// skips cleanly if sqldiff isn't on PATH.
func TestDiffCLISqldiffPresentStreamsOutput(t *testing.T) {
	testutil.RequireSQLdiff(t)
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	call(t, store, "checkpoint", "app", "left")
	exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").Run()
	call(t, store, "checkpoint", "app", "right")

	out := call(t, store, "diff", "app@main@left", "app@main@right")
	if !strings.Contains(out, "INSERT INTO") {
		t.Fatalf("sqldiff output should contain an INSERT statement for the added row, got:\n%s", out)
	}
}

// TestDiffCLIIdenticalSidesProduceNoSqldiffOutput proves the sqldiff path
// really runs (not just "doesn't error") by diffing a checkpoint against
// itself and checking sqldiff prints nothing.
func TestDiffCLIIdenticalSidesProduceNoSqldiffOutput(t *testing.T) {
	testutil.RequireSQLdiff(t)
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	call(t, store, "checkpoint", "app", "v1")

	out := call(t, store, "diff", "app@main@v1", "app@main@v1")
	// The only line runDiff itself ever prints is the left/right header —
	// everything else is sqldiff's own stdout, which must be empty for two
	// identical sides.
	wantHeader := "left:  app@main@v1 right: app@main@v1\n"
	if out != wantHeader {
		t.Fatalf("diffing a checkpoint against itself should produce only the left/right header and no sqldiff output, got:\n%q", out)
	}
}

// TestDiffCLIPrintsLeftRightHeaderInBothModes is the fix for the reviewer's
// IMPORTANT finding: neither mode's output used to record which target was
// which side. Both the default sqldiff mode and --summary must now print a
// "left: ... right: ..." header naming the two raw target strings the
// caller passed, and --summary's own table header row must use those same
// target strings as its two count columns (not bare "LEFT"/"RIGHT") so a
// reader never has to scroll back up to know which count is which side.
func TestDiffCLIPrintsLeftRightHeaderInBothModes(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	left, right := seedTwoSidesForDiff(t, store)

	summaryOut := call(t, store, "diff", left, right, "--summary")
	if !strings.Contains(summaryOut, "left:  "+left+" right: "+right) {
		t.Fatalf("--summary output missing the left/right header naming %q and %q; full output:\n%s", left, right, summaryOut)
	}
	// The table's own column headers must also name the targets, not bare
	// LEFT/RIGHT.
	if !strings.Contains(summaryOut, left) || !strings.Contains(summaryOut, right) {
		t.Fatalf("--summary table headers must use the target strings %q/%q, got:\n%s", left, right, summaryOut)
	}

	testutil.RequireSQLdiff(t)
	sqldiffOut := call(t, store, "diff", left, right)
	if !strings.Contains(sqldiffOut, "left:  "+left+" right: "+right) {
		t.Fatalf("default (sqldiff) mode output missing the left/right header naming %q and %q; full output:\n%s", left, right, sqldiffOut)
	}
}
