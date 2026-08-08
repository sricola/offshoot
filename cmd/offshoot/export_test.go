package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

func sqliteRows(t *testing.T, dbPath string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", dbPath, "SELECT count(*) FROM users;").Output()
	if err != nil {
		t.Fatalf("sqlite3 %s: %v", dbPath, err)
	}
	return strings.TrimSpace(string(out))
}

// TestExportCLIParsesTripleAtFormAndExportsHistoricalCheckpoint pins the
// CLI's "db@branch@checkpoint" target parsing end to end: it must resolve
// to the NAMED checkpoint's content, not the branch's current head.
func TestExportCLIParsesTripleAtFormAndExportsHistoricalCheckpoint(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE users (name); INSERT INTO users VALUES ('ada');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app", "v1")
	if out, err := exec.Command("sqlite3", path, "INSERT INTO users VALUES ('grace');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app", "v2")

	dst := filepath.Join(t.TempDir(), "out.db")
	out := strings.TrimSpace(call(t, store, "export", "app@main@v1", dst))
	if out != dst {
		t.Fatalf("export printed %q, want %q", out, dst)
	}
	if got := sqliteRows(t, dst); got != "1" {
		t.Fatalf("export at v1 rows = %s, want 1 (must not include v2's write)", got)
	}

	// Bare "db@branch" (no checkpoint) exports head.
	dst2 := filepath.Join(t.TempDir(), "out2.db")
	call(t, store, "export", "app@main", dst2)
	if got := sqliteRows(t, dst2); got != "2" {
		t.Fatalf("export at head rows = %s, want 2", got)
	}
}

// TestExportCLIRefusesOverwriteWithoutForce pins the CLI wiring for
// ops.Export's refuse/force semantics.
func TestExportCLIRefusesOverwriteWithoutForce(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	exec.Command("sqlite3", path, "CREATE TABLE users (name);").Run()
	call(t, store, "checkpoint", "app", "v1")

	dst := filepath.Join(t.TempDir(), "out.db")
	if err := run([]string{"-store", store, "export", "app", dst}); err != nil {
		t.Fatalf("first export: %v", err)
	}
	if err := run([]string{"-store", store, "export", "app", dst}); err == nil {
		t.Fatal("export must refuse to overwrite an existing destination without --force")
	}
	if err := run([]string{"-store", store, "export", "app", dst, "--force"}); err != nil {
		t.Fatalf("export --force: %v", err)
	}
}

// TestExportCLIRejectsBadTarget pins ParseExportTarget's arity check at the
// CLI boundary: more than two '@'-separated components is refused.
func TestExportCLIRejectsBadTarget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "export", "app@br@cp@extra", "out.db"}); err == nil {
		t.Fatal("export with a 4-component target must be refused")
	}
}

// TestCheckoutAtCLIRequiresBothFlagsTogether pins that --at and --read-only
// must be given together — neither alone is a valid checkout invocation.
func TestCheckoutAtCLIRequiresBothFlagsTogether(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if err := run([]string{"-store", dir, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "create", "app"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", dir, "checkout", "app", "--at", "v1"}); err == nil {
		t.Fatal("checkout --at without --read-only must be refused")
	}
	if err := run([]string{"-store", dir, "checkout", "app", "--read-only"}); err == nil {
		t.Fatal("checkout --read-only without --at must be refused")
	}
	if err := run([]string{"-store", dir, "checkout", "app", "--force"}); err == nil {
		t.Fatal("checkout --force without --at --read-only must be refused")
	}
}

// TestCheckoutAtCLIMaterializesSeparateReadOnlyPath exercises the full
// `checkout --at <checkpoint> --read-only [--force]` CLI path.
func TestCheckoutAtCLIMaterializesSeparateReadOnlyPath(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE users (name); INSERT INTO users VALUES ('ada');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app", "v1")

	roPath := strings.TrimSpace(call(t, store, "checkout", "app", "--at", "v1", "--read-only"))
	if roPath == path {
		t.Fatal("the read-only checkout path must differ from the writable checkout path")
	}
	if got := sqliteRows(t, roPath); got != "1" {
		t.Fatalf("read-only checkout rows = %s, want 1", got)
	}
	fi, err := os.Stat(roPath)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o444 {
		t.Fatalf("read-only checkout perm = %o, want 0444", perm)
	}

	// A second call without --force is a cache hit: same path, no error.
	again := strings.TrimSpace(call(t, store, "checkout", "app", "--at", "v1", "--read-only"))
	if again != roPath {
		t.Fatalf("repeat checkout --at --read-only path = %q, want %q", again, roPath)
	}

	// --force re-materializes without error.
	if err := run([]string{"-store", store, "checkout", "app", "--at", "v1", "--read-only", "--force"}); err != nil {
		t.Fatalf("checkout --at --read-only --force: %v", err)
	}
}

// TestCheckoutAtCLIRejectsPathTraversalCheckpoint pins the CRITICAL fix at
// the CLI boundary: a --at value crafted to escape checkouts-ro (e.g. onto
// the branch's own writable checkout path) must be refused, not served as
// a "cache hit".
func TestCheckoutAtCLIRejectsPathTraversalCheckpoint(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE users (name); INSERT INTO users VALUES ('writable');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	for _, cp := range []string{"../../../etc/passwd", "..", "a/b", "../../checkouts/app/main"} {
		if err := run([]string{"-store", store, "checkout", "app", "--at", cp, "--read-only"}); err == nil {
			t.Fatalf("checkout --at %q --read-only must be refused (path traversal)", cp)
		}
	}

	// The writable checkout must be untouched: still writable, same content.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o200 == 0 {
		t.Fatal("the writable checkout must still be writable")
	}
	if got := sqliteRows(t, path); got != "1" {
		t.Fatalf("writable checkout rows = %s, want 1", got)
	}
}
