// Package testutil holds cross-package test helpers. It is a normal (non
// _test.go) Go package so that _test.go files in any package can import it;
// nothing in it is compiled into production binaries because only test files
// import it.
package testutil

import (
	"os"
	"os/exec"
	"testing"
)

// RequireExec skips the calling test when the named external binary is not
// on PATH — EXCEPT under CI (env CI non-empty; GitHub Actions sets CI=true
// automatically), where a missing binary is a hard failure instead. Without
// the CI teeth, a CI image that silently loses the binary would skip the
// bulk of the integration suite and the run would still go green — the
// exact hole this helper exists to close (audit §4.1). Only call this for
// binaries the CI jobs that run `go test ./...` explicitly install and
// verify (today: sqlite3 and sqldiff — see .github/workflows/ci.yml's
// "Install sqlite3 CLI" steps); for a binary CI installs only in a
// dedicated job, see RequirePromtool for the pattern to copy.
func RequireExec(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s not on PATH but CI is set: refusing to skip integration coverage", name)
		}
		t.Skipf("%s not on PATH", name)
	}
}

// RequireSQLite3 gates a test on the sqlite3 CLI (fail-under-CI,
// skip-locally — see RequireExec).
func RequireSQLite3(t *testing.T) { t.Helper(); RequireExec(t, "sqlite3") }

// RequireSQLdiff gates a test on the sqldiff CLI (Ubuntu: sqlite3-tools;
// macOS: brew's sqldiff formula). Fail-under-CI, skip-locally — see
// RequireExec.
func RequireSQLdiff(t *testing.T) { t.Helper(); RequireExec(t, "sqldiff") }

// RequirePromtool skips the calling test when promtool is not on PATH.
// Deliberately NOT fail-under-bare-CI like RequireExec: ci.yml's main
// `test` job runs `go test ./...` WITHOUT installing promtool — by design,
// the promtool gate lives in the separate metrics-lint job, which downloads
// a pinned promtool and then runs only the TestPromtoolCheck* tests. That
// job sets OFFSHOOT_REQUIRE_PROMTOOL=1, which arms the hard-failure path
// here, so deleting the install step (or a botched download) can never turn
// the metrics gate into a silent green skip; every other job keeps its
// loud, documented skip.
func RequirePromtool(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("promtool"); err != nil {
		if os.Getenv("OFFSHOOT_REQUIRE_PROMTOOL") != "" {
			t.Fatalf("promtool not on PATH but OFFSHOOT_REQUIRE_PROMTOOL is set: refusing to skip the metrics gate")
		}
		t.Skip("SKIPPING: promtool not found on PATH — install it from " +
			"https://github.com/prometheus/prometheus/releases to run this exposition-format " +
			"check locally; CI installs it explicitly (ci.yml's metrics-lint job).")
	}
}
