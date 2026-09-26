package main

import (
	"errors"
	"fmt"
	"io"
	"runtime"

	"github.com/sricola/offshoot/internal/ops"
)

// sqldiffNotFoundError is runDiff's error when the `sqldiff` binary isn't on
// PATH — sqldiff ships as a SEPARATE binary from the `sqlite3` CLI on most
// distributions (this codebase already depends on the plain `sqlite3` CLI
// being present for its own test suite — see requireSQLite3ForCLI — but that
// binary alone does not include sqldiff). The per-OS hint below is verified,
// not guessed:
//
//   - Debian/Ubuntu: confirmed against the actual `ubuntu-latest` GitHub
//     Actions image (Ubuntu 24.04) that `apt-get install sqlite3` alone does
//     NOT put sqldiff on PATH, and that `sqlite3-tools` is the separate
//     package (`apt-cache show sqlite3-tools` on that image lists
//     `/usr/bin/sqldiff` in its contents) that does — this is also what
//     .github/workflows/ci.yml now installs for the `offshoot diff` tests
//     that need a real sqldiff.
//   - macOS: verified against a real Homebrew install on this machine that
//     the general `sqlite` formula (keg-only, 13 files) does NOT include
//     sqldiff — only plain sqlite3. Homebrew ships sqldiff as its OWN
//     separate formula, confirmed by installing it directly:
//     `brew install sqldiff` puts a working `/opt/homebrew/bin/sqldiff` (or
//     the Intel-prefix equivalent) straight on PATH, no keg-only PATH
//     surgery needed, unlike the `sqlite` formula.
func sqldiffNotFoundError() error {
	hint := "sudo apt-get install sqlite3-tools   # Debian/Ubuntu: sqldiff ships in this separate package"
	if runtime.GOOS == "darwin" {
		hint = "brew install sqldiff   # macOS: sqldiff is its own Homebrew formula, not part of `sqlite`"
	}
	return fmt.Errorf(`offshoot diff: sqldiff not found on PATH

sqldiff ships separately from the sqlite3 CLI. Install it:
  %s

Or skip sqldiff entirely with 'offshoot diff ... --summary' for a
content-aware per-table summary (added/removed/changed rows) instead`, hint)
}

// runDiff implements `offshoot diff <db>@<branch>[@checkpoint]
// <db>@<branch>[@checkpoint] [--summary] [--table T]`: materializes both
// sides read-only (ops.Workspace.MaterializeForDiff — the read-only
// checkout-at cache for a named checkpoint, a private fresh export for
// head; see that function's doc comment for the staleness reasoning) and
// either runs sqldiff over the two materialized paths (default) or prints
// the shared content-aware per-table summary (--summary, no sqldiff
// dependency at all). table, when non-empty, restricts either mode to that
// one table.
//
// The two sides may name the same db or two entirely different ones —
// cross-db diff is a legitimate eval-comparison shape (Milestone 3 Task 6)
// and nothing here assumes otherwise.
func runDiff(w *ops.Workspace, out io.Writer, leftTarget, rightTarget string, summary bool, table string) error {
	ldb, lbranch, lcp, err := ops.ParseExportTarget(leftTarget)
	if err != nil {
		return err
	}
	rdb, rbranch, rcp, err := ops.ParseExportTarget(rightTarget)
	if err != nil {
		return err
	}

	left, err := w.MaterializeForDiff(ldb, lbranch, lcp)
	if err != nil {
		return fmt.Errorf("offshoot diff: materializing %s: %w", leftTarget, err)
	}
	defer left.Close()

	right, err := w.MaterializeForDiff(rdb, rbranch, rcp)
	if err != nil {
		return fmt.Errorf("offshoot diff: materializing %s: %w", rightTarget, err)
	}
	defer right.Close()

	// Print which target is which side BEFORE either mode's own output —
	// this is the only place that ever prints the raw target strings the
	// caller typed, so it's the only trust anchor tying "left"/"right" (or
	// a --summary table's column headers, or an un-labeled sqldiff
	// transcript) back to an actual db@branch[@checkpoint]. Both modes get
	// it: sqldiff's own output never mentions which file was DB1 vs DB2 by
	// name, and --summary's LEFT/RIGHT columns are meaningless without it.
	fmt.Fprintf(out, "left:  %s right: %s\n", leftTarget, rightTarget)

	if summary {
		rows, err := ops.DiffSummary(left.Path, right.Path)
		if err != nil {
			return fmt.Errorf("offshoot diff --summary: %w", err)
		}
		if table != "" {
			filtered := rows[:0]
			for _, d := range rows {
				if d.Table == table {
					filtered = append(filtered, d)
				}
			}
			if len(filtered) == 0 {
				return fmt.Errorf("offshoot diff --summary: no table %q on either side", table)
			}
			rows = filtered
		}
		return ops.FormatDiffSummary(out, ops.DiffReportOf(rows), leftTarget, rightTarget)
	}

	if err := ops.Sqldiff(left.Path, right.Path, table, out); err != nil {
		if errors.Is(err, ops.ErrSqldiffMissing) {
			return sqldiffNotFoundError()
		}
		// ops.Sqldiff already prefixes its errors with "ops: diff: ..."; an
		// extra "offshoot diff: " wrap here would double up (e.g.
		// "offshoot diff: ops: diff: sqldiff: ..."), so return it as-is.
		return err
	}
	return nil
}
