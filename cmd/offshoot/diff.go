package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"text/tabwriter"

	"github.com/offshoot-db/offshoot/internal/ops"
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
table-level row-count comparison instead`, hint)
}

// runDiff implements `offshoot diff <db>@<branch>[@checkpoint]
// <db>@<branch>[@checkpoint] [--summary]`: materializes both sides
// read-only (ops.Workspace.MaterializeForDiff — the read-only checkout-at
// cache for a named checkpoint, a private fresh export for head; see that
// function's doc comment for the staleness reasoning) and either runs
// sqldiff over the two materialized paths (default) or prints a
// stdlib-only table-level row-count summary (--summary, no sqldiff
// dependency at all).
//
// The two sides may name the same db or two entirely different ones —
// cross-db diff is a legitimate eval-comparison shape (Milestone 3 Task 6)
// and nothing here assumes otherwise.
func runDiff(w *ops.Workspace, out io.Writer, leftTarget, rightTarget string, summary bool) error {
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

	if summary {
		return printDiffSummary(out, left.Path, right.Path)
	}

	if _, err := exec.LookPath("sqldiff"); err != nil {
		return sqldiffNotFoundError()
	}
	cmd := exec.Command("sqldiff", left.Path, right.Path)
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("offshoot diff: sqldiff: %w", err)
	}
	return nil
}

// printDiffSummary renders ops.DiffSummary's per-table row-count diff as an
// aligned table (text/tabwriter — stdlib, no new dependency), one line per
// table: its name, each side's row count ("-" when the table doesn't exist
// on that side), and a STATUS column spelling out exactly what changed —
// "added"/"removed" for a table only on one side, "changed (+N)"/
// "changed (-N)" for a differing row count, "same" otherwise. A trailing
// summary line gives the totals so a caller doesn't have to count rows of
// output to answer "did anything change at all."
func printDiffSummary(out io.Writer, leftPath, rightPath string) error {
	rows, err := ops.DiffSummary(leftPath, rightPath)
	if err != nil {
		return fmt.Errorf("offshoot diff --summary: %w", err)
	}

	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TABLE\tLEFT\tRIGHT\tSTATUS")

	var added, removed, changed, same int
	for _, d := range rows {
		leftCol, rightCol, status := "-", "-", ""
		switch {
		case !d.LeftExists:
			rightCol = fmt.Sprintf("%d", d.Right)
			status = "added"
			added++
		case !d.RightExists:
			leftCol = fmt.Sprintf("%d", d.Left)
			status = "removed"
			removed++
		case d.Delta() != 0:
			leftCol = fmt.Sprintf("%d", d.Left)
			rightCol = fmt.Sprintf("%d", d.Right)
			status = fmt.Sprintf("changed (%+d)", d.Delta())
			changed++
		default:
			leftCol = fmt.Sprintf("%d", d.Left)
			rightCol = fmt.Sprintf("%d", d.Right)
			status = "same"
			same++
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", d.Table, leftCol, rightCol, status)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(out, "%d tables: %d same, %d changed, %d added, %d removed\n",
		len(rows), same, changed, added, removed)
	return nil
}
