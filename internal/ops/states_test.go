package ops

import (
	"database/sql"
	"os"
	"os/exec"
	"testing"
	"time"
)

// requireSQLiteCLI skips the test if the sqlite3 CLI isn't on PATH — every
// test in this file that needs to write real content into a checkout uses
// it, matching the skip pattern already established across ops_test.go
// (e.g. TestForkWarnsOnStaleCheckout).
func requireSQLiteCLI(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
}

// TestBranchStateIdleFreshBranch: a brand-new branch has no lease, no
// checkout — none of active/dirty/detached apply, so it reads idle. This is
// the addition the design spec's original taxonomy lacked (see
// BranchStateAt's doc comment): Plan-2's CLI/at-rest mode has no daemon, so
// a branch with nothing going on still needs a name.
func TestBranchStateIdleFreshBranch(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "idle" {
		t.Fatalf("state = %q, want idle", state)
	}
}

// TestBranchStateActiveWithLiveLease: a live lease on the ref is "active"
// regardless of the daemon/session machinery — ops.BranchState computes
// this from store.LeaseLive alone, the same liveness check
// store.AcquireLease itself uses to decide whether to refuse a rival.
func TestBranchStateActiveWithLiveLease(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "holder-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "active" {
		t.Fatalf("state = %q, want active", state)
	}
}

// TestBranchStateDirtyAfterUncheckpointedEdit: a checkout whose content has
// diverged from its sidecar-recorded fingerprint, with the sidecar's
// recorded IDENTITY (lineage/epoch/txid) still matching the ref, is
// checkoutState's "modified" verdict — un-checkpointed local edits, exactly
// what "dirty" names.
func TestBranchStateDirtyAfterUncheckpointedEdit(t *testing.T) {
	requireSQLiteCLI(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "dirty" {
		t.Fatalf("state = %q, want dirty", state)
	}
}

// TestBranchStateDirtyForCommittedButUncheckpointedWAL pins the CRITICAL
// fix: a committed write can sit in a checkout's WAL, invisible to a bare
// hash of the main file, until something folds it in. BranchStateAt must
// quiesce (checkpoint) before hashing, the same step every other identity/
// hash comparison in this package already takes (checkoutState's own
// callers, CheckoutProven/warnIfUncheckpointed, always quiesce first) — a
// version that skipped it would hash only the stale main-file bytes here
// and misreport "idle".
//
// Reproduction: a SECOND, unleased connection (db2) sets
// wal_autocheckpoint=0, commits a write, and — critically — stays OPEN
// through the BranchState call. SQLite normally runs an implicit
// checkpoint when the LAST connection to a WAL database closes; keeping
// db2 open is what defeats that and isolates the bug this test exists to
// catch from that unrelated close-time behavior.
func TestBranchStateDirtyForCommittedButUncheckpointedWAL(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	db2, err := sql.Open("sqlite3", path+"?_busy_timeout=3000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db2.Close() })
	db2.SetMaxOpenConns(1)
	if _, err := db2.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db2.Exec("CREATE TABLE t (v); INSERT INTO t VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	// db2 is deliberately left open here, not closed — see doc comment.

	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "dirty" {
		t.Fatalf("state = %q, want dirty (a bare hash without quiescing first would wrongly see idle)", state)
	}
}

// TestBranchStateBusyCheckoutIsDirty pins the defined fallback for
// BranchStateAt's own quiesce call reporting busy (errQuiesceBusy): a live
// connection actively using an UNLEASED checkout right now is itself
// treated as "dirty" rather than "idle" — there's a real, current chance
// of un-checkpointed content this check can't directly observe while
// blocked, and idle would understate that.
//
// A TRUNCATE checkpoint only reports busy when a blocked reader's pinned
// snapshot is actually standing between it and completion — pinning a
// reader against an otherwise-idle, nothing-to-checkpoint database (as
// internal/session/stress_test.go's "blocker" pattern does, but ahead of a
// separate writer that goes on to add the backlog the pinned reader then
// blocks the checkpoint from truncating past) does not by itself force
// busy=1. So this test pins the read snapshot FIRST, exactly as that
// pattern does, then commits new content on a separate connection — now
// the pinned reader's snapshot predates that commit, which is what
// actually blocks the TRUNCATE checkpoint quiesce attempts.
func TestBranchStateBusyCheckoutIsDirty(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	blocker, err := sql.Open("sqlite3", path+"?_busy_timeout=3000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blocker.Close() })
	blocker.SetMaxOpenConns(1)
	if _, err := blocker.Exec("BEGIN; SELECT count(*) FROM sqlite_master;"); err != nil {
		t.Fatal(err)
	}

	writer, err := sql.Open("sqlite3", path+"?_busy_timeout=3000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec("CREATE TABLE t (v); INSERT INTO t VALUES (1);"); err != nil {
		t.Fatal(err)
	}

	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "dirty" {
		t.Fatalf("state = %q, want dirty (a busy, unleased checkout is itself evidence of activity)", state)
	}
}

// TestBranchStateDetachedAfterPromoteRefreshFailure manufactures the
// orphan-checkout case Promote itself documents on its own doc comment:
// its post-repoint checkout refresh is best-effort, and a busy checkout at
// the moment of repoint leaves the OLD sidecar (old lineage) in place while
// the ref has already moved to a NEW one. This mirrors
// TestRollbackReportsRepointOnRefreshFailure's trick for forcing that
// refresh to fail (replacing the checkout file with a directory at the
// same path, so quiesce's sql.Open can never succeed), applied to Promote
// specifically per this task's own guidance to "verify by reading
// Promote".
func TestBranchStateDetachedAfterPromoteRefreshFailure(t *testing.T) {
	requireSQLiteCLI(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	// Give "main" (the promote target) an existing checkout + sidecar.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// A source branch with different content to promote onto main.
	if _, err := w.Fork("app", "main", "source", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	srcPath, err := w.Checkout("app", "source")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", srcPath,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "source", "v1", nil); err != nil {
		t.Fatal(err)
	}

	// Force main's post-promote checkout refresh to fail: replace the
	// checkout file with a directory at the same path.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Promote("app", "source", "main", true); err == nil {
		t.Fatal("promote must report the checkout refresh failure")
	}

	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage == before.Lineage {
		t.Fatal("ref must have repointed to a new lineage despite the refresh failure")
	}

	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "detached" {
		t.Fatalf("state = %q, want detached", state)
	}
}

// TestBranchStateIdleForStaleSameLineageCheckout pins the boundary
// BranchStateAt's doc comment calls out explicitly: a checkout whose
// sidecar-recorded LINEAGE still matches the ref, but whose epoch/txid
// lags (the branch advanced within the same lineage — e.g. a daemon
// session's flush — since this checkout was last refreshed), is NOT
// detached. It just needs a re-materialize, so it reads idle, the same
// bucket checkoutState's "stale" verdict falls into here whenever the
// lineage itself didn't change.
func TestBranchStateIdleForStaleSameLineageCheckout(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}
	// Advance the ref within the SAME lineage without touching the
	// checkout or its sidecar — mimics a daemon session flushing a
	// segment past this checkout's txid.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID++
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}
	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "idle" {
		t.Fatalf("state = %q, want idle (same-lineage stale checkout is not detached)", state)
	}
}

// TestBranchStatePrecedenceActiveOverDirty pins the documented precedence:
// active (a live lease) outranks dirty (local un-checkpointed edits) even
// when both conditions hold on the same branch at once.
func TestBranchStatePrecedenceActiveOverDirty(t *testing.T) {
	requireSQLiteCLI(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.AcquireLease("app", "main", "holder-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "active" {
		t.Fatalf("state = %q, want active (must outrank dirty)", state)
	}
}

// TestBranchStateIdleForCorruptSidecar: a checkout exists but its .sum
// sidecar doesn't decode as a valid current-format record — no provenance
// to act on, so this falls to idle rather than guessing dirty or detached
// (mirrors checkoutState's own "unknown" verdict for the same input).
func TestBranchStateIdleForCorruptSidecar(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".sum", []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	state, err := w.BranchState("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if state != "idle" {
		t.Fatalf("state = %q, want idle", state)
	}
}

// TestStatusReportsExactlyOneStateAcrossAMatrixOfBranches is the
// exactly-one-state-per-branch invariant test: a single workspace with one
// branch manufactured into each of the four ops-computable states (active,
// dirty, detached, idle — pending/error are daemon-only, see
// BranchStateAt's doc comment), verified all at once via Status() the way
// an operator would actually see them, not just via isolated single-branch
// BranchState calls.
func TestStatusReportsExactlyOneStateAcrossAMatrixOfBranches(t *testing.T) {
	requireSQLiteCLI(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// main: never checked out, never leased -> idle.

	if _, err := w.Fork("app", "main", "active-branch", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "active-branch", "holder-a", time.Minute); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Fork("app", "main", "dirty-branch", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	dirtyPath, err := w.Checkout("app", "dirty-branch")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", dirtyPath,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	if _, err := w.Fork("app", "main", "detached-branch", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	detachedPath, err := w.Checkout("app", "detached-branch")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "detached-source", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	detSrcPath, err := w.Checkout("app", "detached-source")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", detSrcPath,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "detached-source", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(detachedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(detachedPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Promote("app", "detached-source", "detached-branch", false); err == nil {
		t.Fatal("promote must report the checkout refresh failure")
	}

	sts, err := w.Status()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	valid := map[string]bool{"active": true, "dirty": true, "detached": true, "idle": true}
	for _, s := range sts {
		if !valid[s.State] {
			t.Fatalf("branch %s@%s has invalid state %q", s.DB, s.Branch, s.State)
		}
		got[s.Branch] = s.State
	}
	want := map[string]string{
		"main":            "idle",
		"active-branch":   "active",
		"dirty-branch":    "dirty",
		"detached-branch": "detached",
		"detached-source": "idle",
	}
	for branch, wantState := range want {
		if got[branch] != wantState {
			t.Fatalf("branch %q state = %q, want %q (all: %+v)", branch, got[branch], wantState, got)
		}
	}
}
