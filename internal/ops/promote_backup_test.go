package ops

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// seedPromotePair builds app@main with one row checkpointed as v1 and an
// attempt-1 fork carrying a second row, ready to promote.
func seedPromotePair(t *testing.T) *Workspace {
	t.Helper()
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "attempt-1", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ap, err := w.Checkout("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", ap, "INSERT INTO t VALUES (99);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "attempt-1", "winner", nil); err != nil {
		t.Fatal(err)
	}
	return w
}

func branchRows(t *testing.T, w *Workspace, db, branch string) string {
	t.Helper()
	p, err := w.Checkout(db, branch)
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

// TestPromoteKeepsSafetyFork: promote's repoint is the one verb whose
// inverse the user otherwise has to build by hand (fork the target first).
// By default promote now builds it: the target's pre-promote head survives
// as a shared fork named <target>-pre-promote, TTL'd, marked as promote's
// own, and one promote away from undoing the repoint.
func TestPromoteKeepsSafetyFork(t *testing.T) {
	w := seedPromotePair(t)
	res, err := w.PromoteWith("app", "attempt-1", "main", PromoteOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "main"+PromoteBackupSuffix {
		t.Fatalf("Backup = %q, want %q", res.Backup, "main"+PromoteBackupSuffix)
	}
	if got := branchRows(t, w, "app", "main"); got != "2" {
		t.Fatalf("promoted main rows = %s, want 2", got)
	}
	if got := branchRows(t, w, "app", res.Backup); got != "1" {
		t.Fatalf("safety fork rows = %s, want the pre-promote 1", got)
	}
	ref, _, err := w.Store.GetRef("app", res.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Meta[PromoteBackupMetaKey] != "main" {
		t.Fatalf("safety fork must carry %s=main, got meta %v", PromoteBackupMetaKey, ref.Meta)
	}
	if ref.TTL != DefaultPromoteBackupTTL.String() {
		t.Fatalf("safety fork TTL = %q, want the default %s", ref.TTL, DefaultPromoteBackupTTL)
	}
	if ref.Base == nil {
		t.Fatal("safety fork must be a shared fork (base pointer), not a materialized copy")
	}
	if ref.Protected {
		t.Fatal("safety fork must not inherit the target's protected flag")
	}
	// The safety fork must still see main's OLD checkpoints through its
	// fork point: v1 lived on main's abandoned lineage, and rollback to it
	// is exactly what the backup exists to make possible.
	if _, err := w.Fork("app", res.Backup, "restore-v1", "fork", 0, nil); err != nil {
		t.Fatalf("fork the safety fork at its fork point: %v", err)
	}

	// Undo: promote the safety fork back onto main. The source IS the
	// target's safety fork, so promote skips minting a new one (it would
	// have to replace the very branch being promoted) and says so.
	undo, err := w.PromoteWith("app", res.Backup, "main", PromoteOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if undo.Backup != "" {
		t.Fatalf("undo-promote must not mint a safety fork, got %q", undo.Backup)
	}
	if got := branchRows(t, w, "app", "main"); got != "1" {
		t.Fatalf("after undo main rows = %s, want 1", got)
	}
	if _, _, err := w.Store.GetRef("app", res.Backup); err != nil {
		t.Fatalf("the safety fork must survive being promoted from: %v", err)
	}
}

// TestPromoteReplacesItsOwnSafetyFork: a second promote onto the same target
// replaces the previous safety fork (one rolling undo point per target, so
// the branch namespace and the pinned old-lineage storage stay bounded).
func TestPromoteReplacesItsOwnSafetyFork(t *testing.T) {
	w := seedPromotePair(t)
	if _, err := w.PromoteWith("app", "attempt-1", "main", PromoteOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	first, _, err := w.Store.GetRef("app", "main"+PromoteBackupSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "attempt-2", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ap, _ := w.Checkout("app", "attempt-2")
	if out, err := exec.Command("sqlite3", ap, "INSERT INTO t VALUES (100);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "attempt-2", "winner", nil); err != nil {
		t.Fatal(err)
	}
	res, err := w.PromoteWith("app", "attempt-2", "main", PromoteOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := w.Store.GetRef("app", res.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lineage == first.Lineage {
		t.Fatal("second promote must replace the safety fork, not keep the first")
	}
	if got := branchRows(t, w, "app", res.Backup); got != "2" {
		t.Fatalf("replaced safety fork rows = %s, want main's pre-second-promote 2", got)
	}
	if got := branchRows(t, w, "app", "main"); got != "3" {
		t.Fatalf("main rows = %s, want 3", got)
	}
}

// TestPromoteRefusesToReplaceAForeignBranch: the safety-fork name is only
// ever replaced when the branch there is promote's own (marker meta). A
// user's branch that happens to carry the name is never destroyed — the
// promote refuses, and the target is untouched.
func TestPromoteRefusesToReplaceAForeignBranch(t *testing.T) {
	w := seedPromotePair(t)
	if _, err := w.Fork("app", "main", "main"+PromoteBackupSuffix, "", 0, nil); err != nil {
		t.Fatal(err)
	}
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.PromoteWith("app", "attempt-1", "main", PromoteOptions{Force: true})
	if err == nil || !strings.Contains(err.Error(), "not a promote safety fork") {
		t.Fatalf("promote must refuse to replace a foreign branch, got err=%v", err)
	}
	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage != before.Lineage || after.HeadTXID != before.HeadTXID {
		t.Fatalf("target must be untouched after a refused promote: before=%+v after=%+v", before, after)
	}
	// Opting out sidesteps the collision entirely.
	res, err := w.PromoteWith("app", "attempt-1", "main", PromoteOptions{Force: true, NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "" {
		t.Fatalf("NoBackup must report no safety fork, got %q", res.Backup)
	}
}

// TestPromoteNoBackupAndTTL: opting out leaves no safety fork behind, and
// an explicit BackupTTL is honored verbatim.
func TestPromoteNoBackupAndTTL(t *testing.T) {
	w := seedPromotePair(t)
	res, err := w.PromoteWith("app", "attempt-1", "main", PromoteOptions{Force: true, NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "" {
		t.Fatalf("NoBackup: Backup = %q", res.Backup)
	}
	if _, _, err := w.Store.GetRef("app", "main"+PromoteBackupSuffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("NoBackup must leave no safety fork, got err=%v", err)
	}
	// Promote back with a custom TTL to exercise the option.
	res, err = w.PromoteWith("app", "attempt-1", "main", PromoteOptions{Force: true, BackupTTL: 2 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", res.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if ref.TTL != (2 * time.Hour).String() {
		t.Fatalf("safety fork TTL = %q, want 2h0m0s", ref.TTL)
	}
}

// TestPromoteCompatWrapperBacksUp: the four-argument Promote is the
// PromoteWith default — safety fork on.
func TestPromoteCompatWrapperBacksUp(t *testing.T) {
	w := seedPromotePair(t)
	if _, err := w.Promote("app", "attempt-1", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.GetRef("app", "main"+PromoteBackupSuffix); err != nil {
		t.Fatalf("Promote must mint the safety fork by default: %v", err)
	}
}
