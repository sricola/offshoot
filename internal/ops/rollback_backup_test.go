package ops

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
)

// TestRollbackKeepsSafetyFork: rollback main to v1 after a second
// checkpoint keeps the pre-rollback head as a TTL'd shared safety fork
// (mirroring promote's <target>-pre-promote), and that fork can itself be
// promoted back to undo the rollback.
func TestRollbackKeepsSafetyFork(t *testing.T) {
	w := seedPromotePair(t)
	// seedPromotePair leaves app@main at v1 (1 row); add a second checkpoint
	// on main so rollback has somewhere to roll back FROM.
	mp, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	if got := branchRows(t, w, "app", "main"); got != "2" {
		t.Fatalf("main rows before rollback = %s, want 2", got)
	}

	res, err := w.RollbackWith("app", "main", "v1", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "main"+RollbackBackupSuffix {
		t.Fatalf("Backup = %q, want %q", res.Backup, "main"+RollbackBackupSuffix)
	}
	if got := branchRows(t, w, "app", "main"); got != "1" {
		t.Fatalf("main rows after rollback = %s, want 1", got)
	}
	if got := branchRows(t, w, "app", res.Backup); got != "2" {
		t.Fatalf("safety fork rows = %s, want the pre-rollback 2", got)
	}
	ref, _, err := w.Store.GetRef("app", res.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Meta[RollbackBackupMetaKey] != "main" {
		t.Fatalf("safety fork must carry %s=main, got meta %v", RollbackBackupMetaKey, ref.Meta)
	}
	if ref.TTL != DefaultPromoteBackupTTL.String() {
		t.Fatalf("safety fork TTL = %q, want the default %s", ref.TTL, DefaultPromoteBackupTTL)
	}
	if ref.Base == nil {
		t.Fatal("safety fork must be a shared fork (base pointer), not a materialized copy")
	}

	// Undo: promote the safety fork back onto main.
	undo, err := w.PromoteWith("app", res.Backup, "main", PromoteOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := branchRows(t, w, "app", "main"); got != "2" {
		t.Fatalf("after undo main rows = %s, want 2", got)
	}
	_ = undo
}

// TestRollbackReplacesItsOwnSafetyFork: a second rollback of the same
// branch replaces the first safety fork (different lineage), keeping the
// branch namespace and pinned storage bounded.
func TestRollbackReplacesItsOwnSafetyFork(t *testing.T) {
	w := seedPromotePair(t)
	mp, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (3);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v3", nil); err != nil {
		t.Fatal(err)
	}

	res1, err := w.RollbackWith("app", "main", "v2", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := w.Store.GetRef("app", res1.Backup)
	if err != nil {
		t.Fatal(err)
	}

	// main is now at v2 (2 rows) with v1 and v2 checkpoints kept. Roll back
	// again to v1.
	res2, err := w.RollbackWith("app", "main", "v1", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Backup != res1.Backup {
		t.Fatalf("Backup = %q, want the same safety-fork name %q", res2.Backup, res1.Backup)
	}
	second, _, err := w.Store.GetRef("app", res2.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lineage == first.Lineage {
		t.Fatal("second rollback must replace the safety fork, not keep the first")
	}
	if got := branchRows(t, w, "app", res2.Backup); got != "2" {
		t.Fatalf("replaced safety fork rows = %s, want main's pre-second-rollback 2", got)
	}
	if got := branchRows(t, w, "app", "main"); got != "1" {
		t.Fatalf("main rows = %s, want 1", got)
	}
}

// TestRollbackRefusesToReplaceAForeignBranch: a user's branch that happens
// to carry the safety-fork name (no marker meta) is never destroyed —
// rollback refuses, and the branch is untouched. NoBackup sidesteps.
func TestRollbackRefusesToReplaceAForeignBranch(t *testing.T) {
	w := seedPromotePair(t)
	mp, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "main"+RollbackBackupSuffix, "", 0, nil); err != nil {
		t.Fatal(err)
	}
	before, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.RollbackWith("app", "main", "v1", RollbackOptions{})
	if err == nil || !strings.Contains(err.Error(), "not a rollback safety fork") {
		t.Fatalf("rollback must refuse to replace a foreign branch, got err=%v", err)
	}
	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage != before.Lineage || after.HeadTXID != before.HeadTXID {
		t.Fatalf("branch must be untouched after a refused rollback: before=%+v after=%+v", before, after)
	}
	// Opting out sidesteps the collision entirely.
	res, err := w.RollbackWith("app", "main", "v1", RollbackOptions{NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "" {
		t.Fatalf("NoBackup must report no safety fork, got %q", res.Backup)
	}
}

// TestRollbackCompatWrapperBacksUp: the three-argument Rollback is the
// RollbackWith default — safety fork on.
func TestRollbackCompatWrapperBacksUp(t *testing.T) {
	w := seedPromotePair(t)
	mp, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Rollback("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.GetRef("app", "main"+RollbackBackupSuffix); err != nil {
		t.Fatalf("Rollback must mint the safety fork by default: %v", err)
	}
}

// TestRollbackNoBackupAndTTL: opting out leaves no safety fork behind, and
// an explicit BackupTTL is honored verbatim.
func TestRollbackNoBackupAndTTL(t *testing.T) {
	w := seedPromotePair(t)
	mp, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v2", nil); err != nil {
		t.Fatal(err)
	}
	res, err := w.RollbackWith("app", "main", "v1", RollbackOptions{NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Backup != "" {
		t.Fatalf("NoBackup: Backup = %q", res.Backup)
	}
	if _, _, err := w.Store.GetRef("app", "main"+RollbackBackupSuffix); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("NoBackup must leave no safety fork, got err=%v", err)
	}
	// Re-checkpoint v2 doesn't exist anymore after the rollback (it's
	// dropped); recreate the scenario to exercise a custom TTL.
	mp, err = w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", mp, "INSERT INTO t VALUES (3);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v3", nil); err != nil {
		t.Fatal(err)
	}
	res, err = w.RollbackWith("app", "main", "v1", RollbackOptions{BackupTTL: 2 * time.Hour})
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
