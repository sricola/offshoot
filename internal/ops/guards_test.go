package ops

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestCheckpointRefusesDetachedCheckout: checkpointing a checkout whose
// sidecar lineage is not the branch's current lineage would snapshot the
// old content onto the new lineage and silently undo the promote that
// moved it. It is refused with the ways out named; --force overrides.
func TestCheckpointRefusesDetachedCheckout(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	fpath := mustCheckout(t, w, "app", "f")
	mustSQL(t, fpath, "INSERT INTO t (v) VALUES (randomblob(10));")
	if _, err := w.Checkpoint("app", "f", "work", nil); err != nil {
		t.Fatal(err)
	}
	mainPath := mustCheckout(t, w, "app", "main")
	before, _ := readSidecar(mainPath)
	if _, err := w.PromoteWith("app", "f", "main", PromoteOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	// The promote refreshed main's checkout; put the OLD identity back on
	// its sidecar to model a refresh that never ran.
	if err := StampSumHashOnly(mainPath, before.Hash, before.Lineage, before.Epoch, before.TXID, before.PostApplyChecksum, before.ChainID); err != nil {
		t.Fatal(err)
	}
	if state, err := w.BranchState("app", "main"); err != nil || state != "detached" {
		t.Fatalf("setup: state=%q err=%v, want detached", state, err)
	}
	_, err := w.CheckpointWith("app", "main", "after", nil, CheckpointOptions{})
	if err == nil || !strings.Contains(err.Error(), "detached") || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("checkpoint on a detached checkout: got %v, want a detached refusal naming --force", err)
	}
	if ref := refOf(t, w, "app", "main"); ref.Checkpoints["after"].TXID != 0 {
		t.Fatal("the refused checkpoint landed on the ref")
	}
	if _, err := w.CheckpointWith("app", "main", "after", nil, CheckpointOptions{Force: true}); err != nil {
		t.Fatalf("--force must checkpoint it anyway: %v", err)
	}
}

// TestAtRestVerbsRefuseLiveLease: checkpoint, rollback, promote-onto and
// compact refuse a branch under a live lease (a daemon session or `lease
// acquire`), with an error that unwraps to store.ErrLeaseHeld. --force
// overrides the three repoints, which clear the lease and fence that
// writer; it never overrides a checkpoint's refusal. A lease on promote's
// SOURCE never blocks.
func TestAtRestVerbsRefuseLiveLease(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "tester", time.Minute); err != nil {
		t.Fatal(err)
	}
	wantLease := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), "live lease") || !strings.Contains(err.Error(), "--force") {
			t.Fatalf("%s under a live lease: got %v, want a live-lease refusal naming --force", what, err)
		}
	}
	_, err := w.CheckpointWith("app", "main", "cp", nil, CheckpointOptions{})
	wantLease("checkpoint", err)
	_, err = w.RollbackWith("app", "main", "seed", RollbackOptions{})
	wantLease("rollback", err)
	_, err = w.PromoteWith("app", "f", "main", PromoteOptions{})
	wantLease("promote onto", err)
	_, err = w.CompactWith("app", "main", CompactOptions{})
	wantLease("compact", err)
	_, err = w.CheckpointWith("app", "main", "cp", nil, CheckpointOptions{Force: true})
	wantLease("checkpoint --force", err)

	// A lease on the source does not block promoting it.
	if _, err := w.AcquireLease("app", "f", "tester", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PromoteWith("app", "f", "main", PromoteOptions{Force: true}); err != nil {
		t.Fatalf("promote from a leased source (forced past main's protection and lease): %v", err)
	}
}

// TestCheckpointRefusesLiveLeaseEvenWithForce: a live lease refuses an
// at-rest checkpoint with one message whether or not --force is given, and
// the refusal leaves the ref exactly as it was. Before this, --force wrote
// the head and a checkpoint entry under the session's own epoch and left
// its lease in place: two writers interleaved under one epoch.
func TestCheckpointRefusesLiveLeaseEvenWithForce(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(10));")
	if _, err := w.AcquireLease("app", "main", "tester", time.Minute); err != nil {
		t.Fatal(err)
	}
	held := refOf(t, w, "app", "main")
	want := fmt.Sprintf("ops: app@main has a live lease held by %q until %s (an open daemon session, or 'offshoot lease acquire'); --force cannot take over a live lease; close the session (or wait for the other checkpoint) and retry, or, if its holder is gone, free it with 'offshoot lease release app@main'",
		held.LeaseHolder, held.LeaseExpiry)
	for _, force := range []bool{false, true} {
		_, err := w.CheckpointWith("app", "main", "cp", nil, CheckpointOptions{Force: force})
		if !errors.Is(err, store.ErrLeaseHeld) || err.Error() != want {
			t.Fatalf("checkpoint (force=%v) under a live lease: %v\nwant: %s", force, err, want)
		}
		if after := refOf(t, w, "app", "main"); !reflect.DeepEqual(after, held) {
			t.Fatalf("a refused checkpoint (force=%v) changed the ref:\n got %+v\nwant %+v", force, after, held)
		}
	}
}

// TestLeaseRefusalsNameAnInProgressCheckpoint: when the live lease is an
// at-rest checkpoint's, every at-rest verb's refusal unwraps to
// store.ErrLeaseHeld and names the holder, and all but destroy say a
// checkpoint is in progress, so the user waits instead of hunting for a
// session to close.
func TestLeaseRefusalsNameAnInProgressCheckpoint(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustFork(t, w, "app", "main", "work", "seed")
	holder := checkpointHolderPrefix + LocalHolder() + "/0123abcd"
	if _, err := w.AcquireLease("app", "work", holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, cpErr := w.CheckpointWith("app", "work", "cp", nil, CheckpointOptions{})
	_, rbErr := w.RollbackWith("app", "work", "fork", RollbackOptions{NoBackup: true})
	_, prErr := w.PromoteWith("app", "main", "work", PromoteOptions{NoBackup: true})
	_, cmErr := w.CompactWith("app", "work", CompactOptions{})
	dsErr := w.Destroy("app", "work", false)
	for what, err := range map[string]error{"checkpoint": cpErr, "rollback": rbErr, "promote onto": prErr, "compact": cmErr, "destroy": dsErr} {
		if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), holder) {
			t.Fatalf("%s under a checkpoint's lease: %v, want a lease-held refusal naming %s", what, err, holder)
		}
		if what != "destroy" && !strings.Contains(err.Error(), "another checkpoint is in progress") {
			t.Fatalf("%s refusal does not say a checkpoint is in progress: %v", what, err)
		}
	}
}

// TestCheckoutWarnsBeforeDiscardingStaleEdits: a checkout the branch has
// moved away from is replaced on checkout; if it also holds edits nobody
// checkpointed, the user is told, exactly as for a "modified" checkout.
func TestCheckoutWarnsBeforeDiscardingStaleEdits(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	mainPath := mustCheckout(t, w, "app", "main")
	before, _ := readSidecar(mainPath)
	if _, err := w.PromoteWith("app", "f", "main", PromoteOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	restamp := func() {
		t.Helper()
		if err := StampSumHashOnly(mainPath, before.Hash, before.Lineage, before.Epoch, before.TXID, before.PostApplyChecksum, before.ChainID); err != nil {
			t.Fatal(err)
		}
	}
	restamp()
	// Stale but untouched: no warning.
	if out := captureStderr(t, func() {
		if _, err := w.CheckoutProven("app", "main"); err != nil {
			t.Fatal(err)
		}
	}); strings.Contains(out, "un-checkpointed") {
		t.Fatalf("stale checkout with no edits warned: %q", out)
	}
	// Stale AND edited: the edits are about to be lost; say so.
	restamp()
	mustSQL(t, mainPath, "INSERT INTO t (v) VALUES (randomblob(10));")
	if out := captureStderr(t, func() {
		if _, err := w.CheckoutProven("app", "main"); err != nil {
			t.Fatal(err)
		}
	}); !strings.Contains(out, "un-checkpointed changes") {
		t.Fatalf("stale checkout with edits did not warn: %q", out)
	}
}

// TestCreateFromImportsACommittedSnapshotOfALiveSource: an import reads
// the source under a SQLite read transaction, so it carries exactly the
// committed state — not a torn byte copy — even while another connection
// holds an open write transaction on the source.
func TestCreateFromImportsACommittedSnapshotOfALiveSource(t *testing.T) {
	testutil.RequireSQLite3(t)
	src := filepath.Join(t.TempDir(), "live.db")
	mustSQL(t, src, "PRAGMA journal_mode=WAL; CREATE TABLE t (v); INSERT INTO t VALUES (1), (2);")
	writer, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	tx, err := writer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT INTO t VALUES (3)"); err != nil {
		t.Fatal(err)
	}

	w := newWS(t)
	if err := w.CreateFrom("imp", src); err != nil {
		t.Fatalf("import of a source with an open write transaction: %v", err)
	}
	path := mustCheckout(t, w, "imp", "main")
	if got := strings.TrimSpace(querySQL(t, path, "SELECT COUNT(*) FROM t;")); got != "2" {
		t.Fatalf("imported %s rows, want the 2 committed ones", got)
	}
	if mode := strings.TrimSpace(querySQL(t, path, "PRAGMA journal_mode;")); mode != "wal" {
		t.Fatalf("imported checkout journal_mode %q, want wal", mode)
	}
}
