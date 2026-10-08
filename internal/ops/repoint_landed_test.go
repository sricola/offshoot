package ops

// The tests here pin that a repoint's ref write that landed but reported a
// failure leaves the new lineage's objects in place and the branch
// readable, for every verb that mints a lineage: Fork, RollbackWith,
// PromoteWith and Compact. landsThenFails (checkpoint_lease_test.go)
// forwards the write to the store and reports the given error once.

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// refPutOf matches the ref write of db@branch. Only the repoint writes that
// ref while the fake is installed, so the key alone singles out the
// repoint's own write.
func refPutOf(db, branch string) func(key string, data []byte) bool {
	refKey := store.RefKey(db, branch)
	return func(key string, _ []byte) bool { return key == refKey }
}

var landedErrors = map[string]error{
	"reported as a lost race": store.ErrCAS,
	"reported as a timeout":   errors.New("fake: request timed out"),
}

// TestForkLandedWriteKeepsBaseAndIsReadable: a fork whose create-only ref
// write landed but reported a failure succeeds, keeps the child's
// base.json, and the child reads back as main's head.
func TestForkLandedWriteKeepsBaseAndIsReadable(t *testing.T) {
	for name, perr := range landedErrors {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			path := chainCacheSeed(t, w) // app@main with a snapshot and a segment checkpoint
			real := w.Store.B
			w.Store.B = &landsThenFails{Backend: real, match: refPutOf("app", "child"), err: perr}
			_, err := w.Fork("app", "main", "child", "", 0, nil)
			w.Store.B = real
			if err != nil {
				t.Fatalf("fork whose ref write landed returned %v", err)
			}
			child := refOf(t, w, "app", "child")
			if child.Base == nil {
				t.Fatal("child did not share")
			}
			if _, _, err := w.Store.B.Get(store.BaseKey(child.Lineage)); err != nil {
				t.Fatalf("base.json of the landed fork was deleted: %v", err)
			}
			assertHeadIsCheckout(t, w, "app", "child", "fork", path)
		})
	}
}

// TestForkLostRaceStillRemovesOrphan: a fork that genuinely loses to an
// existing branch still removes its fresh lineage's base.json and leaves
// the existing branch alone.
func TestForkLostRaceStillRemovesOrphan(t *testing.T) {
	w := newWS(t)
	chainCacheSeed(t, w)
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	before := refOf(t, w, "app", "child")
	// A second fork at the same name loses to the existing branch: its
	// fresh lineage's base.json must not survive.
	var orphan string
	prevNew := observeNewLineage
	observeNewLineage = func(id string) { orphan = id }
	t.Cleanup(func() { observeNewLineage = prevNew })
	_, err := w.Fork("app", "main", "child", "", 0, nil)
	if err == nil || !strings.Contains(err.Error(), "already exists (offshoot status lists branches)") {
		t.Fatalf("second fork at a taken name: err = %v, want already exists", err)
	}
	if orphan == "" {
		t.Fatal("observeNewLineage saw no lineage")
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(orphan)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphan base.json %s not removed: %v", orphan, err)
	}
	if after := refOf(t, w, "app", "child"); after.Lineage != before.Lineage {
		t.Fatal("the existing branch was repointed by the losing fork")
	}
}

// TestRollbackLandedWriteKeepsObjects: a rollback whose ref write landed
// but reported a failure succeeds on both paths, keeps the new lineage's
// base.json or copied snapshot, and the branch resolves to the target
// checkpoint.
func TestRollbackLandedWriteKeepsObjects(t *testing.T) {
	for _, materialize := range []bool{false, true} {
		for name, perr := range landedErrors {
			t.Run(fmt.Sprintf("materialize=%v/%s", materialize, name), func(t *testing.T) {
				w := newWS(t)
				path := chainCacheSeed(t, w) // checkpoints "a" (snapshot) and "b" (segment)
				mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
				mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})
				real := w.Store.B
				w.Store.B = &landsThenFails{Backend: real, match: refPutOf("app", "main"), err: perr}
				_, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true, Materialize: materialize})
				w.Store.B = real
				if err != nil {
					t.Fatalf("rollback whose ref write landed returned %v", err)
				}
				ref := refOf(t, w, "app", "main")
				if materialize {
					if _, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, 1, ref.HeadTXID)); err != nil {
						t.Fatalf("copied snapshot deleted: %v", err)
					}
				} else if _, _, err := w.Store.B.Get(store.BaseKey(ref.Lineage)); err != nil {
					t.Fatalf("base.json deleted: %v", err)
				}
				if _, err := w.Store.Chain(ref.Lineage, ref.HeadTXID); err != nil {
					t.Fatalf("branch unreadable after a landed rollback: %v", err)
				}
				assertHeadIsCheckout(t, w, "app", "main", "b", path)
			})
		}
	}
}

// TestPromoteLandedWriteKeepsObjects: a promote whose ref write landed but
// reported a failure succeeds on both paths, keeps the new lineage's
// base.json or copied snapshot, and the target reads back as the source's
// checkout.
func TestPromoteLandedWriteKeepsObjects(t *testing.T) {
	for _, materialize := range []bool{false, true} {
		for name, perr := range landedErrors {
			t.Run(fmt.Sprintf("materialize=%v/%s", materialize, name), func(t *testing.T) {
				w := newWS(t)
				path := sharedChildSeed(t, w)
				mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
				mustCheckpointWith(t, w, "app", "child", "c1", CheckpointOptions{})
				real := w.Store.B
				w.Store.B = &landsThenFails{Backend: real, match: refPutOf("app", "main"), err: perr}
				_, err := w.PromoteWith("app", "child", "main", PromoteOptions{NoBackup: true, Force: true, Materialize: materialize})
				w.Store.B = real
				if err != nil {
					t.Fatalf("promote whose ref write landed returned %v", err)
				}
				ref := refOf(t, w, "app", "main")
				if materialize {
					if _, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, 1, ref.HeadTXID)); err != nil {
						t.Fatalf("copied snapshot deleted: %v", err)
					}
				} else if _, _, err := w.Store.B.Get(store.BaseKey(ref.Lineage)); err != nil {
					t.Fatalf("base.json deleted: %v", err)
				}
				assertHeadIsCheckout(t, w, "app", "main", "promote", path)
			})
		}
	}
}

// TestCompactLandedWriteKeepsCopies: a compact whose ref write landed but
// reported a failure succeeds, keeps every checkpoint's copy in the new
// lineage, and the branch reads back as its checkout. The compacted branch
// is a shared child, since main is already self-contained and compacting
// it is a no-op that writes no ref.
func TestCompactLandedWriteKeepsCopies(t *testing.T) {
	for name, perr := range landedErrors {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			path := sharedChildSeed(t, w)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "child", "c", CheckpointOptions{})
			before := refOf(t, w, "app", "child")
			real := w.Store.B
			w.Store.B = &landsThenFails{Backend: real, match: refPutOf("app", "child"), err: perr}
			_, err := w.Compact("app", "child")
			w.Store.B = real
			if err != nil {
				t.Fatalf("compact whose ref write landed returned %v", err)
			}
			ref := refOf(t, w, "app", "child")
			if ref.Lineage == before.Lineage {
				t.Fatal("compact did not repoint the shared child")
			}
			for name, c := range ref.Checkpoints {
				if _, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, c.Epoch, c.TXID)); err != nil {
					t.Fatalf("compact's copy for checkpoint %s deleted: %v", name, err)
				}
			}
			assertHeadIsCheckout(t, w, "app", "child", "compact", path)
		})
	}
}

// failingGetAfterPut is landsThenFails plus one failing Get of the ref key
// after the write: the settle cannot tell whether the write landed.
type failingGetAfterPut struct {
	*landsThenFails
	refKey string
	failed atomic.Bool
}

func (b *failingGetAfterPut) Get(key string) ([]byte, string, error) {
	if key == b.refKey && b.hits.Load() > 0 && b.failed.CompareAndSwap(false, true) {
		return nil, "", errors.New("fake: ref unreadable")
	}
	return b.landsThenFails.Get(key)
}

// TestRepointSettleUnknownKeepsObjects: when the ref cannot be re-read
// after a failed write, the fork says the write may have landed, names the
// lineage it kept, and keeps that lineage's objects; here the write did
// land, so the branch reads.
func TestRepointSettleUnknownKeepsObjects(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	real := w.Store.B
	inner := &landsThenFails{Backend: real, match: refPutOf("app", "child"), err: store.ErrCAS}
	w.Store.B = &failingGetAfterPut{landsThenFails: inner, refKey: store.RefKey("app", "child")}
	var minted string
	prevNew := observeNewLineage
	observeNewLineage = func(id string) { minted = id }
	t.Cleanup(func() { observeNewLineage = prevNew })
	_, err := w.Fork("app", "main", "child", "", 0, nil)
	w.Store.B = real
	if err == nil || !strings.Contains(err.Error(), "may have landed") {
		t.Fatalf("fork with an unreadable ref after a failed write: err = %v, want an error saying the write may have landed", err)
	}
	if minted == "" || !strings.Contains(err.Error(), minted) {
		t.Fatalf("settle error does not name the kept lineage %q: %v", minted, err)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(minted)); err != nil {
		t.Fatalf("base.json deleted although the write's fate was unknown: %v", err)
	}
	// The write did land, so the branch is there and readable.
	assertHeadIsCheckout(t, w, "app", "child", "fork", path)
}
