package ops

// The tests here pin that a repoint's ref write that landed but reported a
// failure leaves the new lineage's objects in place and the branch
// readable, for every verb that mints a lineage: Fork, RollbackWith,
// PromoteWith and Compact. landsThenFails (checkpoint_lease_test.go)
// forwards the write to the store and reports the given error once.

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// TestForkLostRaceLeavesOrphanForGC: a fork that loses to an existing
// branch leaves the existing branch alone and its fresh lineage's
// base.json in place, since Fork has no previous lineage to prove its
// write did not land (see settleRepoint); GC then reclaims the orphan.
func TestForkLostRaceLeavesOrphanForGC(t *testing.T) {
	w := newWS(t)
	chainCacheSeed(t, w)
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	before := refOf(t, w, "app", "child")
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
	if _, _, err := w.Store.B.Get(store.BaseKey(orphan)); err != nil {
		t.Fatalf("orphan base.json %s removed although the write's fate was not proven: %v", orphan, err)
	}
	if after := refOf(t, w, "app", "child"); after.Lineage != before.Lineage {
		t.Fatal("the existing branch was repointed by the losing fork")
	}
	gcTwice(t, w)
	if _, _, err := w.Store.B.Get(store.BaseKey(orphan)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphan base.json %s not reclaimed by GC: %v", orphan, err)
	}
	assertHeadIsCheckout(t, w, "app", "child", "fork", w.CheckoutPath("app", "main"))
}

// gcTwice runs GC with no grace twice: the first pass tombstones what is
// unreachable, the second deletes it.
func gcTwice(t *testing.T, w *Workspace) {
	t.Helper()
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
	}
}

// newLineageObject is the object a repoint's new lineage owns at txid: its
// base.json when it shares, else its copied snapshot.
func newLineageObject(w *Workspace, lineage string, txid uint64) string {
	if _, _, err := w.Store.B.Get(store.BaseKey(lineage)); err == nil {
		return store.BaseKey(lineage)
	}
	return store.SnapshotKey(lineage, 1, txid)
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
	if !errors.Is(err, store.ErrCAS) || !strings.Contains(err.Error(), "offshoot status") {
		t.Fatalf("settle error must wrap the write's error and name the next step: %v", err)
	}
	if _, _, err := w.Store.B.Get(store.BaseKey(minted)); err != nil {
		t.Fatalf("base.json deleted although the write's fate was unknown: %v", err)
	}
	// The write did land, so the branch is there and readable.
	assertHeadIsCheckout(t, w, "app", "child", "fork", path)
}

// runBeforeRefPut runs run once, just before forwarding the first PutIf of
// refKey, so that write's compare-and-swap loses to whatever run writes.
type runBeforeRefPut struct {
	store.Backend
	refKey string
	run    func()
	fired  atomic.Int32
}

func (b *runBeforeRefPut) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.refKey && b.fired.CompareAndSwap(0, 1) {
		b.run()
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// checkpointBytes materializes db@branch at checkpoint name and returns
// its bytes.
func checkpointBytes(t *testing.T, w *Workspace, db, branch, name string) []byte {
	t.Helper()
	at, err := w.CheckoutAt(db, branch, name, true)
	if err != nil {
		t.Fatalf("checkout-at %s@%s:%s: %v", db, branch, name, err)
	}
	return readFile(t, at)
}

// TestRollbackLandedThenRepointedAwayKeepsObjects: a rollback's write
// lands but reports a lost race, and in that window another writer forks
// the branch from the new lineage and repoints the branch away. The settle
// sees a third lineage, cannot tell this from a lost race, and keeps the
// new lineage's objects, so the fork taken in between stays readable.
func TestRollbackLandedThenRepointedAwayKeepsObjects(t *testing.T) {
	for _, materialize := range []bool{false, true} {
		t.Run(fmt.Sprintf("materialize=%v", materialize), func(t *testing.T) {
			w := newWS(t)
			path := chainCacheSeed(t, w)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})
			want := checkpointBytes(t, w, "app", "main", "b")
			txidB := refOf(t, w, "app", "main").Checkpoints["b"].TXID
			real := w.Store.B
			var landed string
			after := func() {
				r, _, err := w.Store.GetRef("app", "main")
				if err != nil {
					t.Errorf("re-read in window: %v", err)
					return
				}
				landed = r.Lineage
				if _, err := w.Fork("app", "main", "keep", "", 0, nil); err != nil {
					t.Errorf("fork in window: %v", err)
				}
				if _, err := w.RollbackWith("app", "main", "a", RollbackOptions{NoBackup: true, Materialize: true}); err != nil {
					t.Errorf("second rollback in window: %v", err)
				}
			}
			w.Store.B = &landsThenFails{Backend: real, match: refPutOf("app", "main"), err: store.ErrCAS, after: after}
			_, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true, Materialize: materialize})
			w.Store.B = real
			if err == nil || !strings.Contains(err.Error(), "rollback lost a race (retry)") || !errors.Is(err, store.ErrCAS) {
				t.Fatalf("rollback repointed away in its window: err = %v, want the lost-race error", err)
			}
			if _, _, err := w.Store.B.Get(newLineageObject(w, landed, txidB)); err != nil {
				t.Fatalf("the landed lineage %s's object was deleted: %v", landed, err)
			}
			keep := refOf(t, w, "app", "keep")
			if _, err := w.Store.Chain(keep.Lineage, keep.HeadTXID); err != nil {
				t.Fatalf("the fork taken from the landed lineage is unreadable: %v", err)
			}
			if got := checkpointBytes(t, w, "app", "keep", "fork"); !bytes.Equal(got, want) {
				t.Fatal("the fork taken from the landed lineage differs from checkpoint b")
			}
		})
	}
}

// TestRollbackLostRaceLeavesObjectsForGC: another repoint lands before the
// rollback's write, whose compare-and-swap then fails. The ref names a
// third lineage, so the rollback keeps its new lineage's objects (see
// settleRepoint) and GC reclaims them.
func TestRollbackLostRaceLeavesObjectsForGC(t *testing.T) {
	for _, materialize := range []bool{false, true} {
		t.Run(fmt.Sprintf("materialize=%v", materialize), func(t *testing.T) {
			w := newWS(t)
			path := chainCacheSeed(t, w)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})
			txidB := refOf(t, w, "app", "main").Checkpoints["b"].TXID
			var minted []string
			prevNew := observeNewLineage
			observeNewLineage = func(id string) { minted = append(minted, id) }
			t.Cleanup(func() { observeNewLineage = prevNew })
			real := w.Store.B
			w.Store.B = &runBeforeRefPut{Backend: real, refKey: store.RefKey("app", "main"), run: func() {
				if _, err := w.RollbackWith("app", "main", "a", RollbackOptions{NoBackup: true}); err != nil {
					t.Errorf("rival rollback: %v", err)
				}
			}}
			_, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true, Materialize: materialize})
			w.Store.B = real
			if err == nil || !strings.Contains(err.Error(), "rollback lost a race (retry)") || !errors.Is(err, store.ErrCAS) {
				t.Fatalf("rollback that lost to a rival repoint: err = %v, want the lost-race error", err)
			}
			if len(minted) != 2 {
				t.Fatalf("minted %v, want this rollback's lineage and the rival's", minted)
			}
			ours, rival := minted[0], minted[1]
			if got := refOf(t, w, "app", "main").Lineage; got != rival {
				t.Fatalf("main names %s, want the rival's lineage %s", got, rival)
			}
			key := newLineageObject(w, ours, txidB)
			if _, _, err := w.Store.B.Get(key); err != nil {
				t.Fatalf("%s removed although the ref named a third lineage: %v", key, err)
			}
			gcTwice(t, w)
			if _, _, err := w.Store.B.Get(key); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("%s not reclaimed by GC: %v", key, err)
			}
			assertHeadIsCheckout(t, w, "app", "main", "a", path)
		})
	}
}

// TestRollbackLostToRefWriteRemovesObjects: a ref write that keeps the
// lineage (a touch, as a flush or a lease renewal would write) lands before
// the rollback's write. The re-read finds the ref still on the old lineage,
// which proves the rollback's write did not land, so its new lineage's
// objects are removed at once.
func TestRollbackLostToRefWriteRemovesObjects(t *testing.T) {
	for _, materialize := range []bool{false, true} {
		t.Run(fmt.Sprintf("materialize=%v", materialize), func(t *testing.T) {
			w := newWS(t)
			path := chainCacheSeed(t, w)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})
			before := refOf(t, w, "app", "main")
			txidB := before.Checkpoints["b"].TXID
			var minted string
			prevNew := observeNewLineage
			observeNewLineage = func(id string) { minted = id }
			t.Cleanup(func() { observeNewLineage = prevNew })
			var key string
			real := w.Store.B
			w.Store.B = &runBeforeRefPut{Backend: real, refKey: store.RefKey("app", "main"), run: func() {
				key = newLineageObject(w, minted, txidB)
				r, etag, err := w.Store.GetRef("app", "main")
				if err != nil {
					t.Errorf("rival read: %v", err)
					return
				}
				r.Touch(time.Now().Add(time.Second))
				if _, err := w.Store.PutRef("app", "main", r, etag); err != nil {
					t.Errorf("rival touch: %v", err)
				}
			}}
			_, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true, Materialize: materialize})
			w.Store.B = real
			if err == nil || !strings.Contains(err.Error(), "rollback lost a race (retry)") || !errors.Is(err, store.ErrCAS) {
				t.Fatalf("rollback that lost to a touch: err = %v, want the lost-race error", err)
			}
			if _, _, err := w.Store.B.Get(key); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("%s not removed although the ref stayed on the old lineage: %v", key, err)
			}
			if got := refOf(t, w, "app", "main").Lineage; got != before.Lineage {
				t.Fatalf("main moved to %s, want %s", got, before.Lineage)
			}
			assertHeadIsCheckout(t, w, "app", "main", "c", path)
		})
	}
}

// TestForkLandedThenDestroyedKeepsObjects: a materialized fork's write
// lands but reports a timeout, and in that window another writer forks a
// grandchild from the new branch and destroys the new branch. The settle
// finds no ref, cannot tell this from a write that never landed, and keeps
// the new lineage's snapshot, which the grandchild's base pointer names.
func TestForkLandedThenDestroyedKeepsObjects(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	want := readFile(t, path)
	prevMat := forkMaterializeForTest
	forkMaterializeForTest = true
	t.Cleanup(func() { forkMaterializeForTest = prevMat })
	var minted []string
	prevNew := observeNewLineage
	observeNewLineage = func(id string) { minted = append(minted, id) }
	t.Cleanup(func() { observeNewLineage = prevNew })
	real := w.Store.B
	after := func() {
		forkMaterializeForTest = false
		if _, err := w.Fork("app", "x", "y", "", 0, nil); err != nil {
			t.Errorf("grandchild fork in window: %v", err)
		}
		if err := w.Destroy("app", "x", true); err != nil {
			t.Errorf("destroy in window: %v", err)
		}
	}
	w.Store.B = &landsThenFails{Backend: real, match: refPutOf("app", "x"), err: errors.New("fake: request timed out"), after: after}
	_, err := w.Fork("app", "main", "x", "", 0, nil)
	w.Store.B = real
	if err == nil || !strings.Contains(err.Error(), "fake: request timed out") {
		t.Fatalf("fork whose branch was destroyed in its window: err = %v, want the timeout", err)
	}
	if len(minted) == 0 {
		t.Fatal("observeNewLineage saw no lineage")
	}
	y := refOf(t, w, "app", "y")
	if y.Base == nil || y.Base.Lineage != minted[0] {
		t.Fatalf("precondition: y's base %+v does not name x's lineage %s", y.Base, minted[0])
	}
	if _, err := w.Store.Chain(y.Lineage, y.HeadTXID); err != nil {
		t.Fatalf("the grandchild is unreadable: %v", err)
	}
	if got := checkpointBytes(t, w, "app", "y", "fork"); !bytes.Equal(got, want) {
		t.Fatal("the grandchild differs from main's head")
	}
}

// TestRollbackSettleUnknownKeepsObjects: when the ref cannot be re-read
// after a rollback's failed write, the rollback says the write may have
// landed, wraps the write's error, and runs no cleanup; here the write did
// land, so the branch resolves on the new lineage.
func TestRollbackSettleUnknownKeepsObjects(t *testing.T) {
	w := newWS(t)
	path := chainCacheSeed(t, w)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})
	want := checkpointBytes(t, w, "app", "main", "b")
	txidB := refOf(t, w, "app", "main").Checkpoints["b"].TXID
	real := w.Store.B
	inner := &landsThenFails{Backend: real, match: refPutOf("app", "main"), err: store.ErrCAS}
	w.Store.B = &failingGetAfterPut{landsThenFails: inner, refKey: store.RefKey("app", "main")}
	var minted string
	prevNew := observeNewLineage
	observeNewLineage = func(id string) { minted = id }
	t.Cleanup(func() { observeNewLineage = prevNew })
	_, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true})
	w.Store.B = real
	if err == nil || !strings.Contains(err.Error(), "may have landed") || minted == "" || !strings.Contains(err.Error(), minted) || !errors.Is(err, store.ErrCAS) {
		t.Fatalf("rollback with an unreadable ref after a failed write: err = %v, want the may-have-landed error naming %s", err, minted)
	}
	if _, _, err := w.Store.B.Get(newLineageObject(w, minted, txidB)); err != nil {
		t.Fatalf("the new lineage's object was deleted although the write's fate was unknown: %v", err)
	}
	if ref := refOf(t, w, "app", "main"); ref.Lineage != minted {
		t.Fatalf("main names %s, want the landed lineage %s", ref.Lineage, minted)
	}
	if got := checkpointBytes(t, w, "app", "main", "b"); !bytes.Equal(got, want) {
		t.Fatal("main at b differs from b before the rollback")
	}
}
