package ops

import (
	"bytes"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// storeDataBytes is the total size of every object under the local store's
// data/ prefix.
func storeDataBytes(t *testing.T, w *Workspace) int64 {
	t.Helper()
	var n int64
	err := filepath.WalkDir(filepath.Join(w.Spec, "data"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		n += info.Size()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func hasBase(w *Workspace, lineage string) bool {
	_, _, err := w.Store.B.Get(store.BaseKey(lineage))
	return !errors.Is(err, store.ErrNotFound)
}

// seedAB seeds db main with size bytes, checkpoints "a", writes one more row
// and checkpoints "b". It returns the checkout path and an independent
// export of "a".
func seedAB(t *testing.T, w *Workspace, db string, size int) (string, []byte) {
	t.Helper()
	path := seedRows(t, w, db, size, 4000)
	mustCheckpointWith(t, w, db, "main", "a", CheckpointOptions{})
	a := exportBytes(t, w, db, "main", "a")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(3000));")
	mustCheckpointWith(t, w, db, "main", "b", CheckpointOptions{})
	return path, a
}

func TestRollbackToKeptCheckpointIsShared(t *testing.T) {
	w := newWS(t)
	_, a := seedAB(t, w, "app", 32<<20)
	before := refOf(t, w, "app", "main")
	storeBefore := storeDataBytes(t, w)

	res, err := w.RollbackWith("app", "main", "a", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Shared {
		t.Fatal("rollback to a kept checkpoint below the floor did not share")
	}
	if grew := storeDataBytes(t, w) - storeBefore; grew >= 8<<10 {
		t.Fatalf("store grew by %d bytes, want < 8 KiB", grew)
	}
	ref := refOf(t, w, "app", "main")
	if ref.Base == nil || ref.Base.Lineage != before.Lineage || ref.Base.TXID != before.Checkpoints["a"].TXID {
		t.Fatalf("ref.Base = %+v, want {%s %d}", ref.Base, before.Lineage, before.Checkpoints["a"].TXID)
	}
	if !hasBase(w, ref.Lineage) {
		t.Fatal("no base.json for the new lineage")
	}
	if got, want := ref.Checkpoints["a"], before.Checkpoints["a"]; got.TXID != want.TXID || got.Epoch != want.Epoch || got.Kind != want.Kind {
		t.Fatalf("kept checkpoint a = %+v, want it unchanged %+v", got, want)
	}
	if _, ok := ref.Checkpoints["b"]; ok {
		t.Fatal("checkpoint b past the rollback target was kept")
	}
	if !bytes.Equal(readFile(t, res.Path), a) {
		t.Fatal("checkout differs from an export of a")
	}
	assertCheckoutMatches(t, w, "app", "main", res.Path)
	at, err := w.CheckoutAt("app", "main", "a", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, at), a) {
		t.Fatal("checkout-at a differs from the export of a")
	}
}

func TestPromoteIsSharedAndSourceStaysResolvable(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<20)
	mustFork(t, w, "app", "main", "attempt", "")
	apath := mustCheckout(t, w, "app", "attempt")
	mustSQL(t, apath, "INSERT INTO t (v) VALUES (randomblob(500));")
	mustCheckpointWith(t, w, "app", "attempt", "done", CheckpointOptions{})
	head := exportBytes(t, w, "app", "attempt", "")
	src := refOf(t, w, "app", "attempt")

	res, err := w.PromoteWith("app", "attempt", "main", PromoteOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Shared {
		t.Fatal("promote below the floor did not share")
	}
	ref := refOf(t, w, "app", "main")
	if ref.Base == nil || ref.Base.Lineage != src.Lineage || ref.Base.TXID != src.HeadTXID {
		t.Fatalf("ref.Base = %+v, want {%s %d}", ref.Base, src.Lineage, src.HeadTXID)
	}
	mpath := w.CheckoutPath("app", "main")
	if !bytes.Equal(readFile(t, mpath), head) {
		t.Fatal("main checkout differs from the attempt head")
	}
	assertCheckoutMatches(t, w, "app", "main", mpath)

	if err := w.Destroy("app", "attempt", true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(exportBytes(t, w, "app", "main", ""), head) {
		t.Fatal("main no longer materializes the promoted head once the source is destroyed")
	}
}

func TestGCAfterSharedRollbackReclaimsTheAbandonedFuture(t *testing.T) {
	w := newWS(t)
	path, a := seedAB(t, w, "app", 1<<20)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(3000));")
	mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{Snapshot: true})
	old := refOf(t, w, "app", "main")
	aTXID := old.Checkpoints["a"].TXID
	oldKeys, err := w.Store.B.List(store.LineagePrefix(old.Lineage))
	if err != nil {
		t.Fatal(err)
	}

	res, err := w.RollbackWith("app", "main", "a", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Shared {
		t.Fatal("rollback did not share")
	}
	if err := w.Destroy("app", res.Backup, true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := w.GC(0); err != nil {
			t.Fatal(err)
		}
	}
	var above, below int
	for _, k := range oldKeys {
		m, ok := store.ParseMemberKey(k)
		if !ok {
			continue
		}
		switch {
		case m.MaxTXID > aTXID:
			above++
			if storeHas(w, k) {
				t.Errorf("%s (txid %d > a's %d) survived GC", k, m.MaxTXID, aTXID)
			}
		default:
			below++
			if !storeHas(w, k) {
				t.Errorf("%s (txid %d <= a's %d) was reclaimed", k, m.MaxTXID, aTXID)
			}
		}
	}
	if above == 0 || below == 0 {
		t.Fatalf("test precondition: %d objects above a, %d at or below", above, below)
	}
	if !bytes.Equal(readFile(t, mustCheckout(t, w, "app", "main")), a) {
		t.Fatal("checkout after GC differs from the export of a")
	}
	if !bytes.Equal(exportBytes(t, w, "app", "main", ""), a) {
		t.Fatal("export after GC differs from the export of a")
	}
}

func TestCompactAfterSharedRollbackDetaches(t *testing.T) {
	w := newWS(t)
	_, a := seedAB(t, w, "app", 1<<20)
	res, err := w.RollbackWith("app", "main", "a", RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Shared {
		t.Fatal("rollback did not share")
	}
	if _, err := w.Compact("app", "main"); err != nil {
		t.Fatal(err)
	}
	ref := refOf(t, w, "app", "main")
	if ref.Base != nil || hasBase(w, ref.Lineage) {
		t.Fatalf("after compact: ref.Base %+v, base.json present %v; want neither", ref.Base, hasBase(w, ref.Lineage))
	}
	members, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || !members[0].Snapshot || !bytes.HasPrefix([]byte(members[0].Key), []byte(store.LineagePrefix(ref.Lineage))) {
		t.Fatalf("compacted head chain = %+v, want one snapshot in the branch's own lineage", members)
	}
	if !bytes.Equal(exportBytes(t, w, "app", "main", ""), a) {
		t.Fatal("compacted content differs from the export of a")
	}
}

// TestRepeatedRollbacksHitTheDepthFloor: a shared rollback adds no chain
// members, but segments checkpointed on top of it do; once a checkpoint's
// resolved chain reaches the floor, rolling back to it copies instead.
func TestRepeatedRollbacksHitTheDepthFloor(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	w.SnapshotEvery = 3
	path, _ := seedAB(t, w, "app", 1<<20)
	if k := refOf(t, w, "app", "main").Checkpoints["b"].Kind; k != "segment" {
		t.Fatalf("test precondition: b is a %s, want a segment", k)
	}
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(3000));")
	mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})

	res, err := w.RollbackWith("app", "main", "b", RollbackOptions{NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Shared {
		t.Fatal("rollback to b (chain of 2, floor 3) did not share")
	}
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(3000));")
	if r := mustCheckpointWith(t, w, "app", "main", "d", CheckpointOptions{}); r.Kind != "segment" {
		t.Fatalf("test precondition: d is a %s, want a segment", r.Kind)
	}
	ref := refOf(t, w, "app", "main")
	members, err := w.Store.Chain(ref.Lineage, ref.Checkpoints["d"].TXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Fatalf("test precondition: d's resolved chain has %d members, want 3", len(members))
	}
	d := exportBytes(t, w, "app", "main", "d")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(3000));")
	mustCheckpointWith(t, w, "app", "main", "e", CheckpointOptions{})

	res, err = w.RollbackWith("app", "main", "d", RollbackOptions{NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Shared {
		t.Fatal("rollback to d (chain at the floor) shared instead of copying")
	}
	ref = refOf(t, w, "app", "main")
	if ref.Base != nil || hasBase(w, ref.Lineage) {
		t.Fatalf("materialized rollback: ref.Base %+v, base.json present %v; want neither", ref.Base, hasBase(w, ref.Lineage))
	}
	if !bytes.Equal(readFile(t, res.Path), d) {
		t.Fatal("checkout differs from the export of d")
	}
}

func TestMaterializeOptionForcesCopy(t *testing.T) {
	w := newWS(t)
	_, a := seedAB(t, w, "app", 1<<20)
	res, err := w.RollbackWith("app", "main", "a", RollbackOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Shared {
		t.Fatal("--materialize rollback shared")
	}
	ref := refOf(t, w, "app", "main")
	if ref.Base != nil || hasBase(w, ref.Lineage) {
		t.Fatalf("materialized rollback: ref.Base %+v, base.json present %v; want neither", ref.Base, hasBase(w, ref.Lineage))
	}
	if c := ref.Checkpoints["a"]; c.Epoch != 1 || c.Kind != "snapshot" {
		t.Fatalf("copied checkpoint a = %+v, want epoch 1 kind snapshot", c)
	}
	if !bytes.Equal(readFile(t, res.Path), a) {
		t.Fatal("checkout differs from the export of a")
	}

	mustFork(t, w, "app", "main", "attempt", "")
	pres, err := w.PromoteWith("app", "attempt", "main", PromoteOptions{Force: true, Materialize: true, NoBackup: true})
	if err != nil {
		t.Fatal(err)
	}
	if pres.Shared {
		t.Fatal("--materialize promote shared")
	}
	ref = refOf(t, w, "app", "main")
	if ref.Base != nil || hasBase(w, ref.Lineage) {
		t.Fatalf("materialized promote: ref.Base %+v, base.json present %v; want neither", ref.Base, hasBase(w, ref.Lineage))
	}
	if !bytes.Equal(exportBytes(t, w, "app", "main", ""), a) {
		t.Fatal("promoted content differs from the export of a")
	}
}
