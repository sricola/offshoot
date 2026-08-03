package ops

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

// TestCheckoutReadsASnapshotPlusSegments builds a chain by hand in the store
// and proves every read path resolves it.
func TestCheckoutReadsASnapshotPlusSegments(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('base');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "base"); err != nil {
		t.Fatal(err)
	}

	// Hand-build the next state and store it as a SEGMENT, advancing the ref.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	next := path + ".next"
	if err := copyFile(path, next); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", next,
		"INSERT INTO t VALUES ('from-segment');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	segTXID := ref.HeadTXID + 1
	pageSize, commit, changed := changedPagesForTest(t, path, next)
	pre, err := ltxio.ChecksumDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	post, err := ltxio.ChecksumDatabase(next)
	if err != nil {
		t.Fatal(err)
	}
	var seg bytes.Buffer
	if err := ltxio.EncodeSegment(pageSize, commit, segTXID, segTXID, pre, post, changed, &seg); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.B.PutIf(
		store.SegmentKey(ref.Lineage, ref.Epoch, segTXID, segTXID), seg.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID, ref.HeadEpoch = segTXID, ref.Epoch
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	// A fresh checkout must contain both rows.
	got, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout across a chain: %v", err)
	}
	out, err := exec.Command("sqlite3", got, "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "2\n" {
		t.Fatalf("rows = %q err=%v (segment not applied)", out, err)
	}
}

// TestForkFromAChainProducesASingleSnapshot proves a child's lineage is not a
// copied chain: it holds exactly one snapshot object, so it stays independent.
func TestForkFromAChainProducesASingleSnapshot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('base');").Run()
	if _, err := w.Checkpoint("app", "main", "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", ""); err != nil {
		t.Fatal(err)
	}
	cref, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(cref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("a fork's lineage must hold exactly one snapshot, got %v", keys)
	}
	if m, ok := store.ParseMemberKey(keys[0]); !ok || !m.Snapshot {
		t.Fatalf("fork's only object must be a snapshot: %s", keys[0])
	}
}

// TestForkAtHeadAfterASegmentProducesASingleSnapshot proves the fork read
// side resolves the chain (not a direct snapshot lookup) when HEAD sits past
// the last snapshot on a hand-built segment, and still lands exactly one
// snapshot object in the child's fresh lineage.
func TestForkAtHeadAfterASegmentProducesASingleSnapshot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('base');").Run()
	if _, err := w.Checkpoint("app", "main", "base"); err != nil {
		t.Fatal(err)
	}

	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	next := path + ".next"
	if err := copyFile(path, next); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", next,
		"INSERT INTO t VALUES ('from-segment');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	segTXID := ref.HeadTXID + 1
	pageSize, commit, changed := changedPagesForTest(t, path, next)
	pre, err := ltxio.ChecksumDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	post, err := ltxio.ChecksumDatabase(next)
	if err != nil {
		t.Fatal(err)
	}
	var seg bytes.Buffer
	if err := ltxio.EncodeSegment(pageSize, commit, segTXID, segTXID, pre, post, changed, &seg); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.B.PutIf(
		store.SegmentKey(ref.Lineage, ref.Epoch, segTXID, segTXID), seg.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID, ref.HeadEpoch = segTXID, ref.Epoch
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	// Fork at HEAD, which now sits past the last snapshot on the segment.
	if _, err := w.Fork("app", "main", "child", ""); err != nil {
		t.Fatalf("fork at head across a chain: %v", err)
	}
	cref, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(cref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("a fork's lineage must hold exactly one snapshot, got %v", keys)
	}
	if m, ok := store.ParseMemberKey(keys[0]); !ok || !m.Snapshot {
		t.Fatalf("fork's only object must be a snapshot: %s", keys[0])
	}

	cpath, err := w.Checkout("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", cpath, "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "2\n" {
		t.Fatalf("child rows = %q err=%v (fork did not include the segment's data)", out, err)
	}
}

// changedPagesForTest diffs two quiesced databases into segment pages. after
// is checkpointed (WAL truncated) first so the comparison and the returned
// commit size reflect its fully-merged on-disk state, regardless of whether
// the process that wrote it already triggered SQLite's own close-time
// auto-checkpoint.
func changedPagesForTest(t *testing.T, before, after string) (uint32, uint32, []ltxio.Page) {
	t.Helper()
	if err := quiesce(after); err != nil {
		t.Fatalf("quiesce %s: %v", after, err)
	}
	beforeData, err := os.ReadFile(before)
	if err != nil {
		t.Fatal(err)
	}
	afterData, err := os.ReadFile(after)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterData) < 32 {
		t.Fatalf("%s too short to be a SQLite database", after)
	}
	pageSize := uint32(binary.BigEndian.Uint16(afterData[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	commit := binary.BigEndian.Uint32(afterData[28:32])

	var changed []ltxio.Page
	for pgno := uint32(1); pgno <= commit; pgno++ {
		start := int64(pgno-1) * int64(pageSize)
		end := start + int64(pageSize)
		if end > int64(len(afterData)) {
			t.Fatalf("%s: page %d extends past EOF", after, pgno)
		}
		newData := afterData[start:end]
		var oldData []byte
		if end <= int64(len(beforeData)) {
			oldData = beforeData[start:end]
		}
		if !bytes.Equal(oldData, newData) {
			changed = append(changed, ltxio.Page{Pgno: pgno, Data: append([]byte(nil), newData...)})
		}
	}
	return pageSize, commit, changed
}

// TestCheckoutIgnoresAFencedWritersObject is the end-to-end consequence of
// Plan 4's fencing guarantee on Plan 7's read path: when a writer is fenced
// after uploading its object but before its ref write lands, the next holder
// writes the same TXID under a higher epoch. A checkout must materialize the
// live writer's content, never the orphan's.
func TestCheckoutIgnoresAFencedWritersObject(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('live');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "live"); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Build what a fenced writer would have left: a valid snapshot with
	// different content, at the SAME txid, under a LOWER epoch.
	orphan := filepath.Join(t.TempDir(), "orphan.db")
	if out, err := exec.Command("sqlite3", orphan,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('fenced');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	var buf bytes.Buffer
	if err := ltxio.EncodeSnapshot(orphan, ref.HeadTXID, &buf); err != nil {
		t.Fatal(err)
	}
	if ref.HeadEpoch < 2 {
		// Raise the live epoch so a lower one exists to be fenced.
		ref.Epoch, ref.HeadEpoch = ref.HeadEpoch+1, ref.HeadEpoch+1
		live, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.HeadEpoch-1, ref.HeadTXID))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Store.B.Put(
			store.SnapshotKey(ref.Lineage, ref.HeadEpoch, ref.HeadTXID), live); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
			t.Fatal(err)
		}
	}
	// The orphan sits at the now-superseded epoch.
	if err := w.Store.B.Put(
		store.SnapshotKey(ref.Lineage, ref.HeadEpoch-1, ref.HeadTXID), buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	got, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", got, "SELECT v FROM t;").Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "live\n" {
		t.Fatalf("checkout materialized %q — a fenced writer's object was reachable", out)
	}
}
