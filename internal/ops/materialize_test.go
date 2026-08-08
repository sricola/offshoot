package ops

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestCheckoutReadsASnapshotPlusSegments builds a chain by hand in the store
// and proves every read path resolves it.
func TestCheckoutReadsASnapshotPlusSegments(t *testing.T) {
	testutil.RequireSQLite3(t)
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
	if _, err := w.Checkpoint("app", "main", "base", nil); err != nil {
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

// TestForkFromAChainProducesASingleSnapshot proves a MATERIALIZED child's
// lineage is not a copied chain: it holds exactly one snapshot object, so it
// stays independent. Since the copy-on-write shared fork landed, a plain
// Fork of a short chain shares instead (see TestSharedForkWritesZeroDataObjects),
// so this pins the materialize branch — the fork-time snapshot floor — via
// the test hook.
func TestForkFromAChainProducesASingleSnapshot(t *testing.T) {
	testutil.RequireSQLite3(t)
	SetForkMaterializeForTest(true)
	t.Cleanup(func() { SetForkMaterializeForTest(false) })
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('base');").Run()
	if _, err := w.Checkpoint("app", "main", "base", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
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
// snapshot object in the child's fresh lineage. Pinned to the materialize
// branch via the test hook — see TestForkFromAChainProducesASingleSnapshot's
// doc comment.
func TestForkAtHeadAfterASegmentProducesASingleSnapshot(t *testing.T) {
	testutil.RequireSQLite3(t)
	SetForkMaterializeForTest(true)
	t.Cleanup(func() { SetForkMaterializeForTest(false) })
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('base');").Run()
	if _, err := w.Checkpoint("app", "main", "base", nil); err != nil {
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
	if _, err := w.Fork("app", "main", "child", "", 0, nil); err != nil {
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
	testutil.RequireSQLite3(t)
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
	if _, err := w.Checkpoint("app", "main", "live", nil); err != nil {
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
	if _, err := ltxio.EncodeSnapshot(orphan, ref.HeadTXID, &buf); err != nil {
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

// countingReadCloser wraps an io.ReadCloser returned by a
// countingReaderGetterBackend's GetReader, recording exactly one Close back
// to the backend's counters — idempotent, so a caller that Close()s twice
// (belt-and-suspenders, as lazyReader's own defer does) never double-counts.
type countingReadCloser struct {
	io.ReadCloser
	b      *countingReaderGetterBackend
	closed bool
}

func (c *countingReadCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.b.mu.Lock()
	c.b.closes++
	c.b.curOpen--
	c.b.mu.Unlock()
	return c.ReadCloser.Close()
}

// countingReaderGetterBackend wraps a real Backend (which must itself
// implement store.ReaderGetter — Local does) and counts GetReader activity:
// how many streams were opened, how many were closed, and the HIGH-WATER
// MARK of streams open at the same time. materializeMembersAtStreaming must
// keep that high-water mark at 1 (never buffer the whole chain's objects
// concurrently) and open==close (never leak a stream).
type countingReaderGetterBackend struct {
	store.Backend
	mu      sync.Mutex
	opens   int
	closes  int
	curOpen int
	maxOpen int
}

func newCountingReaderGetterBackend(b store.Backend) *countingReaderGetterBackend {
	return &countingReaderGetterBackend{Backend: b}
}

func (b *countingReaderGetterBackend) GetReader(key string) (io.ReadCloser, string, error) {
	rg, ok := b.Backend.(store.ReaderGetter)
	if !ok {
		return nil, "", fmt.Errorf("wrapped backend %T does not implement store.ReaderGetter", b.Backend)
	}
	r, etag, err := rg.GetReader(key)
	if err != nil {
		return nil, "", err
	}
	b.mu.Lock()
	b.opens++
	b.curOpen++
	if b.curOpen > b.maxOpen {
		b.maxOpen = b.curOpen
	}
	b.mu.Unlock()
	return &countingReadCloser{ReadCloser: r, b: b}, etag, nil
}

// hideReaderGetterBackend wraps a real Backend but exposes only
// store.Backend's method set: embedding an INTERFACE field promotes exactly
// that interface's methods, never a method (like GetReader) the underlying
// concrete value happens to also implement — so
// materializeMembersAt's `w.Store.B.(store.ReaderGetter)` type assertion
// fails on this wrapper even though Local underneath does implement it.
// This is the same technique rpcCountBackend/perKeyBackend already use to
// hide BatchDeleter (see rpc_count_test.go, gc_batch_test.go), reused here
// to force and prove the fallback (no-ReaderGetter) materialize path.
type hideReaderGetterBackend struct {
	store.Backend
}

// buildHandSegmentedChain checkpoints a fresh snapshot ("base", v=1) on
// app/main, then hand-builds n additional single-transaction segments on
// top of it (v=2..n+1), advancing the ref after each — mirroring
// TestCheckoutReadsASnapshotPlusSegments's technique but looped, so the
// chain has a snapshot AND several segments for a streaming test to walk.
// Returns the resolved chain (n+1 members: 1 snapshot + n segments) and the
// lineage.
func buildHandSegmentedChain(t *testing.T, w *Workspace, n int) ([]store.ChainMember, string) {
	t.Helper()
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "base", nil); err != nil {
		t.Fatal(err)
	}

	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	cur := path
	for i := 0; i < n; i++ {
		next := fmt.Sprintf("%s.next%d", path, i)
		if err := copyFile(cur, next); err != nil {
			t.Fatal(err)
		}
		mustSQL(t, next, fmt.Sprintf("INSERT INTO t VALUES (%d);", i+2))
		segTXID := ref.HeadTXID + 1
		pageSize, commit, changed := changedPagesForTest(t, cur, next)
		pre, err := ltxio.ChecksumDatabase(cur)
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
		ref.HeadTXID = segTXID
		cur = next
	}
	ref.HeadEpoch = ref.Epoch
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	members, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != n+1 {
		t.Fatalf("built chain has %d members, want %d (1 snapshot + %d segments)", len(members), n+1, n)
	}
	return members, ref.Lineage
}

// TestMaterializeStreamsOneMemberAtATimeAndClosesEveryReader is the
// read-side streaming proof (perf audit H3, offshoot task 9a):
// materializeMembersAtStreaming must never hold more than one chain
// member's bytes/stream open at once, must close every stream it opens (no
// FD leak on the success path), and must produce output byte-identical to
// the pre-streaming buffered path.
func TestMaterializeStreamsOneMemberAtATimeAndClosesEveryReader(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	members, lineage := buildHandSegmentedChain(t, w, 3)

	dstBuffered := filepath.Join(t.TempDir(), "buffered.db")
	wantChecksum, err := materializeMembersAtBuffered(w.Store.B, lineage, members, dstBuffered)
	if err != nil {
		t.Fatalf("buffered materialize: %v", err)
	}

	cb := newCountingReaderGetterBackend(w.Store.B)
	dstStreamed := filepath.Join(t.TempDir(), "streamed.db")
	gotChecksum, err := materializeMembersAtStreaming(cb, lineage, members, dstStreamed)
	if err != nil {
		t.Fatalf("streaming materialize: %v", err)
	}

	if gotChecksum != wantChecksum {
		t.Fatalf("streaming checksum = %d, buffered checksum = %d — must match", gotChecksum, wantChecksum)
	}
	wantBytes, err := os.ReadFile(dstBuffered)
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := os.ReadFile(dstStreamed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantBytes, gotBytes) {
		t.Fatalf("streamed output (%d bytes) is not byte-identical to buffered output (%d bytes)",
			len(gotBytes), len(wantBytes))
	}

	if cb.opens != len(members) {
		t.Fatalf("opened %d streams, want exactly %d (one per chain member)", cb.opens, len(members))
	}
	if cb.closes != cb.opens {
		t.Fatalf("opened %d streams but closed %d — stream/FD leak", cb.opens, cb.closes)
	}
	if cb.maxOpen > 1 {
		t.Fatalf("max concurrently open streams = %d, want <= 1 (the chain must be applied one member at a time, not all buffered upfront)", cb.maxOpen)
	}
}

// TestMaterializeStreamingClosesReadersOnMidApplyError proves the streaming
// path's close discipline holds on the ERROR path too, not just success:
// when MaterializeChain rejects an EARLY segment (corrupted bytes,
// simulating any mid-chain apply failure), the members already opened
// before the failure (the snapshot and that segment) are still closed via
// the top-level defer, and every member AFTER the failure point is never
// opened at all — its lazyReader stays untouched, so closing it is a no-op.
func TestMaterializeStreamingClosesReadersOnMidApplyError(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	members, lineage := buildHandSegmentedChain(t, w, 3) // snapshot + 3 segments

	// Corrupt the FIRST segment (members[1], not the last) so segments after
	// it (members[2], members[3]) are provably never reached/opened.
	corrupted := members[1]
	data, _, err := w.Store.B.Get(corrupted.Key)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), data...)
	for i := range corrupt {
		corrupt[i] ^= 0xff
	}
	if err := w.Store.B.Put(corrupted.Key, corrupt); err != nil {
		t.Fatal(err)
	}

	cb := newCountingReaderGetterBackend(w.Store.B)
	dst := filepath.Join(t.TempDir(), "should-fail.db")
	if _, err := materializeMembersAtStreaming(cb, lineage, members, dst); err == nil {
		t.Fatal("materializing a chain with a corrupted early segment: want error, got nil")
	}

	// Exactly the snapshot and the corrupted segment were opened; the two
	// segments after it were never reached.
	if want := 2; cb.opens != want {
		t.Fatalf("opened %d streams, want exactly %d (snapshot + the corrupted segment; later segments must never be opened)", cb.opens, want)
	}
	if cb.closes != cb.opens {
		t.Fatalf("opened %d streams but closed %d — a mid-apply error must still close every stream it opened", cb.opens, cb.closes)
	}
}

// TestMaterializeFallsBackWithoutReaderGetter proves a backend that does NOT
// implement store.ReaderGetter still materializes correctly via the
// original buffered path — the streaming path (task 9a) is additive, never
// a hard requirement.
func TestMaterializeFallsBackWithoutReaderGetter(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	members, lineage := buildHandSegmentedChain(t, w, 2)

	wrapped := hideReaderGetterBackend{Backend: w.Store.B}
	if _, ok := interface{}(wrapped).(store.ReaderGetter); ok {
		t.Fatal("hideReaderGetterBackend unexpectedly implements store.ReaderGetter — test setup is broken")
	}
	w.Store.B = wrapped

	dst := filepath.Join(t.TempDir(), "fallback.db")
	checksum, err := w.materializeMembersAt(lineage, members, dst)
	if err != nil {
		t.Fatalf("fallback materialize: %v", err)
	}
	if checksum == 0 {
		t.Fatal("fallback materialize returned a zero checksum")
	}

	out, err := exec.Command("sqlite3", dst, "SELECT count(*) FROM t;").Output()
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("%d\n", len(members)); string(out) != want {
		t.Fatalf("rows = %q, want %q (snapshot row + one per segment)", out, want)
	}
}
