package ops

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// seedRows creates db, checks main out, and fills table t with roughly size
// bytes of rows WITHOUT checkpointing, returning the checkout path.
func seedRows(t *testing.T, w *Workspace, db string, size, rowBytes int) string {
	t.Helper()
	testutil.RequireSQLite3(t)
	if err := w.Create(db); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, db, "main")
	mustSQL(t, path, fmt.Sprintf(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);"+
			"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < %d) "+
			"INSERT INTO t (v) SELECT randomblob(%d) FROM n;", size/rowBytes, rowBytes))
	return path
}

func mustCheckpointWith(t *testing.T, w *Workspace, db, branch, name string, opts CheckpointOptions) CheckpointResult {
	t.Helper()
	res, err := w.CheckpointWith(db, branch, name, nil, opts)
	if err != nil {
		t.Fatalf("checkpoint %s: %v", name, err)
	}
	return res
}

// assertSegmentHead checks a segment checkpoint against the store's real
// bytes: the segment object exists at its key with res.Bytes bytes, its
// trailer's post-apply checksum equals a full rescan of the checkout, and an
// independent materialization of the checkpoint equals the checkout byte for
// byte.
func assertSegmentHead(t *testing.T, w *Workspace, db, branch, name, checkout string, res CheckpointResult) []byte {
	t.Helper()
	if res.Kind != "segment" {
		t.Fatalf("%s: kind %q, want segment", name, res.Kind)
	}
	ref := refOf(t, w, db, branch)
	data, _, err := w.Store.B.Get(store.SegmentKey(ref.Lineage, ref.Epoch, res.TXID, res.TXID))
	if err != nil {
		t.Fatalf("%s: segment object: %v", name, err)
	}
	if int64(len(data)) != res.Bytes {
		t.Fatalf("%s: result reports %d bytes, object is %d", name, res.Bytes, len(data))
	}
	trailer, err := ltxio.TrailerPostApplyChecksum(data)
	if err != nil {
		t.Fatal(err)
	}
	full, err := ltxio.ChecksumDatabase(checkout)
	if err != nil {
		t.Fatal(err)
	}
	if trailer != full {
		t.Fatalf("%s: segment trailer post-apply %016x, ChecksumDatabase(checkout) %016x", name, trailer, full)
	}
	at, err := w.CheckoutAt(db, branch, name, false)
	if err != nil {
		t.Fatalf("%s: checkout-at: %v", name, err)
	}
	if !bytes.Equal(readFile(t, at), readFile(t, checkout)) {
		t.Fatalf("%s: materialized head differs from the checkout", name)
	}
	return data
}

func TestSecondCheckpointWritesASegment(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	path := seedRows(t, w, "app", 8<<20, 4000)
	if res := mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("a: kind %q, want snapshot (the seed rewrote nearly every page)", res.Kind)
	}
	if _, err := os.Stat(shadowPath(path)); err != nil {
		t.Fatalf("no shadow after a checkpoint on a clone-capable filesystem: %v", err)
	}
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	res := mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{})
	data := assertSegmentHead(t, w, "app", "main", "b", path, res)
	if len(data) >= 64<<10 {
		t.Fatalf("segment is %d bytes for a one-row insert, want < 64 KiB", len(data))
	}
	if res.Pages == 0 {
		t.Fatal("segment reports 0 pages for a one-row insert")
	}
}

func TestDeltaCheckpointsRoundTripUnderRandomMutations(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	testutil.RequireSQLite3(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, "app", "main")
	// auto_vacuum=INCREMENTAL (the VACUUM applies it to the already
	// initialized file) lets a round shrink the file by a few pages
	// (incremental_vacuum), which a segment can carry; a VACUUM rewrites
	// nearly every page and usually becomes a snapshot.
	mustSQL(t, path, "PRAGMA auto_vacuum = INCREMENTAL; VACUUM; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);"+
		"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 2000) "+
		"INSERT INTO t (v) SELECT randomblob(1000) FROM n;")
	mustCheckpointWith(t, w, "app", "main", "seed", CheckpointOptions{})
	keep := t.TempDir()
	rng := rand.New(rand.NewSource(1))
	kinds := map[string]int{}
	shrinkSegments, growSegments := 0, 0
	prevSize := int64(len(readFile(t, path)))
	const rounds = 30
	for n := 1; n <= rounds; n++ {
		var stmts []string
		for i := 0; i < 1+rng.Intn(4); i++ {
			switch rng.Intn(3) {
			case 0:
				stmts = append(stmts, fmt.Sprintf(
					"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < %d) "+
						"INSERT INTO t (v) SELECT randomblob(%d) FROM n;", 1+rng.Intn(20), 50+rng.Intn(1500)))
			case 1:
				stmts = append(stmts, fmt.Sprintf("UPDATE t SET v = randomblob(%d) WHERE id %% %d = %d;",
					50+rng.Intn(1500), 40+rng.Intn(60), rng.Intn(40)))
			case 2:
				stmts = append(stmts, fmt.Sprintf("DELETE FROM t WHERE id %% %d = %d;", 50+rng.Intn(50), rng.Intn(50)))
			}
		}
		if n%7 == 0 {
			// Drop the newest rows, then VACUUM: the file shrinks.
			stmts = append(stmts, "DELETE FROM t WHERE id > (SELECT max(id) - 150 FROM t);", "VACUUM;")
		} else if n%3 == 0 {
			// Drop the newest rows and give their pages back: a shrink by
			// a few dozen pages.
			stmts = append(stmts, "DELETE FROM t WHERE id > (SELECT max(id) - 60 FROM t);", "PRAGMA incremental_vacuum;")
		}
		if n%11 == 0 {
			stmts = append(stmts, "INSERT INTO t (v) VALUES (randomblob(300000));")
		}
		mustSQL(t, path, strings.Join(stmts, " "))
		if err := quiesce(path); err != nil {
			t.Fatal(err)
		}
		kept := filepath.Join(keep, fmt.Sprintf("%d.db", n))
		if err := os.WriteFile(kept, readFile(t, path), 0o644); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("r%d", n)
		res := mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{})
		kinds[res.Kind]++
		size := int64(len(readFile(t, path)))
		if res.Kind == "segment" {
			assertSegmentHead(t, w, "app", "main", name, path, res)
			if size < prevSize {
				shrinkSegments++
			}
			if size > prevSize {
				growSegments++
			}
		}
		rec, ok := readSidecar(path)
		if !ok {
			t.Fatalf("%s: no sidecar", name)
		}
		if sum, err := ltxio.ChecksumDatabase(path); err != nil || rec.PostApplyChecksum != sum {
			t.Fatalf("%s: sidecar post-apply %016x, ChecksumDatabase %016x (%v)", name, rec.PostApplyChecksum, sum, err)
		}
		prevSize = size
	}
	t.Logf("kinds %v; segments that shrank the file %d, grew it %d", kinds, shrinkSegments, growSegments)
	if kinds["segment"] < rounds/2 {
		t.Fatalf("only %d of %d rounds wrote a segment; the delta path is barely exercised", kinds["segment"], rounds)
	}
	if shrinkSegments == 0 || growSegments == 0 {
		t.Fatalf("segments shrank the file %d times and grew it %d times; both must be exercised", shrinkSegments, growSegments)
	}
	for n := 1; n <= rounds; n++ {
		at, err := w.CheckoutAt("app", "main", fmt.Sprintf("r%d", n), false)
		if err != nil {
			t.Fatalf("r%d: %v", n, err)
		}
		if !bytes.Equal(readFile(t, at), readFile(t, filepath.Join(keep, fmt.Sprintf("%d.db", n)))) {
			t.Fatalf("r%d: materialized checkpoint differs from the checkout it was taken from", n)
		}
	}
}

func TestShadowIdentityMismatchFallsBackToSnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	rec, ok := readSidecar(path)
	if !ok || !rec.Shadow {
		t.Fatalf("sidecar %+v (ok %v), want a shadow recorded", rec, ok)
	}
	rec.TXID++
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".sum", raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if res := mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("kind %q with a sidecar identity that is not the head, want snapshot", res.Kind)
	}
	// The snapshot re-established the shadow: the next one is a segment.
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	assertSegmentHead(t, w, "app", "main", "c", path, mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{}))
}

func TestEveryBoundthCheckpointIsASnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	w.SnapshotEvery = 4
	path := seedRows(t, w, "app", 1<<20, 1000)
	var kinds []string
	for i := 0; i < 8; i++ {
		if i > 0 {
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		}
		name := fmt.Sprintf("c%d", i)
		res := mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{})
		if res.Kind == "segment" {
			assertSegmentHead(t, w, "app", "main", name, path, res)
		}
		kinds = append(kinds, res.Kind)
	}
	want := "snapshot,segment,segment,segment,snapshot,segment,segment,segment"
	if got := strings.Join(kinds, ","); got != want {
		t.Fatalf("kinds = %s, want %s", got, want)
	}
}

func TestLargeChangeFractionForcesSnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 2<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "UPDATE t SET v = randomblob(4000) WHERE id % 10 != 0;")
	if res := mustCheckpointWith(t, w, "app", "main", "most", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("kind %q after rewriting most rows, want snapshot", res.Kind)
	}
}

func TestSnapshotOptionForces(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	res := mustCheckpointWith(t, w, "app", "main", "forced", CheckpointOptions{Snapshot: true})
	if res.Kind != "snapshot" {
		t.Fatalf("kind %q with Snapshot set, want snapshot", res.Kind)
	}
	ref := refOf(t, w, "app", "main")
	data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.Epoch, res.TXID))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != res.Bytes {
		t.Fatalf("result reports %d bytes, snapshot object is %d", res.Bytes, len(data))
	}
}

func TestNoShadowWhenReflinkUnsupported(t *testing.T) {
	w := newWS(t)
	reflinkUnsupportedForTest = true
	t.Cleanup(func() { reflinkUnsupportedForTest = false })
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	for _, name := range []string{"x", "y"} {
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		if res := mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{}); res.Kind != "snapshot" {
			t.Fatalf("%s: kind %q without clone support, want snapshot", name, res.Kind)
		}
		if _, err := os.Stat(shadowPath(path)); !os.IsNotExist(err) {
			t.Fatalf("%s: a shadow exists on a filesystem that cannot clone: %v", name, err)
		}
		if rec, ok := readSidecar(path); !ok || rec.Shadow {
			t.Fatalf("%s: sidecar %+v (ok %v), want shadow=false", name, rec, ok)
		}
	}
	if !bytes.Equal(readFile(t, path), exportBytes(t, w, "app", "main", "")) {
		t.Fatal("checkout differs from an independent export of the head")
	}
}

func TestForkFromSegmentHeadMaterializes(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	for i := 0; i < 3; i++ {
		mustSQL(t, path, fmt.Sprintf("INSERT INTO t (v) VALUES (randomblob(%d));", 100+i))
		name := fmt.Sprintf("s%d", i)
		assertSegmentHead(t, w, "app", "main", name, path, mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{}))
	}
	mustFork(t, w, "app", "main", "child", "")
	child := mustCheckout(t, w, "app", "child")
	if !bytes.Equal(readFile(t, child), readFile(t, path)) {
		t.Fatal("child checkout differs from the parent checkout it was forked from at a segment head")
	}
	assertCheckoutMatches(t, w, "app", "child", child)
}

// TestDivergedShadowFallsBackToSnapshot: a shadow whose bytes are not the
// recorded head (here, overwritten behind the sidecar's back) must never
// be diffed against; the checkpoint is a snapshot and the head is exact.
func TestDivergedShadowFallsBackToSnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	f, err := os.OpenFile(shadowPath(path), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xAB}, 64), 8192); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if res := mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("kind %q against a diverged shadow, want snapshot", res.Kind)
	}
	if !bytes.Equal(readFile(t, path), exportBytes(t, w, "app", "main", "")) {
		t.Fatal("checkout differs from an independent export of the head")
	}
}

// TestLateSnapshotLoserDeletesItsSnapshot: a checkpoint that read the ref
// before a rival committed a segment, and planned after the rival's stamp,
// writes a snapshot at the same txid and loses the CAS. It must delete that
// snapshot, or the chain would anchor the head on it instead of the
// winner's segment.
func TestLateSnapshotLoserDeletesItsSnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")

	paused, resume := make(chan struct{}), make(chan struct{})
	first := true
	checkpointAfterQuiesceForTest = func() {
		if first {
			first = false
			close(paused)
			<-resume
		}
	}
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	loserErr := make(chan error, 1)
	go func() {
		_, err := w.CheckpointWith("app", "main", "loser", nil, CheckpointOptions{})
		loserErr <- err
	}()
	<-paused
	winner := mustCheckpointWith(t, w, "app", "main", "winner", CheckpointOptions{})
	if winner.Kind != "segment" {
		t.Fatalf("winner kind %q, want segment", winner.Kind)
	}
	close(resume)
	if err := <-loserErr; err == nil || !strings.Contains(err.Error(), "lost a race") {
		t.Fatalf("late checkpoint error = %v, want a lost race", err)
	}
	ref := refOf(t, w, "app", "main")
	if storeHas(w, store.SnapshotKey(ref.Lineage, ref.Epoch, winner.TXID)) {
		t.Fatal("the late loser's snapshot survived beside the winning segment")
	}
	assertSegmentHead(t, w, "app", "main", "winner", path, winner)
}

// storeHas reports whether key is present in w's store, listing it rather
// than fetching it.
func storeHas(w *Workspace, key string) bool {
	keys, err := w.Store.B.List(key)
	if err != nil {
		return false
	}
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// TestSnapshotLoserKeepsSharedKeyWhenWinnerIsAlsoSnapshot: a segment that a
// crashed earlier attempt orphaned at the next txid must not make a losing
// snapshot writer delete the snapshot key it shares with the winning
// snapshot. The loser reads the winner's kind from its checkpoint entry,
// not from which keys exist.
func TestSnapshotLoserKeepsSharedKeyWhenWinnerIsAlsoSnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	ref := refOf(t, w, "app", "main")
	txid := ref.HeadTXID + 1
	orphan := store.SegmentKey(ref.Lineage, ref.Epoch, txid, txid)
	if _, err := w.Store.B.PutIf(orphan, []byte("orphan from a crashed attempt"), ""); err != nil {
		t.Fatal(err)
	}

	paused, resume := make(chan struct{}), make(chan struct{})
	first := true
	checkpointAfterQuiesceForTest = func() {
		if first {
			first = false
			close(paused)
			<-resume
		}
	}
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	loserErr := make(chan error, 1)
	go func() {
		_, err := w.CheckpointWith("app", "main", "loser", nil, CheckpointOptions{Snapshot: true})
		loserErr <- err
	}()
	<-paused
	winner := mustCheckpointWith(t, w, "app", "main", "winner", CheckpointOptions{Snapshot: true})
	close(resume)
	if err := <-loserErr; err == nil || !strings.Contains(err.Error(), "lost a race") {
		t.Fatalf("late checkpoint error = %v, want a lost race", err)
	}
	if winner.TXID != txid {
		t.Fatalf("winner at txid %d, want %d", winner.TXID, txid)
	}
	if got := refOf(t, w, "app", "main").Checkpoints["winner"].Kind; got != "snapshot" {
		t.Fatalf("winner's recorded kind %q, want snapshot", got)
	}
	if !storeHas(w, store.SnapshotKey(ref.Lineage, ref.Epoch, txid)) {
		t.Fatal("the losing snapshot writer deleted the snapshot key it shares with the winner")
	}
	if !storeHas(w, orphan) {
		t.Fatal("the orphan segment was touched; only the loser's own key is ever deleted")
	}
	at, err := w.CheckoutAt("app", "main", "winner", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, at), readFile(t, path)) {
		t.Fatal("winner materializes to bytes that differ from the checkout")
	}
}

// TestStaleSnapshotAtNextTXIDForcesASnapshot: a snapshot attempt that
// uploaded its object and then never landed its ref (a crash between PutIf
// and PutRef, or a PutRef failure that was not a CAS loss) leaves a
// snapshot at the next txid with no sidecar or shadow stamped. The next
// checkpoint would plan a segment at that same txid; the pre-write probe
// finds the stale snapshot and writes a snapshot over it instead, so no
// segment ever sits beside it and the head resolves to this checkpoint's
// own content.
func TestStaleSnapshotAtNextTXIDForcesASnapshot(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	ref := refOf(t, w, "app", "main")
	txid := ref.HeadTXID + 1
	var stale bytes.Buffer
	if _, err := ltxio.EncodeSnapshot(path, txid, &stale); err != nil {
		t.Fatal(err)
	}
	staleKey := store.SnapshotKey(ref.Lineage, ref.Epoch, txid)
	if _, err := w.Store.B.PutIf(staleKey, stale.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	// The checkout moves on after the failed attempt, so the stale snapshot
	// no longer matches what the next checkpoint commits.
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(200));")
	// Without the stale object this checkpoint would be a segment.
	headMembers, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := w.planSegment(path, ref, headMembers, CheckpointOptions{}); !ok {
		t.Fatal("precondition: planSegment would not choose a segment here")
	}

	res := mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{})
	if res.Kind != "snapshot" || res.TXID != txid {
		t.Fatalf("checkpoint = %+v, want a snapshot at txid %d", res, txid)
	}
	segKey := store.SegmentKey(ref.Lineage, ref.Epoch, txid, txid)
	if storeHas(w, segKey) {
		t.Fatal("a segment was written beside the stale snapshot")
	}
	data, _, err := w.Store.B.Get(staleKey)
	if err != nil {
		t.Fatalf("snapshot at txid %d: %v", txid, err)
	}
	if bytes.Equal(data, stale.Bytes()) {
		t.Fatal("the stale snapshot's bytes survived; the checkpoint did not overwrite them")
	}
	if int64(len(data)) != res.Bytes {
		t.Fatalf("result reports %d bytes, object is %d", res.Bytes, len(data))
	}
	members, err := w.Store.Chain(ref.Lineage, txid)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Key != staleKey || !members[0].Snapshot {
		t.Fatalf("head resolves through %+v, want exactly the snapshot %s", members, staleKey)
	}
	at, err := w.CheckoutAt("app", "main", "b", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, at), readFile(t, path)) {
		t.Fatal("the checkpoint materializes to bytes that differ from the checkout")
	}

	// The next checkpoint, with no stale object ahead of it, is a segment
	// again.
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
	next := mustCheckpointWith(t, w, "app", "main", "c", CheckpointOptions{})
	assertSegmentHead(t, w, "app", "main", "c", path, next)
}

// TestDestroyRemovesShadowAndItsTemp: Destroy removes a checkout's shadow
// and a leftover shadow temp from an interrupted refresh along with the
// checkout, so neither outlives the branch.
func TestDestroyRemovesShadowAndItsTemp(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<16)
	mustFork(t, w, "app", "main", "b", "seed")
	path := mustCheckout(t, w, "app", "b")
	if _, err := os.Stat(shadowPath(path)); err != nil {
		t.Fatalf("precondition: no shadow after checkout: %v", err)
	}
	if err := os.WriteFile(shadowPath(path)+".tmp", []byte("interrupted"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "b", false); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{path, path + ".sum", shadowPath(path), shadowPath(path) + ".tmp"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s survived destroy: %v", p, err)
		}
	}
}

// TestCheckpointResolvesChainOnce: an at-rest checkpoint resolves the
// head's chain (one List of the lineage prefix on a remote backend) at most
// once — CheckpointWith resolves it and hands it to planSegment, which
// never re-resolves — and not at all when no segment is possible (a forced
// snapshot, or no shadow because the filesystem cannot clone).
func TestCheckpointResolvesChainOnce(t *testing.T) {
	countResolves := func(t *testing.T, w *Workspace, name string, opts CheckpointOptions) (CheckpointResult, int) {
		t.Helper()
		lineage := refOf(t, w, "app", "main").Lineage
		cb := newRPCCountBackend(w.Store.B)
		orig := w.Store.B
		w.Store.B = cb
		defer func() { w.Store.B = orig }()
		res := mustCheckpointWith(t, w, "app", "main", name, opts)
		return res, cb.listCount(store.LineagePrefix(lineage))
	}

	t.Run("segment", func(t *testing.T) {
		w := newWS(t)
		requireClone(t, w)
		path := seedRows(t, w, "app", 1<<20, 4000)
		mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		res, n := countResolves(t, w, "b", CheckpointOptions{})
		if res.Kind != "segment" {
			t.Fatalf("kind %q, want segment", res.Kind)
		}
		if n != 1 {
			t.Fatalf("segment checkpoint resolved the chain %d times, want exactly 1", n)
		}
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		if _, n := countResolves(t, w, "c", CheckpointOptions{Snapshot: true}); n != 0 {
			t.Fatalf("forced snapshot resolved the chain %d times, want 0", n)
		}
	})
	t.Run("no shadow", func(t *testing.T) {
		w := newWS(t)
		reflinkUnsupportedForTest = true
		t.Cleanup(func() { reflinkUnsupportedForTest = false })
		path := seedRows(t, w, "app", 1<<20, 4000)
		mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		if res, n := countResolves(t, w, "b", CheckpointOptions{}); res.Kind != "snapshot" || n != 0 {
			t.Fatalf("no-shadow checkpoint: kind %q, resolved the chain %d times; want snapshot, 0", res.Kind, n)
		}
	})
}
