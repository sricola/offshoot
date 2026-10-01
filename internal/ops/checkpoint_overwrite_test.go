package ops

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// casGate wraps a workspace's backend so a test can hold every PutIf of one
// ref key (the checkpoint's ref CAS) until it releases that call: each held
// call sends its own release channel on arrived and waits for it to close.
// holdObject arms a one-shot hold of the next PutIf of one object key (a
// checkpoint's create-only upload) the same way, on objArrived. Head
// forwards to the wrapped backend's Header, or fails with headErr.
type casGate struct {
	store.Backend
	refKey     string
	on         atomic.Bool
	arrived    chan chan struct{}
	headErr    error
	objKey     string
	objArmed   atomic.Bool
	objArrived chan chan struct{}
	getErrKey  string
}

// Get fails for getErrKey, and forwards everything else.
func (g *casGate) Get(key string) ([]byte, string, error) {
	if g.getErrKey != "" && key == g.getErrKey {
		return nil, "", errors.New("get unavailable")
	}
	return g.Backend.Get(key)
}

func gateRefCAS(w *Workspace, db, branch string) *casGate {
	g := &casGate{Backend: w.Store.B, refKey: store.RefKey(db, branch), arrived: make(chan chan struct{}), objArrived: make(chan chan struct{})}
	w.Store.B = g
	return g
}

// holdObject arms the one-shot hold of key's next PutIf. Call it before
// the checkpoint that should be held starts.
func (g *casGate) holdObject(key string) {
	g.objKey = key
	g.objArmed.Store(true)
}

func (g *casGate) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == g.refKey && g.on.Load() {
		release := make(chan struct{})
		g.arrived <- release
		<-release
	}
	if key == g.objKey && g.objArmed.CompareAndSwap(true, false) {
		release := make(chan struct{})
		g.objArrived <- release
		<-release
	}
	return g.Backend.PutIf(key, data, ifMatch)
}

func (g *casGate) Head(key string) (string, int64, error) {
	if g.headErr != nil {
		return "", 0, g.headErr
	}
	return g.Backend.(store.Header).Head(key)
}

// countOverwrites installs ObserveCheckpointOverwrite for the test and
// returns its counter.
func countOverwrites(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	ObserveCheckpointOverwrite = func() { n.Add(1) }
	t.Cleanup(func() { ObserveCheckpointOverwrite = nil })
	return &n
}

// raceSnapshots runs two forced-snapshot checkpoints, "a" and "b", on
// app@main so that a uploads its object first, b overwrites it at the same
// key, a then wins the ref CAS and b loses it. between runs after a's
// upload and before b's encode. It returns a's result.
func raceSnapshots(t *testing.T, w *Workspace, g *casGate, between func()) CheckpointResult {
	t.Helper()
	g.on.Store(true)
	defer g.on.Store(false)
	type result struct {
		res CheckpointResult
		err error
	}
	aDone, bDone := make(chan result, 1), make(chan result, 1)
	go func() {
		res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
		aDone <- result{res, err}
	}()
	releaseA := <-g.arrived
	between()
	go func() {
		res, err := w.CheckpointWith("app", "main", "b", nil, CheckpointOptions{Snapshot: true})
		bDone <- result{res, err}
	}()
	releaseB := <-g.arrived
	close(releaseA)
	a := <-aDone
	if a.err != nil {
		t.Fatalf("checkpoint a: %v", a.err)
	}
	close(releaseB)
	if b := <-bDone; b.err == nil || !strings.Contains(b.err.Error(), "lost a race") {
		t.Fatalf("checkpoint b error = %v, want a lost race", b.err)
	}
	return a.res
}

// TestOverwriteWithIdenticalBytesStaysTrusted: two snapshot checkpoints
// race on one branch with a write to the checkout between their encodes;
// the loser encoded the newer state, and its object replaces the winner's
// at the shared key. The overwrite is detected and counted, but the store
// now holds exactly what the checkout holds, so the winner stamps that
// content's checksum as trusted (not its own encode's), keeps a shadow,
// and reads clean; the next checkpoint can be a segment and materializes
// to the checkout's bytes.
func TestOverwriteWithIdenticalBytesStaysTrusted(t *testing.T) {
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		t.Run(name, func(t *testing.T) {
			w := newW(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			overwrites := countOverwrites(t)
			g := gateRefCAS(w, "app", "main")

			a := raceSnapshots(t, w, g, func() {
				mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
			})
			if a.Kind != "snapshot" {
				t.Fatalf("winner kind %q, want snapshot", a.Kind)
			}
			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != a.TXID {
				t.Fatalf("head txid %d, want the winner's %d", ref.HeadTXID, a.TXID)
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d, want 1", got)
			}
			data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.Epoch, a.TXID))
			if err != nil {
				t.Fatal(err)
			}
			stored, err := ltxio.TrailerPostApplyChecksum(data)
			if err != nil {
				t.Fatal(err)
			}
			live, err := ltxio.ChecksumDatabase(path)
			if err != nil {
				t.Fatal(err)
			}
			if stored != live {
				t.Fatalf("the race did not leave the newer encode in the store: stored %016x, checkout %016x", stored, live)
			}
			assertTrustedStamp(t, path)
			rec, _ := readSidecar(path)
			if rec.TXID != a.TXID || rec.Lineage != ref.Lineage {
				t.Fatalf("sidecar identity %s/%d, want %s/%d", rec.Lineage, rec.TXID, ref.Lineage, a.TXID)
			}
			if state, sum := checkoutState(path, ref); state != "clean" || sum != stored {
				t.Fatalf("checkoutState = %q %016x, want clean %016x", state, sum, stored)
			}

			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			next := mustCheckpointWith(t, w, "app", "main", "next", CheckpointOptions{})
			if next.Kind != "segment" {
				t.Fatalf("checkpoint after a trusted overwrite: kind %q, want segment", next.Kind)
			}
			at, err := w.CheckoutAt("app", "main", "next", false)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("materialized head differs from the checkout after the next checkpoint")
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d after the next checkpoint, want 1", got)
			}
		})
	}
}

// TestOlderEncodeOverwriteReadsModified: the loser encodes first, then a
// write lands, then the winner encodes the newer state and uploads it; the
// loser's older object then replaces the winner's at the shared key before
// the winner's CAS. The head now resolves to the older content while the
// checkout holds the newer: the winner must stamp checksum 0 with a hash no
// file matches, so the checkout reads "modified", a fork warns, and the
// next checkpoint is a snapshot that materializes to the checkout's bytes.
func TestOlderEncodeOverwriteReadsModified(t *testing.T) {
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		t.Run(name, func(t *testing.T) {
			w := newW(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			overwrites := countOverwrites(t)
			g := gateRefCAS(w, "app", "main")
			before := refOf(t, w, "app", "main")
			snapKey := store.SnapshotKey(before.Lineage, before.Epoch, before.HeadTXID+1)
			g.holdObject(snapKey)
			g.on.Store(true)

			type result struct {
				res CheckpointResult
				err error
			}
			bDone, aDone := make(chan result, 1), make(chan result, 1)
			go func() { // the older encode
				res, err := w.CheckpointWith("app", "main", "b", nil, CheckpointOptions{Snapshot: true})
				bDone <- result{res, err}
			}()
			releaseBObj := <-g.objArrived
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
			go func() { // the newer encode, which wins
				res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
				aDone <- result{res, err}
			}()
			releaseA := <-g.arrived
			close(releaseBObj) // b's create-only put fails, and it overwrites a's object
			releaseB := <-g.arrived
			g.on.Store(false)
			close(releaseA)
			a := <-aDone
			if a.err != nil {
				t.Fatalf("checkpoint a: %v", a.err)
			}
			close(releaseB)
			if b := <-bDone; b.err == nil || !strings.Contains(b.err.Error(), "lost a race") {
				t.Fatalf("checkpoint b error = %v, want a lost race", b.err)
			}

			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != a.res.TXID {
				t.Fatalf("head txid %d, want the winner's %d", ref.HeadTXID, a.res.TXID)
			}
			data, _, err := w.Store.B.Get(snapKey)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := ltxio.TrailerPostApplyChecksum(data)
			if err != nil {
				t.Fatal(err)
			}
			live, err := ltxio.ChecksumDatabase(path)
			if err != nil {
				t.Fatal(err)
			}
			if stored == live {
				t.Fatal("the race did not leave the older encode in the store")
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d, want 1", got)
			}
			assertDistrustedStamp(t, w, path, ref)

			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			assertRecoveringSnapshot(t, w, path, "next")
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d after the next checkpoint, want 1", got)
			}
		})
	}
}

// TestWriteBetweenEncodeAndStampDistrusts: with no racer at all, a write
// that lands in the checkout after the encode and before the stamp (here,
// while the ref CAS is held) must not get the encode's checksum stamped
// against the new bytes: the store holds the pre-write content, so the
// checkout reads "modified" and the next checkpoint is a snapshot.
func TestWriteBetweenEncodeAndStampDistrusts(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	overwrites := countOverwrites(t)
	g := gateRefCAS(w, "app", "main")
	g.on.Store(true)

	done := make(chan error, 1)
	var res CheckpointResult
	go func() {
		var err error
		res, err = w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
		done <- err
	}()
	release := <-g.arrived
	g.on.Store(false)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if res.Kind != "segment" {
		t.Fatalf("kind %q, want segment (the shadow was valid)", res.Kind)
	}
	if got := overwrites.Load(); got != 0 {
		t.Fatalf("overwrite counter %d, want 0: nothing replaced the object", got)
	}
	ref := refOf(t, w, "app", "main")
	at, err := w.CheckoutAt("app", "main", "a", false)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(readFile(t, at), readFile(t, path)) {
		t.Fatal("the committed head equals the checkout; the write did not land in the window")
	}
	assertDistrustedStamp(t, w, path, ref)
	assertRecoveringSnapshot(t, w, path, "next")
}

// TestMixedKindRaceDistrustsTheSegmentWinner: a snapshot checkpoint encodes
// first and is held before its upload; a write lands; a segment checkpoint
// plans (no snapshot at txid yet), uploads and reaches its CAS; then the
// snapshot is uploaded and its writer is held before its CAS, so it never
// reaches its loser-side cleanup while the segment wins. The head now
// resolves to the snapshot's older content (the resolver anchors on the
// newest snapshot at or below it), so the winner must not trust its own
// segment's checksum: checksum 0, no shadow, "modified", one overwrite
// counted; the next checkpoint is a snapshot whose materialization equals
// the checkout. Released afterwards, the live loser still deletes its
// snapshot.
func TestMixedKindRaceDistrustsTheSegmentWinner(t *testing.T) {
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		t.Run(name, func(t *testing.T) {
			w := newW(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			overwrites := countOverwrites(t)
			g := gateRefCAS(w, "app", "main")
			before := refOf(t, w, "app", "main")
			txid := before.HeadTXID + 1
			snapKey := store.SnapshotKey(before.Lineage, before.Epoch, txid)
			g.holdObject(snapKey)
			g.on.Store(true)

			type result struct {
				res CheckpointResult
				err error
			}
			snapDone, segDone := make(chan result, 1), make(chan result, 1)
			go func() {
				res, err := w.CheckpointWith("app", "main", "snap", nil, CheckpointOptions{Snapshot: true})
				snapDone <- result{res, err}
			}()
			releaseSnapObj := <-g.objArrived
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
			go func() {
				res, err := w.CheckpointWith("app", "main", "seg", nil, CheckpointOptions{})
				segDone <- result{res, err}
			}()
			releaseSeg := <-g.arrived
			close(releaseSnapObj)
			releaseSnap := <-g.arrived // the snapshot is up; its writer is held before its CAS
			g.on.Store(false)
			close(releaseSeg)
			seg := <-segDone
			if seg.err != nil {
				t.Fatalf("segment checkpoint: %v", seg.err)
			}
			if seg.res.Kind != "segment" || seg.res.TXID != txid {
				t.Fatalf("winner %s at %d, want a segment at %d", seg.res.Kind, seg.res.TXID, txid)
			}
			if !storeHas(w, snapKey) {
				t.Fatal("the loser's snapshot is not beside the winning segment")
			}
			ref := refOf(t, w, "app", "main")
			at, err := w.CheckoutAt("app", "main", "seg", false)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("the head resolves to the checkout's bytes; the snapshot did not anchor it")
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d, want 1", got)
			}
			assertDistrustedStamp(t, w, path, ref)

			assertRecoveringSnapshot(t, w, path, "next")

			close(releaseSnap)
			if r := <-snapDone; r.err == nil || !strings.Contains(r.err.Error(), "lost a race") {
				t.Fatalf("snapshot checkpoint error = %v, want a lost race", r.err)
			}
			if storeHas(w, snapKey) {
				t.Fatal("the live loser did not delete its snapshot beside the winning segment")
			}
			at, err = w.CheckoutAt("app", "main", "next", true)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("the head no longer equals the checkout after the loser's cleanup")
			}
		})
	}
}

// assertDistrustedStamp checks a checkout's sidecar after a checkpoint that
// could not vouch for its content: the head's identity, checksum 0, the
// untrusted hash, no shadow (and no shadow file), a "modified" verdict, and
// a fork warning about un-checkpointed changes.
func assertDistrustedStamp(t *testing.T, w *Workspace, path string, ref store.Ref) {
	t.Helper()
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("no sidecar after the checkpoint")
	}
	if rec.TXID != ref.HeadTXID || rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch {
		t.Fatalf("sidecar identity %s/%d/%d, want %s/%d/%d", rec.Lineage, rec.Epoch, rec.TXID, ref.Lineage, ref.HeadEpoch, ref.HeadTXID)
	}
	if rec.PostApplyChecksum != 0 || rec.Shadow || rec.Hash != untrustedHash(ref.HeadTXID) {
		t.Fatalf("sidecar: checksum %016x shadow %v hash %q, want 0, false, %q", rec.PostApplyChecksum, rec.Shadow, rec.Hash, untrustedHash(ref.HeadTXID))
	}
	if _, err := os.Stat(shadowPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shadow still present (stat err %v)", err)
	}
	if state, sum := checkoutState(path, ref); state != "modified" || sum != 0 {
		t.Fatalf("checkoutState = %q %016x, want modified 0", state, sum)
	}
	if out := captureStderr(t, func() { w.warnIfUncheckpointed("app", "main", ref) }); !strings.Contains(out, "un-checkpointed changes") {
		t.Fatalf("a fork would not warn; stderr %q", out)
	}
}

// assertRecoveringSnapshot takes the checkpoint after a distrusted stamp:
// it must be a snapshot, and must materialize to the checkout's bytes.
func assertRecoveringSnapshot(t *testing.T, w *Workspace, path, name string) {
	t.Helper()
	next := mustCheckpointWith(t, w, "app", "main", name, CheckpointOptions{})
	if next.Kind != "snapshot" {
		t.Fatalf("checkpoint after a distrusted stamp: kind %q, want snapshot", next.Kind)
	}
	at, err := w.CheckoutAt("app", "main", name, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, at), readFile(t, path)) {
		t.Fatal("materialized head differs from the checkout after the recovering snapshot")
	}
	assertTrustedStamp(t, path)
}

// TestIdenticalOverwriteIsNotFlagged: the same race with nothing written
// between the encodes. The two objects can differ in bytes (an LTX header
// carries an encode timestamp) but not in content, so nothing is flagged:
// the checksum and shadow stay.
func TestIdenticalOverwriteIsNotFlagged(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	overwrites := countOverwrites(t)
	g := gateRefCAS(w, "app", "main")

	raceSnapshots(t, w, g, func() {})
	if got := overwrites.Load(); got != 0 {
		t.Fatalf("overwrite counter %d, want 0", got)
	}
	assertTrustedStamp(t, path)
}

// TestOverwriteDetectionSkipsWhenHeadUnsupported pins the contract that a
// failing Head never fails a checkpoint: it is logged, and the checkpoint
// stamps its checksum and shadow as usual.
func TestOverwriteDetectionSkipsWhenHeadUnsupported(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	overwrites := countOverwrites(t)
	g := gateRefCAS(w, "app", "main")
	g.headErr = errors.New("head unsupported")

	var err error
	stderr := captureStderr(t, func() {
		_, err = w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
	})
	if err != nil {
		t.Fatalf("checkpoint with a failing Head: %v", err)
	}
	if !strings.Contains(stderr, "head unsupported") {
		t.Fatalf("the Head failure was not logged; stderr %q", stderr)
	}
	if got := overwrites.Load(); got != 0 {
		t.Fatalf("overwrite counter %d, want 0", got)
	}
	assertTrustedStamp(t, path)
}

// TestOrphanOverwriteIsNotFlagged: a checkpoint that overwrites an orphan a
// crashed attempt left at its key (the unconditional-Put path, which returns
// no etag) and wins the CAS holds its own content, so nothing is flagged.
func TestOrphanOverwriteIsNotFlagged(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	ref := refOf(t, w, "app", "main")
	var orphan bytes.Buffer
	if _, err := ltxio.EncodeSnapshot(path, ref.HeadTXID+1, &orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.B.PutIf(store.SnapshotKey(ref.Lineage, ref.Epoch, ref.HeadTXID+1), orphan.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(200));")
	overwrites := countOverwrites(t)

	if res := mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{}); res.Kind != "snapshot" {
		t.Fatalf("kind %q, want snapshot over the orphan", res.Kind)
	}
	if got := overwrites.Load(); got != 0 {
		t.Fatalf("overwrite counter %d, want 0", got)
	}
	assertTrustedStamp(t, path)
}

// TestUnverifiableOverwriteIsLoggedAndDistrusted: when the object cannot
// be fetched after an etag mismatch (here, our own unconditional overwrite
// of an orphan, which returns no etag), the checkpoint still succeeds, the
// failure is logged, it counts as an overwrite, and the stamp is distrusted.
func TestUnverifiableOverwriteIsLoggedAndDistrusted(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	ref := refOf(t, w, "app", "main")
	key := store.SnapshotKey(ref.Lineage, ref.Epoch, ref.HeadTXID+1)
	if _, err := w.Store.B.PutIf(key, []byte("orphan from a crashed attempt"), ""); err != nil {
		t.Fatal(err)
	}
	overwrites := countOverwrites(t)
	g := gateRefCAS(w, "app", "main")
	g.getErrKey = key

	var err error
	stderr := captureStderr(t, func() {
		_, err = w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	})
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !strings.Contains(stderr, "could not verify checkpoint object") || !strings.Contains(stderr, "get unavailable") {
		t.Fatalf("the unverifiable object was not logged; stderr %q", stderr)
	}
	if got := overwrites.Load(); got != 1 {
		t.Fatalf("overwrite counter %d, want 1", got)
	}
	g.getErrKey = ""
	assertDistrustedStamp(t, w, path, refOf(t, w, "app", "main"))
}

// assertTrustedStamp checks a checkout's sidecar records its real post-apply
// checksum and a shadow that exists.
func assertTrustedStamp(t *testing.T, path string) {
	t.Helper()
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("no sidecar")
	}
	want, err := ltxio.ChecksumDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec.PostApplyChecksum != want {
		t.Fatalf("sidecar checksum %016x, want %016x", rec.PostApplyChecksum, want)
	}
	if !rec.Shadow {
		t.Fatal("sidecar records no shadow")
	}
	if _, err := os.Stat(shadowPath(path)); err != nil {
		t.Fatalf("shadow missing: %v", err)
	}
}
