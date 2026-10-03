package ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// casGate wraps a workspace's backend so a test can hold the checkpoint's
// head write: every PutIf of one ref key whose body releases the lease (see
// releasesLease) is held until the test releases it, each held call sending
// its own release channel on arrived and waiting for it to close; the lease
// acquire and its renewals carry the holder and pass straight through.
// holdObject arms a one-shot hold of the next PutIf of one object key (a
// checkpoint's create-only upload) the same way, on objArrived. Head
// forwards to the wrapped backend's Header, or fails with headErr, or
// returns the forwarded etag rewritten by headEtag when that is set. Every
// Get is counted per key in gets. DeleteIf forwards, so a Destroy through
// the gate keeps the wrapped backend's conditional delete.
type casGate struct {
	store.Backend
	refKey     string
	on         atomic.Bool
	arrived    chan chan struct{}
	headErr    error
	headEtag   func(etag string) string
	objKey     string
	objArmed   atomic.Bool
	objArrived chan chan struct{}
	getErrKey  string
	mu         sync.Mutex
	gets       map[string]int
}

// Get fails for getErrKey, and forwards everything else, counting each
// call against its key.
func (g *casGate) Get(key string) ([]byte, string, error) {
	g.mu.Lock()
	if g.gets == nil {
		g.gets = map[string]int{}
	}
	g.gets[key]++
	g.mu.Unlock()
	if g.getErrKey != "" && key == g.getErrKey {
		return nil, "", errors.New("get unavailable")
	}
	return g.Backend.Get(key)
}

// getCount is how many times key has been Get through the gate.
func (g *casGate) getCount(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gets[key]
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
	if key == g.refKey && g.on.Load() && releasesLease(data) {
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

// DeleteIf keeps the wrapped backend's conditional delete visible through
// the gate (store.DeleteRefIf type-asserts for it), as refWriteRecorder
// does. Without it a Destroy through the gate falls back to Local's plain
// Delete, which takes no lock: a lease renewal that passed its etag check
// just before can then rename the ref back into place after the delete,
// and the destroyed branch returns, still leased.
func (g *casGate) DeleteIf(key, ifMatch string) error {
	if cd, ok := g.Backend.(store.ConditionalDeleter); ok {
		return cd.DeleteIf(key, ifMatch)
	}
	return g.Backend.Delete(key)
}

func (g *casGate) Head(key string) (string, int64, error) {
	if g.headErr != nil {
		return "", 0, g.headErr
	}
	etag, size, err := g.Backend.(store.Header).Head(key)
	if err == nil && g.headEtag != nil {
		etag = g.headEtag(etag)
	}
	return etag, size, err
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

// releasesLease reports whether a ref body carries no lease holder: an at-rest
// checkpoint's head write, which advances the head and releases the lease in
// one write.
func releasesLease(data []byte) bool {
	var r struct {
		LeaseHolder string `json:"lease_holder"`
	}
	return json.Unmarshal(data, &r) == nil && r.LeaseHolder == ""
}

// checkpointWhileHeld runs CheckpointWith(app@main, "a") with g holding its
// head write, runs during while it is held (its object is already in the
// store and its lease is still live), then releases it and returns its
// result.
func checkpointWhileHeld(t *testing.T, w *Workspace, g *casGate, opts CheckpointOptions, during func()) (CheckpointResult, error) {
	t.Helper()
	type result struct {
		res CheckpointResult
		err error
	}
	done := make(chan result, 1)
	g.on.Store(true)
	go func() {
		res, err := w.CheckpointWith("app", "main", "a", nil, opts)
		done <- result{res, err}
	}()
	var release chan struct{}
	select {
	case release = <-g.arrived:
	case r := <-done:
		g.on.Store(false)
		t.Fatalf("the checkpoint ended before its head write: %v", r.err)
	}
	g.on.Store(false)
	during()
	close(release)
	r := <-done
	return r.res, r.err
}

// TestOverwriteWithIdenticalBytesStaysTrusted: while a snapshot checkpoint
// is held at its head write, a write lands on the checkout and an object of
// that newer state replaces the checkpoint's own at its private key. No
// offshoot writer can do that any more (the key is under an epoch only this
// checkpoint's lease minted); the replace stands for anything outside
// offshoot that does. The overwrite is detected and counted, but the store
// now holds exactly what the checkout holds, so the stamp is trusted, the
// shadow kept, the checkout reads clean, and the next checkpoint can be a
// segment that materializes to the checkout's bytes.
func TestOverwriteWithIdenticalBytesStaysTrusted(t *testing.T) {
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		t.Run(name, func(t *testing.T) {
			w := newW(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			overwrites := countOverwrites(t)
			before := refOf(t, w, "app", "main")
			key := privateSnapshotKey(before)
			g := gateRefCAS(w, "app", "main")

			a, err := checkpointWhileHeld(t, w, g, CheckpointOptions{Snapshot: true}, func() {
				mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
				if err := w.Store.B.Put(key, encodeCheckout(t, path, before.HeadTXID+1)); err != nil {
					t.Fatal(err)
				}
			})
			if err != nil {
				t.Fatalf("checkpoint a: %v", err)
			}
			if a.Kind != "snapshot" {
				t.Fatalf("kind %q, want snapshot", a.Kind)
			}
			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != a.TXID {
				t.Fatalf("head txid %d, want %d", ref.HeadTXID, a.TXID)
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d, want 1", got)
			}
			data, _, err := w.Store.B.Get(key)
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
				t.Fatalf("the replace did not leave the newer encode in the store: stored %016x, checkout %016x", stored, live)
			}
			assertTrustedStamp(t, path)
			rec, _ := readSidecar(path)
			if rec.TXID != a.TXID || rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch {
				t.Fatalf("sidecar identity %s/%d/%d, want %s/%d/%d", rec.Lineage, rec.Epoch, rec.TXID, ref.Lineage, ref.HeadEpoch, a.TXID)
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

// TestOlderEncodeOverwriteReadsModified: an object of the checkout's OLDER
// state replaces the checkpoint's own at its private key while the
// checkpoint is held at its head write. The head now resolves to the older
// content while the checkout holds the newer: the checkpoint must stamp
// checksum 0 with a hash no file matches, so the checkout reads
// "modified", a fork warns, and the next checkpoint is a snapshot that
// materializes to the checkout's bytes.
func TestOlderEncodeOverwriteReadsModified(t *testing.T) {
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		t.Run(name, func(t *testing.T) {
			w := newW(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			overwrites := countOverwrites(t)
			before := refOf(t, w, "app", "main")
			key := privateSnapshotKey(before)
			older := encodeCheckout(t, path, before.HeadTXID+1)
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
			g := gateRefCAS(w, "app", "main")

			a, err := checkpointWhileHeld(t, w, g, CheckpointOptions{Snapshot: true}, func() {
				if err := w.Store.B.Put(key, older); err != nil {
					t.Fatal(err)
				}
			})
			if err != nil {
				t.Fatalf("checkpoint a: %v", err)
			}
			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != a.TXID {
				t.Fatalf("head txid %d, want %d", ref.HeadTXID, a.TXID)
			}
			data, _, err := w.Store.B.Get(key)
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
				t.Fatal("the replace did not leave the older encode in the store")
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
// while the head write is held) must not get the encode's checksum stamped
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
	res, err := checkpointWhileHeld(t, w, g, CheckpointOptions{}, func() {
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
	})
	if err != nil {
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
	if out := captureStderr(t, func() { w.warnIfUncheckpointed("app", "main", ref, "forking last committed state") }); !strings.Contains(out, "un-checkpointed changes") {
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

// TestIdenticalOverwriteIsNotFlagged: an object of the same checkout state
// replaces the checkpoint's own at its private key: byte-different (an LTX
// header carries an encode timestamp), content-equal, so nothing is
// flagged: the checksum and shadow stay.
func TestIdenticalOverwriteIsNotFlagged(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	overwrites := countOverwrites(t)
	before := refOf(t, w, "app", "main")
	g := gateRefCAS(w, "app", "main")

	if _, err := checkpointWhileHeld(t, w, g, CheckpointOptions{Snapshot: true}, func() {
		if err := w.Store.B.Put(privateSnapshotKey(before), encodeCheckout(t, path, before.HeadTXID+1)); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
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

// TestReformattedEtagSkipsTheGet: an S3-compatible provider may return
// the same object's etag in a different shape on HEAD than on PUT (bare
// where the put was quoted, a weak "W/" prefix, upper-case hex). That is
// still our own object, so the post-CAS check must trust it from the Head
// alone: no Get of the object, nothing counted as an overwrite, and the
// stamp trusted. A genuinely different etag still costs the Get, which
// then finds the same content and is likewise not counted.
func TestReformattedEtagSkipsTheGet(t *testing.T) {
	reformat := func(etag string) string {
		return `W/"` + strings.ToUpper(strings.Trim(etag, `"`)) + `"`
	}
	different := func(string) string { return `"not-the-etag-the-put-returned"` }
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		for _, tc := range []struct {
			name     string
			rewrite  func(string) string
			wantGets int
		}{
			{"reformatted", reformat, 0},
			{"different", different, 1},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				w := newW(t)
				requireClone(t, w)
				seedDB(t, w, "app", 1<<20)
				path := w.CheckoutPath("app", "main")
				mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
				before := refOf(t, w, "app", "main")
				key := privateSnapshotKey(before)
				overwrites := countOverwrites(t)
				g := gateRefCAS(w, "app", "main")
				g.headEtag = tc.rewrite

				if res := mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{Snapshot: true}); res.Kind != "snapshot" {
					t.Fatalf("kind %q, want snapshot", res.Kind)
				}
				if got := g.getCount(key); got != tc.wantGets {
					t.Fatalf("the checkpoint Get its own object %d times, want %d", got, tc.wantGets)
				}
				if got := overwrites.Load(); got != 0 {
					t.Fatalf("overwrite counter %d, want 0", got)
				}
				assertTrustedStamp(t, path)
			})
		}
	}
}

// TestUnverifiableOverwriteIsLoggedAndDistrusted: when the object at the
// private key no longer carries the checkpoint's etag and cannot be
// fetched, the checkpoint still succeeds, the failure is logged, it counts
// as an overwrite, and the stamp is distrusted.
func TestUnverifiableOverwriteIsLoggedAndDistrusted(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	key := privateSnapshotKey(before)
	overwrites := countOverwrites(t)
	g := gateRefCAS(w, "app", "main")
	g.getErrKey = key

	var err error
	stderr := captureStderr(t, func() {
		_, err = checkpointWhileHeld(t, w, g, CheckpointOptions{Snapshot: true}, func() {
			if perr := w.Store.B.Put(key, []byte("not an LTX object")); perr != nil {
				t.Fatal(perr)
			}
		})
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
