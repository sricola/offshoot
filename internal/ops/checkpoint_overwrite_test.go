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
// Head forwards to the wrapped backend's Header, or fails with headErr.
type casGate struct {
	store.Backend
	refKey  string
	on      atomic.Bool
	arrived chan chan struct{}
	headErr error
}

func gateRefCAS(w *Workspace, db, branch string) *casGate {
	g := &casGate{Backend: w.Store.B, refKey: store.RefKey(db, branch), arrived: make(chan chan struct{})}
	w.Store.B = g
	return g
}

func (g *casGate) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == g.refKey && g.on.Load() {
		release := make(chan struct{})
		g.arrived <- release
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

// TestSameKindOverwriteIsDetectedAndUntrusted: two snapshot checkpoints race
// on one branch with a write to the checkout between their encodes, so the
// loser's different bytes replace the winner's object at the shared key.
// The winner's post-CAS check sees it: no checksum is trusted, the shadow
// is gone, the overwrite is counted once, and the next checkpoint is a
// snapshot whose materialization equals the checkout.
func TestSameKindOverwriteIsDetectedAndUntrusted(t *testing.T) {
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
			rec, ok := readSidecar(path)
			if !ok {
				t.Fatal("no sidecar after the winning checkpoint")
			}
			if rec.PostApplyChecksum != 0 || rec.Shadow {
				t.Fatalf("sidecar after an overwrite: checksum %016x shadow %v, want 0 and false", rec.PostApplyChecksum, rec.Shadow)
			}
			if rec.TXID != a.TXID || rec.Lineage != ref.Lineage {
				t.Fatalf("sidecar identity %s/%d, want %s/%d", rec.Lineage, rec.TXID, ref.Lineage, a.TXID)
			}
			if _, err := os.Stat(shadowPath(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("shadow still present after an overwrite (stat err %v)", err)
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d, want 1", got)
			}

			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			next := mustCheckpointWith(t, w, "app", "main", "next", CheckpointOptions{})
			if next.Kind != "snapshot" {
				t.Fatalf("checkpoint after an overwrite: kind %q, want snapshot", next.Kind)
			}
			at, err := w.CheckoutAt("app", "main", "next", false)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("materialized head differs from the checkout after the recovering snapshot")
			}
			if got := overwrites.Load(); got != 1 {
				t.Fatalf("overwrite counter %d after the next checkpoint, want 1", got)
			}
		})
	}
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
