package ops

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// TestCheckpointHolderIsPerCall: every at-rest checkpoint call gets its own
// holder, "checkpoint:<host>/<pid>/<8 hex>". The nonce is what makes the
// epoch private: AcquireLease treats the same holder on a live lease as a
// self-renew with no epoch bump, so two calls in one process sharing a
// holder would share an epoch and an object key.
func TestCheckpointHolderIsPerCall(t *testing.T) {
	shape := regexp.MustCompile(`^checkpoint:` + regexp.QuoteMeta(LocalHolder()) + `/[0-9a-f]{8}$`)
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		h := newCheckpointHolder()
		if !shape.MatchString(h) {
			t.Fatalf("holder %q is not checkpoint:<host>/<pid>/<8 hex>", h)
		}
		if !isCheckpointHolder(h) {
			t.Fatalf("isCheckpointHolder(%q) = false", h)
		}
		if seen[h] {
			t.Fatalf("holder %q repeated within 100 calls", h)
		}
		seen[h] = true
	}
	if isCheckpointHolder(LocalHolder()) {
		t.Fatal("a session's holder reads as a checkpoint's")
	}
}

// privateSnapshotKey is the snapshot key the next at-rest checkpoint writes
// on a branch whose ref is before: its lease acquire bumps the epoch by
// exactly one (no live lease, as in every test that calls this).
func privateSnapshotKey(before store.Ref) string {
	return store.SnapshotKey(before.Lineage, before.Epoch+1, before.HeadTXID+1)
}

// encodeCheckout quiesces the checkout at path and encodes it as a snapshot
// at txid: the bytes some writer other than the checkpoint under test puts
// at a key.
func encodeCheckout(t *testing.T, path string, txid uint64) []byte {
	t.Helper()
	if err := quiesce(path); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := ltxio.EncodeSnapshot(path, txid, &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// assertLeaseReleased checks app@main after a checkpoint that failed after
// its acquire: no lease left, the acquire's epoch bump in place, and the
// head where before had it.
func assertLeaseReleased(t *testing.T, w *Workspace, before store.Ref) {
	t.Helper()
	ref := refOf(t, w, "app", "main")
	if ref.LeaseHolder != "" || ref.LeaseExpiry != "" {
		t.Fatalf("a failed checkpoint left its lease: holder %q until %s", ref.LeaseHolder, ref.LeaseExpiry)
	}
	if ref.Epoch != before.Epoch+1 || ref.HeadTXID != before.HeadTXID || ref.HeadEpoch != before.HeadEpoch {
		t.Fatalf("after a failed checkpoint: epoch %d head %d@%d, want epoch %d head %d@%d",
			ref.Epoch, ref.HeadTXID, ref.HeadEpoch, before.Epoch+1, before.HeadTXID, before.HeadEpoch)
	}
}

// assertBranchShowsCheckpoint checks what a user sees of app@main while an
// at-rest checkpoint holding its lease as holder runs: BranchState (the
// function the daemon's branches op calls, which both SDKs' Branch.state
// carry), Status (offshoot status) and Leases (offshoot lease list).
func assertBranchShowsCheckpoint(t *testing.T, w *Workspace, holder string) {
	t.Helper()
	if state, err := w.BranchState("app", "main"); err != nil || state != "active" {
		t.Fatalf("BranchState during a checkpoint = %q, %v; want active", state, err)
	}
	rows, err := w.Status()
	if err != nil {
		t.Fatal(err)
	}
	listed := false
	for _, r := range rows {
		if r.DB == "app" && r.Branch == "main" {
			listed = true
			if r.State != "active" {
				t.Fatalf("offshoot status during a checkpoint: app@main state %q, want active", r.State)
			}
		}
	}
	if !listed {
		t.Fatal("offshoot status during a checkpoint lists no app@main")
	}
	leases, err := w.Leases()
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range leases {
		if l.DB == "app" && l.Branch == "main" && l.Holder == holder && !l.Expired {
			return
		}
	}
	t.Fatalf("lease list during a checkpoint = %+v, want app@main held by %s", leases, holder)
}

// failObjectPuts fails every PutIf under data/ without writing anything: an
// upload that never reaches the store.
type failObjectPuts struct{ store.Backend }

func (b failObjectPuts) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, "data/") {
		return "", errors.New("upload unavailable")
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// carriesCheckpointLease reports whether a ref body names an at-rest
// checkpoint as its lease holder: a checkpoint's acquire, and its renewals.
func carriesCheckpointLease(data []byte) bool {
	var r struct {
		LeaseHolder string `json:"lease_holder"`
	}
	return json.Unmarshal(data, &r) == nil && isCheckpointHolder(r.LeaseHolder)
}

// landsThenFails forwards the first PutIf that match accepts to the store
// and then reports failure anyway: the AWS SDK's retry answering 412 to its
// own first attempt that landed, or a timeout that lost the response.
// after, when set, runs between the two: whatever other writers do before
// the caller hears back.
type landsThenFails struct {
	store.Backend
	match func(key string, data []byte) bool
	err   error
	after func()
	hits  atomic.Int32
}

func (b *landsThenFails) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err != nil || !b.match(key, data) || b.hits.Add(1) > 1 {
		return etag, err
	}
	if b.after != nil {
		b.after()
	}
	return "", b.err
}

// acquiresLandThenFail forwards every ref PutIf that carries a checkpoint
// lease to the store and reports each one as a lost race anyway: an acquire
// that keeps landing without the checkpoint ever hearing so.
type acquiresLandThenFail struct {
	store.Backend
	refKey string
	hits   atomic.Int32
}

func (b *acquiresLandThenFail) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err != nil || key != b.refKey || !carriesCheckpointLease(data) {
		return etag, err
	}
	b.hits.Add(1)
	return "", fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS)
}

// runBeforeAcquire runs run once, just before forwarding the first ref PutIf
// that carries a checkpoint lease, so that acquire's compare-and-swap loses
// to whatever run writes.
type runBeforeAcquire struct {
	store.Backend
	refKey string
	run    func()
	fired  atomic.Int32
}

func (b *runBeforeAcquire) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.refKey && carriesCheckpointLease(data) && b.fired.CompareAndSwap(0, 1) {
		b.run()
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// TestConcurrentAtRestCheckpointsAreSerialized: while one at-rest
// checkpoint holds the branch lease, the branch reads active with the
// checkpoint as holder (status, lease list, the daemon's branch state),
// every other checkpoint on the branch is refused at once with the
// lease-held error naming the checkpoint, and leaves the lease alone;
// released, the first commits, and the head is its own object under its
// own epoch.
func TestConcurrentAtRestCheckpointsAreSerialized(t *testing.T) {
	for name, newW := range map[string]func(*testing.T) *Workspace{"local": newWS, "s3": newWSOnFakeS3} {
		t.Run(name, func(t *testing.T) {
			w := newW(t)
			seedDB(t, w, "app", 1<<16)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			paused, resume := make(chan struct{}), make(chan struct{})
			var pauseOnce, resumeOnce sync.Once
			release := func() { resumeOnce.Do(func() { close(resume) }) }
			t.Cleanup(release)
			checkpointAfterQuiesceForTest = func() { pauseOnce.Do(func() { close(paused); <-resume }) }
			t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })

			var first CheckpointResult
			firstDone := make(chan error, 1)
			go func() {
				var err error
				first, err = w.CheckpointWith("app", "main", "first", nil, CheckpointOptions{})
				firstDone <- err
			}()
			<-paused
			held := refOf(t, w, "app", "main")
			if !isCheckpointHolder(held.LeaseHolder) {
				t.Fatalf("a running checkpoint holds no checkpoint lease: holder %q", held.LeaseHolder)
			}
			assertBranchShowsCheckpoint(t, w, held.LeaseHolder)
			for i := 0; i < 3; i++ {
				_, err := w.CheckpointWith("app", "main", "rival-"+string(rune('a'+i)), nil, CheckpointOptions{})
				if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), "another checkpoint is in progress") {
					t.Fatalf("rival %d while a checkpoint holds the lease: %v, want a lease-held refusal naming the checkpoint", i, err)
				}
			}
			if after := refOf(t, w, "app", "main"); after.Epoch != held.Epoch || after.LeaseHolder != held.LeaseHolder {
				t.Fatalf("a refused rival changed the lease: %q@%d -> %q@%d", held.LeaseHolder, held.Epoch, after.LeaseHolder, after.Epoch)
			}
			release()
			if err := <-firstDone; err != nil {
				t.Fatalf("first checkpoint: %v", err)
			}
			ref := refOf(t, w, "app", "main")
			if ref.LeaseHolder != "" || ref.HeadTXID != first.TXID || ref.HeadEpoch != held.Epoch {
				t.Fatalf("after the first commits: holder %q head %d@%d, want no lease, head %d@%d", ref.LeaseHolder, ref.HeadTXID, ref.HeadEpoch, first.TXID, held.Epoch)
			}
			for n := range ref.Checkpoints {
				if strings.HasPrefix(n, "rival-") {
					t.Fatalf("refused rival %s was recorded", n)
				}
			}
			members, err := w.Store.Chain(ref.Lineage, first.TXID)
			if err != nil {
				t.Fatal(err)
			}
			if last := members[len(members)-1]; last.MaxTXID != first.TXID || last.Epoch != held.Epoch {
				t.Fatalf("the head resolves through %+v, want the first checkpoint's object at epoch %d", last, held.Epoch)
			}
			at, err := w.CheckoutAt("app", "main", "first", false)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("the head does not materialize to the first checkpoint's checkout")
			}
		})
	}
}

// heldUpload waits for g to hold the checkpoint's object upload (armed with
// holdObject) and returns a func that releases it; calling it twice is
// harmless. A checkpoint that ends before its upload fails the test instead
// of hanging it, and the upload is released at cleanup too, so a failing
// test never strands the checkpoint goroutine.
func heldUpload(t *testing.T, g *casGate, done <-chan error) func() {
	t.Helper()
	select {
	case held := <-g.objArrived:
		var once sync.Once
		release := func() { once.Do(func() { close(held) }) }
		t.Cleanup(release)
		return release
	case err := <-done:
		t.Fatalf("the checkpoint ended before its upload: %v", err)
		return nil
	}
}

// TestCheckpointHoldsTheLeaseThroughItsUpload: the lease spans the object
// upload, not only the planning before it: while the create-only put is
// held, the ref names the checkpoint as live holder at the epoch its key
// carries, so another writer's acquire and another checkpoint are refused;
// released, the checkpoint commits.
func TestCheckpointHoldsTheLeaseThroughItsUpload(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	g := gateRefCAS(w, "app", "main")
	g.holdObject(privateSnapshotKey(before))
	done := make(chan error, 1)
	go func() {
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
		done <- err
	}()
	release := heldUpload(t, g, done)

	ref := refOf(t, w, "app", "main")
	if !isCheckpointHolder(ref.LeaseHolder) || ref.Epoch != before.Epoch+1 || !store.LeaseLive(ref, time.Now()) {
		t.Fatalf("during the upload: holder %q epoch %d expiry %s, want a live checkpoint lease at epoch %d",
			ref.LeaseHolder, ref.Epoch, ref.LeaseExpiry, before.Epoch+1)
	}
	if _, err := w.Store.AcquireLease("app", "main", "daemon-b", time.Minute, time.Now()); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("an acquire during the checkpoint's upload: %v, want ErrLeaseHeld", err)
	}
	if _, err := w.CheckpointWith("app", "main", "rival", nil, CheckpointOptions{}); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("a checkpoint during another's upload: %v, want ErrLeaseHeld", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if after := refOf(t, w, "app", "main"); after.LeaseHolder != "" || after.HeadEpoch != before.Epoch+1 {
		t.Fatalf("after the upload is released: holder %q head epoch %d, want no lease, head epoch %d", after.LeaseHolder, after.HeadEpoch, before.Epoch+1)
	}
}

// TestTwoCheckpointsInOneProcessGetDistinctEpochs: two checkpoints from one
// process hold distinct holders (the per-call nonce), so each acquire is a
// fresh one that bumps the epoch, and each object goes under its own key.
func TestTwoCheckpointsInOneProcessGetDistinctEpochs(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	path := w.CheckoutPath("app", "main")
	var holders []string
	checkpointAfterQuiesceForTest = func() { holders = append(holders, refOf(t, w, "app", "main").LeaseHolder) }
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{})
	ref := refOf(t, w, "app", "main")
	if ea, eb := ref.Checkpoints["a"].Epoch, ref.Checkpoints["b"].Epoch; eb != ea+1 {
		t.Fatalf("checkpoint epochs a=%d b=%d, want b one past a (each acquire bumps)", ea, eb)
	}
	shape := regexp.MustCompile(`^checkpoint:` + regexp.QuoteMeta(LocalHolder()) + `/[0-9a-f]{8}$`)
	if len(holders) != 2 || holders[0] == holders[1] || !shape.MatchString(holders[0]) || !shape.MatchString(holders[1]) {
		t.Fatalf("holders %q, want two distinct checkpoint:<host>/<pid>/<8 hex>", holders)
	}
}

// TestCheckpointReleasesLeaseInHeadWrite: a checkpoint writes the ref twice,
// the acquire and the head write, and the head write both advances the head
// and clears the lease: no separate release write.
func TestCheckpointReleasesLeaseInHeadWrite(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	cb := newRPCCountBackend(w.Store.B)
	w.Store.B = cb
	res := mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
	w.Store.B = cb.Backend
	ref := refOf(t, w, "app", "main")
	if ref.LeaseHolder != "" || ref.LeaseExpiry != "" {
		t.Fatalf("lease left after a committed checkpoint: %q until %s", ref.LeaseHolder, ref.LeaseExpiry)
	}
	if ref.Epoch != before.Epoch+1 || ref.HeadEpoch != ref.Epoch || ref.HeadTXID != res.TXID || ref.Checkpoints["a"].Epoch != ref.Epoch {
		t.Fatalf("ref after the checkpoint: epoch %d head %d@%d entry epoch %d, want epoch %d, head %d under it",
			ref.Epoch, ref.HeadTXID, ref.HeadEpoch, ref.Checkpoints["a"].Epoch, before.Epoch+1, res.TXID)
	}
	if n := cb.putIfCount(store.RefKey("app", "main")); n != 2 {
		t.Fatalf("the ref was written %d times, want 2: the acquire, and one head write that also releases the lease", n)
	}
}

// TestCheckpointBuildsOnACommitBeforeItsAcquire: a checkpoint that commits
// just before this checkpoint's acquire moves the head and the epoch.
// Planning from the ref its acquire read and wrote, this checkpoint lands
// at the next txid, as a segment over the other's state.
func TestCheckpointBuildsOnACommitBeforeItsAcquire(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	var between CheckpointResult
	fired := false
	checkpointBeforeAcquireForTest = func() {
		if fired {
			return
		}
		fired = true
		between = mustCheckpointWith(t, w, "app", "main", "between", CheckpointOptions{})
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(200));")
	}
	t.Cleanup(func() { checkpointBeforeAcquireForTest = nil })
	res := mustCheckpointWith(t, w, "app", "main", "after", CheckpointOptions{})
	if res.TXID != between.TXID+1 {
		t.Fatalf("checkpoint at txid %d, want %d: it planned from a ref older than its acquire's", res.TXID, between.TXID+1)
	}
	ref := refOf(t, w, "app", "main")
	if ref.Checkpoints["after"].Epoch != ref.Checkpoints["between"].Epoch+1 {
		t.Fatalf("epochs between=%d after=%d, want after one past between", ref.Checkpoints["between"].Epoch, ref.Checkpoints["after"].Epoch)
	}
	assertSegmentHead(t, w, "app", "main", "after", path, res)
}

// TestCheckpointFailureReleasesLease: a checkpoint that fails after its
// acquire releases the lease at once, so the next checkpoint (or a session
// open) is not refused for a whole TTL.
func TestCheckpointFailureReleasesLease(t *testing.T) {
	t.Run("busy checkout", func(t *testing.T) {
		w := newWS(t)
		seedDB(t, w, "app", 1<<16)
		path := w.CheckoutPath("app", "main")
		before := refOf(t, w, "app", "main")
		conn, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Exec("PRAGMA journal_mode=WAL"); err != nil {
			t.Fatal(err)
		}
		tx, err := conn.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO t (v) VALUES (randomblob(10))"); err != nil {
			t.Fatal(err)
		}
		if _, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{}); err == nil {
			t.Fatal("a checkpoint under an open write transaction must fail")
		}
		assertLeaseReleased(t, w, before)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		// Closed, not just idle, before the retry: the checkpoint's own
		// open and close of the file must not share it with this handle.
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
	})
	t.Run("upload fails", func(t *testing.T) {
		w := newWS(t)
		seedDB(t, w, "app", 1<<16)
		mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(10));")
		before := refOf(t, w, "app", "main")
		orig := w.Store.B
		w.Store.B = failObjectPuts{orig}
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
		w.Store.B = orig
		if err == nil || !strings.Contains(err.Error(), "upload unavailable") {
			t.Fatalf("checkpoint with a failing upload: %v", err)
		}
		assertLeaseReleased(t, w, before)
		mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
	})
	// A name taken just before the acquire is refused on the ref the
	// acquire reads, before it writes: no lease is taken, so there is none
	// to release, and the epoch stays where the other checkpoint left it.
	t.Run("name taken just before the acquire", func(t *testing.T) {
		w := newWS(t)
		seedDB(t, w, "app", 1<<16)
		mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(10));")
		fired := false
		checkpointBeforeAcquireForTest = func() {
			if fired {
				return
			}
			fired = true
			mustCheckpointWith(t, w, "app", "main", "dup", CheckpointOptions{})
		}
		t.Cleanup(func() { checkpointBeforeAcquireForTest = nil })
		if _, err := w.CheckpointWith("app", "main", "dup", nil, CheckpointOptions{}); err == nil || !strings.Contains(err.Error(), `checkpoint "dup" already exists`) {
			t.Fatalf("a name taken just before the acquire: %v", err)
		}
		ref := refOf(t, w, "app", "main")
		won := ref.Checkpoints["dup"]
		if ref.LeaseHolder != "" || ref.HeadTXID != won.TXID || ref.Epoch != won.Epoch {
			t.Fatalf("after the refusal: holder %q head %d epoch %d, want no lease, head %d, epoch %d (nothing written)",
				ref.LeaseHolder, ref.HeadTXID, ref.Epoch, won.TXID, won.Epoch)
		}
	})
}

// TestCrashAfterAcquireLeavesReclaimableLease: a checkpoint that dies after
// its acquire and upload leaves its lease and an uncommitted object. Within
// the TTL the next checkpoint is refused (a checkpoint is, as far as anyone
// can tell, in progress); after it, the next one reclaims the lease under a
// higher epoch, and its own object, not the dead one's, is the head.
func TestCrashAfterAcquireLeavesReclaimableLease(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	txid := before.HeadTXID + 1
	// The slow work (quiesce, a 1 MiB encode, a sqlite3 run) is done before
	// the dead lease is taken, so only a Put and one refused checkpoint (a
	// ref read) fall inside its TTL, even under -race with every package
	// running at once.
	deadBytes := encodeCheckout(t, path, txid)
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(200));")
	dead, err := w.Store.AcquireLease("app", "main", newCheckpointHolder(), 2*time.Second, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Store.B.Put(store.SnapshotKey(before.Lineage, dead.Epoch, txid), deadBytes); err != nil {
		t.Fatal(err)
	}

	_, err = w.CheckpointWith("app", "main", "within-ttl", nil, CheckpointOptions{})
	if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), "another checkpoint is in progress") {
		t.Fatalf("checkpoint within the dead lease's TTL: %v, want a lease-held refusal naming the checkpoint", err)
	}
	time.Sleep(time.Until(dead.Expiry) + 20*time.Millisecond)
	res := mustCheckpointWith(t, w, "app", "main", "after", CheckpointOptions{})
	ref := refOf(t, w, "app", "main")
	if res.TXID != txid || ref.Epoch != dead.Epoch+1 || ref.HeadEpoch != ref.Epoch || ref.LeaseHolder != "" {
		t.Fatalf("after reclaiming: txid %d epoch %d head epoch %d holder %q, want txid %d, epoch %d, head under it, no lease",
			res.TXID, ref.Epoch, ref.HeadEpoch, ref.LeaseHolder, txid, dead.Epoch+1)
	}
	at, err := w.CheckoutAt("app", "main", "after", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, at), readFile(t, path)) {
		t.Fatal("the head resolves to the dead attempt's object, not the reclaiming checkpoint's")
	}
}

// TestCheckpointReclaimsAnExpiredLease: a lease a crashed session left
// expired does not need --force; the checkpoint reclaims it with a higher
// epoch and commits.
func TestCheckpointReclaimsAnExpiredLease(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(10));")
	dead, err := w.AcquireLease("app", "main", LocalHolder(), 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(dead.Expiry) + 20*time.Millisecond)
	res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	if err != nil {
		t.Fatalf("an expired lease must be reclaimed without --force: %v", err)
	}
	ref := refOf(t, w, "app", "main")
	if ref.Epoch != dead.Epoch+1 || ref.HeadTXID != res.TXID || ref.LeaseHolder != "" {
		t.Fatalf("after reclaiming: epoch %d head %d holder %q, want epoch %d, head %d, no lease", ref.Epoch, ref.HeadTXID, ref.LeaseHolder, dead.Epoch+1, res.TXID)
	}
}

// TestAcquireThatLandedIsAdopted: a lease acquire that landed but reported
// failure leaves this call's own holder on the ref: the S3 SDK's retry
// answering 412 to its own landed first attempt (which the store reports as
// a lost acquisition race), or a timeout that lost the response. The
// holder is unique to the call, so the checkpoint recognises the lease as
// its own and adopts it from the ref as it stands, under the epoch the
// landed write minted, rather than refusing itself as "another checkpoint"
// and leaving that lease to block the branch for a whole TTL. Adopting it
// writes nothing: a second acquire could land and report failure the same
// way.
func TestAcquireThatLandedIsAdopted(t *testing.T) {
	for name, failure := range map[string]error{
		"412 after landing":     fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS),
		"timeout after landing": errors.New("store: s3 conditional put refs/app/main: context deadline exceeded"),
	} {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			seedDB(t, w, "app", 1<<16)
			mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
			before := refOf(t, w, "app", "main")
			refKey := store.RefKey("app", "main")
			b := &landsThenFails{Backend: w.Store.B, err: failure, match: func(key string, data []byte) bool {
				return key == refKey && carriesCheckpointLease(data)
			}}
			w.Store.B = b
			res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
			w.Store.B = b.Backend
			if err != nil {
				t.Fatalf("a checkpoint whose acquire landed but reported failure: %v", err)
			}
			if n := b.hits.Load(); n != 1 {
				t.Fatalf("%d ref writes carried a checkpoint lease, want 1: the landed acquire, adopted without another write", n)
			}
			ref := refOf(t, w, "app", "main")
			if ref.LeaseHolder != "" || ref.Epoch != before.Epoch+1 || ref.HeadTXID != res.TXID || ref.HeadEpoch != ref.Epoch {
				t.Fatalf("after adopting the landed acquire: holder %q epoch %d head %d@%d, want no lease, epoch %d (one bump), head %d under it",
					ref.LeaseHolder, ref.Epoch, ref.HeadTXID, ref.HeadEpoch, before.Epoch+1, res.TXID)
			}
		})
	}
}

// TestAcquireLostToATouchIsRetried: an acquire that loses its
// compare-and-swap to a write that leaves no lease (a touch, protect, a TTL
// change) has met no other writer of the branch, so the checkpoint retries
// the acquire once and commits, and the touch's TTL survives.
func TestAcquireLostToATouchIsRetried(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	ttl := time.Hour
	b := &runBeforeAcquire{Backend: w.Store.B, refKey: store.RefKey("app", "main"), run: func() {
		if _, err := w.Touch("app", "main", &ttl, time.Now()); err != nil {
			t.Error(err)
		}
	}}
	w.Store.B = b
	res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if err != nil {
		t.Fatalf("a checkpoint whose acquire lost to a touch: %v", err)
	}
	if b.fired.Load() != 1 {
		t.Fatal("precondition: no touch landed ahead of the acquire")
	}
	ref := refOf(t, w, "app", "main")
	if ref.TTL != ttl.String() {
		t.Fatalf("the touch's TTL was lost: %q", ref.TTL)
	}
	if ref.LeaseHolder != "" || ref.Epoch != before.Epoch+1 || ref.HeadTXID != res.TXID {
		t.Fatalf("after the retried acquire: holder %q epoch %d head %d, want no lease, epoch %d, head %d",
			ref.LeaseHolder, ref.Epoch, ref.HeadTXID, before.Epoch+1, res.TXID)
	}
}

// TestAcquireLostToACompletedCheckpointIsRefused: an acquire that loses its
// compare-and-swap to another checkpoint, which then runs start to finish
// before this one re-reads the ref, finds no live lease but a newer epoch.
// That is the concurrent checkpoint the lease exists to turn away, so this
// one is refused with a lease-held error rather than retried into a second
// checkpoint of the same state.
func TestAcquireLostToACompletedCheckpointIsRefused(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	b := &runBeforeAcquire{Backend: w.Store.B, refKey: store.RefKey("app", "main"), run: func() {
		if _, err := w.CheckpointWith("app", "main", "other", nil, CheckpointOptions{}); err != nil {
			t.Errorf("the checkpoint that runs inside this one's acquire: %v", err)
		}
	}}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), "written by another checkpoint") {
		t.Fatalf("a checkpoint whose acquire lost to a whole other checkpoint: %v, want a lease-held refusal", err)
	}
	ref := refOf(t, w, "app", "main")
	if _, ok := ref.Checkpoints["a"]; ok {
		t.Fatal("the refused checkpoint was recorded")
	}
	other, ok := ref.Checkpoints["other"]
	if !ok || ref.HeadTXID != other.TXID || ref.Epoch != before.Epoch+1 || ref.LeaseHolder != "" {
		t.Fatalf("after the refusal: head %d epoch %d holder %q entry %+v, want the other checkpoint's head, epoch %d, no lease",
			ref.HeadTXID, ref.Epoch, ref.LeaseHolder, other, before.Epoch+1)
	}
}

// TestAcquireThatAlwaysReportsFailureIsAdopted: a store where every write
// carrying the checkpoint's lease lands but reports a lost race (a proxy
// whose timeout is shorter than the backend's write latency, in front of
// the S3 SDK's retry) still commits: the acquire is adopted from the ref
// without a second acquire write, which would only land and fail the same
// way, and the head write carries no lease.
func TestAcquireThatAlwaysReportsFailureIsAdopted(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	b := &acquiresLandThenFail{Backend: w.Store.B, refKey: store.RefKey("app", "main")}
	w.Store.B = b
	res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if err != nil {
		t.Fatalf("a checkpoint whose every lease write reports failure: %v", err)
	}
	if n := b.hits.Load(); n != 1 {
		t.Fatalf("%d ref writes carried the checkpoint's lease, want 1 (the acquire)", n)
	}
	ref := refOf(t, w, "app", "main")
	if ref.LeaseHolder != "" || ref.Epoch != before.Epoch+1 || ref.HeadTXID != res.TXID {
		t.Fatalf("after the adopted acquire: holder %q epoch %d head %d, want no lease, epoch %d, head %d",
			ref.LeaseHolder, ref.Epoch, ref.HeadTXID, before.Epoch+1, res.TXID)
	}
}

// TestRefusedAdoptionReleasesItsLease: an acquire that landed but reported
// failure is adopted only once the checkpoint's checks pass on the ref it
// re-reads. A checkout detached in between (a repoint that could not
// refresh it) is refused there, and the lease the landed acquire left is
// released, so the branch is not refused to every writer for a whole TTL.
func TestRefusedAdoptionReleasesItsLease(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("setup: the checkout has no sidecar")
	}
	refKey := store.RefKey("app", "main")
	b := &landsThenFails{Backend: w.Store.B, err: fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS),
		match: func(key string, data []byte) bool { return key == refKey && carriesCheckpointLease(data) },
		after: func() {
			if err := StampSumHashOnly(path, rec.Hash, "another-lineage", rec.Epoch, rec.TXID, rec.PostApplyChecksum, rec.ChainID); err != nil {
				t.Error(err)
			}
		}}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if !errors.Is(err, ErrDetachedCheckout) {
		t.Fatalf("a checkpoint whose checkout detached before it adopted its landed acquire: %v, want a detached refusal", err)
	}
	if b.hits.Load() != 1 {
		t.Fatal("precondition: no acquire landed")
	}
	assertLeaseReleased(t, w, before)
}

// landsAfterTheSettleRead answers the first ref PutIf that carries a
// checkpoint lease with a lost race without writing it, and lands that
// write just after the next read of the ref returns, running after once it
// has: an S3 409 answered while the write's first attempt was still in
// flight, which lands once the checkpoint has re-read the ref and found
// nothing of its own.
type landsAfterTheSettleRead struct {
	store.Backend
	refKey string
	after  func()

	mu      sync.Mutex
	state   int // 0 before the acquire write, 1 while it is held, 2 once it landed
	held    []byte
	heldIf  string
	landErr error
}

func (b *landsAfterTheSettleRead) PutIf(key string, data []byte, ifMatch string) (string, error) {
	b.mu.Lock()
	if b.state == 0 && key == b.refKey && carriesCheckpointLease(data) {
		b.state, b.held, b.heldIf = 1, data, ifMatch
		b.mu.Unlock()
		return "", fmt.Errorf("%w: conflict with an attempt still in flight", store.ErrCAS)
	}
	b.mu.Unlock()
	return b.Backend.PutIf(key, data, ifMatch)
}

func (b *landsAfterTheSettleRead) Get(key string) ([]byte, string, error) {
	data, etag, err := b.Backend.Get(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	if key == b.refKey && b.state == 1 {
		b.state = 2
		if _, b.landErr = b.Backend.PutIf(key, b.held, b.heldIf); b.landErr == nil && b.after != nil {
			b.after()
		}
	}
	return data, etag, err
}

// TestLateAcquireRefusedOnRetryReleasesItsLease: an acquire answered with a
// lost race whose write lands only after the checkpoint re-read the ref
// and found nothing of its own leaves the checkpoint retrying, and the
// retry's read carries our own lease. A checkout detached in between is
// refused there, and that lease is released, so a checkpoint that never
// started does not leave the branch refused to every writer for a TTL.
func TestLateAcquireRefusedOnRetryReleasesItsLease(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("setup: the checkout has no sidecar")
	}
	b := &landsAfterTheSettleRead{Backend: w.Store.B, refKey: store.RefKey("app", "main"), after: func() {
		if err := StampSumHashOnly(path, rec.Hash, "another-lineage", rec.Epoch, rec.TXID, rec.PostApplyChecksum, rec.ChainID); err != nil {
			t.Error(err)
		}
	}}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if b.state != 2 || b.landErr != nil {
		t.Fatalf("precondition: the held acquire did not land after the settle read (state %d, %v)", b.state, b.landErr)
	}
	if !errors.Is(err, ErrDetachedCheckout) {
		t.Fatalf("a checkpoint whose retry read found its own late acquire on a detached checkout: %v, want a detached refusal", err)
	}
	assertLeaseReleased(t, w, before)
}

// TestStragglerUnderOldEpochCannotAnchorHead: objects earlier writers left
// under the current epoch and never committed (an attempt from before
// checkpoints took the lease, or a fenced one) — a snapshot and a segment at
// the txid the next checkpoint takes, and a snapshot beyond it — neither
// anchor nor join the head's chain, do not change what the checkpoint
// writes, and are reclaimed by GC while the checkpoint's own object stays.
func TestStragglerUnderOldEpochCannotAnchorHead(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts CheckpointOptions
		kind string
	}{
		{"segment winner", CheckpointOptions{}, "segment"},
		{"snapshot winner", CheckpointOptions{Snapshot: true}, "snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			before := refOf(t, w, "app", "main")
			txid := before.HeadTXID + 1
			stale := encodeCheckout(t, path, txid)
			stragglers := map[string][]byte{
				store.SnapshotKey(before.Lineage, before.Epoch, txid):      stale,
				store.SegmentKey(before.Lineage, before.Epoch, txid, txid): []byte("straggler segment"),
				store.SnapshotKey(before.Lineage, before.Epoch, txid+1):    stale,
			}
			for k, v := range stragglers {
				if err := w.Store.B.Put(k, v); err != nil {
					t.Fatal(err)
				}
			}
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(300));")
			overwrites := countOverwrites(t)

			res := mustCheckpointWith(t, w, "app", "main", "next", tc.opts)
			if res.Kind != tc.kind || res.TXID != txid {
				t.Fatalf("checkpoint = %+v, want a %s at txid %d", res, tc.kind, txid)
			}
			ref := refOf(t, w, "app", "main")
			members, err := w.Store.Chain(ref.Lineage, res.TXID)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range members {
				if _, bad := stragglers[m.Key]; bad {
					t.Fatalf("the head's chain uses the straggler %s", m.Key)
				}
			}
			head := members[len(members)-1]
			if head.MaxTXID != txid || head.Epoch != before.Epoch+1 || head.Epoch != ref.HeadEpoch {
				t.Fatalf("head member %+v, want txid %d at epoch %d", head, txid, before.Epoch+1)
			}
			at, err := w.CheckoutAt("app", "main", "next", false)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("the head does not materialize to the checkout")
			}
			if n := overwrites.Load(); n != 0 {
				t.Fatalf("overwrite counter %d, want 0: nothing touched the checkpoint's own key", n)
			}
			for i := 0; i < 2; i++ { // the first pass tombstones, the second sweeps
				if _, _, err := w.GC(0); err != nil {
					t.Fatal(err)
				}
			}
			for k := range stragglers {
				if storeHas(w, k) {
					t.Fatalf("GC kept the straggler %s", k)
				}
			}
			if !storeHas(w, head.Key) {
				t.Fatal("GC removed the head's own object")
			}
		})
	}
}

// stealLease hands db@branch's lease to holder at a higher epoch, the way a
// reclaim after expiry would, retrying a compare-and-swap lost to a renewal.
func stealLease(t *testing.T, w *Workspace, db, branch, holder string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		ref, etag, err := w.Store.GetRef(db, branch)
		if err != nil {
			t.Fatal(err)
		}
		ref.Epoch++
		ref.LeaseHolder = holder
		ref.LeaseExpiry = time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
		_, err = w.Store.PutRef(db, branch, ref, etag)
		if err == nil {
			return
		}
		if !errors.Is(err, store.ErrCAS) {
			t.Fatal(err)
		}
	}
	t.Fatal("could not steal the lease in 50 attempts")
}

// waitFor polls cond every 5ms until it holds, failing after 5s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// destroyForce runs `destroy --force`, retrying when a lease renewal lands
// between its read and its claim and so wins the claim's compare-and-swap,
// or, past half the lease, writes over the claim before the conditional
// delete (store.RenewLease). The backend must offer the conditional delete
// (store.ConditionalDeleter), as Local does: through a test wrapper that
// hides it, Destroy falls back to a plain Delete, which a renewal already
// past its checks can undo, bringing the branch back still leased.
func destroyForce(t *testing.T, w *Workspace, db, branch string) {
	t.Helper()
	if _, ok := w.Store.B.(store.ConditionalDeleter); !ok {
		t.Fatalf("destroyForce through %T, which hides the backend's conditional delete", w.Store.B)
	}
	for i := 0; i < 20; i++ {
		err := w.Destroy(db, branch, true)
		if err == nil {
			return
		}
		if !errors.Is(err, store.ErrCAS) {
			t.Fatal(err)
		}
	}
	t.Fatal("destroy --force kept losing its claim to renewals")
}

// renewTerminal installs checkpointRenewTerminalForTest for the test and
// returns the channel it reports on: the terminal error a checkpoint's
// renewer saw, sent just before the renewer cancels the checkpoint and
// exits. A test that holds the upload waits on it to know the renewer,
// not the head write's premise check, caught the loss.
func renewTerminal(t *testing.T) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	checkpointRenewTerminalForTest = func(err error) {
		select {
		case ch <- err:
		default:
		}
	}
	t.Cleanup(func() { checkpointRenewTerminalForTest = nil })
	return ch
}

// awaitRenewTerminal waits up to 5s for the renewer's terminal error.
func awaitRenewTerminal(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for the renewer to report %s", what)
		return nil
	}
}

// leasedBranch creates app and acquires main's lease for holder.
func leasedBranch(t *testing.T, holder string, ttl time.Duration) (*Workspace, store.Lease) {
	t.Helper()
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	l, err := w.AcquireLease("app", "main", holder, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return w, l
}

// stallBackend holds the first PutIf under prefix until release is
// closed, closing stalled when it starts: an upload slower than the lease
// TTL, for exactly as long as the test needs.
type stallBackend struct {
	store.Backend
	prefix  string
	once    sync.Once
	stalled chan struct{}
	release chan struct{}
}

func (b *stallBackend) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, b.prefix) {
		first := false
		b.once.Do(func() { close(b.stalled); first = true })
		if first {
			<-b.release
		}
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// flakyRefGets fails the next n Gets of key with a transient error.
type flakyRefGets struct {
	store.Backend
	key string
	n   atomic.Int32
}

func (b *flakyRefGets) Get(key string) ([]byte, string, error) {
	if key == b.key && b.n.Add(-1) >= 0 {
		return nil, "", errors.New("transient: connection reset")
	}
	return b.Backend.Get(key)
}

func TestRenewErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		err      error
		terminal bool
	}{
		{fmt.Errorf("%w: app@main now held by %q at epoch 3", store.ErrLeaseLost, "thief"), true},
		{fmt.Errorf("%w: no branch app@main", store.ErrNotFound), true},
		{fmt.Errorf("store: renew lease on app@main: %w", store.ErrCAS), false},
		{fmt.Errorf("%w: app@main; not renewing over the claim", store.ErrDeleting), false},
		{fmt.Errorf("%w: app@main; not renewing over the claim", store.ErrReaping), false},
		{errors.New("store: s3 get refs/app/main: connection reset"), false},
	} {
		if got := renewErrTerminal(tc.err); got != tc.terminal {
			t.Errorf("renewErrTerminal(%v) = %v, want %v", tc.err, got, tc.terminal)
		}
	}
}

// TestCheckpointRenewerKeepsTheLeaseLive: renewals keep a lease live well
// past its TTL, and once stop returns no renewal runs.
func TestCheckpointRenewerKeepsTheLeaseLive(t *testing.T) {
	w, l := leasedBranch(t, newCheckpointHolder(), 200*time.Millisecond)
	r := w.startCheckpointRenewer(l, 200*time.Millisecond, 20*time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	if !store.LeaseLive(refOf(t, w, "app", "main"), time.Now()) {
		t.Fatal("the lease lapsed while the renewer ran")
	}
	if err := r.stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped := refOf(t, w, "app", "main").LeaseExpiry
	time.Sleep(60 * time.Millisecond)
	if got := refOf(t, w, "app", "main").LeaseExpiry; got != stopped {
		t.Fatalf("a renewal ran after stop returned: expiry %s -> %s", stopped, got)
	}
}

func TestCheckpointRenewerReportsLeaseLoss(t *testing.T) {
	w, l := leasedBranch(t, newCheckpointHolder(), time.Second)
	r := w.startCheckpointRenewer(l, time.Second, 10*time.Millisecond)
	defer r.stop()
	stealLease(t, w, "app", "main", "thief")
	waitFor(t, "the renewer to report the loss", func() bool { return r.lost() != nil })
	if err := r.stop(); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stop after a stolen lease: %v, want ErrLeaseLost", err)
	}
}

func TestCheckpointRenewerReportsDestroyedBranch(t *testing.T) {
	w, l := leasedBranch(t, newCheckpointHolder(), time.Second)
	r := w.startCheckpointRenewer(l, time.Second, 10*time.Millisecond)
	defer r.stop()
	destroyForce(t, w, "app", "main")
	waitFor(t, "the renewer to report the destroyed branch", func() bool { return r.lost() != nil })
	if err := r.stop(); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stop after destroy: %v, want ErrNotFound", err)
	}
}

// TestCheckpointRenewerRidesOutTransientErrors: a renewal that fails for any
// other reason is retried on the next tick, and the lease stays live.
func TestCheckpointRenewerRidesOutTransientErrors(t *testing.T) {
	w, l := leasedBranch(t, newCheckpointHolder(), 300*time.Millisecond)
	fb := &flakyRefGets{Backend: w.Store.B, key: store.RefKey("app", "main")}
	fb.n.Store(3)
	w.Store.B = fb
	r := w.startCheckpointRenewer(l, 300*time.Millisecond, 20*time.Millisecond)
	waitFor(t, "the failing reads to be spent", func() bool { return fb.n.Load() < 0 })
	time.Sleep(100 * time.Millisecond)
	if err := r.stop(); err != nil {
		t.Fatalf("stop after transient errors: %v, want nil", err)
	}
	w.Store.B = fb.Backend
	if !store.LeaseLive(refOf(t, w, "app", "main"), time.Now()) {
		t.Fatal("the lease lapsed across transient renewal errors")
	}
}

// TestCheckpointRenewsWhileUploading: an upload that outlasts the lease TTL
// keeps the lease live, so a writer that tries to take the branch past the
// TTL is refused and the checkpoint commits. The upload is held until that
// refusal is in: the sleep only bounds from below how long it has run, so
// a slow test goroutine cannot let the upload finish first.
func TestCheckpointRenewsWhileUploading(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	const ttl, every = 500 * time.Millisecond, 50 * time.Millisecond
	sb := &stallBackend{Backend: w.Store.B, prefix: "data/", stalled: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(sb.release) }) }
	t.Cleanup(release)
	w.Store.B = sb
	done := make(chan error, 1)
	go func() {
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{LeaseTTL: ttl, RenewEvery: every})
		done <- err
	}()
	select {
	case <-sb.stalled:
	case err := <-done:
		t.Fatalf("the checkpoint ended before its upload: %v", err)
	}
	time.Sleep(ttl + 300*time.Millisecond)
	if _, err := w.Store.AcquireLease("app", "main", "thief", time.Minute, time.Now()); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("a writer past the acquire's TTL: %v, want ErrLeaseHeld (renewals keep the lease live)", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("checkpoint with a slow upload: %v", err)
	}
	w.Store.B = sb.Backend
	ref := refOf(t, w, "app", "main")
	if ref.LeaseHolder != "" || ref.Checkpoints["a"].TXID != ref.HeadTXID {
		t.Fatalf("after the slow checkpoint: holder %q, head %d, entry %+v", ref.LeaseHolder, ref.HeadTXID, ref.Checkpoints["a"])
	}
}

// TestRenewLeaseLostAbortsBeforeRefWrite: a lease stolen while the object
// uploads is caught by the renewer, which cancels the checkpoint before any
// head write: it reports the loss, the head does not move, the thief's
// lease is untouched, and the checkpoint deletes its own object, which
// nothing names.
func TestRenewLeaseLostAbortsBeforeRefWrite(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	key := privateSnapshotKey(before)
	terminal := renewTerminal(t)
	g := gateRefCAS(w, "app", "main")
	g.holdObject(key)
	done := make(chan error, 1)
	go func() {
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true, LeaseTTL: time.Second, RenewEvery: 10 * time.Millisecond})
		done <- err
	}()
	release := heldUpload(t, g, done)
	stealLease(t, w, "app", "main", "thief")
	// The upload is held, so the checkpoint itself reads nothing until it is
	// released: only its renewer can see the steal. Wait until it has, then
	// let the upload land.
	if err := awaitRenewTerminal(t, terminal, "the stolen lease"); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("the renewer's terminal error: %v, want ErrLeaseLost", err)
	}
	release()
	err := <-done
	if !errors.Is(err, store.ErrLeaseLost) || !strings.Contains(err.Error(), "its lease ended while it ran") {
		t.Fatalf("checkpoint whose lease was stolen mid-upload: %v, want the renewer's ErrLeaseLost", err)
	}
	ref := refOf(t, w, "app", "main")
	if ref.HeadTXID != before.HeadTXID || ref.LeaseHolder != "thief" {
		t.Fatalf("after the lost checkpoint: head %d holder %q, want head %d, the thief's lease", ref.HeadTXID, ref.LeaseHolder, before.HeadTXID)
	}
	if _, ok := ref.Checkpoints["a"]; ok {
		t.Fatal("the lost checkpoint was recorded")
	}
	if storeHas(w, key) {
		t.Fatal("the lost checkpoint's object survived")
	}
}

// repointSetup returns a workspace whose app@work is a fork of main with an
// edit in its checkout for a checkpoint to commit.
func repointSetup(t *testing.T) *Workspace {
	t.Helper()
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustFork(t, w, "app", "main", "work", "seed")
	mustSQL(t, mustCheckout(t, w, "app", "work"), "INSERT INTO t (v) VALUES (randomblob(100));")
	return w
}

// pausedCheckpoint starts a checkpoint "a" of app@work that pauses after its
// quiesce, holding the branch lease, and returns the ref while it is
// paused, the channel its result arrives on, and a func that lets it go on
// (harmless to call twice; it also runs at cleanup).
func pausedCheckpoint(t *testing.T, w *Workspace) (store.Ref, <-chan error, func()) {
	t.Helper()
	paused, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(release)
	checkpointAfterQuiesceForTest = func() { pauseOnce.Do(func() { close(paused); <-resume }) }
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	done := make(chan error, 1)
	go func() {
		// The default lease (30 s, renewed every 10 s): no renewal falls
		// inside the test. A forced repoint reads the ref, resolves a chain
		// and writes objects before its compare-and-swap, and a renewal
		// landing in that window makes it lose; under load that window
		// outlasts any short renewal interval, and the repoint would never
		// win. The head write's premise reports the lease lost either way,
		// renewals or not.
		_, err := w.CheckpointWith("app", "work", "a", nil, CheckpointOptions{})
		done <- err
	}()
	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("the checkpoint ended before it paused: %v", err)
	}
	return refOf(t, w, "app", "work"), done, release
}

// TestRepointVerbsDuringCheckpoint: while a checkpoint holds the branch,
// unforced rollback, promote onto, compact and destroy are refused naming
// it, and the checkpoint then commits. Forced, each repoint clears the
// checkpoint's lease; the checkpoint then fails without committing, leaves
// the repointed ref alone, and deletes its own object. (destroy --force has
// its own test, TestDestroyForceDuringCheckpointAbortsIt.)
func TestRepointVerbsDuringCheckpoint(t *testing.T) {
	t.Run("unforced verbs are refused", func(t *testing.T) {
		w := repointSetup(t)
		held, done, release := pausedCheckpoint(t, w)
		_, rbErr := w.RollbackWith("app", "work", "fork", RollbackOptions{NoBackup: true})
		_, prErr := w.PromoteWith("app", "main", "work", PromoteOptions{NoBackup: true})
		_, cmErr := w.CompactWith("app", "work", CompactOptions{})
		dsErr := w.Destroy("app", "work", false)
		for what, err := range map[string]error{"rollback": rbErr, "promote onto": prErr, "compact": cmErr, "destroy": dsErr} {
			if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), held.LeaseHolder) {
				t.Fatalf("unforced %s during a checkpoint: %v, want a lease-held refusal naming %s", what, err, held.LeaseHolder)
			}
		}
		release()
		if err := <-done; err != nil {
			t.Fatalf("the checkpoint after the refused repoints: %v", err)
		}
	})
	for _, tc := range []struct {
		name    string
		repoint func(w *Workspace) error
	}{
		{"rollback --force", func(w *Workspace) error {
			_, err := w.RollbackWith("app", "work", "fork", RollbackOptions{Force: true, NoBackup: true})
			return err
		}},
		{"promote --onto --force", func(w *Workspace) error {
			_, err := w.PromoteWith("app", "main", "work", PromoteOptions{Force: true, NoBackup: true})
			return err
		}},
		// compact repoints the branch at a fresh lineage under epoch 1, so
		// the checkpoint's epoch no longer matches either.
		{"compact --force", func(w *Workspace) error {
			_, err := w.CompactWith("app", "work", CompactOptions{Force: true})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := repointSetup(t)
			held, done, release := pausedCheckpoint(t, w)
			if err := tc.repoint(w); err != nil {
				t.Fatal(err)
			}
			repointed := refOf(t, w, "app", "work")
			if repointed.LeaseHolder != "" || repointed.Lineage == held.Lineage {
				t.Fatalf("setup: after %s the ref has lineage %s (was %s) and holder %q", tc.name, repointed.Lineage, held.Lineage, repointed.LeaseHolder)
			}
			release()
			if err := <-done; !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("checkpoint after %s: %v, want ErrLeaseLost", tc.name, err)
			}
			if after := refOf(t, w, "app", "work"); !reflect.DeepEqual(after, repointed) {
				t.Fatalf("the failed checkpoint changed the ref:\n got %+v\nwant %+v", after, repointed)
			}
			txid := held.HeadTXID + 1
			for _, k := range []string{store.SnapshotKey(held.Lineage, held.Epoch, txid), store.SegmentKey(held.Lineage, held.Epoch, txid, txid)} {
				if storeHas(w, k) {
					t.Fatalf("the failed checkpoint left its object %s", k)
				}
			}
		})
	}
}

// TestLeaseReleaseDuringCheckpointFailsIt: `offshoot lease release` on a
// running checkpoint's lease makes it fail without committing; it deletes
// its object and the branch stays usable.
func TestLeaseReleaseDuringCheckpointFailsIt(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	paused, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(release)
	checkpointAfterQuiesceForTest = func() { pauseOnce.Do(func() { close(paused); <-resume }) }
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	done := make(chan error, 1)
	go func() {
		// The default lease: no renewal falls inside the test, so the
		// release below never loses its compare-and-swap to one, and the
		// head write's premise catches the release either way.
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
		done <- err
	}()
	<-paused
	held := refOf(t, w, "app", "main")
	for i := 0; ; i++ {
		err := w.ReleaseLease(store.Lease{DB: "app", Branch: "main", Holder: held.LeaseHolder, Epoch: held.Epoch})
		if err == nil {
			break
		}
		if !errors.Is(err, store.ErrCAS) || i == 20 {
			t.Fatal(err)
		}
	}
	release()
	if err := <-done; !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("checkpoint after `lease release`: %v, want ErrLeaseLost", err)
	}
	ref := refOf(t, w, "app", "main")
	if ref.HeadTXID != held.HeadTXID {
		t.Fatalf("head moved to %d, want %d", ref.HeadTXID, held.HeadTXID)
	}
	txid := held.HeadTXID + 1
	for _, k := range []string{store.SnapshotKey(held.Lineage, held.Epoch, txid), store.SegmentKey(held.Lineage, held.Epoch, txid, txid)} {
		if storeHas(w, k) {
			t.Fatalf("the failed checkpoint left its object %s", k)
		}
	}
	checkpointAfterQuiesceForTest = nil
	mustCheckpointWith(t, w, "app", "main", "b", CheckpointOptions{})
}

// TestDestroyForceDuringCheckpointAbortsIt: `destroy --force` while a
// checkpoint uploads reaches it as a terminal ErrNotFound from a renewal;
// the checkpoint deletes its object and never recreates the branch.
func TestDestroyForceDuringCheckpointAbortsIt(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustFork(t, w, "app", "main", "work", "seed")
	mustSQL(t, mustCheckout(t, w, "app", "work"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "work")
	key := privateSnapshotKey(before)
	terminal := renewTerminal(t)
	g := gateRefCAS(w, "app", "work")
	g.holdObject(key)
	done := make(chan error, 1)
	go func() {
		// Renewals every 100ms, not 10ms: one landing between destroy's
		// read and its claim makes the claim lose and retry
		// (destroyForce), so a short interval could starve it.
		_, err := w.CheckpointWith("app", "work", "a", nil, CheckpointOptions{Snapshot: true, LeaseTTL: 2 * time.Second, RenewEvery: 100 * time.Millisecond})
		done <- err
	}()
	release := heldUpload(t, g, done)
	destroyForce(t, w, "app", "work")
	if err := awaitRenewTerminal(t, terminal, "the destroyed branch"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the renewer's terminal error: %v, want ErrNotFound", err)
	}
	release()
	err := <-done
	if !errors.Is(err, store.ErrNotFound) || !strings.Contains(err.Error(), "its lease ended while it ran") {
		t.Fatalf("checkpoint of a branch destroyed mid-upload: %v, want the renewer's ErrNotFound", err)
	}
	if storeHas(w, key) {
		t.Fatal("the aborted checkpoint's object survived")
	}
	if _, _, err := w.Store.GetRef("app", "work"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the destroyed branch came back: %v", err)
	}
}

// recordsCheckpoint reports whether a ref body records a checkpoint called
// name.
func recordsCheckpoint(data []byte, name string) bool {
	var r struct {
		Checkpoints map[string]json.RawMessage `json:"checkpoints"`
	}
	if json.Unmarshal(data, &r) != nil {
		return false
	}
	_, ok := r.Checkpoints[name]
	return ok
}

// refusingHeadWrites answers ErrCAS, without writing, to every ref PutIf
// that records the checkpoint name: a ref under a storm of metadata writes.
type refusingHeadWrites struct {
	store.Backend
	refKey, name string
	refused      atomic.Int32
}

func (b *refusingHeadWrites) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.refKey && recordsCheckpoint(data, b.name) {
		b.refused.Add(1)
		return "", fmt.Errorf("%w: refused for the test", store.ErrCAS)
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// TestFinalCASRetriesAfterConcurrentTouch: a touch landing between the
// checkpoint's premise read and its head write loses the head write its
// compare-and-swap; the premise still holds, so the checkpoint re-reads,
// reapplies and commits, and the touch's TTL survives.
func TestFinalCASRetriesAfterConcurrentTouch(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	g := gateRefCAS(w, "app", "main")
	ttl := time.Hour
	res, err := checkpointWhileHeld(t, w, g, CheckpointOptions{}, func() {
		if _, err := w.Touch("app", "main", &ttl, time.Now()); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatalf("a touch between the premise read and the head write must be retried past: %v", err)
	}
	ref := refOf(t, w, "app", "main")
	if ref.TTL != ttl.String() {
		t.Fatalf("the touch's TTL was lost: %q", ref.TTL)
	}
	if ref.HeadTXID != res.TXID || ref.LeaseHolder != "" {
		t.Fatalf("head %d holder %q, want head %d, no lease", ref.HeadTXID, ref.LeaseHolder, res.TXID)
	}
}

// TestFinalCASGivesUpAfterThreeLosses: three lost compare-and-swaps with the
// premise intact end the checkpoint with a retryable error and release its
// lease. The head never reached its txid, but the object is kept: on S3 a
// conflict answer does not rule out an earlier attempt of the head write
// still landing (TestHeadWriteHiddenBehindAConflictKeepsItsObject).
func TestFinalCASGivesUpAfterThreeLosses(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	b := &refusingHeadWrites{Backend: w.Store.B, refKey: store.RefKey("app", "main"), name: "a"}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
	w.Store.B = b.Backend
	if !errors.Is(err, store.ErrCAS) || !strings.Contains(err.Error(), "lost 3 compare-and-swaps") || !strings.Contains(err.Error(), "retry") {
		t.Fatalf("checkpoint whose head write always loses: %v, want a retryable compare-and-swap error", err)
	}
	if n := b.refused.Load(); n != 3 {
		t.Fatalf("head write attempted %d times, want 3", n)
	}
	assertLeaseReleased(t, w, before)
	if !storeHas(w, privateSnapshotKey(before)) {
		t.Fatal("the object was deleted although a refused head write can still land on S3")
	}
}

// TestHeadWriteThatLandedIsNotUndone: a head write that landed but reported
// failure is recognised from the ref (our name at our txid and epoch), so
// the checkpoint succeeds, its object stays, and the checkout is stamped.
func TestHeadWriteThatLandedIsNotUndone(t *testing.T) {
	for name, failure := range map[string]error{
		"412 after landing":     fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS),
		"timeout after landing": errors.New("store: s3 conditional put refs/app/main: context deadline exceeded"),
	} {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			requireClone(t, w)
			seedDB(t, w, "app", 1<<20)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			refKey := store.RefKey("app", "main")
			b := &landsThenFails{Backend: w.Store.B, err: failure, match: func(key string, data []byte) bool {
				return key == refKey && recordsCheckpoint(data, "a")
			}}
			w.Store.B = b
			res, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
			w.Store.B = b.Backend
			if err != nil {
				t.Fatalf("a head write that landed was reported as failed: %v", err)
			}
			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != res.TXID || ref.LeaseHolder != "" || ref.Checkpoints["a"].TXID != res.TXID {
				t.Fatalf("ref after a landed head write: head %d holder %q entry %+v", ref.HeadTXID, ref.LeaseHolder, ref.Checkpoints["a"])
			}
			at, err := w.CheckoutAt("app", "main", "a", false)
			if err != nil {
				t.Fatalf("the committed head does not materialize (object deleted?): %v", err)
			}
			if !bytes.Equal(readFile(t, at), readFile(t, path)) {
				t.Fatal("the committed head differs from the checkout")
			}
			assertTrustedStamp(t, path)
		})
	}
}

// TestRetriedObjectPutOfOurOwnBytesIsAccepted: a create-only put that landed
// but answered ErrCAS (the SDK retry hitting its own first attempt) finds
// our own bytes at the key, so the checkpoint proceeds; nothing is counted
// as an overwrite and the stamp is trusted.
func TestRetriedObjectPutOfOurOwnBytesIsAccepted(t *testing.T) {
	w := newWS(t)
	requireClone(t, w)
	seedDB(t, w, "app", 1<<20)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	overwrites := countOverwrites(t)
	b := &landsThenFails{Backend: w.Store.B, err: fmt.Errorf("%w: key exists", store.ErrCAS), match: func(key string, _ []byte) bool {
		return strings.HasPrefix(key, "data/")
	}}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if err != nil {
		t.Fatalf("a retried put of our own bytes failed the checkpoint: %v", err)
	}
	if n := overwrites.Load(); n != 0 {
		t.Fatalf("overwrite counter %d, want 0", n)
	}
	assertTrustedStamp(t, path)
}

// TestForeignObjectAtPrivateKeyFailsTheCheckpoint: bytes that are not ours
// at a key only this checkpoint's epoch names can only be corruption; the
// checkpoint fails, leaves them in place, and releases its lease.
func TestForeignObjectAtPrivateKeyFailsTheCheckpoint(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	key := privateSnapshotKey(before)
	foreign := []byte("written by something other than offshoot")
	if err := w.Store.B.Put(key, foreign); err != nil {
		t.Fatal(err)
	}
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
	if !errors.Is(err, store.ErrCAS) || !strings.Contains(err.Error(), "already exists under this checkpoint's own epoch") {
		t.Fatalf("checkpoint over a foreign object at its key: %v", err)
	}
	data, _, gerr := w.Store.B.Get(key)
	if gerr != nil || !bytes.Equal(data, foreign) {
		t.Fatalf("the foreign object was overwritten or removed: %q %v", data, gerr)
	}
	assertLeaseReleased(t, w, before)
}

// dropsFirstHeadWrite drops the first ref PutIf that records the checkpoint
// name without writing it, runs run in its place, and reports err: a head
// write that never reached the store, with another write landing meanwhile.
type dropsFirstHeadWrite struct {
	store.Backend
	refKey, name string
	err          error
	run          func()
	fired        atomic.Int32
}

func (b *dropsFirstHeadWrite) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.refKey && recordsCheckpoint(data, b.name) && b.fired.CompareAndSwap(0, 1) {
		b.run()
		return "", b.err
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// TestHeadWriteThatFailedThenLostTheLeaseDeletesItsObject: a head write
// that never landed, followed by a reclaim of the lease before the
// checkpoint re-reads, fails the premise after a write was sent. The ref
// still has the lineage's head below the checkpoint's txid, which proves no
// head write naming the object landed, so the checkpoint deletes it and
// reports the loss, leaving the reclaimer's lease alone.
func TestHeadWriteThatFailedThenLostTheLeaseDeletesItsObject(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	b := &dropsFirstHeadWrite{Backend: w.Store.B, refKey: store.RefKey("app", "main"), name: "a",
		err: errors.New("store: s3 conditional put refs/app/main: context deadline exceeded")}
	b.run = func() { stealLease(t, w, "app", "main", "thief") }
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
	w.Store.B = b.Backend
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("checkpoint whose head write failed and whose lease was then taken: %v, want ErrLeaseLost", err)
	}
	if b.fired.Load() != 1 {
		t.Fatal("precondition: no head write was dropped")
	}
	ref := refOf(t, w, "app", "main")
	if ref.HeadTXID != before.HeadTXID || ref.LeaseHolder != "thief" {
		t.Fatalf("after the lost checkpoint: head %d holder %q, want head %d, the thief's lease", ref.HeadTXID, ref.LeaseHolder, before.HeadTXID)
	}
	if _, ok := ref.Checkpoints["a"]; ok {
		t.Fatal("the lost checkpoint was recorded")
	}
	if storeHas(w, privateSnapshotKey(before)) {
		t.Fatal("the object was left behind although the ref proves nothing names it")
	}
}

// errHeadWriteTimeout is a head write the client gave up on: no verdict
// from the store, which may still apply it.
var errHeadWriteTimeout = errors.New("store: s3 conditional put refs/app/main: context deadline exceeded")

// verdictlessHeadWrites answers the ref PutIfs that record the checkpoint
// name in turn from script: "cas" refuses one without writing, a
// compare-and-swap lost to a metadata write; "pending" holds one back
// without writing and reports errHeadWriteTimeout, a request the client gave
// up on that the store has yet to apply; "pending-cas" holds one back the
// same way but reports a lost compare-and-swap, as S3's 409 does when the
// SDK's retry of the write collides with its own first attempt still in
// flight. The held write is applied, under the etag it was sent with, just
// before the second ref read after the script runs out. The checkpoint's
// give-up read is the first, so that is the release's: the last moment the
// store could still apply it, since the release then moves the etag on.
type verdictlessHeadWrites struct {
	store.Backend
	refKey, name string
	script       []string
	mu           sync.Mutex
	sent, reads  int
	held         func() error
	applied      bool
	appliedErr   error
}

func (b *verdictlessHeadWrites) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key != b.refKey || !recordsCheckpoint(data, b.name) {
		return b.Backend.PutIf(key, data, ifMatch)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sent == len(b.script) {
		return b.Backend.PutIf(key, data, ifMatch)
	}
	step := b.script[b.sent]
	b.sent++
	if step == "pending" || step == "pending-cas" {
		body := append([]byte(nil), data...)
		b.held = func() error {
			_, err := b.Backend.PutIf(key, body, ifMatch)
			return err
		}
		if step == "pending" {
			return "", errHeadWriteTimeout
		}
		return "", fmt.Errorf("%w: s3 409 ConditionalRequestConflict", store.ErrCAS)
	}
	return "", fmt.Errorf("%w: refused for the test", store.ErrCAS)
}

func (b *verdictlessHeadWrites) Get(key string) ([]byte, string, error) {
	if key == b.refKey {
		b.mu.Lock()
		if b.sent == len(b.script) && b.held != nil {
			if b.reads++; b.reads == 2 {
				b.appliedErr, b.applied = b.held(), true
				b.held = nil
			}
		}
		b.mu.Unlock()
	}
	return b.Backend.Get(key)
}

// TestHeadWriteWithoutAVerdictKeepsItsObject: a head write that timed out
// got no verdict from the store, which can still apply it while the ref
// keeps the etag it was sent against. Giving up with the premise intact
// leaves that etag in place, so the checkpoint keeps its object and says it
// may have committed rather than that it lost compare-and-swaps; when the
// store then applies the write, the head names an object that is still
// there. The timeout can be the last attempt, or an earlier one whose etag
// the later, refused attempts shared, so the last error alone does not
// settle it.
func TestHeadWriteWithoutAVerdictKeepsItsObject(t *testing.T) {
	for name, script := range map[string][]string{
		"last attempt timed out":  {"cas", "cas", "pending"},
		"first attempt timed out": {"pending", "cas", "cas"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			seedDB(t, w, "app", 1<<16)
			path := w.CheckoutPath("app", "main")
			mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
			before := refOf(t, w, "app", "main")
			b := &verdictlessHeadWrites{Backend: w.Store.B, refKey: store.RefKey("app", "main"), name: "a", script: script}
			w.Store.B = b
			_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
			w.Store.B = b.Backend
			if !errors.Is(err, errHeadWriteTimeout) || !strings.Contains(err.Error(), "may have committed") || strings.Contains(err.Error(), "compare-and-swaps") {
				t.Fatalf("checkpoint whose head write got no verdict: %v, want a may-have-committed error wrapping the timeout", err)
			}
			if !b.applied || b.appliedErr != nil {
				t.Fatalf("precondition: the held head write was not applied before the release (applied %v: %v)", b.applied, b.appliedErr)
			}
			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != before.HeadTXID+1 || ref.Checkpoints["a"].TXID != ref.HeadTXID || ref.LeaseHolder != "" {
				t.Fatalf("after the late head write: head %d entry %+v holder %q", ref.HeadTXID, ref.Checkpoints["a"], ref.LeaseHolder)
			}
			if !storeHas(w, privateSnapshotKey(before)) {
				t.Fatal("the object was deleted while a head write the store could still apply named it")
			}
			if _, err := w.CheckoutAt("app", "main", "a", false); err != nil {
				t.Fatalf("the head the late write committed does not materialize: %v", err)
			}
		})
	}
}

// TestHeadWriteThatLandedThenMovedKeepsItsObject: a head write that landed
// but reported a timeout, after which the checkpoint cannot tell from the
// ref that it did. Either the branch was forked at the new checkpoint (the
// fork reads through its object) and then rolled back, which the landed
// write's lease release lets through unforced and which replaces the
// lineage, or the ref could not be read at all. The ref proves neither
// that the write landed nor that it did not, so the checkpoint keeps its
// object, reports that it may have committed, and the fork, or the head,
// materializes.
func TestHeadWriteThatLandedThenMovedKeepsItsObject(t *testing.T) {
	refKey := store.RefKey("app", "main")
	recordsA := func(key string, data []byte) bool { return key == refKey && recordsCheckpoint(data, "a") }
	setup := func(t *testing.T) (*Workspace, store.Ref, []byte) {
		t.Helper()
		w := newWS(t)
		seedDB(t, w, "app", 1<<16)
		path := w.CheckoutPath("app", "main")
		mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
		return w, refOf(t, w, "app", "main"), encodeCheckout(t, path, 1)
	}
	// sameContent reports whether the database at path holds the content the
	// checkpoint encoded (a snapshot's checksum is of its pages, not the txid
	// it was encoded at).
	sameContent := func(t *testing.T, path string, want []byte) bool {
		t.Helper()
		wantSum, err := ltxio.TrailerPostApplyChecksum(want)
		if err != nil {
			t.Fatal(err)
		}
		gotSum, err := ltxio.TrailerPostApplyChecksum(encodeCheckout(t, path, 1))
		if err != nil {
			t.Fatal(err)
		}
		return gotSum == wantSum
	}

	t.Run("forked then rolled back", func(t *testing.T) {
		w, before, want := setup(t)
		b := &landsThenFails{Backend: w.Store.B, err: errHeadWriteTimeout, match: recordsA, after: func() {
			if _, err := w.Fork("app", "main", "kid", "a", 0, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := w.RollbackWith("app", "main", "seed", RollbackOptions{NoBackup: true}); err != nil {
				t.Fatal(err)
			}
		}}
		w.Store.B = b
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
		w.Store.B = b.Backend
		if !errors.Is(err, errHeadWriteTimeout) || !strings.Contains(err.Error(), "may have committed") || !strings.Contains(err.Error(), "branch moved") {
			t.Fatalf("checkpoint whose landed head write was followed by a fork and a rollback: %v, want a may-have-committed error", err)
		}
		if !storeHas(w, privateSnapshotKey(before)) {
			t.Fatal("the object was deleted while a fork reads through it")
		}
		if !sameContent(t, mustCheckout(t, w, "app", "kid"), want) {
			t.Fatal("the fork at the landed checkpoint does not hold the checkpoint's content")
		}
	})

	t.Run("ref unreadable", func(t *testing.T) {
		w, before, want := setup(t)
		fb := &flakyRefGets{Backend: w.Store.B, key: refKey}
		b := &landsThenFails{Backend: fb, err: errHeadWriteTimeout, match: recordsA, after: func() { fb.n.Store(1 << 20) }}
		w.Store.B = b
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
		fb.n.Store(0)
		w.Store.B = fb.Backend
		if err == nil || !strings.Contains(err.Error(), "may have committed") || !strings.Contains(err.Error(), "could not be re-read") {
			t.Fatalf("checkpoint whose landed head write could not be confirmed: %v, want a may-have-committed error", err)
		}
		if !storeHas(w, privateSnapshotKey(before)) {
			t.Fatal("the object was deleted although the ref could not be read to show nothing names it")
		}
		ref := refOf(t, w, "app", "main")
		if ref.HeadTXID != before.HeadTXID+1 || ref.Checkpoints["a"].TXID != ref.HeadTXID {
			t.Fatalf("precondition: the head write did not land: head %d entry %+v", ref.HeadTXID, ref.Checkpoints["a"])
		}
		at, err := w.CheckoutAt("app", "main", "a", false)
		if err != nil {
			t.Fatalf("the committed head does not materialize: %v", err)
		}
		if !sameContent(t, at, want) {
			t.Fatal("the committed head does not hold the checkpoint's content")
		}
	})
}

// TestCheckpointChecksTheRefItsAcquireReads: the detached-checkout check
// runs on the ref the lease acquire reads, before it writes, so a `promote
// --onto --force` that leaves the checkout detached (a busy one it could
// not refresh, modelled by putting the old identity back on its sidecar)
// keeps the checkpoint from putting the old content on the new lineage and
// silently undoing the promote, however close to the acquire it lands:
//
//   - just before the acquire reads the ref: the check sees the new
//     lineage and refuses as detached, with nothing written;
//   - between the acquire's read and its write: the acquire's
//     compare-and-swap loses, and the re-read finds another writer came
//     and went, which is refused like a live lease.
//
// Either way the promoted ref keeps its head and epoch, no lease is left,
// and nothing is uploaded.
func TestCheckpointChecksTheRefItsAcquireReads(t *testing.T) {
	for _, tc := range []struct {
		name string
		when string
		want func(error) bool
	}{
		{"promote before the acquire's read", "hook", func(err error) bool { return errors.Is(err, ErrDetachedCheckout) }},
		{"promote inside the acquire", "acquire", func(err error) bool {
			return errors.Is(err, store.ErrLeaseHeld) && strings.Contains(err.Error(), "written by another checkpoint, session or repoint")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			seedDB(t, w, "app", 1<<16)
			if _, err := w.Fork("app", "main", "f", "", 0, nil); err != nil {
				t.Fatal(err)
			}
			mustSQL(t, mustCheckout(t, w, "app", "f"), "INSERT INTO t (v) VALUES (randomblob(10));")
			if _, err := w.Checkpoint("app", "f", "work", nil); err != nil {
				t.Fatal(err)
			}
			mainPath := mustCheckout(t, w, "app", "main")
			old, ok := readSidecar(mainPath)
			if !ok {
				t.Fatal("setup: main's checkout has no sidecar")
			}
			var promoted store.Ref
			promote := func() {
				if _, err := w.PromoteWith("app", "f", "main", PromoteOptions{Force: true, NoBackup: true}); err != nil {
					t.Error(err)
					return
				}
				if err := StampSumHashOnly(mainPath, old.Hash, old.Lineage, old.Epoch, old.TXID, old.PostApplyChecksum, old.ChainID); err != nil {
					t.Error(err)
				}
				ref, _, err := w.Store.GetRef("app", "main")
				if err != nil {
					t.Error(err)
				}
				promoted = ref
			}
			var orig store.Backend
			switch tc.when {
			case "hook":
				fired := false
				checkpointBeforeAcquireForTest = func() {
					if !fired {
						fired = true
						promote()
					}
				}
				t.Cleanup(func() { checkpointBeforeAcquireForTest = nil })
			case "acquire":
				orig = w.Store.B
				w.Store.B = &runBeforeAcquire{Backend: orig, refKey: store.RefKey("app", "main"), run: func() {
					// The promote runs through the unwrapped store, so this
					// wrapper sees only the checkpoint's writes.
					w.Store.B = orig
					promote()
				}}
			}
			_, err := w.CheckpointWith("app", "main", "after", nil, CheckpointOptions{})
			if orig != nil {
				w.Store.B = orig
			}
			if promoted.Lineage == "" || promoted.Lineage == old.Lineage {
				t.Fatalf("setup: the promote did not repoint main (lineage %q, was %q)", promoted.Lineage, old.Lineage)
			}
			if !tc.want(err) {
				t.Fatalf("checkpoint of a checkout the promote detached: %v", err)
			}
			ref := refOf(t, w, "app", "main")
			if ref.LeaseHolder != "" || ref.Lineage != promoted.Lineage || ref.HeadTXID != promoted.HeadTXID || ref.HeadEpoch != promoted.HeadEpoch || ref.Epoch != promoted.Epoch {
				t.Fatalf("after the refused checkpoint: holder %q lineage %s head %d@%d epoch %d; want the promoted ref untouched: lineage %s, head %d@%d, epoch %d",
					ref.LeaseHolder, ref.Lineage, ref.HeadTXID, ref.HeadEpoch, ref.Epoch, promoted.Lineage, promoted.HeadTXID, promoted.HeadEpoch, promoted.Epoch)
			}
			if _, ok := ref.Checkpoints["after"]; ok {
				t.Fatal("the refused checkpoint was recorded")
			}
			txid := promoted.HeadTXID + 1
			for epoch := promoted.Epoch; epoch <= promoted.Epoch+1; epoch++ {
				for _, k := range []string{store.SnapshotKey(promoted.Lineage, epoch, txid), store.SegmentKey(promoted.Lineage, epoch, txid, txid)} {
					if storeHas(w, k) {
						t.Fatalf("the refused checkpoint uploaded %s", k)
					}
				}
			}
		})
	}
}

// TestLeaseLostBeforeUploadSkipsTheUpload: a lease the renewer finds gone
// before the upload starts ends the checkpoint there, with no upload: a
// key under an epoch the checkpoint no longer holds is garbage the moment
// it lands, and the delete that would follow can fail and leave it for GC.
func TestLeaseLostBeforeUploadSkipsTheUpload(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	key := privateSnapshotKey(before)
	terminal := renewTerminal(t)
	rec := newRPCCountBackend(w.Store.B)
	w.Store.B = rec
	checkpointAfterQuiesceForTest = func() {
		stealLease(t, w, "app", "main", "thief")
		if err := awaitRenewTerminal(t, terminal, "the stolen lease"); !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("the renewer's terminal error: %v, want ErrLeaseLost", err)
		}
	}
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true, LeaseTTL: time.Second, RenewEvery: 10 * time.Millisecond})
	w.Store.B = rec.Backend
	if !errors.Is(err, store.ErrLeaseLost) || !strings.Contains(err.Error(), "its lease ended while it ran") {
		t.Fatalf("checkpoint whose lease was stolen before its upload: %v, want the renewer's ErrLeaseLost", err)
	}
	if n := rec.putIfCount(key); n != 0 {
		t.Fatalf("the checkpoint uploaded its object %d time(s) after its renewer found the lease gone", n)
	}
	if ref := refOf(t, w, "app", "main"); ref.HeadTXID != before.HeadTXID || ref.LeaseHolder != "thief" {
		t.Fatalf("after the lost checkpoint: head %d holder %q, want head %d, the thief's lease", ref.HeadTXID, ref.LeaseHolder, before.HeadTXID)
	}
}

// TestDestroyClaimBeforeHeadWriteFailsTheCheckpoint: `destroy --force`
// claims the ref (Deleting, with the lease fields left alone) and then
// quiesces the checkout before its conditional delete. A checkpoint whose
// head write re-reads the ref in that window finds the claim and fails
// without committing: it deletes its object, and neither its head write
// nor its release moves the claim's etag, so the destroy's conditional
// delete still goes through.
func TestDestroyClaimBeforeHeadWriteFailsTheCheckpoint(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	key := privateSnapshotKey(before)
	g := gateRefCAS(w, "app", "main")
	g.holdObject(key)
	done := make(chan error, 1)
	go func() {
		// The default renewal interval (a third of 30 s) puts no renewal
		// inside this test, so the claim's etag stays the one the delete
		// compares against unless the checkpoint itself writes the ref.
		_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
		done <- err
	}()
	release := heldUpload(t, g, done)
	// Destroy's claim write, by hand, so the test holds the window open.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.Deleting = true
	ref.DeletingAt = time.Now().UTC().Format(time.RFC3339Nano)
	claimEtag, err := w.Store.PutRef("app", "main", ref, etag)
	if err != nil {
		t.Fatal(err)
	}
	release()
	err = <-done
	if !errors.Is(err, store.ErrDeleting) || !strings.Contains(err.Error(), "did not commit") {
		t.Fatalf("checkpoint that found a destroy claim at its head write: %v, want a did-not-commit ErrDeleting", err)
	}
	if storeHas(w, key) {
		t.Fatal("the failed checkpoint left its object")
	}
	if err := w.Store.DeleteRefIf("app", "main", claimEtag); err != nil {
		t.Fatalf("destroy's conditional delete after the failed checkpoint: %v (the checkpoint wrote over the claim)", err)
	}
}

// TestHeadWriteHiddenBehindAConflictKeepsItsObject: on S3 a 409 conflict,
// which the store reports as a lost compare-and-swap, can be the SDK's
// retry of the head write colliding with its own first attempt, still in
// flight after a 5xx or a dropped connection. Every attempt then reads as
// refused with the premise intact, and the checkpoint gives up, but the
// first attempt can still land. It keeps its object, so when that write
// lands the head names an object that is there.
func TestHeadWriteHiddenBehindAConflictKeepsItsObject(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	b := &verdictlessHeadWrites{Backend: w.Store.B, refKey: store.RefKey("app", "main"), name: "a", script: []string{"pending-cas", "cas", "cas"}}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
	w.Store.B = b.Backend
	if !errors.Is(err, store.ErrCAS) || !strings.Contains(err.Error(), "lost 3 compare-and-swaps") {
		t.Fatalf("checkpoint whose head writes all read as conflicts: %v", err)
	}
	if !b.applied || b.appliedErr != nil {
		t.Fatalf("precondition: the first head write did not land after the checkpoint gave up (applied %v: %v)", b.applied, b.appliedErr)
	}
	ref := refOf(t, w, "app", "main")
	if ref.HeadTXID != before.HeadTXID+1 || ref.Checkpoints["a"].TXID != ref.HeadTXID || ref.LeaseHolder != "" {
		t.Fatalf("after the late head write: head %d entry %+v holder %q", ref.HeadTXID, ref.Checkpoints["a"], ref.LeaseHolder)
	}
	if !storeHas(w, privateSnapshotKey(before)) {
		t.Fatal("the object was deleted while a head write hidden behind a conflict could still land")
	}
	if _, err := w.CheckoutAt("app", "main", "a", false); err != nil {
		t.Fatalf("the head the late write committed does not materialize: %v", err)
	}
}

// claimAfterHeldHeadWrite holds back the first ref PutIf that records the
// checkpoint name, reporting errHeadWriteTimeout (a write the store has yet
// to apply), and runs claim just before the next ref read: a destroy or
// reap claim landing after the head write was sent.
type claimAfterHeldHeadWrite struct {
	store.Backend
	refKey, name string
	claim        func()
	armed        atomic.Int32
	held         func() error
}

func (b *claimAfterHeldHeadWrite) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == b.refKey && b.held == nil && recordsCheckpoint(data, b.name) {
		body := append([]byte(nil), data...)
		b.held = func() error {
			_, err := b.Backend.PutIf(key, body, ifMatch)
			return err
		}
		b.armed.Store(1)
		return "", errHeadWriteTimeout
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

func (b *claimAfterHeldHeadWrite) Get(key string) ([]byte, string, error) {
	if key == b.refKey && b.armed.CompareAndSwap(1, 0) {
		b.claim()
	}
	return b.Backend.Get(key)
}

// TestHeadWriteSentBeforeAClaimKeepsItsObject: a head write that got no
// verdict, then a `destroy --force` or a reap that claims the branch before
// the checkpoint re-reads it. The claim fails the premise, but it is the
// one change that can be undone to the very bytes the write was sent
// against: Destroy unwinds its claim when its quiesce or conditional delete
// fails, and the reaper clears a stale one, after which the store can still
// apply the held write. The checkpoint keeps its object and says it may
// have committed, so the head that write then sets names an object that is
// there.
func TestHeadWriteSentBeforeAClaimKeepsItsObject(t *testing.T) {
	for _, tc := range []struct {
		name   string
		claim  func(*store.Ref)
		unwind func(t *testing.T, w *Workspace)
		want   error
	}{
		{"destroy", func(r *store.Ref) { r.Deleting, r.DeletingAt = true, time.Now().UTC().Format(time.RFC3339Nano) },
			func(t *testing.T, w *Workspace) { w.unwindDeletingClaim("app", "main") }, store.ErrDeleting},
		{"reap", func(r *store.Ref) { r.Reaping = true }, func(t *testing.T, w *Workspace) {
			ref, etag, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.clearStaleReapingClaim("app", "main", ref, etag); err != nil {
				t.Fatal(err)
			}
		}, store.ErrReaping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWS(t)
			seedDB(t, w, "app", 1<<16)
			mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
			before := refOf(t, w, "app", "main")
			b := &claimAfterHeldHeadWrite{Backend: w.Store.B, refKey: store.RefKey("app", "main"), name: "a"}
			b.claim = func() {
				ref, etag, err := w.Store.GetRef("app", "main")
				if err != nil {
					t.Error(err)
					return
				}
				tc.claim(&ref)
				if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
					t.Error(err)
				}
			}
			w.Store.B = b
			_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
			w.Store.B = b.Backend
			if !errors.Is(err, tc.want) || !errors.Is(err, errHeadWriteTimeout) || !strings.Contains(err.Error(), "may have committed") {
				t.Fatalf("checkpoint whose sent head write was followed by a %s claim: %v, want a may-have-committed error wrapping the claim and the timeout", tc.name, err)
			}
			key := privateSnapshotKey(before)
			if !storeHas(w, key) {
				t.Fatal("the object was deleted while a head write the store could still apply names it")
			}
			tc.unwind(t, w)
			if b.held == nil {
				t.Fatal("precondition: no head write was held")
			}
			if err := b.held(); err != nil {
				t.Fatalf("precondition: the held head write did not land after the claim was undone: %v", err)
			}
			ref := refOf(t, w, "app", "main")
			if ref.HeadTXID != before.HeadTXID+1 || ref.Checkpoints["a"].TXID != ref.HeadTXID {
				t.Fatalf("after the late head write: head %d entry %+v", ref.HeadTXID, ref.Checkpoints["a"])
			}
			if _, err := w.CheckoutAt("app", "main", "a", false); err != nil {
				t.Fatalf("the head the late write committed does not materialize: %v", err)
			}
		})
	}
}

// afterObjectPut forwards PutIf and, right after the create-only put of key
// lands, runs after.
type afterObjectPut struct {
	store.Backend
	key   string
	after func()
	fired atomic.Int32
}

func (b *afterObjectPut) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err == nil && key == b.key && b.fired.CompareAndSwap(0, 1) {
		b.after()
	}
	return etag, err
}

// TestOlderBinaryCheckpointUnderOurLeaseKeepsTheHeadsObject: an offshoot
// older than the checkpoint lease runs `checkpoint --force` while a new
// checkpoint holds the branch. It goes past the live lease, plans under the
// epoch the ref carries, the new checkpoint's private one, and so computes
// the same key; it finds the key taken, overwrites it unconditionally, and
// moves the head to it, leaving the lease in place. The new checkpoint
// cannot commit, but the head now names its key, so it keeps that object
// (the branch stays materializable) and says the branch moved under its own
// lease, rather than that it lost the lease to its own holder.
func TestOlderBinaryCheckpointUnderOurLeaseKeepsTheHeadsObject(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	path := w.CheckoutPath("app", "main")
	mustSQL(t, path, "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	key := privateSnapshotKey(before)
	oldBytes := encodeCheckout(t, path, before.HeadTXID+1)
	b := &afterObjectPut{Backend: w.Store.B, key: key}
	var holder string
	b.after = func() {
		// What v0.2.16's CheckpointWith does with --force under a live lease.
		ref, etag, err := w.Store.GetRef("app", "main")
		if err != nil {
			t.Error(err)
			return
		}
		holder = ref.LeaseHolder
		if _, err := b.Backend.PutIf(key, oldBytes, ""); !errors.Is(err, store.ErrCAS) {
			t.Errorf("the older binary's create-only put: %v, want ErrCAS", err)
		}
		if err := b.Backend.Put(key, oldBytes); err != nil {
			t.Error(err)
		}
		txid := ref.HeadTXID + 1
		ref.HeadTXID, ref.HeadEpoch = txid, ref.Epoch
		ref.SetCheckpoint("old", store.Checkpoint{TXID: txid, Epoch: ref.Epoch, CreatedAt: nowStamp(), Kind: "snapshot"})
		if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
			t.Errorf("the older binary's head write: %v", err)
		}
	}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "new", nil, CheckpointOptions{Snapshot: true})
	w.Store.B = b.Backend
	if b.fired.Load() != 1 {
		t.Fatal("precondition: the older binary's checkpoint did not run")
	}
	if !errors.Is(err, store.ErrLeaseLost) || !strings.Contains(err.Error(), "moved under this checkpoint's own lease") || strings.Contains(err.Error(), fmt.Sprintf("held by %q", holder)) {
		t.Fatalf("checkpoint whose branch an older binary moved under its lease: %v", err)
	}
	if !storeHas(w, key) {
		t.Fatal("the checkpoint deleted the object the head names")
	}
	if _, err := w.CheckoutAt("app", "main", "old", false); err != nil {
		t.Fatalf("the head the older binary committed does not materialize: %v", err)
	}
}

// countRefGets counts Gets of key and forwards the backend's conditional
// delete, so a test can wait on reads of the ref while Destroy's delete
// stays conditional.
type countRefGets struct {
	store.Backend
	key string
	n   atomic.Int32
}

func (b *countRefGets) Get(key string) ([]byte, string, error) {
	if key == b.key {
		b.n.Add(1)
	}
	return b.Backend.Get(key)
}

func (b *countRefGets) DeleteIf(key, ifMatch string) error {
	return b.Backend.(store.ConditionalDeleter).DeleteIf(key, ifMatch)
}

// TestRenewalsLeaveADestroyClaimAlone: `destroy --force` claims the ref and
// quiesces the checkout before its conditional delete, which compares
// against the claim's etag. A checkpoint renewing its lease meanwhile reads
// the claim and does not write over it, so the delete goes through however
// many renewals fall inside the quiesce, and the checkpoint then fails on
// the destroyed branch.
func TestRenewalsLeaveADestroyClaimAlone(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	refKey := store.RefKey("app", "main")
	cb := &countRefGets{Backend: w.Store.B, key: refKey}
	w.Store.B = cb
	var deleteErr error
	deleted := false
	checkpointAfterQuiesceForTest = func() {
		var claimEtag string
		for i := 0; ; i++ {
			ref, etag, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			ref.Deleting, ref.DeletingAt = true, time.Now().UTC().Format(time.RFC3339Nano)
			if claimEtag, err = w.Store.PutRef("app", "main", ref, etag); err == nil {
				break
			}
			if !errors.Is(err, store.ErrCAS) || i == 20 {
				t.Fatal(err)
			}
		}
		// Destroy's quiesce of a busy checkout, long enough for the
		// renewer to read the claimed ref three times.
		n := cb.n.Load()
		waitFor(t, "three renewals over the claim", func() bool { return cb.n.Load() >= n+3 })
		deleteErr, deleted = w.Store.DeleteRefIf("app", "main", claimEtag), true
	}
	t.Cleanup(func() { checkpointAfterQuiesceForTest = nil })
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true, LeaseTTL: 2 * time.Second, RenewEvery: 10 * time.Millisecond})
	w.Store.B = cb.Backend
	if !deleted || deleteErr != nil {
		t.Fatalf("destroy's conditional delete after renewals over its claim: %v (a renewal moved the claim's etag)", deleteErr)
	}
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("checkpoint of a branch destroyed while it ran: %v, want ErrNotFound", err)
	}
	if _, _, err := w.Store.GetRef("app", "main"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the destroyed branch came back: %v", err)
	}
}

// conflictingObjectPuts answers every create-only put under data/ with a
// lost compare-and-swap without writing, and fails reads of those keys with
// getErr when it is set: a 409 against an earlier attempt still in flight,
// with nothing (yet) to read back, or a read-back that fails.
type conflictingObjectPuts struct {
	store.Backend
	getErr error
}

func (b conflictingObjectPuts) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, "data/") {
		return "", fmt.Errorf("%w: s3 409 ConditionalRequestConflict", store.ErrCAS)
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

func (b conflictingObjectPuts) Get(key string) ([]byte, string, error) {
	if strings.HasPrefix(key, "data/") && b.getErr != nil {
		return nil, "", b.getErr
	}
	return b.Backend.Get(key)
}

// TestObjectPutConflictWithNothingToReadIsNotCorruption: a create-only put
// refused as a taken key is corruption only when the key holds bytes that
// are not ours. With nothing there (on S3, a 409 against an earlier attempt
// of the same put still in flight), or a read-back that fails, the
// checkpoint fails with a retryable error that says what happened, and
// releases its lease.
func TestObjectPutConflictWithNothingToReadIsNotCorruption(t *testing.T) {
	readFailure := errors.New("transient: connection reset")
	for name, tc := range map[string]struct {
		getErr error
		want   string
	}{
		"nothing there":     {nil, "nothing is there"},
		"read-back failure": {readFailure, "reading it back failed"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWS(t)
			seedDB(t, w, "app", 1<<16)
			mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
			before := refOf(t, w, "app", "main")
			orig := w.Store.B
			w.Store.B = conflictingObjectPuts{Backend: orig, getErr: tc.getErr}
			_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{Snapshot: true})
			w.Store.B = orig
			if !errors.Is(err, store.ErrCAS) || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "retry") || strings.Contains(err.Error(), "corruption") {
				t.Fatalf("checkpoint whose object put conflicted with nothing readable: %v", err)
			}
			if tc.getErr != nil && !errors.Is(err, tc.getErr) {
				t.Fatalf("the read-back's error is not wrapped: %v", err)
			}
			assertLeaseReleased(t, w, before)
		})
	}
}

// failRefWrites fails every ref PutIf with a store error that is not a
// compare-and-swap: a store that cannot be written right now.
type failRefWrites struct{ store.Backend }

func (b failRefWrites) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if strings.HasPrefix(key, "refs/") {
		return "", errors.New("store unreachable")
	}
	return b.Backend.PutIf(key, data, ifMatch)
}

// TestReleaseFailureLogsTheLeasesRealExpiry: a release that fails is
// logged with the expiry the ref carries, which renewals have moved past
// the one the lease was acquired with, so the log does not say the lease
// has lapsed while it is still live.
func TestReleaseFailureLogsTheLeasesRealExpiry(t *testing.T) {
	w, l := leasedBranch(t, newCheckpointHolder(), time.Minute)
	renewed, err := w.Store.RenewLease(l, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	orig := w.Store.B
	w.Store.B = failRefWrites{orig}
	out := captureStderr(t, func() { w.releaseCheckpointLease(l) })
	w.Store.B = orig
	if want := "it expires at " + renewed.Expiry.Format(time.RFC3339); !strings.Contains(out, want) {
		t.Fatalf("release failure logged %q, want it to say %q", out, want)
	}
}
