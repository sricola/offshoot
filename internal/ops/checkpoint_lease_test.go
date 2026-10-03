package ops

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
type landsThenFails struct {
	store.Backend
	match func(key string, data []byte) bool
	err   error
	hits  atomic.Int32
}

func (b *landsThenFails) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err != nil || !b.match(key, data) || b.hits.Add(1) > 1 {
		return etag, err
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
	var held chan struct{}
	select {
	case held = <-g.objArrived:
	case err := <-done:
		t.Fatalf("the checkpoint ended before its upload: %v", err)
	}
	var once sync.Once
	release := func() { once.Do(func() { close(held) }) }
	t.Cleanup(release)

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
// between this checkpoint's first ref read and its acquire moves the head
// and the epoch under it. Planning from the ref the acquire returned, this
// checkpoint lands at the next txid, as a segment over the other's state.
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
		t.Fatalf("checkpoint at txid %d, want %d: it planned from the ref it first read", res.TXID, between.TXID+1)
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
	t.Run("name taken before the acquire", func(t *testing.T) {
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
			t.Fatalf("a name taken between the first read and the acquire: %v", err)
		}
		ref := refOf(t, w, "app", "main")
		won := ref.Checkpoints["dup"]
		if ref.LeaseHolder != "" || ref.HeadTXID != won.TXID || ref.Epoch != won.Epoch+1 {
			t.Fatalf("after the refused re-check: holder %q head %d epoch %d, want no lease, head %d, epoch %d",
				ref.LeaseHolder, ref.HeadTXID, ref.Epoch, won.TXID, won.Epoch+1)
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
// its own and adopts it under the epoch the landed write minted, rather
// than refusing itself as "another checkpoint" and leaving that lease to
// block the branch for a whole TTL.
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
			if n := b.hits.Load(); n < 2 {
				t.Fatalf("%d acquires carried a checkpoint lease, want the failed one and the one that adopted it", n)
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

// TestAcquireThatKeepsFailingReleasesItsLease: when the acquire and its one
// retry both land but report failure, the checkpoint gives up, and releases
// the lease its holder left on the ref rather than leaving the branch
// refused to every writer for a whole TTL.
func TestAcquireThatKeepsFailingReleasesItsLease(t *testing.T) {
	w := newWS(t)
	seedDB(t, w, "app", 1<<16)
	mustSQL(t, w.CheckoutPath("app", "main"), "INSERT INTO t (v) VALUES (randomblob(100));")
	before := refOf(t, w, "app", "main")
	b := &acquiresLandThenFail{Backend: w.Store.B, refKey: store.RefKey("app", "main")}
	w.Store.B = b
	_, err := w.CheckpointWith("app", "main", "a", nil, CheckpointOptions{})
	w.Store.B = b.Backend
	if err == nil {
		t.Fatal("a checkpoint whose every acquire reports failure must fail")
	}
	if n := b.hits.Load(); n != 2 {
		t.Fatalf("the acquire was tried %d times, want 2 (one retry)", n)
	}
	assertLeaseReleased(t, w, before)
	mustCheckpointWith(t, w, "app", "main", "a", CheckpointOptions{})
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
