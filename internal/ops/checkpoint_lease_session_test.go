package ops_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// leaseSeededMain creates app with a one-row table on main, checks it out,
// checkpoints it as "base", and returns the checkout path.
func leaseSeededMain(t *testing.T, w *ops.Workspace) string {
	t.Helper()
	testutil.RequireSQLite3(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	leaseSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "base", nil); err != nil {
		t.Fatal(err)
	}
	return path
}

func leaseSQL(t *testing.T, path, stmt string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", path, stmt).CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite3 %q: %v: %s", stmt, err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestForceCannotTakeOverLiveLease: `checkpoint --force` on a branch with a
// live session is refused with the exact message, leaves the session's
// lease, epoch and head untouched, and the session keeps writing.
func TestForceCannotTakeOverLiveLease(t *testing.T) {
	w := newWS(t)
	leaseSeededMain(t, w)
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", LeaseTTL: time.Minute, RenewEvery: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	leaseSQL(t, s.CheckoutPath(), "INSERT INTO t VALUES (2);")
	held, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("ops: app@main has a live lease held by %q until %s (an open daemon session, or 'offshoot lease acquire'); --force cannot take over a live lease; close the session and retry, or, if the process that holds it has exited, free it with 'offshoot lease release app@main --holder %s' (never while that daemon still runs: a session it reopened may hold the lease)",
		held.LeaseHolder, held.LeaseExpiry, held.LeaseHolder)
	_, err = w.CheckpointWith("app", "main", "forced", nil, ops.CheckpointOptions{Force: true})
	if !errors.Is(err, store.ErrLeaseHeld) || err.Error() != want {
		t.Fatalf("checkpoint --force under a session: %v\nwant: %s", err, want)
	}
	after, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.LeaseHolder != held.LeaseHolder || after.Epoch != held.Epoch || after.HeadTXID != held.HeadTXID || after.HeadEpoch != held.HeadEpoch {
		t.Fatalf("the refused checkpoint changed the ref: %+v -> %+v", held, after)
	}
	leaseSQL(t, s.CheckoutPath(), "INSERT INTO t VALUES (3);")
	txid, err := s.Flush("", nil)
	if err != nil || s.Err() != nil {
		t.Fatalf("the session's flush after the refused checkpoint: %v (session err %v)", err, s.Err())
	}
	if txid <= held.HeadTXID {
		t.Fatalf("flush at txid %d, want past %d", txid, held.HeadTXID)
	}
}

// TestSessionOpenDuringAtRestCheckpointIsRefused: an at-rest checkpoint
// holds the branch lease while it runs, so a session opening on the branch
// meanwhile gets the lease-held error naming the checkpoint, and opens
// normally once the checkpoint has committed.
func TestSessionOpenDuringAtRestCheckpointIsRefused(t *testing.T) {
	w := newWS(t)
	path := leaseSeededMain(t, w)
	leaseSQL(t, path, "INSERT INTO t VALUES (2);")
	paused, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(release)
	ops.SetCheckpointAfterQuiesceForTest(func() { pauseOnce.Do(func() { close(paused); <-resume }) })
	t.Cleanup(func() { ops.SetCheckpointAfterQuiesceForTest(nil) })
	done := make(chan error, 1)
	go func() {
		_, err := w.Checkpoint("app", "main", "a", nil)
		done <- err
	}()
	<-paused
	if s, err := session.Open(context.Background(), session.Options{WS: w, DB: "app", Branch: "main"}); err == nil {
		s.Close()
		t.Fatal("a session opened on a branch an at-rest checkpoint holds")
	} else if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), `"checkpoint:`) {
		t.Fatalf("session open during an at-rest checkpoint: %v, want a lease-held refusal naming the checkpoint", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	s, err := session.Open(context.Background(), session.Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatalf("session open after the checkpoint committed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCheckpointPlansFromTheAcquiredRef: a session that opens, flushes and
// closes just before the checkpoint's acquire moves the head and the epoch.
// The checkpoint plans from the ref its acquire read and wrote, so it
// commits at the txid after the session's flush, and its head materializes
// to the checkout, the session's row included.
func TestCheckpointPlansFromTheAcquiredRef(t *testing.T) {
	w := newWS(t)
	leaseSeededMain(t, w)
	var flushed, sessEpoch uint64
	fired := false
	ops.SetCheckpointBeforeAcquireForTest(func() {
		if fired {
			return
		}
		fired = true
		s, err := session.Open(context.Background(), session.Options{WS: w, DB: "app", Branch: "main"})
		if err != nil {
			t.Fatalf("session open just before the acquire: %v", err)
		}
		leaseSQL(t, s.CheckoutPath(), "INSERT INTO t VALUES (2);")
		if flushed, err = s.Flush("", nil); err != nil {
			t.Fatal(err)
		}
		sessEpoch = s.Lease().Epoch
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() { ops.SetCheckpointBeforeAcquireForTest(nil) })
	res, err := w.CheckpointWith("app", "main", "after", nil, ops.CheckpointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.TXID != flushed+1 {
		t.Fatalf("checkpoint at txid %d, want %d: it planned from a ref older than its acquire's", res.TXID, flushed+1)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.HeadTXID != res.TXID || ref.Checkpoints["after"].Epoch <= sessEpoch {
		t.Fatalf("head %d entry epoch %d, want head %d under an epoch past the session's %d", ref.HeadTXID, ref.Checkpoints["after"].Epoch, res.TXID, sessEpoch)
	}
	at, err := w.CheckoutAt("app", "main", "after", false)
	if err != nil {
		t.Fatal(err)
	}
	if got := leaseSQL(t, at, "SELECT group_concat(v) FROM t;"); got != "1,2" {
		t.Fatalf("the committed head holds %q, want 1,2", got)
	}
}

// TestAtRestCheckpointIsRefusedWhileASessionCloses: a session's Close keeps
// renewing its lease until it releases it, past the engine shutdown, the
// sidecar stamp and the shadow refresh, which can outlast the lease's own
// TTL. An at-rest checkpoint, forced or not, is refused under that lease
// for the whole close, with the session's holder named and the advice to
// close the session and retry, and writes nothing: it never quiesces the
// checkout the closing engine still owns. Had the lease lapsed while the
// close ran, the checkpoint would have reclaimed it and committed under the
// closing session. Once the close releases the lease, the checkpoint goes
// through and the close reports no error. The close is held in
// session.CloseReleaseHook past the expiry the lease had when it got there.
func TestAtRestCheckpointIsRefusedWhileASessionCloses(t *testing.T) {
	w := newWS(t)
	leaseSeededMain(t, w)
	const ttl = 1500 * time.Millisecond
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", LeaseTTL: ttl, RenewEvery: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseSQL(t, s.CheckoutPath(), "INSERT INTO t VALUES (2);")
	if _, err := s.Flush("", nil); err != nil {
		t.Fatal(err)
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	var proceedOnce sync.Once
	release := func() { proceedOnce.Do(func() { close(proceed) }) }
	session.CloseReleaseHook = func() {
		close(entered)
		<-proceed
	}
	t.Cleanup(func() { session.CloseReleaseHook = nil; release() })
	closeErr := make(chan error, 1)
	go func() { closeErr <- s.Close() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("Close never reached CloseReleaseHook")
	}
	session.CloseReleaseHook = nil // the held Close has already read it

	held, heldEtag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	exp, err := time.Parse(time.RFC3339Nano, held.LeaseExpiry)
	if err != nil {
		t.Fatal(err)
	}
	// The hook runs right after a checkpoint's quiesce: a refused checkpoint
	// must never get that far on the checkout the closing engine owns.
	var quiesced atomic.Int32
	ops.SetCheckpointAfterQuiesceForTest(func() { quiesced.Add(1) })
	t.Cleanup(func() { ops.SetCheckpointAfterQuiesceForTest(nil) })
	attempts := 0
	for force := false; attempts == 0 || time.Now().Before(exp.Add(200*time.Millisecond)); force = !force {
		attempts++
		_, err := w.CheckpointWith("app", "main", "during-close", nil, ops.CheckpointOptions{Force: force})
		if !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), fmt.Sprintf("held by %q", held.LeaseHolder)) ||
			!strings.Contains(err.Error(), "close the session and retry") {
			release()
			<-closeErr
			t.Fatalf("checkpoint (force %v) while the session closes, %d attempts in: %v; want a refusal under the session's lease", force, attempts, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		release()
		<-closeErr
		t.Fatal(err)
	}
	if etag == heldEtag {
		release()
		<-closeErr
		t.Fatal("precondition: no renewal landed while the close was held")
	}
	if n := quiesced.Load(); n != 0 {
		release()
		<-closeErr
		t.Fatalf("%d refused checkpoints quiesced the checkout the closing session still owns", n)
	}
	ops.SetCheckpointAfterQuiesceForTest(nil)
	if ref.LeaseHolder != held.LeaseHolder || ref.Epoch != held.Epoch || ref.HeadTXID != held.HeadTXID || ref.Checkpoints["during-close"].TXID != 0 {
		release()
		<-closeErr
		t.Fatalf("the refused checkpoints changed the ref: %+v -> %+v", held, ref)
	}
	release()
	if err := <-closeErr; err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.Err() != nil {
		t.Fatalf("the session failed while closing: %v", s.Err())
	}
	res, err := w.CheckpointWith("app", "main", "after-close", nil, ops.CheckpointOptions{})
	if err != nil {
		t.Fatalf("checkpoint after the close released the lease: %v", err)
	}
	if res.TXID != held.HeadTXID+1 {
		t.Fatalf("checkpoint after the close at txid %d, want %d", res.TXID, held.HeadTXID+1)
	}
}
