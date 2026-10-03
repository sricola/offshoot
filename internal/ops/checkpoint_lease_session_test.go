package ops_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
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
	want := fmt.Sprintf("ops: app@main has a live lease held by %q until %s (an open daemon session, or 'offshoot lease acquire'); --force cannot take over a live lease; close the session (or wait for the other checkpoint) and retry",
		held.LeaseHolder, held.LeaseExpiry)
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
// closes between the checkpoint's first ref read and its acquire moves the
// head and the epoch under it. The checkpoint plans from the ref its
// acquire returned, so it commits at the txid after the session's flush,
// and its head materializes to the checkout, the session's row included.
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
			t.Fatalf("session open between the first read and the acquire: %v", err)
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
		t.Fatalf("checkpoint at txid %d, want %d: it planned from the ref it first read", res.TXID, flushed+1)
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
