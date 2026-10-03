package ops_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
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
