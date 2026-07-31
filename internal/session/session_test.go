package session

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
)

func newWS(t *testing.T) *ops.Workspace {
	t.Helper()
	w, err := ops.Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func requireSQLite(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestOpenHoldsLeaseAndCaptures(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.Lease().Holder == "" || s.Lease().Epoch < 2 {
		t.Fatalf("lease = %+v", s.Lease())
	}
	// The branch is leased in the store, not just in memory.
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != s.Lease().Holder {
		t.Fatalf("ref holder = %q, want %q", ref.LeaseHolder, s.Lease().Holder)
	}

	// An agent writes to the checkout with no coordination at all.
	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// The replica converges without the writer ever being paused.
	waitFor(t, 10*time.Second, "replica to converge", func() bool {
		out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
		return err == nil && string(out) == "2\n"
	})
	if s.Err() != nil {
		t.Fatalf("session errored: %v", s.Err())
	}
}

func TestOpenRefusesLeasedBranch(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", Holder: "one"})
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	if _, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", Holder: "two"}); err == nil {
		t.Fatal("a second session on a leased branch must be refused")
	}
}

func TestCloseReleasesLeaseAndCleansUp(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	replica := s.ReplicaPath()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close must be idempotent: %v", err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("Close must release the lease, holder = %q", ref.LeaseHolder)
	}
	if _, err := os.Stat(replica); !os.IsNotExist(err) {
		t.Fatalf("Close must remove the scratch replica, stat err = %v", err)
	}
	// The branch is immediately acquirable by someone else.
	if _, err := w.AcquireLease("app", "main", "next", ops.DefaultLeaseTTL); err != nil {
		t.Fatalf("branch not acquirable after Close: %v", err)
	}
}
