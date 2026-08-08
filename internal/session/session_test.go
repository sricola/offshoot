package session

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/testutil"
)

// captureStderr redirects os.Stderr for the duration of fn and returns
// everything written to it — the same helper internal/ops and internal/store
// already use for their own stderr-log tests.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = wr
	fn()
	wr.Close()
	os.Stderr = orig
	out, _ := io.ReadAll(r)
	return string(out)
}

func newWS(t *testing.T) *ops.Workspace {
	t.Helper()
	w, err := ops.Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
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
	testutil.RequireSQLite3(t)
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
	testutil.RequireSQLite3(t)
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
	testutil.RequireSQLite3(t)
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

func TestOpenReleasesLeaseWhenCheckoutFails(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Create a directory at the exact checkout path to make the rename fail.
	checkoutPath := w.CheckoutPath("app", "main")
	if err := os.MkdirAll(filepath.Dir(checkoutPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(checkoutPath, 0755); err != nil {
		t.Fatal(err)
	}

	// Open should fail due to checkout failure.
	_, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err == nil {
		t.Fatal("Open must fail when checkout path already exists as a directory")
	}

	// The branch must be immediately acquirable, proving the lease was released.
	if _, err := w.AcquireLease("app", "main", "next", ops.DefaultLeaseTTL); err != nil {
		t.Fatalf("branch not acquirable after Open failure: %v", err)
	}
}

// TestSessionTransitionLogsOpenedFlushedClosed pins task 7's structured
// transition-log contract: Open, a manual Flush, and Close each write one
// "offshoot: session: db@branch: event key=value ..." line to stderr,
// matching the daemon janitor's own "offshoot: janitor: ..." prefix family
// (see internal/daemon/server.go's StartJanitor) rather than inventing a
// second log format. FlushEvery is left at its default (0, manual only), so
// the single "flushed" line asserted below is unambiguously this test's own
// manual call, not a background auto-flush tick.
func TestSessionTransitionLogsOpenedFlushedClosed(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}

	var txid uint64
	out := captureStderr(t, func() {
		s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", Holder: "logtest"})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		waitFor(t, 10*time.Second, "capture", func() bool {
			out, err := exec.Command("sqlite3", s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
			return err == nil && string(out) == "1\n"
		})
		txid, err = s.Flush("", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	const prefix = "offshoot: session: app@main: "
	if !strings.Contains(out, prefix+`opened holder="logtest" epoch=`) {
		t.Fatalf("missing/malformed opened line in:\n%s", out)
	}
	wantFlushed := fmt.Sprintf("%sflushed kind=%q txid=%d", prefix, "manual", txid)
	if !strings.Contains(out, wantFlushed) {
		t.Fatalf("missing/malformed flushed line (want %q) in:\n%s", wantFlushed, out)
	}
	if !strings.Contains(out, prefix+"closed") {
		t.Fatalf("missing closed line in:\n%s", out)
	}
}

// TestSessionFencedTransitionIsLogged extends TestFlushAfterFencingIsRefused's
// scenario with the "fenced" transition log: fail()'s first call must write
// one "offshoot: session: db@branch: fenced cause=..." line, quoting the
// underlying ErrFenced cause.
func TestSessionFencedTransitionIsLogged(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main",
		Holder: "session-a", LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}

	out := captureStderr(t, func() {
		if _, err := s.Flush("", nil); err == nil {
			t.Fatal("Flush after fencing must fail")
		}
	})
	const prefix = "offshoot: session: app@main: fenced cause="
	if !strings.Contains(out, prefix) {
		t.Fatalf("missing fenced transition log in:\n%s", out)
	}
}
