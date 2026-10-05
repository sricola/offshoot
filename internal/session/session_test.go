package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
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
	if out, err := sqlite3CLI(s.CheckoutPath(),
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// The replica converges without the writer ever being paused.
	waitFor(t, 10*time.Second, "replica to converge", func() bool {
		out, err := sqlite3CLI(s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
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
		if out, err := sqlite3CLI(s.CheckoutPath(),
			"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		waitFor(t, 10*time.Second, "capture", func() bool {
			out, err := sqlite3CLI(s.ReplicaPath(), "SELECT count(*) FROM t;").Output()
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
	// Open and Close inside the capture: the session's goroutines log to
	// os.Stderr, so the global must be swapped before they start and
	// restored only after Close has joined them (a data race otherwise).
	out := captureStderr(t, func() {
		s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main",
			Holder: "session-a", LeaseTTL: time.Nanosecond})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Flush("", nil); err == nil {
			t.Fatal("Flush after fencing must fail")
		}
	})
	const prefix = "offshoot: session: app@main: fenced cause="
	if !strings.Contains(out, prefix) {
		t.Fatalf("missing fenced transition log in:\n%s", out)
	}
}

// acquireLandsThenFails forwards the first app@main ref PutIf that carries
// a session: lease to the store and reports it as a lost race anyway: the
// S3 SDK's retry answering 412 to its own first attempt that landed, or a
// timeout that lost the response. readFailures, set before Open, is how
// many reads of the ref fail right after that: a settle re-read that
// cannot see what landed.
type acquireLandsThenFails struct {
	store.Backend
	readFailures int32
	hits         atomic.Int32
	failReads    atomic.Int32
}

func (b *acquireLandsThenFails) PutIf(key string, data []byte, ifMatch string) (string, error) {
	etag, err := b.Backend.PutIf(key, data, ifMatch)
	if err != nil || key != store.RefKey("app", "main") || !ops.IsSessionHolder(refHolder(data)) || b.hits.Add(1) > 1 {
		return etag, err
	}
	b.failReads.Store(b.readFailures)
	return "", fmt.Errorf("%w: precondition failed on the retry", store.ErrCAS)
}

func (b *acquireLandsThenFails) Get(key string) ([]byte, string, error) {
	if key == store.RefKey("app", "main") && b.failReads.Add(-1) >= 0 {
		return nil, "", errors.New("test: injected ref read failure")
	}
	return b.Backend.Get(key)
}

func refHolder(ref []byte) string {
	var r struct {
		LeaseHolder string `json:"lease_holder"`
	}
	_ = json.Unmarshal(ref, &r)
	return r.LeaseHolder
}

func refOf(t *testing.T, w *ops.Workspace) store.Ref {
	t.Helper()
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// TestOpenUsesAUniqueSessionHolder: with no Options.Holder, each Open holds
// the lease as its own session:<host>/<pid>/<nonce>, so a reopen in the
// same process is a fresh acquire under a new epoch, never a self-renew of
// an earlier session's lease.
func TestOpenUsesAUniqueSessionHolder(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	shape := regexp.MustCompile(`^session:` + regexp.QuoteMeta(ops.LocalHolder()) + `/[0-9a-f]{8}$`)
	var leases []store.Lease
	for i := 0; i < 2; i++ {
		s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
		if err != nil {
			t.Fatal(err)
		}
		l := s.Lease()
		if ref := refOf(t, w); ref.LeaseHolder != l.Holder || !shape.MatchString(ref.LeaseHolder) {
			t.Fatalf("open %d: ref holder %q, session holder %q, want both to match %s", i, ref.LeaseHolder, l.Holder, shape)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		leases = append(leases, l)
	}
	if leases[0].Holder == leases[1].Holder || leases[0].Epoch == leases[1].Epoch {
		t.Fatalf("two opens share a holder or an epoch: %+v, %+v", leases[0], leases[1])
	}
}

// TestSecondOpenWhileFirstIsLiveIsRefusedAndLeavesTheLeaseAlone: a second
// Open in the same process, with the default holder, is refused by the
// first's live lease rather than renewing it in place.
func TestSecondOpenWhileFirstIsLiveIsRefusedAndLeavesTheLeaseAlone(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	before := refOf(t, w)
	if before.LeaseHolder != s1.Lease().Holder || before.Epoch != s1.Lease().Epoch {
		t.Fatalf("ref %q@%d, want s1's %+v", before.LeaseHolder, before.Epoch, s1.Lease())
	}
	s2, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err == nil {
		s2.Close()
		t.Fatal("a second Open while the first is live succeeded")
	}
	if !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("second Open = %v, want ErrLeaseHeld", err)
	}
	if after := refOf(t, w); after.LeaseHolder != before.LeaseHolder || after.Epoch != before.Epoch {
		t.Fatalf("the refused Open moved the lease: %q@%d -> %q@%d", before.LeaseHolder, before.Epoch, after.LeaseHolder, after.Epoch)
	}
}

// TestOpenAdoptsAnAcquireThatLandedButReportedFailure: an acquire that
// landed but reported a lost race is settled by re-reading the ref; the
// ref names the holder this Open generated, so the session adopts that
// lease instead of failing and leaving the branch leased to nobody.
func TestOpenAdoptsAnAcquireThatLandedButReportedFailure(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	b := &acquireLandsThenFails{Backend: w.Store.B}
	w.Store.B = b
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatalf("Open after an acquire that landed: %v", err)
	}
	if b.hits.Load() == 0 {
		t.Fatal("the injected acquire failure never fired")
	}
	l := s.Lease()
	ref := refOf(t, w)
	if !ops.IsSessionHolder(l.Holder) || ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch {
		t.Fatalf("session lease %+v, ref %q@%d: want the adopted lease on the ref", l, ref.LeaseHolder, ref.Epoch)
	}
	if want, _ := time.Parse(time.RFC3339Nano, ref.LeaseExpiry); !l.Expiry.Equal(want) {
		t.Fatalf("adopted expiry %v, want the ref's %v", l.Expiry, want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if ref := refOf(t, w); ref.LeaseHolder != "" {
		t.Fatalf("Close left the adopted lease on the ref: holder %q", ref.LeaseHolder)
	}
}

// TestOpenSettleReadFailureNamesTheExpiry: when the acquire landed but
// reported failure and the settle re-read fails too, Open cannot know
// whether it holds the lease; its error says so and names the latest time
// that lease can lapse.
func TestOpenSettleReadFailureNamesTheExpiry(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	w.Store.B = &acquireLandsThenFails{Backend: w.Store.B, readFailures: 1}
	start := time.Now()
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err == nil {
		s.Close()
		t.Fatal("Open succeeded though it could not settle its acquire")
	}
	end := time.Now()
	if !strings.Contains(err.Error(), "may have landed") {
		t.Fatalf("Open = %v, want it to say the acquire may have landed", err)
	}
	stamp := regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ`).FindString(err.Error())
	expiry, perr := time.Parse(time.RFC3339, stamp)
	if perr != nil {
		t.Fatalf("Open = %v, want an RFC3339 expiry in it", err)
	}
	landed, _ := time.Parse(time.RFC3339Nano, refOf(t, w).LeaseExpiry)
	if expiry.Before(landed) || expiry.Before(start.Add(ops.DefaultLeaseTTL)) || expiry.After(end.Add(ops.DefaultLeaseTTL+time.Second)) {
		t.Fatalf("named expiry %v, want no earlier than the landed lease's %v and within a second of a TTL after the open", expiry, landed)
	}
}

// TestFailedCheckoutReleasesWithRetry: a checkout that fails after the
// acquire releases the lease with Close's bounded retry, so one failed
// release write does not leave the branch leased for a TTL.
func TestFailedCheckoutReleasesWithRetry(t *testing.T) {
	testutil.RequireSQLite3(t)
	old := releaseRetryPause
	releaseRetryPause = time.Millisecond
	t.Cleanup(func() { releaseRetryPause = old })
	for _, mode := range []string{"cas", "transient"} {
		t.Run(mode, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			// A directory at the checkout path fails CheckoutProven's rename.
			checkoutPath := w.CheckoutPath("app", "main")
			if err := os.MkdirAll(checkoutPath, 0755); err != nil {
				t.Fatal(err)
			}
			f := &releaseFaults{Backend: w.Store.B, mode: mode}
			f.n.Store(1)
			w.Store.B = f
			if _, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"}); err == nil {
				t.Fatal("Open succeeded with a directory at the checkout path")
			}
			if f.n.Load() > 0 {
				t.Fatal("the injected release failure never fired")
			}
			if ref := refOf(t, w); ref.LeaseHolder != "" {
				t.Fatalf("the failed Open left its lease on the ref: holder %q", ref.LeaseHolder)
			}
			if err := os.Remove(checkoutPath); err != nil {
				t.Fatal(err)
			}
			s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
			if err != nil {
				t.Fatalf("the next Open: %v", err)
			}
			s.Close()
		})
	}
}

// TestReopenRefusedByCheckpointHolderSaysInProgress: an Open refused by an
// at-rest checkpoint's live lease returns an error ops.CheckpointInProgress
// recognizes, so the daemon can say to retry in a few seconds.
func TestReopenRefusedByCheckpointHolderSaysInProgress(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	holder := "checkpoint:" + ops.LocalHolder() + "/0123abcd"
	if _, err := w.AcquireLease("app", "main", holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
	if err == nil {
		s.Close()
		t.Fatal("Open succeeded under a checkpoint's live lease")
	}
	if !ops.CheckpointInProgress(err) || !errors.Is(err, store.ErrLeaseHeld) || !strings.Contains(err.Error(), holder) {
		t.Fatalf("Open = %v, want a lease-held refusal CheckpointInProgress recognizes, naming %s", err, holder)
	}
	if ref := refOf(t, w); ref.LeaseHolder != holder {
		t.Fatalf("the refused Open moved the checkpoint's lease: holder %q", ref.LeaseHolder)
	}
}
