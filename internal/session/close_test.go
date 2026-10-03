package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestLeaseStaysLiveThroughClose pins that Close keeps renewing the lease
// until it releases it. Close used to stop renewal first, ahead of the
// engine shutdown, the hash, the sidecar stamp and the shadow refresh, so a
// close that outlasted the lease's remaining life let it lapse while the old
// engine still owned the checkout. The close is held in CloseReleaseHook for
// two TTLs; the subject is elapsed time, so this polls the ref across it.
// Renewal every 100 ms against a 1.5 s TTL leaves a renewal 1.4 s of slack
// before a slow tick (a loaded -race run, an fsync per ref write) reads as
// a lapse.
func TestLeaseStaysLiveThroughClose(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	const ttl = 1500 * time.Millisecond
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", LeaseTTL: ttl, RenewEvery: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	holder := s.Lease().Holder

	entered, proceed := make(chan struct{}), make(chan struct{})
	CloseReleaseHook = func() {
		close(entered)
		<-proceed
	}
	t.Cleanup(func() { CloseReleaseHook = nil })
	closeErr := make(chan error, 1)
	go func() { closeErr <- s.Close() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("Close never reached CloseReleaseHook")
	}
	CloseReleaseHook = nil // the held Close has already read it

	until := time.Now().Add(2 * ttl)
	for time.Now().Before(until) {
		ref, _, err := w.Store.GetRef("app", "main")
		if err != nil {
			close(proceed)
			<-closeErr
			t.Fatal(err)
		}
		if ref.LeaseHolder != holder || !store.LeaseLive(ref, time.Now()) {
			close(proceed)
			<-closeErr
			t.Fatalf("lease lapsed while Close was still running: holder %q, expiry %s", ref.LeaseHolder, ref.LeaseExpiry)
		}
		time.Sleep(25 * time.Millisecond)
	}
	close(proceed)
	if err := <-closeErr; err != nil {
		t.Fatalf("close: %v", err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("lease not released after Close: holder %q", ref.LeaseHolder)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("a clean close must not record an error: %v", err)
	}
}

// TestReleaseLeaseRetryPolicy pins which release failures Close retries. A
// CAS lost to a touch or protect, or a transient store error, can succeed on
// a later attempt; ErrLeaseLost means the lease is no longer ours, which
// includes a release that landed but reported failure; ErrNotFound means
// the branch is gone and no attempt can succeed.
func TestReleaseLeaseRetryPolicy(t *testing.T) {
	old := releaseRetryPause
	releaseRetryPause = 0
	t.Cleanup(func() { releaseRetryPause = old })

	transient := errors.New("transient")
	notFound := fmt.Errorf("gone: %w", store.ErrNotFound)
	lost := fmt.Errorf("lost: %w", store.ErrLeaseLost)
	cases := []struct {
		name      string
		errs      []error // one per attempt; past the end, nil
		wantCalls int
		wantErr   error
	}{
		{"first attempt lands", nil, 1, nil},
		{"lease already gone", []error{lost}, 1, nil},
		{"branch destroyed: no retry", []error{notFound}, 1, notFound},
		{"transient then lands", []error{transient, transient}, 3, nil},
		{"landed but reported failure", []error{transient, lost}, 2, nil},
		{"exhausted", []error{transient, transient, transient, nil}, 3, transient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := releaseLease(func(store.Lease) error {
				calls++
				if calls <= len(tc.errs) {
					return tc.errs[calls-1]
				}
				return nil
			}, store.Lease{DB: "app", Branch: "main", Holder: "h", Epoch: 1})
			if calls != tc.wantCalls {
				t.Fatalf("release called %d times, want %d", calls, tc.wantCalls)
			}
			if !errors.Is(err, tc.wantErr) && !(err == nil && tc.wantErr == nil) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// releaseFaults injects failures into the next n lease-release writes to
// app@main: ref PutIfs whose ref carries no lease holder. Nothing else this
// test writes after Open does that. Install it after Create and before Open,
// then arm it.
type releaseFaults struct {
	store.Backend
	mode string // "cas", "transient" or "land-then-fail"
	n    atomic.Int64
}

func (f *releaseFaults) PutIf(key string, data []byte, ifMatch string) (string, error) {
	if key == store.RefKey("app", "main") && !refCarriesLease(data) && f.n.Add(-1) >= 0 {
		switch f.mode {
		case "cas":
			return "", fmt.Errorf("test: injected: %w", store.ErrCAS)
		case "transient":
			return "", errors.New("test: injected transient release failure")
		case "land-then-fail":
			if _, err := f.Backend.PutIf(key, data, ifMatch); err != nil {
				return "", err
			}
			return "", errors.New("test: injected failure after the release landed")
		}
	}
	return f.Backend.PutIf(key, data, ifMatch)
}

func refCarriesLease(ref []byte) bool {
	var r struct {
		LeaseHolder string `json:"lease_holder"`
	}
	return json.Unmarshal(ref, &r) == nil && r.LeaseHolder != ""
}

// TestCloseRetriesRelease drives the retry end to end through a real store.
// A different holder acquiring afterwards proves the release landed: a
// same-holder reopen would renew a stuck lease in place and prove nothing.
func TestCloseRetriesRelease(t *testing.T) {
	testutil.RequireSQLite3(t)
	old := releaseRetryPause
	releaseRetryPause = time.Millisecond
	t.Cleanup(func() { releaseRetryPause = old })

	for _, tc := range []struct {
		mode    string
		n       int64
		wantErr string // "" = Close succeeds and the branch is free
	}{
		{"cas", 1, ""},
		{"transient", 1, ""},
		{"land-then-fail", 1, ""},
		{"transient", releaseAttempts, "injected transient release failure"},
	} {
		t.Run(fmt.Sprintf("%s x%d", tc.mode, tc.n), func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			f := &releaseFaults{Backend: w.Store.B, mode: tc.mode}
			w.Store.B = f
			s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
			if err != nil {
				t.Fatal(err)
			}
			f.n.Store(tc.n)
			err = s.Close()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("close = %v, want an error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("close = %v, want the retried release to succeed", err)
			}
			if _, err := w.AcquireLease("app", "main", "next", ops.DefaultLeaseTTL); err != nil {
				t.Fatalf("branch not free after Close: %v", err)
			}
		})
	}
}

func transitionKV(kv []any, key string) (any, bool) {
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i] == key {
			return kv[i+1], true
		}
	}
	return nil, false
}

// TestClosedAndFencedTransitionsCarryHolderAndEpoch: once holders are per
// session, a subscriber matches a close or a fencing to its session_opened
// by holder and epoch, so both transitions carry them.
func TestClosedAndFencedTransitionsCarryHolderAndEpoch(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := map[string][]any{}
	prev := OnTransition
	OnTransition = func(db, branch, event string, kv []any) {
		mu.Lock()
		got[event] = kv
		mu.Unlock()
	}
	defer func() { OnTransition = prev }()
	check := func(event, holder string, epoch uint64) {
		t.Helper()
		mu.Lock()
		kv := got[event]
		mu.Unlock()
		if h, _ := transitionKV(kv, "holder"); h != holder {
			t.Fatalf("%s holder = %v, want %q (kv %v)", event, h, holder, kv)
		}
		if e, _ := transitionKV(kv, "epoch"); e != epoch {
			t.Fatalf("%s epoch = %v, want %d (kv %v)", event, e, epoch, kv)
		}
	}

	s, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main", Holder: "kv-a"})
	if err != nil {
		t.Fatal(err)
	}
	epoch := s.Lease().Epoch
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	check("closed", "kv-a", epoch)

	f, err := Open(context.Background(), Options{WS: w, DB: "app", Branch: "main",
		Holder: "kv-b", LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fepoch := f.Lease().Epoch
	if _, err := w.AcquireLease("app", "main", "thief", ops.DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Flush("", nil); err == nil {
		t.Fatal("Flush after fencing must fail")
	}
	check("fenced", "kv-b", fepoch)
}
