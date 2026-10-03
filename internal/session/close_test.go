package session

import (
	"context"
	"testing"
	"time"

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
