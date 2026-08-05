package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// TestJanitorReapsExpiredBranchWhileDaemonRuns starts a daemon with a fast
// janitor tick, backdates a forked branch's TTL so it is already expired,
// and waits for the janitor to reap it out from under a live server —
// proving StartJanitor actually drives ops.Reap on a schedule, not just that
// Reap works in isolation (already covered by internal/ops/reap_test.go).
func TestJanitorReapsExpiredBranchWhileDaemonRuns(t *testing.T) {
	srv, w := newServer(t)

	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ref.TTL = "1h"
	ref.TouchedAt = now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "doomed", ref, etag); err != nil {
		t.Fatal(err)
	}

	srv.StartJanitor(50*time.Millisecond, 0)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, err := w.Store.GetRef("app", "doomed"); errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("janitor never reaped the expired branch within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, _, err := w.Store.GetRef("app", "main"); err != nil {
		t.Fatalf("main must survive: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestStartJanitorZeroDisablesIt pins the brief's explicit requirement that
// every <= 0 disables the janitor entirely: an expired-TTL branch must sit
// untouched, and Shutdown (which stops a running janitor as its very first
// step) must still complete cleanly when no janitor was ever started.
func TestStartJanitorZeroDisablesIt(t *testing.T) {
	srv, w := newServer(t)

	if _, err := w.Fork("app", "main", "doomed", "", 0); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "doomed")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ref.TTL = "1h"
	ref.TouchedAt = now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "doomed", ref, etag); err != nil {
		t.Fatal(err)
	}

	srv.StartJanitor(0, 0)

	// Give a real (disabled) janitor every chance to wrongly fire.
	time.Sleep(200 * time.Millisecond)

	if _, _, err := w.Store.GetRef("app", "doomed"); err != nil {
		t.Fatalf("StartJanitor(0, 0) must disable the janitor; expired branch was reaped: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
