package daemon

import (
	"testing"
	"time"
)

// TestJanitorTickClearsStaleDeleteClaim proves janitorTick's Milestone 4
// Task 6b wiring: a Deleting claim landed directly on a ref (simulating a
// crashed Destroy call, exactly as internal/ops's
// TestDestroySelfHealsStaleDeletingClaim does at the ops layer) and stamped
// old enough to be stale is cleared by a single real janitor pass, not just
// by calling ops.Workspace.ClearStaleDeleteClaims in isolation.
func TestJanitorTickClearsStaleDeleteClaim(t *testing.T) {
	srv, w := newServer(t)

	if _, err := w.Fork("app", "main", "crashed", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, etag, err := w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	ref.Deleting = true
	ref.DeletingAt = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	if _, err := w.Store.PutRef("app", "crashed", ref, etag); err != nil {
		t.Fatal(err)
	}

	srv.janitorTick(time.Hour)

	after, _, err := w.Store.GetRef("app", "crashed")
	if err != nil {
		t.Fatal(err)
	}
	if after.Deleting {
		t.Fatal("a real janitor pass must clear a stale Deleting claim")
	}
}
