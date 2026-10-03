package ops

import (
	"regexp"
	"testing"
)

// TestCheckpointHolderIsPerCall: every at-rest checkpoint call gets its own
// holder, "checkpoint:<host>/<pid>/<8 hex>". The nonce is what makes the
// epoch private: AcquireLease treats the same holder on a live lease as a
// self-renew with no epoch bump, so two calls in one process sharing a
// holder would share an epoch and an object key.
func TestCheckpointHolderIsPerCall(t *testing.T) {
	shape := regexp.MustCompile(`^checkpoint:` + regexp.QuoteMeta(LocalHolder()) + `/[0-9a-f]{8}$`)
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		h := newCheckpointHolder()
		if !shape.MatchString(h) {
			t.Fatalf("holder %q is not checkpoint:<host>/<pid>/<8 hex>", h)
		}
		if !isCheckpointHolder(h) {
			t.Fatalf("isCheckpointHolder(%q) = false", h)
		}
		if seen[h] {
			t.Fatalf("holder %q repeated within 100 calls", h)
		}
		seen[h] = true
	}
	if isCheckpointHolder(LocalHolder()) {
		t.Fatal("a session's holder reads as a checkpoint's")
	}
}
