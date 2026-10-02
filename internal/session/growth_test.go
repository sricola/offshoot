package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/testutil"
	"github.com/sricola/offshoot/internal/wal"
)

// TestUncarriedGrowthFailsTheSession: a captured transaction whose commit
// grows the database past the pages it carries — something SQLite never
// writes, injected here through applyFramesHook into the real capture path
// — is refused by recordApply, and the refusal ends the session through the
// engine: Err() reports ErrUncarriedGrowth instead of a segment no reader
// would accept being flushed.
func TestUncarriedGrowthFailsTheSession(t *testing.T) {
	testutil.RequireSQLite3(t)
	var injected atomic.Bool
	applyFramesHook = func(f []wal.Frame) []wal.Frame {
		if len(f) == 0 || !injected.CompareAndSwap(false, true) {
			return f
		}
		out := append([]wal.Frame(nil), f...)
		out[len(out)-1].Header.CommitSize += 3 // three pages nobody carries
		return out
	}
	t.Cleanup(func() { applyFramesHook = nil })

	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// The stderr swap brackets the session's whole lifetime: its engine
	// goroutine reads os.Stderr to log the fence, so swapping the global
	// after Open (or restoring it before Close has joined the goroutines)
	// is a data race the detector catches on a loaded runner.
	var s *Session
	captureStderr(t, func() {
		var err error
		s, err = Open(context.Background(), Options{WS: w, DB: "app", Branch: "main"})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if out, err := sqlite3CLI(s.CheckoutPath(), "CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		waitFor(t, 10*time.Second, "the session to fail", func() bool { return s.Err() != nil })
	})
	if !injected.Load() {
		t.Fatal("the hook never saw a transaction")
	}
	if err := s.Err(); !errors.Is(err, ltxio.ErrUncarriedGrowth) {
		t.Fatalf("Err() = %v, want ErrUncarriedGrowth", err)
	}
}
