package ops

import (
	"os/exec"
	"testing"
	"time"
)

// requireSQLite skips the test when the sqlite3 CLI isn't on PATH, matching
// the skip pattern used throughout this package (see gc_chain_test.go).
func requireSQLite(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
}

// mustExecSQL runs sql against the sqlite3 CLI on path, failing the test with
// combined output on error.
func mustExecSQL(t *testing.T, path, sql string) {
	t.Helper()
	if out, err := exec.Command("sqlite3", path, sql).CombinedOutput(); err != nil {
		t.Fatalf("sqlite3 %s: %v: %s", sql, err, out)
	}
}

func TestTouchSetsClearsAndStampsTTL(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ttl := 90 * time.Minute
	now := time.Now()
	ref, err := w.Touch("app", "main", &ttl, now)
	if err != nil {
		t.Fatal(err)
	}
	if ref.TTL != "1h30m0s" {
		t.Fatalf("TTL = %q, want 1h30m0s", ref.TTL)
	}
	if ref.TouchedAt == "" {
		t.Fatal("Touch must stamp the activity clock")
	}
	// nil ttl = keep the TTL, restamp the clock.
	later := now.Add(time.Minute)
	ref2, err := w.Touch("app", "main", nil, later)
	if err != nil {
		t.Fatal(err)
	}
	if ref2.TTL != "1h30m0s" {
		t.Fatalf("nil ttl must keep TTL, got %q", ref2.TTL)
	}
	if ref2.TouchedAt == ref.TouchedAt {
		t.Fatal("Touch must advance the clock")
	}
	// zero ttl = clear.
	zero := time.Duration(0)
	ref3, err := w.Touch("app", "main", &zero, later)
	if err != nil {
		t.Fatal(err)
	}
	if ref3.TTL != "" {
		t.Fatalf("zero ttl must clear, got %q", ref3.TTL)
	}
}

func TestDurableWritesStampTheClock(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// Checkpoint stamps.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustExecSQL(t, path, "CREATE TABLE t (v);")
	if _, err := w.Checkpoint("app", "main", "cp1"); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.TouchedAt == "" {
		t.Fatal("Checkpoint must stamp TouchedAt")
	}
	// Fork stamps the CHILD and does not touch the parent (spec: creating a
	// child does not extend the parent).
	before := ref.TouchedAt
	if _, err := w.Fork("app", "main", "kid", "", 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	kid, _, err := w.Store.GetRef("app", "kid")
	if err != nil {
		t.Fatal(err)
	}
	if kid.TTL != "2h0m0s" || kid.TouchedAt == "" {
		t.Fatalf("fork -ttl must set child TTL+clock: %+v", kid)
	}
	parent, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if parent.TouchedAt != before {
		t.Fatal("fork must not extend the parent's clock")
	}
}
