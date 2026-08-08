package main

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// TestDemoRunsEndToEnd runs the published demo exactly as a reader would.
// If this fails, the demo in the README is broken for everyone.
func TestDemoRunsEndToEnd(t *testing.T) {
	testutil.RequireSQLite3(t)
	cmd := exec.Command("bash", "run.sh")
	cmd.Env = append(cmd.Environ(), "OFFSHOOT_DEMO_DIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("demo failed: %v\n%s", err, out)
	}
	s := string(out)
	for _, want := range []string{"attempt-1", "attempt-2", "attempt-3", "promoted", "winner"} {
		if !strings.Contains(s, want) {
			t.Errorf("demo output missing %q\n%s", want, s)
		}
	}
	if strings.Contains(strings.ToLower(s), "error") {
		t.Errorf("demo printed an error:\n%s", s)
	}
	// The substring/no-"error" checks above would still pass if the demo
	// promoted the wrong attempt (e.g. attempt-1's unrounded floats, or
	// nothing at all): the winner line and the final table must agree that
	// attempt-3 — the only migration that actually rounds correctly — won.
	if !strings.Contains(s, "winner: attempt-3") {
		t.Errorf("demo did not report attempt-3 as the winner:\n%s", s)
	}
	for _, want := range []string{"1999", "870", "435"} {
		if !strings.Contains(s, want) {
			t.Errorf("demo's final table missing correctly-rounded value %q\n%s", want, s)
		}
	}
}
