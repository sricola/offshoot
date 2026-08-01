package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestDemoRunsEndToEnd runs the published demo exactly as a reader would.
// If this fails, the demo in the README is broken for everyone.
func TestDemoRunsEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
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
}
