package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestProtectCLIRoundTrip: `offshoot protect app@fork` then `offshoot
// destroy app@fork` refuses; `offshoot unprotect app@fork` then destroy
// succeeds; output lines are exactly "protected app@fork" /
// "unprotected app@fork".
func TestProtectCLIRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	call(t, dir, "init")
	call(t, dir, "create", "app")
	call(t, dir, "fork", "app", "fork")

	out := call(t, dir, "protect", "app@fork")
	if got, want := strings.TrimSpace(out), "protected app@fork"; got != want {
		t.Fatalf("protect output = %q, want %q", got, want)
	}

	if err := run([]string{"-store", dir, "destroy", "app@fork"}); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("unforced destroy of a protected branch must refuse, got %v", err)
	}

	out = call(t, dir, "unprotect", "app@fork")
	if got, want := strings.TrimSpace(out), "unprotected app@fork"; got != want {
		t.Fatalf("unprotect output = %q, want %q", got, want)
	}

	if err := run([]string{"-store", dir, "destroy", "app@fork"}); err != nil {
		t.Fatalf("destroy after unprotect: %v", err)
	}
}
