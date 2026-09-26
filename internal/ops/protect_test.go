package ops

import (
	"strings"
	"testing"
)

// TestSetProtectedFlipsTheFlagAndGatesDestroy: protect a fork, then an
// unforced Destroy refuses and Reap never touches it; unprotect, and
// Destroy proceeds. main starts protected (Create's default) and can be
// unprotected too — the flag is policy, not identity.
func TestSetProtectedFlipsTheFlagAndGatesDestroy(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "keep", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	ref, err := w.SetProtected("app", "keep", true)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Protected {
		t.Fatalf("returned ref not protected: %+v", ref)
	}
	if err := w.Destroy("app", "keep", false); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("unforced destroy of a protected branch must refuse, got %v", err)
	}
	if _, err := w.SetProtected("app", "keep", false); err != nil {
		t.Fatal(err)
	}
	if err := w.Destroy("app", "keep", false); err != nil {
		t.Fatalf("destroy after unprotect: %v", err)
	}
	if _, err := w.SetProtected("app", "main", false); err != nil {
		t.Fatal(err)
	}
	got, _, _ := w.Store.GetRef("app", "main")
	if got.Protected {
		t.Fatal("main must be unprotectable")
	}
	if _, err := w.SetProtected("app", "nope", true); err == nil {
		t.Fatal("unknown branch must error")
	}
}
