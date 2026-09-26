package ops

import (
	"strings"
	"testing"
	"time"
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

	// A protection flip is not activity: it must not reset TouchedAt (and
	// so must not incidentally grant a stale, TTL'd branch a fresh TTL
	// window).
	if _, err := w.Fork("app", "main", "ttl-keep", "", time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	before, _, err := w.Store.GetRef("app", "ttl-keep")
	if err != nil {
		t.Fatal(err)
	}
	if before.TouchedAt == "" {
		t.Fatal("fork with a ttl must stamp TouchedAt")
	}
	if _, err := w.SetProtected("app", "ttl-keep", true); err != nil {
		t.Fatal(err)
	}
	if _, err := w.SetProtected("app", "ttl-keep", false); err != nil {
		t.Fatal(err)
	}
	after, _, err := w.Store.GetRef("app", "ttl-keep")
	if err != nil {
		t.Fatal(err)
	}
	if after.TouchedAt != before.TouchedAt {
		t.Fatalf("SetProtected must not touch the activity clock: before=%q after=%q", before.TouchedAt, after.TouchedAt)
	}
}
