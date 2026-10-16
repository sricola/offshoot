package ops

import (
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestWritableCheckoutModeIsExactly0600 pins SECURITY.md "File modes":
// "Store objects and writable checkouts are 0600". Existing tests only
// asserted the owner-write bit, so a 0644 checkout passed them. Two
// subtests cover the two ways a writable checkout's bytes arrive:
//
//   - materialize: reflinkUnsupportedForTest forces the plain
//     materializeMembersAt path (so this half runs on every filesystem,
//     never skips), where the mode is the 0600 of ltxio.MaterializeChain's
//     os.CreateTemp destination. Mutation verified against:
//     ltxio/segment.go MaterializeChain with `os.Chmod(tmpPath, 0o644)`
//     inserted before its finalizeDestination.
//   - clone: a second fork of the same checkpoint is cloned from the
//     by-chain entry (0400) and must be re-moded to 0600 by cloneIntoPlace.
//     Mutation verified against: chainid.go cloneIntoPlace
//     `os.Chmod(tmp, mode)` changed to `os.Chmod(tmp, 0o644)` (the mode
//     CheckoutProven passes at ops.go's materializeFromChain(..., 0o600)
//     only ever reaches this chmod).
func TestWritableCheckoutModeIsExactly0600(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX modes are not enforced on windows")
	}
	mode := func(t *testing.T, p string) os.FileMode {
		t.Helper()
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Mode().Perm()
	}

	t.Run("materialize", func(t *testing.T) {
		w := newWS(t)
		reflinkUnsupportedForTest = true
		t.Cleanup(func() { reflinkUnsupportedForTest = false })
		seedDB(t, w, "app", 1<<20)
		mustFork(t, w, "app", "main", "fresh", "seed")
		kinds := recordKinds(t)
		path := mustCheckout(t, w, "app", "fresh")
		if got := strings.Join(*kinds, ","); got != "materialize" {
			t.Fatalf("checkout source = %q, want one materialize", got)
		}
		if m := mode(t, path); m != 0o600 {
			t.Fatalf("materialized writable checkout mode = %v, want exactly 0600", m)
		}
	})

	t.Run("clone", func(t *testing.T) {
		w := newWS(t)
		requireClone(t, w)
		seedDB(t, w, "app", 1<<20)
		mustFork(t, w, "app", "main", "a", "seed")
		mustFork(t, w, "app", "main", "b", "seed")
		kinds := recordKinds(t)
		pa := mustCheckout(t, w, "app", "a") // populates the by-chain entry
		pb := mustCheckout(t, w, "app", "b") // clones it
		if n := len(*kinds); n != 2 || (*kinds)[1] != "clone" {
			t.Fatalf("checkout sources = %v, want the second to be a clone", *kinds)
		}
		if m := mode(t, pb); m != 0o600 {
			t.Fatalf("cloned writable checkout mode = %v, want exactly 0600", m)
		}
		if m := mode(t, pa); m != 0o600 {
			t.Fatalf("populating writable checkout mode = %v, want exactly 0600", m)
		}
	})
}

// TestReapSkipsProtectedBranchWithExpiredTTL pins reference.md
// "#offshoot-protect" ("is never TTL-reaped") and status.md "Protected
// branches never reaped" at the ops layer, which both the daemon janitor
// and `offshoot mcp`'s reaper call: two forks with a 1h TTL get their
// activity clock pushed 2h into the past by PutRef; one is protected. One
// Reap pass must reap exactly the unprotected one, report no error, and
// leave the protected one present with no reaping claim on it. Mutation
// verified against: reap.go reapOne `if ref.TTL == "" || ref.Protected`
// with `|| ref.Protected` dropped (Reap then claims the protected branch,
// Destroy refuses it, and Reap returns "reap app@keep: ... protected").
func TestReapSkipsProtectedBranchWithExpiredTTL(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	for _, br := range []string{"keep", "gone"} {
		if _, err := w.Fork("app", "main", br, "", time.Hour, nil); err != nil {
			t.Fatal(err)
		}
		ref, etag, err := w.Store.GetRef("app", br)
		if err != nil {
			t.Fatal(err)
		}
		ref.TouchedAt = stale
		ref.Protected = br == "keep"
		if _, err := w.Store.PutRef("app", br, ref, etag); err != nil {
			t.Fatal(err)
		}
	}

	reaped, err := w.Reap(time.Now())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if got := strings.Join(reaped, ","); got != "app@gone" {
		t.Fatalf("reaped = %q, want exactly app@gone", got)
	}
	if _, _, err := w.Store.GetRef("app", "gone"); err == nil {
		t.Fatal("the unprotected expired branch must be gone")
	}
	keep, _, err := w.Store.GetRef("app", "keep")
	if err != nil {
		t.Fatalf("protected expired branch must survive Reap: %v", err)
	}
	if !keep.Protected || keep.Reaping || keep.TouchedAt != stale {
		t.Fatalf("Reap must not touch a protected branch at all: %+v", keep)
	}
}
