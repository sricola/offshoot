package ops

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/testutil"
)

// countFileSum installs observeFileSum for the duration of the test and
// returns a func reporting the running count, restoring the previous hook
// (nil, in every existing test) on cleanup.
func countFileSum(t *testing.T) func() int {
	t.Helper()
	prev := observeFileSum
	n := 0
	observeFileSum = func() { n++ }
	t.Cleanup(func() { observeFileSum = prev })
	return func() int { return n }
}

// TestCleanSkipDoesNotHashWhenFingerprintMatches pins Task 2's whole point:
// once a checkout has been materialized (which stamps its fingerprint —
// size, mtime, SQLite's header change counter — alongside the hash), a
// second Checkout against the SAME head must prove "clean" from the
// fingerprint alone, never falling back to a full-file hash.
func TestCleanSkipDoesNotHashWhenFingerprintMatches(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	// First Checkout materializes and stamps the fingerprint; not part of
	// what's being measured.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}

	count := countFileSum(t)
	res, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 0 {
		t.Fatalf("fileSum called %d times on a clean, fingerprint-matching checkout, want 0", got)
	}
	if !res.Clean {
		t.Fatal("want Clean=true when the fingerprint matches")
	}
}

// TestModifiedCheckoutStillDetectedRollbackJournalAndWAL is the correctness
// counterweight to the clean-skip fast path: a checkout modified out from
// under its sidecar must still be caught, in BOTH SQLite journal modes.
// This is the case a naive size+mtime (or size+mtime with no WAL-awareness)
// fingerprint can miss — see checkoutState's doc comment and the design
// doc's § 2 correctness argument: a WAL-mode write leaves the main file's
// header change counter (and possibly its size/mtime) untouched until a
// checkpoint, so the live file must be read AFTER quiescing (as
// checkoutState's callers already do) for the fingerprint comparison to be
// trustworthy at all.
//
// This exercises checkoutState directly (quiescing first, exactly as its
// production callers CheckoutProven/warnIfUncheckpointed do) rather than the
// full Checkout flow: Checkout's own "modified" branch goes on to
// rematerialize and then re-hash the FRESH content to re-stamp it, which
// would add a second, unrelated fileSum call that has nothing to do with
// what this test is pinning — checkoutState's own hash fallback firing
// exactly once to detect the mismatch.
func TestModifiedCheckoutStillDetectedRollbackJournalAndWAL(t *testing.T) {
	testutil.RequireSQLite3(t)
	for _, journalMode := range []string{"DELETE", "WAL"} {
		t.Run("journal_mode="+journalMode, func(t *testing.T) {
			w := newWS(t)
			if err := w.Create("app"); err != nil {
				t.Fatal(err)
			}
			path, err := w.Checkout("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			mustExecSQL(t, path, "PRAGMA journal_mode="+journalMode+"; CREATE TABLE t (v);")
			if _, err := w.Checkpoint("app", "main", "cp1", nil); err != nil {
				t.Fatal(err)
			}
			// Checkpoint refreshes the sidecar in place at the new head, so
			// the checkout is clean again right after. Now modify it
			// out-of-band, WITHOUT going through Checkpoint, leaving the
			// sidecar's fingerprint stale relative to the file.
			mustExecSQL(t, path, "PRAGMA journal_mode="+journalMode+"; INSERT INTO t VALUES (1);")

			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			// Quiesce first, exactly as CheckoutProven/warnIfUncheckpointed
			// do before ever calling checkoutState: a WAL-mode write leaves
			// the main file's header stale until checkpointed.
			if err := quiesce(path); err != nil {
				t.Fatal(err)
			}

			count := countFileSum(t)
			state, _ := checkoutState(path, ref)
			if state != "modified" {
				t.Fatalf("journal_mode=%s: checkoutState = %q, want %q", journalMode, state, "modified")
			}
			if got := count(); got != 1 {
				t.Fatalf("fileSum called %d times detecting a modified checkout (journal_mode=%s), want exactly 1 (hash fallback taken)", got, journalMode)
			}
		})
	}
}

// TestTouchedButUnchangedCheckoutHashesAndStaysClean covers the mtime-only
// perturbation: os.Chtimes bumps ModTimeNS with the file's BYTES unchanged.
// checkoutState must not blindly trust size+change-counter alone matching;
// it degrades to a hash (since mtime, one of the three fingerprint fields,
// differs) rather than trusting a partial match, finds the content is
// unchanged, and — critically — RE-STAMPS the fingerprint so the NEXT call
// is fast again.
func TestTouchedButUnchangedCheckoutHashesAndStaysClean(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	newTime := time.Now().Add(1 * time.Hour)
	if err := os.Chtimes(path, newTime, newTime); err != nil {
		t.Fatal(err)
	}

	count := countFileSum(t)
	res, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 1 {
		t.Fatalf("fileSum called %d times for a touched-but-unchanged checkout, want exactly 1", got)
	}
	if !res.Clean {
		t.Fatal("a touched-but-byte-identical checkout must still read as clean")
	}

	// The sidecar must have been re-stamped with the NEW fingerprint: a
	// third call takes the fast path again (0 further hashes).
	count2 := countFileSum(t)
	res2, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count2(); got != 0 {
		t.Fatalf("fileSum called %d times on the THIRD call after re-stamp, want 0 (re-stamp did not take)", got)
	}
	if !res2.Clean {
		t.Fatal("want Clean=true on the third call")
	}
}

// TestOldSidecarWithoutFingerprintHashes pins the additive-fields
// contract: a sidecar written before this change (no size/mtime_ns/
// change_counter keys at all) must never be treated as a fast-path match —
// it hashes once (falling back exactly as before this feature existed),
// and then, having verified the content, re-stamps a fresh sidecar so
// later calls take the fast path.
func TestOldSidecarWithoutFingerprintHashes(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := fileSum(path)
	if err != nil {
		t.Fatal(err)
	}
	// Strip the new fields: an old-format sidecar, hand-written with only
	// the fields that predate this task.
	oldFormat := `{"hash":"` + hash + `","lineage":"` + ref.Lineage + `","epoch":` +
		strconv.FormatUint(ref.HeadEpoch, 10) + `,"txid":` + strconv.FormatUint(ref.HeadTXID, 10) + `}`
	if err := os.WriteFile(path+".sum", []byte(oldFormat), 0o644); err != nil {
		t.Fatal(err)
	}

	count := countFileSum(t)
	res, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 1 {
		t.Fatalf("fileSum called %d times against an old-format sidecar, want exactly 1", got)
	}
	if !res.Clean {
		t.Fatal("an old-format sidecar whose hash still matches must still read as clean")
	}

	// Re-stamped: a further call takes the fast path.
	count2 := countFileSum(t)
	res2, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count2(); got != 0 {
		t.Fatalf("fileSum called %d times after the old sidecar was re-stamped, want 0", got)
	}
	if !res2.Clean {
		t.Fatal("want Clean=true after re-stamp")
	}
}
