package ops

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
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

// ageMtime sets path's mtime to a value safely more than
// fingerprintSafetyMargin in the past, without touching its bytes, so that
// a sidecar re-stamped against it afterwards immediately satisfies the
// racily-clean guard (see fingerprintSafetyMargin's doc comment) for any
// later check. Real checkouts satisfy this guard by real elapsed
// wall-clock time; tests use this instead of sleeping past the margin.
func ageMtime(t *testing.T, path string) {
	t.Helper()
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}
}

// TestCleanSkipDoesNotHashWhenFingerprintMatches pins Task 2's whole point:
// once a checkout has been materialized and its fingerprint has settled
// past the racily-clean margin (size, mtime, SQLite's header change
// counter, all recorded alongside a stamp time old enough to trust — see
// fingerprintSafetyMargin's doc comment), a further Checkout against the
// SAME head must prove "clean" from the fingerprint alone, never falling
// back to a full-file hash.
func TestCleanSkipDoesNotHashWhenFingerprintMatches(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Settle the fingerprint comfortably past fingerprintSafetyMargin: a
	// stamp taken just now cannot yet be trusted by a check moments later
	// (git's racily-clean rule), so age the file's mtime and let one
	// hash-and-restamp pass settle the sidecar against it, exactly as a
	// real checkout eventually does once enough wall-clock time has passed
	// since its last write.
	ageMtime(t, path)
	if _, err := w.CheckoutProven("app", "main"); err != nil {
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

// TestModifiedCheckoutWALForcedMatchingMtimeStillDetectedViaRacilyCleanGuard
// pins Critical #1's exact repro: SQLite's WAL-mode commit path can leave
// the main file's size and header change counter completely unchanged
// across several real, content-changing commits, even after
// wal_checkpoint(TRUNCATE) — verified empirically against sqlite3 (see
// sumRecord's doc comment). If mtime also happened to land in the same
// coarse tick as the sidecar's own stamp, a fingerprint comparison with no
// further protection would have nothing left to catch the change with.
//
// This test does not rely on mtime coincidentally colliding on its own —
// it forces the live file's mtime BACK to the exact value the sidecar
// recorded via os.Chtimes, directly constructing the worst case regardless
// of this filesystem's actual mtime resolution, and pins that
// checkoutState still reports "modified" (hashing): the sidecar was
// stamped only moments ago (by the Checkpoint call just above), so the
// racily-clean guard refuses to trust ANY match this soon after that stamp
// — exactly the protection ruling 2 describes ("size + mtime + the
// racily-clean guard carry the WAL case").
func TestModifiedCheckoutWALForcedMatchingMtimeStillDetectedViaRacilyCleanGuard(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustExecSQL(t, path, "PRAGMA journal_mode=WAL; CREATE TABLE t (v);")
	if _, err := w.Checkpoint("app", "main", "cp1", nil); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("expected a readable sidecar after Checkpoint")
	}

	// A real WAL-mode commit, checkpointed exactly as production code
	// already does before ever calling checkoutState.
	mustExecSQL(t, path, "PRAGMA journal_mode=WAL; INSERT INTO t VALUES (1);")
	if err := quiesce(path); err != nil {
		t.Fatal(err)
	}

	// Force the live mtime back to the EXACT value the sidecar recorded.
	stampTime := time.Unix(0, rec.ModTimeNS)
	if err := os.Chtimes(path, stampTime, stampTime); err != nil {
		t.Fatal(err)
	}

	count := countFileSum(t)
	state, _ := checkoutState(path, ref)
	if state != "modified" {
		t.Fatalf("checkoutState = %q, want %q (a WAL write this soon after the stamp must still be caught)", state, "modified")
	}
	if got := count(); got != 1 {
		t.Fatalf("fileSum called %d times, want exactly 1 (hash fallback taken)", got)
	}
}

// TestRacilyCleanGuardHashesWithinMarginThenFastPathOnceSettled pins ruling
// 1 directly: right after a stamp, checkoutState must hash even though
// every raw fingerprint field matches, because the stamp itself is too
// recent to trust (git's racily-clean rule — see fingerprintSafetyMargin's
// doc comment); once the recorded mtime is safely older than its own stamp
// time (by at least fingerprintSafetyMargin), the SAME unchanged file takes
// the fast path. Uses os.Chtimes (via ageMtime) to place the mtime on
// either side of the margin rather than sleeping past it.
func TestRacilyCleanGuardHashesWithinMarginThenFastPathOnceSettled(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}

	// Immediately after materializing, the sidecar's mtime and stamp time
	// are only microseconds apart — well within fingerprintSafetyMargin.
	// Even though the file is untouched, the guard must refuse to trust it.
	count := countFileSum(t)
	res, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 1 {
		t.Fatalf("fileSum called %d times immediately after materializing, want exactly 1 (guard must not trust a just-taken stamp)", got)
	}
	if !res.Clean {
		t.Fatal("want Clean=true (the hash fallback still finds it clean)")
	}

	// Age the recorded mtime comfortably past the margin without touching
	// the file's bytes, simulating real wall-clock time having passed
	// since the last write, then let one more pass settle the sidecar
	// against it.
	ageMtime(t, path)
	if _, err := w.CheckoutProven("app", "main"); err != nil {
		t.Fatal(err)
	}

	count2 := countFileSum(t)
	res2, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got := count2(); got != 0 {
		t.Fatalf("fileSum called %d times once settled, want 0 (fast path expected)", got)
	}
	if !res2.Clean {
		t.Fatal("want Clean=true once settled")
	}
}

// TestTouchedButUnchangedCheckoutHashesAndStaysClean covers the mtime-only
// perturbation: os.Chtimes bumps ModTimeNS with the file's BYTES unchanged.
// checkoutState must not blindly trust size+change-counter alone matching;
// it degrades to a hash (since mtime, one of the three raw fingerprint
// fields, differs) rather than trusting a partial match, finds the content
// is unchanged, and — critically — RE-STAMPS the fingerprint so the NEXT
// call is fast again.
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

	// Touch the mtime into the past (not just "different"): the re-stamp
	// this test triggers below records this same aged mtime alongside a
	// FRESH stamp time, satisfying the racily-clean guard immediately, so
	// the third call below can take the fast path without a real sleep.
	ageMtime(t, path)

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
// change_counter/stamped_ns keys at all) must never be treated as a
// fast-path match — it hashes once (falling back exactly as before this
// feature existed), and then, having verified the content, re-stamps a
// fresh sidecar so later calls take the fast path.
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

	// Age the mtime before hand-writing the old-format sidecar: the
	// re-stamp this test triggers below records this same aged mtime
	// alongside a fresh stamp time, satisfying the racily-clean guard
	// immediately for the SECOND CheckoutProven call's fast-path
	// assertion, without a real sleep.
	ageMtime(t, path)

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

// TestShortHeaderStampSumSucceedsAndCheckoutStateHashes pins ruling 3: a
// file shorter than the 28 bytes needed to read SQLite's header change
// counter is not an error at stamp time — StampSum must still succeed —
// and checkoutState must still fall back to hashing rather than risk
// trusting a fingerprint whose change counter it could not actually read
// (it reads as the zero value, exactly like a pre-fingerprint sidecar's).
func TestShortHeaderStampSumSucceedsAndCheckoutStateHashes(t *testing.T) {
	for _, size := range []int{0, 16} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "short.db")
			if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
				t.Fatal(err)
			}
			hash, err := fileSum(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := StampSum(path, hash, "lineage-x", 1, 1, 0, ""); err != nil {
				t.Fatalf("StampSum on a %d-byte file must succeed, got %v", size, err)
			}
			if _, ok := readSidecar(path); !ok {
				t.Fatal("expected a readable sidecar")
			}

			count := countFileSum(t)
			ref := store.Ref{Lineage: "lineage-x", Epoch: 1, HeadEpoch: 1, HeadTXID: 1}
			state, _ := checkoutState(path, ref)
			if state != "clean" {
				t.Fatalf("checkoutState = %q, want %q (hash fallback should find it clean)", state, "clean")
			}
			if got := count(); got != 1 {
				t.Fatalf("fileSum called %d times for a %d-byte file, want exactly 1 (short header must never enable the fast path)", got, size)
			}
		})
	}
}

// hookFileSum installs fn as observeFileSum for the rest of the test.
func hookFileSum(t *testing.T, fn func()) {
	t.Helper()
	prev := observeFileSum
	observeFileSum = fn
	t.Cleanup(func() { observeFileSum = prev })
}

// TestNoCacheFallbackWriteDuringHashRecordsNoFingerprint pins ruling I2: on
// a filesystem that cannot clone, CheckoutProven hashes the fresh file
// itself, and a write landing while it hashes must leave a stamp with the
// hash and identity but no fingerprint — never the new file's fingerprint
// paired with a hash of other bytes — so the next check hashes again.
func TestNoCacheFallbackWriteDuringHashRecordsNoFingerprint(t *testing.T) {
	testutil.RequireSQLite3(t)
	reflinkUnsupportedForTest = true
	t.Cleanup(func() { reflinkUnsupportedForTest = false })
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path := mustCheckout(t, w, "app", "main")
	mustSQL(t, path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);")
	if _, err := w.Checkpoint("app", "main", "v1", nil); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{path, path + ".sum"} {
		os.Remove(p)
	}

	// The first fileSum is the fallback's own hash: the write lands after
	// the before-fingerprint and before the after-fingerprint.
	calls := 0
	hookFileSum(t, func() {
		calls++
		if calls != 1 {
			return
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Error(err)
			return
		}
		f.Write(make([]byte, 4096))
		f.Close()
	})
	res, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("fileSum called %d times by the no-cache fallback, want 1", calls)
	}
	rec, ok := readSidecar(res.Path)
	if !ok {
		t.Fatal("no sidecar")
	}
	if rec.Hash == "" || rec.TXID != res.Ref.HeadTXID {
		t.Fatalf("sidecar lacks the hash or identity: %+v", rec)
	}
	if rec.Size != 0 || rec.ModTimeNS != 0 || rec.ChangeCounter != 0 || rec.StampedNS != 0 {
		t.Fatalf("a fingerprint was recorded across a write during the hash: %+v", rec)
	}
	// With no fingerprint, the next check can only prove anything by
	// hashing the file again.
	ageMtime(t, res.Path)
	calls = 1 // the hook no longer writes
	before := calls
	checkoutState(res.Path, res.Ref)
	if calls != before+1 {
		t.Fatalf("checkoutState hashed %d times after an unverified stamp, want 1", calls-before)
	}
}

// TestSandwichedStampAnchorsStampedNSBeforeTheHash pins the other half of
// ruling I2: a sandwiched stamp (writeSum, and checkoutState's re-stamp)
// records StampedNS as the time just before its before-fingerprint, not
// the time it writes the sidecar — so a slow hash cannot make a
// fingerprint look older, relative to its stamp, than it was observed.
func TestSandwichedStampAnchorsStampedNSBeforeTheHash(t *testing.T) {
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	res, err := w.CheckoutProven("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	path := res.Path
	const hashTime = 50 * time.Millisecond
	var hashStart time.Time
	hookFileSum(t, func() {
		hashStart = time.Now()
		time.Sleep(hashTime)
	})
	const epsilon = int64(time.Millisecond)

	check := func(what string, before time.Time) {
		t.Helper()
		rec, ok := readSidecar(path)
		if !ok {
			t.Fatalf("%s: no sidecar", what)
		}
		if rec.StampedNS == 0 {
			t.Fatalf("%s: no fingerprint recorded for an unchanged file: %+v", what, rec)
		}
		if rec.StampedNS < before.UnixNano() || rec.StampedNS > hashStart.UnixNano()+epsilon {
			t.Fatalf("%s: StampedNS %d outside [call start %d, hash start %d + epsilon]; a write-time anchor would be >= %d",
				what, rec.StampedNS, before.UnixNano(), hashStart.UnixNano(), hashStart.Add(hashTime).UnixNano())
		}
	}

	before := time.Now()
	if err := writeSum(path, res.Ref.Lineage, res.Ref.HeadEpoch, res.Ref.HeadTXID, 0, ""); err != nil {
		t.Fatal(err)
	}
	check("writeSum", before)

	// checkoutState within the racily-clean margin hashes and re-stamps.
	before = time.Now()
	if st, _ := checkoutState(path, res.Ref); st != "clean" {
		t.Fatalf("checkout state %q, want clean", st)
	}
	check("checkoutState re-stamp", before)
}

// stampRaceBlob is the payload of the one row the stampCheckpoint race
// tests below write: a byte pattern that occurs nowhere else in a fresh
// checkout, so the test can find the row's bytes in the file and change one
// in place — a same-size write that keeps the file a valid database (the
// header, page size and page count are untouched, which is all
// ltxio.ChecksumDatabase reads before hashing the pages).
var stampRaceBlob = bytes.Repeat([]byte{0xA5, 0x5A, 0xC3, 0x3C}, 16)

// stampRaceCheckout materializes a WAL-mode checkout holding one
// stampRaceBlob row and quiesces it, exactly as CheckpointWith leaves the
// file before it takes fpEncode. It returns the path, the fingerprint
// CheckpointWith would take as fpEncode, and the post-apply checksum of
// exactly those bytes — the encSum the encode would compute, and the
// headSum the store resolves the head to once that checkpoint wins its CAS.
func stampRaceCheckout(t *testing.T) (path string, fpEncode fingerprint, encSum uint64) {
	t.Helper()
	testutil.RequireSQLite3(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, path, "CREATE TABLE t (v BLOB); INSERT INTO t VALUES (x'"+hex.EncodeToString(stampRaceBlob)+"');")
	if err := quiesce(path); err != nil {
		t.Fatal(err)
	}
	if fpEncode, err = stampFingerprint(path); err != nil {
		t.Fatal(err)
	}
	if !fpEncode.walMode {
		t.Fatal("checkout is not in WAL mode; the change counter would carry the match and this test would not exercise the size+mtime-only case")
	}
	if encSum, err = ltxio.ChecksumDatabase(path); err != nil {
		t.Fatal(err)
	}
	return path, fpEncode, encSum
}

// overwriteSameSizeSameTick changes one byte of the stampRaceBlob row in
// path and then restores the file's mtime to exactly what it was: the
// foreign same-size write that lands in the same coarse mtime tick as the
// stat that produced fpEncode, reproduced deterministically with
// os.Chtimes instead of a 1 s-resolution filesystem. It asserts that the
// live fingerprint still equals fpEncode afterwards, so a test that then
// sees stampCheckpoint distrust the stamp knows the margin, not a
// fingerprint mismatch, is what stopped the shortcut.
func overwriteSameSizeSameTick(t *testing.T, path string, fpEncode fingerprint) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data := readFile(t, path)
	i := bytes.Index(data, stampRaceBlob)
	if i < 0 {
		t.Fatal("stampRaceBlob not found in the checkout file")
	}
	data[i] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	fp, err := stampFingerprint(path)
	if err != nil {
		t.Fatal(err)
	}
	if fp != fpEncode {
		t.Fatalf("live fingerprint %+v != fpEncode %+v after the same-size write; the race was not constructed", fp, fpEncode)
	}
}

// TestStampCheckpointDistrustsSameTickWriteWithinMargin pins the
// racily-clean margin on stampCheckpoint's matching-fingerprint shortcut:
// fpEncode was taken at an instant (encodeNS) no older than its own mtime —
// the same coarse mtime tick — and a foreign same-size write then changed
// the content and left the fingerprint identical. The store holds the
// bytes that were encoded (headSum == encSum), the checkout does not, and
// the stamp must say so: trusted=false, untrustedHash, no checksum. Before
// the margin was applied here, fp == fpEncode alone was enough to stamp
// encSum against bytes it does not describe.
func TestStampCheckpointDistrustsSameTickWriteWithinMargin(t *testing.T) {
	path, fpEncode, encSum := stampRaceCheckout(t)
	encodeNS := fpEncode.mtimeNS
	overwriteSameSizeSameTick(t, path, fpEncode)

	trusted, err := stampCheckpoint(path, "lin", 1, 2, encSum, true, encSum, fpEncode, encodeNS, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if trusted {
		t.Fatal("stamp trusted: a same-size write inside the encode fingerprint's mtime tick was stamped as the store's content")
	}
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("no sidecar after the stamp")
	}
	if rec.Hash != untrustedHash(2) || rec.PostApplyChecksum != 0 || rec.StampedNS != 0 {
		t.Fatalf("sidecar hash %q checksum %016x stamped %d, want %q, 0, 0", rec.Hash, rec.PostApplyChecksum, rec.StampedNS, untrustedHash(2))
	}
}

// TestStampCheckpointShortcutTakenOnceSettled proves the fast path is not
// lost: with encodeNS a comfortable fingerprintSafetyMargin past fpEncode's
// mtime and the content unchanged, stampCheckpoint trusts encSum without
// hashing the database. encSum and headSum are a sentinel that no hash of
// the file equals, so only the shortcut can produce trusted=true here —
// the ChecksumDatabase fallback would compute the real checksum, find it
// differs from headSum, and distrust the stamp.
func TestStampCheckpointShortcutTakenOnceSettled(t *testing.T) {
	path, fpEncode, encSum := stampRaceCheckout(t)
	encodeNS := fpEncode.mtimeNS + int64(2*time.Second)
	sentinel := encSum ^ 0xDEADBEEF
	if sentinel == 0 {
		sentinel = 1
	}

	trusted, err := stampCheckpoint(path, "lin", 1, 2, sentinel, true, sentinel, fpEncode, encodeNS, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !trusted {
		t.Fatal("stamp distrusted with a settled, matching encode fingerprint: the shortcut was not taken")
	}
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("no sidecar after the stamp")
	}
	if rec.PostApplyChecksum != sentinel || rec.Size != fpEncode.size || rec.ModTimeNS != fpEncode.mtimeNS {
		t.Fatalf("sidecar checksum %016x size %d mtime %d, want %016x %d %d", rec.PostApplyChecksum, rec.Size, rec.ModTimeNS, sentinel, fpEncode.size, fpEncode.mtimeNS)
	}
}

// TestStampCheckpointWithinMarginFallsBackToHashAndStillTrusts: when the
// margin is not met but nothing actually wrote to the checkout, the
// fallback costs one ChecksumDatabase pass and nothing else — the real
// checksum equals headSum, so the stamp is trusted with the real hash,
// fingerprint and checksum, exactly as the shortcut would have stamped it.
func TestStampCheckpointWithinMarginFallsBackToHashAndStillTrusts(t *testing.T) {
	path, fpEncode, encSum := stampRaceCheckout(t)
	encodeNS := fpEncode.mtimeNS

	trusted, err := stampCheckpoint(path, "lin", 1, 2, encSum, true, encSum, fpEncode, encodeNS, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !trusted {
		t.Fatal("stamp distrusted for unchanged content: the fallback hash should have matched headSum")
	}
	rec, ok := readSidecar(path)
	if !ok {
		t.Fatal("no sidecar after the stamp")
	}
	if rec.Hash == untrustedHash(2) || rec.PostApplyChecksum != encSum || rec.Size != fpEncode.size || rec.ModTimeNS != fpEncode.mtimeNS {
		t.Fatalf("sidecar hash %q checksum %016x size %d mtime %d, want a real hash, %016x %d %d", rec.Hash, rec.PostApplyChecksum, rec.Size, rec.ModTimeNS, encSum, fpEncode.size, fpEncode.mtimeNS)
	}
}
