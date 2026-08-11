package store_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// This file pins store.S3.putMultipart's concurrent-part-upload path (see
// s3_multipart.go's multipartConcurrency and concurrentPartUpload): parts
// really do upload in parallel on the io.ReaderAt path (bounded by
// multipartConcurrency), concurrency=1 stays a deterministic regression
// case, out-of-order completions still land in an ordered CompletedPart
// list, the abort-on-failure guarantee survives concurrency, and the
// non-io.ReaderAt fallback stays strictly sequential regardless of the
// concurrency setting. All five reuse smallMultipartSettings,
// newFakeBackedWithFake, multipartPayload, and onlyReader from
// s3_multipart_test.go (same package).

// TestS3PutReaderIfMultipartConcurrencyParallel proves parts really do
// upload in parallel, not just potentially: it arms storetest.FakeS3's
// rendezvous gate (ArmPartGate) for exactly multipartConcurrency parts, so
// the upload can only complete if that many UploadPart requests are
// simultaneously in flight — a serialized implementation would have every
// request block until FakeS3's partGateTimeout, which would make this test
// fail (via MaxPartsInFlight staying below 3), not hang, but should never
// need to under a correct concurrent implementation. multipartPayload()
// splits into exactly 3 parts at testPartSize, matched to a concurrency of
// 3 so every worker is busy from the start.
func TestS3PutReaderIfMultipartConcurrencyParallel(t *testing.T) {
	smallMultipartSettings(t)
	t.Cleanup(store.SetMultipartConcurrencyForTest(3))

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload() // 2 full parts + a partial third == 3 parts
	f.ArmPartGate(3)

	etag, err := rp.PutReaderIf("data/x/concurrent.ltx", bytes.NewReader(payload), int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (concurrent multipart): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/concurrent.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get after concurrent multipart PutReaderIf: got %d bytes, want %d bytes equal, err %v",
			len(got), len(payload), err)
	}

	if max := f.MaxPartsInFlight(); max < 3 {
		t.Fatalf("MaxPartsInFlight = %d, want >= 3 (the gate only releases once 3 parts arrive "+
			"simultaneously — a serialized implementation could never reach it)", max)
	}

	_, uploadParts, completes, aborts := f.MultipartStats()
	if uploadParts != 3 || completes != 1 || aborts != 0 {
		t.Fatalf("MultipartStats: uploadParts=%d completes=%d aborts=%d, want uploadParts=3 completes=1 aborts=0",
			uploadParts, completes, aborts)
	}
}

// TestS3PutReaderIfMultipartConcurrency1 pins the deterministic,
// effectively-sequential case: with multipartConcurrency forced to 1 (via
// SetMultipartConcurrencyForTest), the io.ReaderAt path must still produce
// a byte-identical object and never have more than one UploadPart request
// in flight at a time — a regression guard for the concurrent code path's
// worker-count-1 edge case (a pool of exactly one worker draining a
// channel), not just the pre-existing pure-sequential-loop behavior it
// replaced.
func TestS3PutReaderIfMultipartConcurrency1(t *testing.T) {
	smallMultipartSettings(t)
	t.Cleanup(store.SetMultipartConcurrencyForTest(1))

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload()
	etag, err := rp.PutReaderIf("data/x/concurrency1.ltx", bytes.NewReader(payload), int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (multipart, concurrency=1): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/concurrency1.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get after concurrency=1 multipart PutReaderIf: got %d bytes, want %d bytes equal, err %v",
			len(got), len(payload), err)
	}
	if max := f.MaxPartsInFlight(); max != 1 {
		t.Fatalf("MaxPartsInFlight = %d, want exactly 1 (multipartConcurrency=1 must upload strictly one part at a time)", max)
	}
}

// TestS3PutReaderIfMultipartOutOfOrderCompletion proves ordering is
// reconstructed from each part's OWN PartNumber, not from completion order:
// the fake HOLDS part 1's UploadPart (storetest.FakeS3.
// HoldPartUntilCompletions) until parts 2 and 3 — uploaded concurrently —
// have fully COMPLETED, then releases it, so the CompletedPart list is
// GUARANTEED to be assembled out of order on every run and every machine
// (the earlier version merely delayed part 1 by 200ms and hoped parts 2
// and 3 finished first — on a slow machine it silently passed without
// exercising out-of-order reconstruction at all; PartHoldTimedOut below
// closes that silent-pass hole). If concurrentPartUpload instead built
// parts in completion order (e.g. via a plain append under a mutex,
// rather than writing into parts[partNum-1]), this would produce a
// shuffled/corrupted object, not a clean byte mismatch — the assertion
// below would still catch it either way, but the corruption framing is
// the point being pinned.
func TestS3PutReaderIfMultipartOutOfOrderCompletion(t *testing.T) {
	smallMultipartSettings(t)
	t.Cleanup(store.SetMultipartConcurrencyForTest(3))

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload() // 3 parts
	f.HoldPartUntilCompletions(1, 2)

	etag, err := rp.PutReaderIf("data/x/outoforder.ltx", bytes.NewReader(payload), int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (multipart, out-of-order completion): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}
	if f.PartHoldTimedOut() {
		t.Fatal("the part-1 hold timed out instead of being released by parts 2 and 3 completing — " +
			"parts did not genuinely overlap, so out-of-order completion was never exercised")
	}

	got, _, err := b.Get("data/x/outoforder.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get after out-of-order multipart completion: got %d bytes, want %d bytes equal, err %v "+
			"(part 1 completed LAST — guaranteed by the hold — but must still land first in the CompletedPart list)",
			len(got), len(payload), err)
	}
}

// TestS3PutReaderIfMultipartConcurrentAbortsOnFailure is
// TestS3PutReaderIfMultipartAbortsOnFailure (s3_multipart_test.go) under
// concurrency: with multiple parts genuinely in flight at once, a failure
// on any one of them must still cancel the rest promptly, surface as the
// operation's error, and AbortMultipartUpload exactly once — the first-
// error-wins-and-cancels-the-rest guarantee concurrentPartUpload's doc
// comment describes. Uses the same "fail every PUT to this key from the
// 2nd attempt onward" fault pattern as the sequential test, robust to which
// of the (possibly concurrent) parts happens to be attempted first and to
// SDK retries.
func TestS3PutReaderIfMultipartConcurrentAbortsOnFailure(t *testing.T) {
	smallMultipartSettings(t)
	t.Cleanup(store.SetMultipartConcurrencyForTest(3))

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	const key = "data/x/concurrent-fail.ltx"
	// Fail every part upload from the second attempt onward.
	failPutsFromSecond(f, key)

	payload := multipartPayload()
	_, err := rp.PutReaderIf(key, bytes.NewReader(payload), int64(len(payload)), "")
	if err == nil {
		t.Fatal("a failed UploadPart under concurrency must surface as an error")
	}

	creates, _, completes, aborts := f.MultipartStats()
	if creates != 1 {
		t.Fatalf("MultipartStats creates = %d, want 1 (the upload was created before it failed)", creates)
	}
	if completes != 0 {
		t.Fatalf("MultipartStats completes = %d, want 0 (a failed upload must never complete)", completes)
	}
	if aborts != 1 {
		t.Fatalf("MultipartStats aborts = %d, want exactly 1 (AbortMultipartUpload must run under concurrency too, exactly once)", aborts)
	}
	if pending := f.PendingMultipartUploads(); pending != 0 {
		t.Fatalf("PendingMultipartUploads = %d after a failed concurrent upload, want 0 (nothing left billing on S3)", pending)
	}
	if _, _, gerr := b.Get(key); !errors.Is(gerr, store.ErrNotFound) {
		t.Fatalf("a failed multipart upload must not create the object, got %v", gerr)
	}
}

// TestS3PutReaderIfMultipartNonReaderAtFallbackStaysSequential pins the
// HARD CONSTRAINT that concurrency is ONLY safe on the io.ReaderAt path:
// even with multipartConcurrency raised well above 1, a source that is
// NEITHER io.ReaderAt NOR io.Seeker (onlyReader, defined in package
// store_test's s3_multipart_test.go — not in storetest — same as
// TestS3PutReaderIfMultipartNonReaderAtFallback)
// must still upload its parts one at a time — it shares one reused buffer
// and reads r strictly in order, so concurrent access would race the
// buffer and read r out of order. MaxPartsInFlight staying at exactly 1
// despite multipartConcurrency==4 is the proof this backend never takes
// the "buffer per worker" shortcut the task explicitly rules out.
func TestS3PutReaderIfMultipartNonReaderAtFallbackStaysSequential(t *testing.T) {
	smallMultipartSettings(t)
	t.Cleanup(store.SetMultipartConcurrencyForTest(4))

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload()
	r := &onlyReader{r: bytes.NewReader(payload)}

	etag, err := rp.PutReaderIf("data/x/fallback-sequential.ltx", r, int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (multipart, non-io.ReaderAt): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/fallback-sequential.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get after non-io.ReaderAt multipart PutReaderIf: got %d bytes, want %d bytes equal, err %v",
			len(got), len(payload), err)
	}

	if max := f.MaxPartsInFlight(); max != 1 {
		t.Fatalf("MaxPartsInFlight = %d, want exactly 1 — the non-io.ReaderAt fallback must stay strictly "+
			"sequential even with multipartConcurrency raised to 4 (see putMultipart's CONCURRENCY doc comment)", max)
	}
}
