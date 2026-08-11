package store_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

// testPartSize is the part size these tests drive store.S3's multipart path
// with, via store.SetPartSizeForTest — S3's real 5 MiB minimum part size
// (store's own minPartSize floor prevents going any lower), not production's
// 64 MiB default. Exercising "several real parts" at the real default would
// need a several-hundred-MiB test payload; overriding down to the true S3
// floor keeps a several-part round trip to a ~10 MiB payload while still
// respecting a real S3 constraint (not an arbitrary shrink).
const testPartSize = 5 * 1024 * 1024 // 5 MiB, == store's minPartSize

// smallMultipartSettings overrides both multipartThreshold and the
// multipart part size low enough that a modest test payload takes the
// multipart path and splits into multiple genuine parts, restoring both via
// t.Cleanup. Every test below that wants to exercise the multipart path
// calls this first.
func smallMultipartSettings(t *testing.T) {
	t.Helper()
	restoreThreshold := store.SetMultipartThresholdForTest(1024) // 1 KiB
	restorePartSize := store.SetPartSizeForTest(testPartSize)
	t.Cleanup(func() {
		restorePartSize()
		restoreThreshold()
	})
}

// newFakeBackedWithFake is newFakeBacked (s3_test.go) but also returns the
// underlying *storetest.FakeS3, so a test can inspect its multipart-request
// counters (MultipartStats/PendingMultipartUploads) or install a fault.
func newFakeBackedWithFake(t *testing.T) (store.Backend, *storetest.FakeS3) {
	t.Helper()
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket:       f.Bucket(),
		Endpoint:     f.URL(),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b, f
}

// failPutsFromSecond installs a fault on f that counts PUT requests to key
// and fails every one from the second onward with HTTP 500 — the shared
// fault pattern behind the abort-on-failure tests (upload, concurrent
// upload, and multipart copy): the first part PUT succeeds so the upload is
// genuinely under way, then every later attempt — including every SDK retry
// — fails, robust to which of possibly-concurrent parts happens to be
// attempted first.
func failPutsFromSecond(f *storetest.FakeS3, key string) {
	var puts int
	f.SetFault(func(method, k string) (int, bool) {
		if method == http.MethodPut && k == key {
			puts++
			if puts >= 2 {
				return 500, true
			}
		}
		return 0, false
	})
}

// onlyReader wraps an io.Reader and exposes nothing else — in particular
// NEITHER io.ReaderAt NOR io.Seeker, even though the underlying
// *bytes.Reader would satisfy both. Used to force store.S3.putMultipart
// onto its buffered-read fallback path (TestS3PutReaderIfMultipartNonReaderAtFallback),
// the same way a non-seekable network stream would in production.
type onlyReader struct{ r io.Reader }

func (o *onlyReader) Read(p []byte) (int, error) { return o.r.Read(p) }

// multipartPayload returns a payload deterministic and large enough to
// force several real parts at testPartSize: two full parts plus a partial
// third, so tests exercise part-boundary handling and last-part truncation.
func multipartPayload() []byte {
	size := 2*testPartSize + 1024*1024 // two full parts + a 1 MiB partial third
	b := make([]byte, size)
	for i := range b {
		// A non-repeating-per-part pattern: catches a part-ordering bug
		// (e.g. parts swapped) that an all-same-byte payload would hide.
		b[i] = byte(i % 251)
	}
	return b
}

// TestS3PutReaderIfMultipartRoundTrip pins the core multipart write path:
// with the threshold and part size forced low, a payload larger than a
// single part uploads via CreateMultipartUpload/UploadPart*/
// CompleteMultipartUpload and reads back byte-identical via Get — proving
// part ordering, concatenation, and no corruption — and the fake actually
// saw a multipart upload (more than one part), not a single PUT.
func TestS3PutReaderIfMultipartRoundTrip(t *testing.T) {
	smallMultipartSettings(t)

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload()
	etag, err := rp.PutReaderIf("data/x/big.ltx", bytes.NewReader(payload), int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (multipart): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf (multipart) must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/big.ltx")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get after multipart PutReaderIf: got %d bytes, want %d bytes, equal=false", len(got), len(payload))
	}

	creates, uploadParts, completes, aborts := f.MultipartStats()
	if creates != 1 || completes != 1 || aborts != 0 {
		t.Fatalf("MultipartStats = creates=%d uploadParts=%d completes=%d aborts=%d, want creates=1 completes=1 aborts=0",
			creates, uploadParts, completes, aborts)
	}
	if uploadParts <= 1 {
		t.Fatalf("uploadParts = %d, want > 1 (payload must have taken the multipart path, not a single PUT)", uploadParts)
	}
	if pending := f.PendingMultipartUploads(); pending != 0 {
		t.Fatalf("PendingMultipartUploads = %d after a successful upload, want 0", pending)
	}
}

// TestS3PutReaderIfMultipartSeekedReaderAt pins the fix for a silent-
// corruption bug: putMultipart's zero-buffer io.ReaderAt path used to read
// each part from ABSOLUTE offset 0 in the underlying data, ignoring
// wherever the caller's reader was actually positioned — correct only for a
// freshly opened file (today's only production caller), wrong for any
// reader a caller had already Seek'd, and wrong silently, only above
// multipartThreshold, only in production. This test opens a real *os.File
// (both io.ReaderAt and io.Seeker, the same shape flush.go passes), seeks
// it partway in, uploads from there, and asserts the object matches only
// the bytes from the seek point onward — not the whole file from byte 0.
func TestS3PutReaderIfMultipartSeekedReaderAt(t *testing.T) {
	smallMultipartSettings(t)

	b, _ := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	prefix := bytes.Repeat([]byte{0xEE}, 1024*1024) // 1 MiB of filler before the seek point
	payload := multipartPayload()
	full := append(append([]byte{}, prefix...), payload...)

	dir := t.TempDir()
	path := filepath.Join(dir, "seeked-source.bin")
	if err := os.WriteFile(path, full, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(int64(len(prefix)), io.SeekStart); err != nil {
		t.Fatal(err)
	}

	etag, err := rp.PutReaderIf("data/x/seeked.ltx", f, int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (multipart, seeked *os.File): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/seeked.ltx")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get after seeked multipart PutReaderIf: got %d bytes, want %d bytes matching the POST-SEEK content "+
			"(a base-offset bug would instead return the file's first %d bytes, i.e. the filler prefix)",
			len(got), len(payload), len(payload))
	}
}

// TestS3PutReaderIfMultipartCASParity pins that CAS semantics are
// indistinguishable between the single-PUT and multipart paths — the same
// create-only and never-written-key cases storetest.RunConformance already
// pins for the single-PUT path (see conformance.go's ReaderPutter subtest)
// — AND that a losing CAS race still aborts the multipart upload it made:
// this is the highest-frequency real leak path (every retried, losing
// writer uploads a full object's worth of parts before losing the
// Complete), so each ErrCAS assertion below is paired with an abort check.
func TestS3PutReaderIfMultipartCASParity(t *testing.T) {
	smallMultipartSettings(t)

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload()
	if _, err := rp.PutReaderIf("data/x/cas.ltx", bytes.NewReader(payload), int64(len(payload)), ""); err != nil {
		t.Fatalf("first create-only multipart write: %v", err)
	}
	if _, _, completes, aborts := f.MultipartStats(); completes != 1 || aborts != 0 {
		t.Fatalf("after the first (winning) write: completes=%d aborts=%d, want completes=1 aborts=0", completes, aborts)
	}

	// A second create-only (ifMatch == "") write to the SAME key must
	// conflict — key already exists — and MUST abort its multipart upload:
	// every part of `second` was already uploaded by the time Complete is
	// rejected.
	second := multipartPayload()
	if _, err := rp.PutReaderIf("data/x/cas.ltx", bytes.NewReader(second), int64(len(second)), ""); !errors.Is(err, store.ErrCAS) {
		t.Fatalf("second create-only multipart write to an existing key: got %v, want store.ErrCAS", err)
	}
	if _, _, completes, aborts := f.MultipartStats(); completes != 1 || aborts != 1 {
		t.Fatalf("after the losing create-only write: completes=%d aborts=%d, want completes=1 (unchanged) aborts=1", completes, aborts)
	}
	if pending := f.PendingMultipartUploads(); pending != 0 {
		t.Fatalf("PendingMultipartUploads = %d after a losing create-only CAS, want 0", pending)
	}

	// A CAS (ifMatch != "") write against a key that was never written must
	// also conflict — nothing to compare against — and also abort.
	third := multipartPayload()
	if _, err := rp.PutReaderIf("data/x/cas-never-written.ltx", bytes.NewReader(third), int64(len(third)), "some-etag"); !errors.Is(err, store.ErrCAS) {
		t.Fatalf("multipart CAS write against a never-written key: got %v, want store.ErrCAS", err)
	}
	if _, _, completes, aborts := f.MultipartStats(); completes != 1 || aborts != 2 {
		t.Fatalf("after the never-written-key CAS: completes=%d aborts=%d, want completes=1 (unchanged) aborts=2", completes, aborts)
	}
	if pending := f.PendingMultipartUploads(); pending != 0 {
		t.Fatalf("PendingMultipartUploads = %d after a never-written-key CAS, want 0", pending)
	}
}

// TestS3PutReaderIfMultipartAbortsOnFailure pins the single most important
// correctness/cost property of the multipart path (see store.S3's
// putMultipart doc comment): an abandoned multipart upload bills for its
// parts on S3 forever, so EVERY error exit after a successful
// CreateMultipartUpload must AbortMultipartUpload. Forces one UploadPart
// call to fail via storetest.FakeS3.SetFault, then asserts the error
// propagates AND no upload is left pending.
func TestS3PutReaderIfMultipartAbortsOnFailure(t *testing.T) {
	smallMultipartSettings(t)

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	const key = "data/x/fail.ltx"
	// Fail the second part's UploadPart, and every SDK retry of it.
	failPutsFromSecond(f, key)

	payload := multipartPayload()
	_, err := rp.PutReaderIf(key, bytes.NewReader(payload), int64(len(payload)), "")
	if err == nil {
		t.Fatal("a failed UploadPart mid-upload must surface as an error")
	}

	creates, _, completes, aborts := f.MultipartStats()
	if creates != 1 {
		t.Fatalf("MultipartStats creates = %d, want 1 (the upload was created before it failed)", creates)
	}
	if completes != 0 {
		t.Fatalf("MultipartStats completes = %d, want 0 (a failed upload must never complete)", completes)
	}
	if aborts != 1 {
		t.Fatalf("MultipartStats aborts = %d, want exactly 1 (AbortMultipartUpload must run on the failure path)", aborts)
	}
	if pending := f.PendingMultipartUploads(); pending != 0 {
		t.Fatalf("PendingMultipartUploads = %d after a failed upload, want 0 (nothing left billing on S3)", pending)
	}
	if _, _, gerr := b.Get(key); !errors.Is(gerr, store.ErrNotFound) {
		t.Fatalf("a failed multipart upload must not create the object, got %v", gerr)
	}
}

// TestS3PutReaderBelowThresholdUnchanged proves the below-threshold path is
// byte-for-byte the same one today's tests already cover: with the
// (un-overridden, real) 5 GiB default threshold, a small upload takes a
// single PutObject — the fake sees zero multipart requests at all.
func TestS3PutReaderBelowThresholdUnchanged(t *testing.T) {
	b, f := newFakeBackedWithFake(t) // default multipartThreshold: 5 GiB, not overridden
	rp := b.(store.ReaderPutter)

	payload := []byte("small payload, well under any threshold")
	etag, err := rp.PutReaderIf("data/x/small.ltx", bytes.NewReader(payload), int64(len(payload)), "")
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/small.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get after below-threshold PutReaderIf: got %q, err %v, want %q", got, err, payload)
	}

	creates, uploadParts, completes, aborts := f.MultipartStats()
	if creates != 0 || uploadParts != 0 || completes != 0 || aborts != 0 {
		t.Fatalf("MultipartStats = creates=%d uploadParts=%d completes=%d aborts=%d, want all 0 (below threshold must take the single-PUT path)",
			creates, uploadParts, completes, aborts)
	}
}

// TestS3PutReaderIfMultipartNonReaderAtFallback pins putMultipart's
// buffered-read fallback: when r is NEITHER an io.ReaderAt NOR an
// io.Seeker (unlike the production caller's *os.File), each part is still
// read and uploaded correctly via the io.LimitReader + reused-buffer path.
func TestS3PutReaderIfMultipartNonReaderAtFallback(t *testing.T) {
	smallMultipartSettings(t)

	b, f := newFakeBackedWithFake(t)
	rp := b.(store.ReaderPutter)

	payload := multipartPayload()
	r := &onlyReader{r: bytes.NewReader(payload)}
	// Sanity: onlyReader must not accidentally satisfy io.ReaderAt or
	// io.Seeker via some promoted method, or this test would not exercise
	// the fallback at all.
	if _, ok := io.Reader(r).(io.ReaderAt); ok {
		t.Fatal("onlyReader must not implement io.ReaderAt")
	}
	if _, ok := io.Reader(r).(io.Seeker); ok {
		t.Fatal("onlyReader must not implement io.Seeker")
	}

	etag, err := rp.PutReaderIf("data/x/non-readerat.ltx", r, int64(len(payload)), "")
	if err != nil {
		t.Fatalf("PutReaderIf (multipart, non-io.ReaderAt): %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	got, _, err := b.Get("data/x/non-readerat.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get after non-io.ReaderAt multipart PutReaderIf: got %d bytes, want %d bytes equal, err %v",
			len(got), len(payload), err)
	}

	_, uploadParts, _, _ := f.MultipartStats()
	if uploadParts <= 1 {
		t.Fatalf("uploadParts = %d, want > 1 (must still take the multipart path)", uploadParts)
	}
}
