package store_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// This file pins store.S3.CopyObject's multipart UploadPartCopy path (see
// s3_multipart.go's copyObjectMultipart): a source over copyObjectMaxBytes
// copies server-side via multipart instead of returning ErrCopyUnsupported, a
// source under it is unaffected (still the single-request path), and the
// same abort-on-every-error-path discipline putMultipart established
// covers a failed multipart copy too. See s3_test.go's
// TestS3CopyObjectOverSizeLimitFallsBack for the still-genuinely-
// unsupported case (a source over S3's 5TiB per-object ceiling).

// TestS3CopyObjectMultipartLargeObject is the off-by-one guard on
// copyObjectMultipart's CopySourceRange arithmetic: with copyObjectMaxBytes
// and the multipart part size both lowered (SetCopyObjectMaxBytesForTest /
// SetPartSizeForTest — a real >5GiB source isn't a reasonable ask of a unit
// test), a modest real payload spans several genuine parts, and the
// destination must be BYTE-IDENTICAL to the source. An inclusive-end bug
// (using offset+n instead of offset+n-1, or vice versa) would silently
// duplicate or drop bytes at every part boundary — a plain length check
// alone wouldn't always catch that, so this compares full content.
func TestS3CopyObjectMultipartLargeObject(t *testing.T) {
	t.Cleanup(store.SetCopyObjectMaxBytesForTest(1024)) // 1 KiB
	t.Cleanup(store.SetPartSizeForTest(testPartSize))   // 5 MiB, == store's minPartSize

	b, f := newFakeBackedWithFake(t)

	payload := multipartPayload() // 2 full parts + a partial third at testPartSize
	if err := b.Put("data/x/copy-src.ltx", payload); err != nil {
		t.Fatal(err)
	}

	err := b.CopyObject("data/x/copy-dst.ltx", "data/x/copy-src.ltx")
	if errors.Is(err, store.ErrCopyUnsupported) {
		t.Fatal("a source over copyObjectMaxBytes but under S3's 5TiB ceiling must copy via multipart, not return ErrCopyUnsupported")
	}
	if err != nil {
		t.Fatalf("CopyObject (multipart copy): %v", err)
	}

	got, _, err := b.Get("data/x/copy-dst.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("dst after multipart CopyObject: got %d bytes, want %d bytes equal, err %v "+
			"(an off-by-one in CopySourceRange's inclusive end would silently corrupt or truncate this)",
			len(got), len(payload), err)
	}

	creates, uploadParts, completes, aborts := f.MultipartStats()
	if creates != 1 || completes != 1 || aborts != 0 {
		t.Fatalf("MultipartStats = creates=%d completes=%d aborts=%d, want creates=1 completes=1 aborts=0",
			creates, completes, aborts)
	}
	if uploadParts <= 1 {
		t.Fatalf("uploadParts = %d, want > 1 (the copy must have taken the multipart UploadPartCopy path, not a single request)", uploadParts)
	}
}

// TestS3CopyObjectSmallUsesSingleRequest proves the below-threshold path is
// unaffected by copyObjectMultipart's addition: with the real (un-
// overridden) 5 GiB copyObjectMaxBytes, a small copy still takes the
// original single-request CopyObject path — the fake sees zero multipart
// requests at all.
func TestS3CopyObjectSmallUsesSingleRequest(t *testing.T) {
	b, f := newFakeBackedWithFake(t) // copyObjectMaxBytes NOT overridden: real 5 GiB

	payload := []byte("small object, well under any multipart-copy threshold")
	if err := b.Put("data/x/small-copy-src.ltx", payload); err != nil {
		t.Fatal(err)
	}

	if err := b.CopyObject("data/x/small-copy-dst.ltx", "data/x/small-copy-src.ltx"); err != nil {
		t.Fatalf("CopyObject (small, single-request path): %v", err)
	}

	got, _, err := b.Get("data/x/small-copy-dst.ltx")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("dst after small CopyObject: got %q, err %v, want %q", got, err, payload)
	}

	creates, uploadParts, completes, aborts := f.MultipartStats()
	if creates != 0 || uploadParts != 0 || completes != 0 || aborts != 0 {
		t.Fatalf("MultipartStats = creates=%d uploadParts=%d completes=%d aborts=%d, want all 0 "+
			"(a below-threshold copy must take the single-request CopyObject path, not multipart)",
			creates, uploadParts, completes, aborts)
	}
}

// TestS3CopyObjectMultipartAbortsOnFailure pins copyObjectMultipart's
// abort-on-every-error-path discipline — the same cost-critical guarantee
// putMultipart's doc comment describes (an abandoned multipart upload bills
// its parts on S3 forever) — for a multipart COPY, not just a multipart
// upload: forces one UploadPartCopy call to fail via storetest.FakeS3's
// fault hook, then asserts the error propagates, AbortMultipartUpload ran
// exactly once, and dst was never created.
func TestS3CopyObjectMultipartAbortsOnFailure(t *testing.T) {
	t.Cleanup(store.SetCopyObjectMaxBytesForTest(1024))
	t.Cleanup(store.SetPartSizeForTest(testPartSize))

	b, f := newFakeBackedWithFake(t)

	payload := multipartPayload()
	const srcKey = "data/x/copy-fail-src.ltx"
	if err := b.Put(srcKey, payload); err != nil {
		t.Fatal(err)
	}

	const dstKey = "data/x/copy-fail-dst.ltx"
	// Fail every part-copy PUT to dst from the second attempt onward.
	failPutsFromSecond(f, dstKey)

	if err := b.CopyObject(dstKey, srcKey); err == nil {
		t.Fatal("a failed UploadPartCopy mid-copy must surface as an error")
	}

	creates, _, completes, aborts := f.MultipartStats()
	if creates != 1 {
		t.Fatalf("MultipartStats creates = %d, want 1 (the multipart copy was created before it failed)", creates)
	}
	if completes != 0 {
		t.Fatalf("MultipartStats completes = %d, want 0 (a failed multipart copy must never complete)", completes)
	}
	if aborts != 1 {
		t.Fatalf("MultipartStats aborts = %d, want exactly 1 (AbortMultipartUpload must run for a failed multipart copy too)", aborts)
	}
	if pending := f.PendingMultipartUploads(); pending != 0 {
		t.Fatalf("PendingMultipartUploads = %d after a failed multipart copy, want 0 (nothing left billing on S3)", pending)
	}
	if _, _, gerr := b.Get(dstKey); !errors.Is(gerr, store.ErrNotFound) {
		t.Fatalf("a failed multipart copy must not create dst, got %v", gerr)
	}
}
