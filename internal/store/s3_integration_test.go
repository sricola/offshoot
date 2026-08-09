package store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

// TestS3RealProvider runs the full Backend conformance suite and the CAS
// probe against a real S3-compatible provider. Set OFFSHOOT_S3_TEST_BUCKET
// (plus OFFSHOOT_S3_ENDPOINT / OFFSHOOT_S3_REGION / OFFSHOOT_S3_PATH_STYLE
// and credentials as needed) to enable it. This is the only evidence that
// justifies listing a provider as supported — the in-process fake proves
// nothing about a real provider's precondition handling.
func TestS3RealProvider(t *testing.T) {
	bucket := os.Getenv("OFFSHOOT_S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("set OFFSHOOT_S3_TEST_BUCKET to run real-provider tests")
	}
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	prefix := "offshoot-itest/" + hex.EncodeToString(buf)

	newBackend := func(t *testing.T) store.Backend {
		t.Helper()
		b, err := store.NewS3(context.Background(), store.S3Config{
			Bucket:       bucket,
			Prefix:       prefix + "/" + freshSegment(t),
			Endpoint:     os.Getenv("OFFSHOOT_S3_ENDPOINT"),
			Region:       os.Getenv("OFFSHOOT_S3_REGION"),
			UsePathStyle: os.Getenv("OFFSHOOT_S3_PATH_STYLE") == "1",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { deleteAll(t, b) })
		return b
	}

	t.Run("Probe", func(t *testing.T) {
		if err := store.ProbeCAS(newBackend(t)); err != nil {
			t.Fatalf("provider failed the CAS probe: %v", err)
		}
	})
	t.Run("Conformance", func(t *testing.T) {
		storetest.RunConformance(t, "", newBackend)
	})
	t.Run("Multipart", func(t *testing.T) {
		// storetest.FakeS3 can never catch a real provider rejecting
		// CompleteMultipartUpload (e.g. AWS's own "InvalidRequest: ... must
		// include the checksum for each part" when a checksum algorithm was
		// attached to the upload but a part's CompletedPart entry dropped
		// it) — it's plain HTTP, so the SDK never even takes the code path
		// that matters here, and it doesn't model per-provider checksum
		// enforcement at all. CAS-on-Complete is precondition handling on a
		// comparatively new API surface, unevenly implemented across S3-
		// compatible providers — exactly the class of thing this file's own
		// doc comment says only a real provider can prove.
		restoreThreshold := store.SetMultipartThresholdForTest(1024) // 1 KiB
		restorePartSize := store.SetPartSizeForTest(5 * 1024 * 1024) // 5 MiB: S3's real minimum part size
		t.Cleanup(func() {
			restorePartSize()
			restoreThreshold()
		})

		b := newBackend(t)
		rp, ok := b.(store.ReaderPutter)
		if !ok {
			t.Fatal("store.S3 must implement store.ReaderPutter")
		}

		// Two full parts plus a partial third, at the overridden 5 MiB part
		// size — enough to force a genuine multipart upload against the
		// real provider without pushing a several-hundred-MiB payload
		// through an integration test.
		payload := make([]byte, 2*5*1024*1024+1024*1024)
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}

		etag, err := rp.PutReaderIf("multipart/big.ltx", bytes.NewReader(payload), int64(len(payload)), "")
		if err != nil {
			t.Fatalf("PutReaderIf (multipart) against a real provider: %v", err)
		}
		if etag == "" {
			t.Error("PutReaderIf (multipart) must return a non-empty etag")
		}

		got, _, err := b.Get("multipart/big.ltx")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("Get after real-provider multipart PutReaderIf: got %d bytes, want %d bytes, equal=false",
				len(got), len(payload))
		}

		// Create-only CAS conflict must be reported correctly by the real
		// provider's CompleteMultipartUpload IfNoneMatch handling — the
		// fake proves this backend's own precondition-mapping logic is
		// right, but only a real provider proves the header is actually
		// honored server-side.
		second := make([]byte, len(payload))
		if _, err := rand.Read(second); err != nil {
			t.Fatal(err)
		}
		if _, err := rp.PutReaderIf("multipart/big.ltx", bytes.NewReader(second), int64(len(second)), ""); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("create-only multipart write to an existing key on a real provider: got %v, want store.ErrCAS", err)
		}
	})
}

// freshSegment returns a unique path segment so sub-tests never share keys.
func freshSegment(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(buf)
}

func deleteAll(t *testing.T, b store.Backend) {
	t.Helper()
	// Best-effort and deliberately non-fatal: this runs in t.Cleanup after
	// the test body has already reported pass/fail, so a cleanup hiccup here
	// must only be logged, never fail or mask the real test result.
	keys, err := b.List("")
	if err != nil {
		t.Logf("cleanup list failed: %v", err)
		return
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			t.Logf("cleanup delete %s: %v", k, err)
		}
	}
}
