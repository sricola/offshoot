package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/store/storetest"
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
