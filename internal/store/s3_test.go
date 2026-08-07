package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

// newFakeBacked returns an S3 backend pointed at a fresh in-process fake.
func newFakeBacked(t *testing.T) store.Backend {
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
	return b
}

func TestS3Conformance(t *testing.T) {
	storetest.RunConformance(t, "", newFakeBacked)
}

// TestS3CopyObjectOverSizeLimitFallsBack pins the >5GB guard documented on
// store.S3.CopyObject: S3's single-request "PUT Object - Copy" supports
// source objects up to 5 GiB, so CopyObject HEADs the source first and
// returns ErrCopyUnsupported for anything over that limit rather than
// letting the copy fail with an opaque API error — the same signal
// ops.Fork's fast path already treats as "fall back to the slow path"
// (Task 6a). Uses storetest.FakeS3's SetSizeOverride to make a tiny stored
// object report a >5GB Content-Length on HEAD, so this test exercises the
// gate without allocating or uploading a real multi-gigabyte object.
func TestS3CopyObjectOverSizeLimitFallsBack(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("data/huge/src", []byte("not actually 6GB")); err != nil {
		t.Fatal(err)
	}
	const sixGiB = 6 * 1024 * 1024 * 1024
	f.SetSizeOverride("data/huge/src", sixGiB)

	if err := b.CopyObject("data/huge/dst", "data/huge/src"); !errors.Is(err, store.ErrCopyUnsupported) {
		t.Fatalf("want ErrCopyUnsupported copying a >5GB source, got %v", err)
	}
	if _, _, err := b.Get("data/huge/dst"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a declined copy must not create dst, got %v", err)
	}

	// Just under the limit: must proceed normally (a real, if small, copy).
	f.SetSizeOverride("data/huge/src", sixGiB/2)
	if err := b.CopyObject("data/huge/dst", "data/huge/src"); err != nil {
		t.Fatalf("copy under the 5GB limit must succeed: %v", err)
	}
	data, _, err := b.Get("data/huge/dst")
	if err != nil || string(data) != "not actually 6GB" {
		t.Fatalf("dst = %q, err = %v, want the source content copied", data, err)
	}
}

func TestS3PrefixIsolatesKeys(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	mk := func(prefix string) store.Backend {
		b, err := store.NewS3(context.Background(), store.S3Config{
			Bucket: f.Bucket(), Prefix: prefix, Endpoint: f.URL(),
			Region: "us-east-1", UsePathStyle: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, bb := mk("tenant-a"), mk("tenant-b")
	if err := a.Put("refs/x", []byte("A")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bb.Get("refs/x"); err == nil {
		t.Fatal("prefixes must isolate keys")
	}
	keys, err := a.List("refs/")
	if err != nil || len(keys) != 1 || keys[0] != "refs/x" {
		t.Fatalf("List must return prefix-stripped keys: %v %v", keys, err)
	}
}

// newWrongBucketBacked returns an S3 backend pointed at the fake server but
// configured with a bucket name the fake does not serve, so every request
// gets S3's real NoSuchBucket response. A caller misconfigured this way must
// see a loud error, never a quiet ErrNotFound/ErrCAS that a retry loop or an
// "empty store" fallback would treat as expected.
func newWrongBucketBacked(t *testing.T) store.Backend {
	t.Helper()
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket:       f.Bucket() + "-does-not-exist",
		Endpoint:     f.URL(),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestS3WrongBucketGetIsLoud(t *testing.T) {
	b := newWrongBucketBacked(t)
	_, _, err := b.Get("some/key")
	if err == nil {
		t.Fatal("Get against a nonexistent bucket must error")
	}
	if errors.Is(err, store.ErrNotFound) {
		t.Fatalf("bucket-level failure must not be reported as ErrNotFound (misconfiguration must not look like an empty store), got %v", err)
	}
	if !strings.Contains(err.Error(), "some/key") {
		t.Fatalf("error must mention the failing operation/key, got %v", err)
	}
}

func TestS3WrongBucketDeleteIsLoud(t *testing.T) {
	b := newWrongBucketBacked(t)
	if err := b.Delete("some/key"); err == nil {
		t.Fatal("Delete against a nonexistent bucket must error, not silently succeed")
	}
}

func TestS3WrongBucketPutIfWithEtagIsLoud(t *testing.T) {
	b := newWrongBucketBacked(t)
	_, err := b.PutIf("some/key", []byte("data"), "some-etag")
	if err == nil {
		t.Fatal("PutIf against a nonexistent bucket must error")
	}
	if errors.Is(err, store.ErrCAS) {
		t.Fatalf("bucket-level failure must not be reported as ErrCAS (a CAS-retry caller would loop forever against a bucket that will never exist), got %v", err)
	}
}

func TestS3WrongBucketPutIfCreateOnlyIsLoud(t *testing.T) {
	b := newWrongBucketBacked(t)
	_, err := b.PutIf("some/key", []byte("data"), "")
	if err == nil {
		t.Fatal("PutIf (create-only) against a nonexistent bucket must error")
	}
	if errors.Is(err, store.ErrCAS) {
		t.Fatalf("bucket-level failure must not be reported as ErrCAS, got %v", err)
	}
}

func TestS3ServerErrorsAreNotSwallowed(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	f.SetFault(func(method, key string) (int, bool) { return 500, true })

	if err := b.Put("k2", []byte("v")); err == nil {
		t.Error("a 500 on Put must surface as an error")
	}
	if _, err := b.PutIf("k3", []byte("v"), ""); err == nil {
		t.Error("a 500 on conditional Put must surface as an error")
	} else if errors.Is(err, store.ErrCAS) {
		t.Error("a 500 must not be reported as a CAS conflict")
	}
	if _, _, err := b.Get("k"); err == nil {
		t.Error("a 500 on Get must surface as an error")
	} else if errors.Is(err, store.ErrNotFound) {
		t.Error("a 500 must not be reported as ErrNotFound")
	}
	if _, err := b.List("k"); err == nil {
		t.Error("a 500 on List must surface as an error")
	}
}

func TestS3ListPaginates(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 1500
	for i := 0; i < n; i++ {
		if err := b.Put(fmt.Sprintf("data/lin/%05d.ltx", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := b.List("data/lin/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != n {
		t.Fatalf("got %d keys across pages, want %d", len(keys), n)
	}
	if keys[0] != "data/lin/00000.ltx" || keys[n-1] != fmt.Sprintf("data/lin/%05d.ltx", n-1) {
		t.Fatalf("pagination lost ordering: first=%s last=%s", keys[0], keys[n-1])
	}
}
