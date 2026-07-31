package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/store/storetest"
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
