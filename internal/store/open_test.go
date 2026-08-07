package store_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

func TestOpenBackendLocalPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	b, err := store.OpenBackend(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBackendFileURL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if _, err := store.OpenBackend(context.Background(), "file://"+dir); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBackendS3URL(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")

	b, err := store.OpenBackend(context.Background(), "s3://"+f.Bucket()+"/tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/x", []byte("v"), ""); err != nil {
		t.Fatal(err)
	}
	keys, err := b.List("refs/")
	if err != nil || len(keys) != 1 || keys[0] != "refs/x" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}

func TestOpenBackendRefusesStoreWithoutCAS(t *testing.T) {
	f := storetest.NewFakeS3(t)
	f.IgnorePreconditions(true)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")

	_, err := store.OpenBackend(context.Background(), "s3://"+f.Bucket())
	if err == nil || !strings.Contains(err.Error(), "conditional writes") {
		t.Fatalf("want a refusal naming conditional writes, got %v", err)
	}
}

func TestOpenBackendRejectsUnknownScheme(t *testing.T) {
	if _, err := store.OpenBackend(context.Background(), "gs://bucket/x"); err == nil {
		t.Fatal("unknown scheme must be refused")
	}
}

// TestStoreIdentityCanonicalizesS3Specs guards the cache-key fix: a store
// identity must reflect the RESOLVED backend (bucket + prefix + env-derived
// endpoint/region/path-style), not just the literal spec text. Two spellings
// of the same spec (with/without a trailing slash on the prefix) must
// collapse to one identity, while the same spec string under a different
// resolved endpoint, region, or path-style must NOT collide.
func TestStoreIdentityCanonicalizesS3Specs(t *testing.T) {
	t.Setenv("OFFSHOOT_S3_ENDPOINT", "http://minio.local:9000")
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")

	base, err := store.StoreIdentity("s3://b/p")
	if err != nil {
		t.Fatal(err)
	}
	trailingSlash, err := store.StoreIdentity("s3://b/p/")
	if err != nil {
		t.Fatal(err)
	}
	if base != trailingSlash {
		t.Fatalf("s3://b/p and s3://b/p/ must share an identity: %q vs %q", base, trailingSlash)
	}

	t.Setenv("OFFSHOOT_S3_ENDPOINT", "http://real-aws.example.com")
	differentEndpoint, err := store.StoreIdentity("s3://b/p")
	if err != nil {
		t.Fatal(err)
	}
	if differentEndpoint == base {
		t.Fatalf("changing OFFSHOOT_S3_ENDPOINT must change identity for the same spec, got %q both times", base)
	}
	t.Setenv("OFFSHOOT_S3_ENDPOINT", "http://minio.local:9000")

	t.Setenv("OFFSHOOT_S3_REGION", "eu-west-1")
	differentRegion, err := store.StoreIdentity("s3://b/p")
	if err != nil {
		t.Fatal(err)
	}
	if differentRegion == base {
		t.Fatalf("changing OFFSHOOT_S3_REGION must change identity for the same spec, got %q both times", base)
	}
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")

	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "0")
	differentPathStyle, err := store.StoreIdentity("s3://b/p")
	if err != nil {
		t.Fatal(err)
	}
	if differentPathStyle == base {
		t.Fatalf("changing OFFSHOOT_S3_PATH_STYLE must change identity for the same spec, got %q both times", base)
	}
}
