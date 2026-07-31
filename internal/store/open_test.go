package store_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/store/storetest"
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
