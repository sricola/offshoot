package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/offshoot-db/offshoot/internal/store"
	"github.com/offshoot-db/offshoot/internal/store/storetest"
)

func TestProbeCASPassesLocal(t *testing.T) {
	b, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ProbeCAS(b); err != nil {
		t.Fatalf("local backend must pass the probe: %v", err)
	}
	keys, _ := b.List("probe/")
	if len(keys) != 0 {
		t.Fatalf("probe must clean up after itself, left: %v", keys)
	}
}

func TestProbeCASPassesFakeS3(t *testing.T) {
	if err := store.ProbeCAS(newFakeBacked(t)); err != nil {
		t.Fatalf("fake honoring preconditions must pass: %v", err)
	}
}

func TestProbeCASRejectsProviderIgnoringPreconditions(t *testing.T) {
	f := storetest.NewFakeS3(t)
	f.IgnorePreconditions(true)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = store.ProbeCAS(b)
	if !errors.Is(err, store.ErrNoCAS) {
		t.Fatalf("want ErrNoCAS for a provider ignoring preconditions, got %v", err)
	}
}
