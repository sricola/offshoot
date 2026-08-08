package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
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

// TestS3GetReader pins store.ReaderGetter's contract on the S3 backend
// (perf audit H3 / task 9a): a streamed read must return the same content
// Get would, over the same key namespace/prefix, and a missing key must map
// to ErrNotFound exactly like Get does.
func TestS3GetReader(t *testing.T) {
	b := newFakeBacked(t)
	rg, ok := b.(store.ReaderGetter)
	if !ok {
		t.Fatal("store.S3 must implement store.ReaderGetter")
	}
	if err := b.Put("data/x/1.ltx", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	r, etag, err := rg.GetReader("data/x/1.ltx")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("streamed content = %q, want %q", got, "hello")
	}
	if etag == "" {
		t.Error("etag must not be empty")
	}

	if _, _, err := rg.GetReader("nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound for a missing key, got %v", err)
	}
}

// TestS3PutReaderIfWithFileBody exercises store.ReaderPutter against an
// *os.File-backed io.Reader specifically — not the bytes.Reader the shared
// conformance suite (storetest.RunConformance) uses everywhere. An *os.File
// is the ACTUAL production shape: Session.flush's snapshot upload always
// passes an *os.File opened on its encode-output scratch (see flush.go's
// snapshot branch). That matters for the AWS SDK's SigV4 body handling —
// this fake server is plain HTTP, so the SDK takes its
// ComputePayloadSHA256-then-RewindStream path (see aws-sdk-go-v2's
// aws/signer/v4/middleware.go: dynamicPayloadSigningMiddleware picks
// UNSIGNED-PAYLOAD only over HTTPS), which reads the body once to hash it
// then calls RewindStream to seek back to the start before the real send —
// a real disk-backed io.Seeker exercises that rewind differently than an
// already-in-memory bytes.Reader would. FakeS3's PUT handler reads the
// request body fully (io.ReadAll(r.Body)) regardless of what produced it,
// so a correct round-trip here proves the SDK actually sent the file's
// bytes over the wire — not zero bytes, not a truncated read from a body
// the rewind left mis-positioned.
func TestS3PutReaderIfWithFileBody(t *testing.T) {
	b := newFakeBacked(t)
	rp, ok := b.(store.ReaderPutter)
	if !ok {
		t.Fatal("store.S3 must implement store.ReaderPutter")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-scratch.ltx")
	// Larger than a single read buffer — meant to look like a real (if
	// tiny) LTX encode-output scratch file, not a handful of bytes.
	payload := []byte(strings.Repeat("file-backed-stream-", 4096))
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	etag, err := rp.PutReaderIf("data/x/from-file.ltx", f, int64(len(payload)), "")
	f.Close()
	if err != nil {
		t.Fatalf("PutReaderIf with *os.File body: %v", err)
	}
	if etag == "" {
		t.Error("PutReaderIf must return a non-empty etag")
	}

	// Round-trip via the buffered Get.
	data, getEtag, err := b.Get("data/x/from-file.ltx")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("Get after file-backed PutReaderIf: got %d bytes, want %d", len(data), len(payload))
	}
	if getEtag != etag {
		t.Errorf("Get's etag %q != PutReaderIf's returned etag %q", getEtag, etag)
	}

	// Round-trip via GetReader too — proves both streaming directions
	// (write from a file, read as a stream) agree on content.
	rg, ok := b.(store.ReaderGetter)
	if !ok {
		t.Fatal("store.S3 must implement store.ReaderGetter")
	}
	r, _, err := rg.GetReader("data/x/from-file.ltx")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("GetReader after file-backed PutReaderIf: got %d bytes, err %v", len(got), err)
	}

	// PutReader (unconditional overwrite) with a FRESH *os.File — mirrors
	// flush.go's orphan-overwrite retry, which reopens the encode scratch
	// from offset 0 after PutReaderIf's own reader is exhausted.
	overwrite := []byte(strings.Repeat("file-backed-overwrite-", 2048))
	if err := os.WriteFile(path, overwrite, 0o644); err != nil {
		t.Fatal(err)
	}
	f2, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	putErr := rp.PutReader("data/x/from-file.ltx", f2, int64(len(overwrite)))
	f2.Close()
	if putErr != nil {
		t.Fatalf("PutReader with *os.File body: %v", putErr)
	}

	data, _, err = b.Get("data/x/from-file.ltx")
	if err != nil || !bytes.Equal(data, overwrite) {
		t.Fatalf("Get after file-backed PutReader overwrite: got %d bytes, err %v", len(data), err)
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

// TestS3DeleteObjectsChunksBatches pins store.S3's BatchDeleter capability
// (perf audit H2): 2500 keys must go out as exactly ceil(2500/1000) = 3
// DeleteObjects requests (the fake enforces the API's 1000-key cap with a
// MalformedXML rejection, so an unchunked request could not sneak through),
// zero per-key DELETE round trips, with every key deleted and every deleted
// key reported back in the caller's un-prefixed form — the backend runs
// under a bucket prefix precisely to prove the batch path namespaces and
// strips keys the same way single Delete does.
func TestS3DeleteObjectsChunksBatches(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Prefix: "team-a", Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Compile-time pin: *store.S3 implements the optional capability.
	var bd store.BatchDeleter = b

	const n = 2500
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("data/lin/%05d.ltx", i)
		f.Seed("team-a/"+keys[i], []byte("x"))
	}

	var mu sync.Mutex
	var posts, deletes int
	f.SetFault(func(method, key string) (int, bool) {
		mu.Lock()
		switch method {
		case http.MethodPost:
			posts++
		case http.MethodDelete:
			deletes++
		}
		mu.Unlock()
		return 0, false // observe only, never fault
	})

	deleted, err := bd.DeleteObjects(keys)
	if err != nil {
		t.Fatal(err)
	}
	if posts != 3 {
		t.Fatalf("2500 keys issued %d DeleteObjects requests, want exactly 3 (1000-key chunks)", posts)
	}
	if deletes != 0 {
		t.Fatalf("batch delete issued %d per-key DELETE round trips, want 0", deletes)
	}
	sort.Strings(deleted)
	if !reflect.DeepEqual(deleted, keys) {
		t.Fatalf("deleted key set mismatch: got %d keys, want %d (un-prefixed caller form)", len(deleted), len(keys))
	}
	if left, err := b.List("data/"); err != nil || len(left) != 0 {
		t.Fatalf("objects left after batch delete: %d err=%v", len(left), err)
	}
}

// TestS3DeleteObjectsPartialFailureAndIdempotency pins the two contract
// edges GC's stone pruning depends on: a key S3 reports a per-key <Error>
// for is NOT in the returned deleted set but IS named in the returned error
// (and its object survives), while a key that never existed still counts as
// deleted (S3's DeleteObjects reports absent keys under Deleted — same
// idempotency as single Delete). Empty input is a no-op.
func TestS3DeleteObjectsPartialFailureAndIdempotency(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var bd store.BatchDeleter = b

	if deleted, err := bd.DeleteObjects(nil); deleted != nil || err != nil {
		t.Fatalf("empty input must be (nil, nil), got (%v, %v)", deleted, err)
	}

	for _, k := range []string{"data/l/ok.ltx", "data/l/stuck.ltx"} {
		if err := b.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	f.SetBatchDeleteError("data/l/stuck.ltx")

	deleted, err := bd.DeleteObjects([]string{"data/l/ok.ltx", "data/l/stuck.ltx", "data/l/never-existed.ltx"})
	if err == nil || !strings.Contains(err.Error(), "data/l/stuck.ltx") {
		t.Fatalf("partial failure must return an error naming the failed key, got: %v", err)
	}
	sort.Strings(deleted)
	want := []string{"data/l/never-existed.ltx", "data/l/ok.ltx"}
	if !reflect.DeepEqual(deleted, want) {
		t.Fatalf("deleted = %v, want %v (absent key counts as deleted; failed key does not)", deleted, want)
	}
	if _, _, err := b.Get("data/l/stuck.ltx"); err != nil {
		t.Fatalf("failed key's object must survive: %v", err)
	}
	if _, _, err := b.Get("data/l/ok.ltx"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("succeeded key must be gone, Get err = %v", err)
	}
}
