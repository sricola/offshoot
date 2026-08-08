// Package storetest provides the shared Backend contract suite and an
// in-process S3 subset server, so every backend is verified against exactly
// the same expectations.
package storetest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// RunConformance exercises the full Backend contract.
func RunConformance(t *testing.T, keyPrefix string, newBackend func(t *testing.T) store.Backend) {
	t.Helper()
	k := func(s string) string { return keyPrefix + s }

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		b := newBackend(t)
		if err := b.Put(k("data/x/1.ltx"), []byte("hello")); err != nil {
			t.Fatal(err)
		}
		data, etag, err := b.Get(k("data/x/1.ltx"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "hello" {
			t.Errorf("data = %q, want %q", data, "hello")
		}
		if etag == "" {
			t.Error("etag must not be empty")
		}
	})

	t.Run("PutOverwritesExistingKey", func(t *testing.T) {
		// ops relies on this: Checkpoint's orphan-recovery path (see the
		// comment in ops.Checkpoint) falls back to an UNCONDITIONAL Put to
		// overwrite a snapshot object left by a crashed or racing prior
		// attempt at the same deterministic key. Every backend must agree
		// that a second Put to an existing key wins outright, not merge,
		// append, or silently no-op.
		b := newBackend(t)
		if err := b.Put(k("data/overwrite"), []byte("first")); err != nil {
			t.Fatal(err)
		}
		_, etag1, err := b.Get(k("data/overwrite"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(k("data/overwrite"), []byte("second")); err != nil {
			t.Fatal(err)
		}
		data, etag2, err := b.Get(k("data/overwrite"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "second" {
			t.Fatalf("content = %q, want %q (second Put must win)", data, "second")
		}
		if etag2 == etag1 {
			t.Error("etag must change when Put overwrites existing content")
		}
	})

	t.Run("GetMissingIsErrNotFound", func(t *testing.T) {
		b := newBackend(t)
		if _, _, err := b.Get(k("nope")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("CreateOnlyRejectsExisting", func(t *testing.T) {
		b := newBackend(t)
		if _, err := b.PutIf(k("refs/a/main"), []byte("v1"), ""); err != nil {
			t.Fatal(err)
		}
		if _, err := b.PutIf(k("refs/a/main"), []byte("v2"), ""); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS on create-only over existing, got %v", err)
		}
		data, _, err := b.Get(k("refs/a/main"))
		if err != nil || string(data) != "v1" {
			t.Fatalf("rejected create-only must not modify content: %q %v", data, err)
		}
	})

	t.Run("CASRejectsStaleEtag", func(t *testing.T) {
		b := newBackend(t)
		etag1, err := b.PutIf(k("refs/a/main"), []byte("v1"), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.PutIf(k("refs/a/main"), []byte("bad"), "sha256-of-nothing"); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS on stale etag, got %v", err)
		}
		etag2, err := b.PutIf(k("refs/a/main"), []byte("v2"), etag1)
		if err != nil {
			t.Fatal(err)
		}
		if etag2 == etag1 {
			t.Error("etag must change when content changes")
		}
		data, _, _ := b.Get(k("refs/a/main"))
		if string(data) != "v2" {
			t.Fatalf("content = %q, want v2", data)
		}
	})

	t.Run("CASOnMissingKeyFails", func(t *testing.T) {
		b := newBackend(t)
		if _, err := b.PutIf(k("refs/absent"), []byte("v"), "some-etag"); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS for CAS against absent key, got %v", err)
		}
	})

	t.Run("ConcurrentCASHasOneWinner", func(t *testing.T) {
		b := newBackend(t)
		etag, err := b.PutIf(k("refs/race"), []byte("seed"), "")
		if err != nil {
			t.Fatal(err)
		}
		const n = 16
		var wg sync.WaitGroup
		wins := make(chan int, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if _, err := b.PutIf(k("refs/race"), []byte(fmt.Sprintf("w%d", idx)), etag); err == nil {
					wins <- idx
				}
			}(i)
		}
		wg.Wait()
		close(wins)
		count := 0
		for range wins {
			count++
		}
		if count != 1 {
			t.Fatalf("exactly one CAS from the same etag must win, got %d", count)
		}
	})

	t.Run("ListSortedFilteredAndDeleted", func(t *testing.T) {
		b := newBackend(t)
		for _, key := range []string{"data/l1/b", "data/l1/a", "data/l2/a"} {
			if err := b.Put(k(key), []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		keys, err := b.List(k("data/l1/"))
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 2 || keys[0] != k("data/l1/a") || keys[1] != k("data/l1/b") {
			t.Fatalf("keys = %v, want sorted [a b] under the prefix", keys)
		}
		if err := b.Delete(k("data/l1/a")); err != nil {
			t.Fatal(err)
		}
		if keys, _ = b.List(k("data/l1/")); len(keys) != 1 || keys[0] != k("data/l1/b") {
			t.Fatalf("after delete keys = %v", keys)
		}
	})

	t.Run("DeleteIsIdempotent", func(t *testing.T) {
		b := newBackend(t)
		if err := b.Delete(k("data/never-existed")); err != nil {
			t.Fatalf("delete of absent key must not error: %v", err)
		}
	})

	// CopyObject: every backend offshoot ships (local since Task 6a, S3
	// since Task 6b) supports it for objects within its own limits, and
	// this subtest exercises the full round-trip + ErrNotFound + overwrite
	// checks below against all of them — local, the in-process S3 fake, and
	// the MinIO-gated real-provider suite all run through this one shared
	// entry point. It still tolerates a backend legitimately returning the
	// sentinel store.ErrCopyUnsupported by skipping rather than failing
	// (the same call this subtest's real caller, ops.Fork's fast path,
	// makes to decide whether to fall back) — S3 itself takes exactly that
	// path for a source object over its 5GB single-request CopyObject
	// limit (see store.S3.CopyObject), just not for anything this subtest's
	// small test objects ever reach.
	t.Run("CopyObject", func(t *testing.T) {
		b := newBackend(t)
		if err := b.Put(k("data/copy/src"), []byte("copy-me")); err != nil {
			t.Fatal(err)
		}
		err := b.CopyObject(k("data/copy/dst"), k("data/copy/src"))
		if errors.Is(err, store.ErrCopyUnsupported) {
			t.Skip("backend does not support CopyObject (ErrCopyUnsupported); callers fall back to the slow path")
		}
		if err != nil {
			t.Fatal(err)
		}
		data, etag, err := b.Get(k("data/copy/dst"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "copy-me" {
			t.Errorf("copied data = %q, want %q", data, "copy-me")
		}
		if etag == "" {
			t.Error("etag must not be empty")
		}
		// The source must be untouched by the copy.
		srcData, _, err := b.Get(k("data/copy/src"))
		if err != nil || string(srcData) != "copy-me" {
			t.Fatalf("source must be unchanged after CopyObject: %q %v", srcData, err)
		}
		if err := b.CopyObject(k("data/copy/dst2"), k("data/copy/nope")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("want ErrNotFound copying an absent source, got %v", err)
		}

		// CopyObject overwrites an existing dst (see store.Backend's doc
		// comment) — unlike PutIf's create-only semantics, every other
		// snapshot/segment write in this codebase uses.
		if err := b.Put(k("data/copy/src2"), []byte("overwritten")); err != nil {
			t.Fatal(err)
		}
		if err := b.CopyObject(k("data/copy/dst"), k("data/copy/src2")); err != nil {
			t.Fatalf("CopyObject onto an existing dst must overwrite, not error: %v", err)
		}
		data, _, err = b.Get(k("data/copy/dst"))
		if err != nil || string(data) != "overwritten" {
			t.Fatalf("dst after overwrite = %q, want %q: %v", data, "overwritten", err)
		}
	})

	// ReaderPutter: an optional capability (perf audit H3, write side —
	// task 9b), not part of Backend itself, so this subtest skips outright
	// on a backend that doesn't implement it rather than failing. Both
	// backends offshoot ships (Local, S3) DO implement it, so this runs for
	// real on both here; it pins that PutReaderIf/PutReader produce content
	// and CAS/create-only outcomes byte-identical to PutIf/Put over the
	// exact same bytes, sourced from an io.Reader instead of a []byte.
	t.Run("ReaderPutter", func(t *testing.T) {
		b := newBackend(t)
		rp, ok := b.(store.ReaderPutter)
		if !ok {
			t.Skip("backend does not implement store.ReaderPutter")
		}

		payload := []byte(strings.Repeat("stream-me-", 4096)) // exercise more than one io.Copy buffer's worth
		etag, err := rp.PutReaderIf(k("stream/create-only"), bytes.NewReader(payload), int64(len(payload)), "")
		if err != nil {
			t.Fatal(err)
		}
		if etag == "" {
			t.Error("PutReaderIf must return a non-empty etag")
		}
		data, getEtag, err := b.Get(k("stream/create-only"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, payload) {
			t.Fatalf("streamed create-only content mismatch: got %d bytes, want %d", len(data), len(payload))
		}
		if getEtag != etag {
			t.Errorf("Get's etag %q != PutReaderIf's returned etag %q", getEtag, etag)
		}

		// Create-only over an existing key must fail with ErrCAS and must
		// not touch the existing content — same contract as PutIf.
		if _, err := rp.PutReaderIf(k("stream/create-only"), bytes.NewReader([]byte("nope")), 4, ""); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS on streamed create-only over existing, got %v", err)
		}
		data, _, err = b.Get(k("stream/create-only"))
		if err != nil || !bytes.Equal(data, payload) {
			t.Fatalf("rejected streamed create-only must not modify content: got %d bytes, err %v", len(data), err)
		}

		// A real CAS (ifMatch = the prior etag) must succeed and change the
		// stored content and etag, same as PutIf.
		second := []byte("second-streamed-value")
		etag2, err := rp.PutReaderIf(k("stream/create-only"), bytes.NewReader(second), int64(len(second)), etag)
		if err != nil {
			t.Fatal(err)
		}
		if etag2 == etag {
			t.Error("etag must change when streamed CAS content changes")
		}
		data, _, err = b.Get(k("stream/create-only"))
		if err != nil || !bytes.Equal(data, second) {
			t.Fatalf("content after streamed CAS = %q, want %q: %v", data, second, err)
		}

		// A stale etag must fail with ErrCAS, same as PutIf.
		if _, err := rp.PutReaderIf(k("stream/create-only"), bytes.NewReader([]byte("stale")), 5, etag); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS on stale streamed etag, got %v", err)
		}

		// A CAS against a key that was NEVER written must also fail with
		// ErrCAS, exactly like PutIf's CASOnMissingKeyFails subtest above —
		// a nonempty ifMatch has nothing to compare against on an absent
		// key, so it's a failed compare, not a create.
		if _, err := rp.PutReaderIf(k("stream/never-written"), bytes.NewReader([]byte("v")), 1, "some-etag"); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS for streamed CAS against an absent key, got %v", err)
		}
		if _, _, err := b.Get(k("stream/never-written")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("a rejected CAS against an absent key must not create it, got %v", err)
		}

		// PutReader is an unconditional overwrite, same as Put.
		third := []byte("third-streamed-value-overwrite")
		if err := rp.PutReader(k("stream/create-only"), bytes.NewReader(third), int64(len(third))); err != nil {
			t.Fatal(err)
		}
		data, _, err = b.Get(k("stream/create-only"))
		if err != nil || !bytes.Equal(data, third) {
			t.Fatalf("content after PutReader overwrite = %q, want %q: %v", data, third, err)
		}

		// PutReader on a brand-new key (no prior content at all) must also
		// succeed unconditionally, same as Put.
		fresh := []byte("fresh-key-via-put-reader")
		if err := rp.PutReader(k("stream/fresh"), bytes.NewReader(fresh), int64(len(fresh))); err != nil {
			t.Fatal(err)
		}
		data, _, err = b.Get(k("stream/fresh"))
		if err != nil || !bytes.Equal(data, fresh) {
			t.Fatalf("PutReader on a fresh key: content = %q, want %q: %v", data, fresh, err)
		}

		// A stream-written object must be readable back via GetReader too,
		// on backends that implement both optional capabilities — proves
		// the two streaming paths (write and read) agree on content.
		if rg, ok := b.(store.ReaderGetter); ok {
			r, _, err := rg.GetReader(k("stream/fresh"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil || !bytes.Equal(got, fresh) {
				t.Fatalf("GetReader over a PutReader-written key: got %d bytes, err %v", len(got), err)
			}
		}
	})
}
