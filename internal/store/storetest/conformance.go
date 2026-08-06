// Package storetest provides the shared Backend contract suite and an
// in-process S3 subset server, so every backend is verified against exactly
// the same expectations.
package storetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/offshoot-db/offshoot/internal/store"
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
}
