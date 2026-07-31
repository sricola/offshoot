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
}
