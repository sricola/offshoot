package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLocalPutGetEtag(t *testing.T) {
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("data/x/1.ltx", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, etag, err := b.Get("data/x/1.ltx")
	if err != nil || string(data) != "hello" || etag == "" {
		t.Fatalf("got %q etag=%q err=%v", data, etag, err)
	}
	if _, _, err := b.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLocalCreateOnly(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if _, err := b.PutIf("refs/a/main", []byte("v1"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/a/main", []byte("v2"), ""); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on create-only over existing, got %v", err)
	}
}

func TestLocalCASUpdate(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	etag1, err := b.PutIf("refs/a/main", []byte("v1"), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/a/main", []byte("bad"), "wrong-etag"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on stale etag, got %v", err)
	}
	etag2, err := b.PutIf("refs/a/main", []byte("v2"), etag1)
	if err != nil || etag2 == etag1 {
		t.Fatalf("CAS update failed: %v", err)
	}
	data, _, _ := b.Get("refs/a/main")
	if string(data) != "v2" {
		t.Fatalf("content = %q", data)
	}
}

func TestLocalCASRace(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	// Seed content must never equal any fmt.Sprint(n) produced below (n in
	// [0,32)); otherwise the goroutine that happens to win with n==0 writes
	// back identical content, leaving the etag unchanged and letting a
	// second goroutine legitimately CAS against the same original etag.
	etag, _ := b.PutIf("refs/a/main", []byte("seed"), "")
	var wg sync.WaitGroup
	wins := make(chan int, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := b.PutIf("refs/a/main", []byte(fmt.Sprint(n)), etag); err == nil {
				wins <- n
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
}

func TestLocalListAndDelete(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	for _, k := range []string{"data/l1/a", "data/l1/b", "data/l2/a"} {
		if err := b.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := b.List("data/l1/")
	if err != nil || len(keys) != 2 || keys[0] != "data/l1/a" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if err := b.Delete("data/l1/a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete("data/l1/a"); err != nil {
		t.Fatal("delete of absent key must not error")
	}
	if keys, _ = b.List("data/l1/"); len(keys) != 1 {
		t.Fatalf("keys=%v", keys)
	}
}

func TestLocalBreaksStaleLock(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	p, err := b.path("refs/a/main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := p + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	stale := time.Now().Add(-1 * time.Minute)
	if err := os.Chtimes(lockPath, stale, stale); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if _, err := b.PutIf("refs/a/main", []byte("v1"), ""); err != nil {
		t.Fatalf("PutIf should break the stale lock and succeed, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("PutIf took too long to break stale lock: %v", elapsed)
	}
}

func TestLocalFreshLockNotBroken(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	p, err := b.path("refs/a/main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := p + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close() // freshly created: mtime is now

	done := make(chan struct{})
	go func() {
		b.PutIf("refs/a/main", []byte("v1"), "")
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("PutIf must not succeed while a fresh lock is held")
	case <-time.After(1 * time.Second):
	}

	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PutIf did not complete after the lock was removed")
	}
}

func TestLocalRejectsTraversal(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if err := b.Put("../evil", []byte("x")); err == nil {
		t.Fatal("want error on path traversal")
	}
}
