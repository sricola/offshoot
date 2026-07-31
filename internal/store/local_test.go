package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
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
	etag, _ := b.PutIf("refs/a/main", []byte("0"), "")
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

func TestLocalRejectsTraversal(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if err := b.Put("../evil", []byte("x")); err == nil {
		t.Fatal("want error on path traversal")
	}
}
