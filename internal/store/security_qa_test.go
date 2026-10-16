package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLocalRefusesEscapingKeysOnEveryOperation pins SECURITY.md "Names and
// paths": "the local store refuses keys that escape its directory" — for
// Get, Put, PutIf, Delete, CopyObject (either side) and List, not just the
// one Put("../evil") the older test covers. The store root sits inside a
// parent that holds exactly one other file; after every refused call the
// root is still empty and the parent is unchanged, so nothing was created
// or listed outside the root. Mutation verified against: local.go
// Local.path's check with the `strings.Contains(key, "..")` clause dropped
// (PutIf/Put then create root/b for "a/../b", and List("../") returns
// "../outside.txt"), and separately with the `strings.HasPrefix(key, "/")`
// clause dropped ("/abs" is then accepted and written as root/abs).
func TestLocalRefusesEscapingKeysOnEveryOperation(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "store")
	b, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "outside.txt"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("good/key", []byte("x")); err != nil {
		t.Fatal(err)
	}

	listDir := func(dir string) string {
		t.Helper()
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, 0, len(ents))
		for _, e := range ents {
			names = append(names, e.Name())
		}
		return strings.Join(names, ",")
	}
	if got := listDir(parent); got != "outside.txt,store" {
		t.Fatalf("test setup: parent holds %q", got)
	}
	if got := listDir(root); got != "good" {
		t.Fatalf("test setup: root holds %q", got)
	}

	refused := func(op, key string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s(%q): accepted, want an invalid-key error", op, key)
		}
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCAS) {
			t.Fatalf("%s(%q): got %v, want the invalid-key refusal, not a lookup/CAS result", op, key, err)
		}
		if !strings.Contains(err.Error(), "invalid key") {
			t.Fatalf("%s(%q): error = %v, want it to say invalid key", op, key, err)
		}
	}

	for _, key := range []string{"/abs", "a/../b", "refs/../x", ".."} {
		_, _, err := b.Get(key)
		refused("Get", key, err)
		refused("Put", key, b.Put(key, []byte("x")))
		_, err = b.PutIf(key, []byte("x"), "")
		refused("PutIf", key, err)
		refused("Delete", key, b.Delete(key))
		refused("CopyObject dst", key, b.CopyObject(key, "good/key"))
		refused("CopyObject src", key, b.CopyObject("good/copy", key))
		keys, err := b.List(key)
		if err != nil {
			t.Fatalf("List(%q): %v", key, err)
		}
		if len(keys) != 0 {
			t.Fatalf("List(%q) = %v, want nothing", key, keys)
		}
	}
	// A directory-style traversal prefix walks nothing outside the root.
	keys, err := b.List("../")
	if err != nil {
		t.Fatalf("List(\"../\"): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("List(\"../\") = %v, want nothing (the parent's outside.txt must never be listed)", keys)
	}

	if got := listDir(parent); got != "outside.txt,store" {
		t.Fatalf("a refused key wrote outside the root: parent now holds %q", got)
	}
	if got := listDir(root); got != "good" {
		t.Fatalf("a refused key wrote inside the root: root now holds %q", got)
	}
	if _, err := os.Stat(filepath.Join(parent, "abs")); !os.IsNotExist(err) {
		t.Fatalf("/abs created something beside the root: %v", err)
	}
}
