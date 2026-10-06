package store

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dirExists reports whether rel (slash-separated, relative to root) is a
// directory on disk.
func dirExists(t *testing.T, root, rel string) bool {
	t.Helper()
	fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return fi.IsDir()
}

func newLocalAt(t *testing.T) (*Local, string) {
	t.Helper()
	root := t.TempDir()
	l, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	return l, root
}

// TestDeleteRemovesEmptiedEpochAndLineageDirectories: a delete that empties
// an epoch directory removes it, and the lineage directory too once that is
// empty, but never data/ or the store root. Every at-rest checkpoint mints
// an epoch, and List walks every directory under a lineage, so directories
// left behind by GC'd objects would make listing cost grow without bound.
func TestDeleteRemovesEmptiedEpochAndLineageDirectories(t *testing.T) {
	l, root := newLocalAt(t)
	a := SnapshotKey("lin", 1, 1)
	b := SnapshotKey("lin", 2, 5)
	for _, k := range []string{a, b} {
		if _, err := l.PutIf(k, []byte("x"), ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Delete(a); err != nil {
		t.Fatal(err)
	}
	if dirExists(t, root, "data/lin/1") {
		t.Fatal("emptied epoch directory data/lin/1 survived the delete")
	}
	if !dirExists(t, root, "data/lin/2") || !dirExists(t, root, "data/lin") {
		t.Fatal("a non-empty epoch or its lineage directory was removed")
	}
	if err := l.Delete(b); err != nil {
		t.Fatal(err)
	}
	if dirExists(t, root, "data/lin/2") || dirExists(t, root, "data/lin") {
		t.Fatal("emptied epoch or lineage directory survived the last delete")
	}
	if !dirExists(t, root, "data") || !dirExists(t, root, ".") {
		t.Fatal("data/ or the store root was removed")
	}
}

// TestDeleteLeavesNonEmptyDirectories: a directory that still holds an
// object stays.
func TestDeleteLeavesNonEmptyDirectories(t *testing.T) {
	l, root := newLocalAt(t)
	a := SnapshotKey("lin", 1, 1)
	b := SegmentKey("lin", 1, 2, 3)
	for _, k := range []string{a, b} {
		if err := l.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Delete(a); err != nil {
		t.Fatal(err)
	}
	if !dirExists(t, root, "data/lin/1") {
		t.Fatal("epoch directory removed while it still held an object")
	}
	if keys, err := l.List("data/lin/"); err != nil || len(keys) != 1 || keys[0] != b {
		t.Fatalf("List = %v, %v; want [%s]", keys, err, b)
	}
}

// TestDeleteKeepsRefsAndTopLevelDirectories: refs/ and data/ survive a
// delete that empties them, as does the store root; emptied directories
// below them go.
func TestDeleteKeepsRefsAndTopLevelDirectories(t *testing.T) {
	l, root := newLocalAt(t)
	for _, k := range []string{"refs/db/feature/x", "data/lin/base.json", "top"} {
		if err := l.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := l.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if dirExists(t, root, "refs/db") || dirExists(t, root, "data/lin") {
		t.Fatal("emptied directories below refs/ or data/ survived")
	}
	if !dirExists(t, root, "refs") || !dirExists(t, root, "data") || !dirExists(t, root, ".") {
		t.Fatal("refs/, data/ or the root was removed")
	}
}

// TestDeleteIfRemovesEmptiedDirectories: the conditional delete tidies the
// same way, after releasing its lock (the lock file lives in the very
// directory it would remove).
func TestDeleteIfRemovesEmptiedDirectories(t *testing.T) {
	l, root := newLocalAt(t)
	k := SnapshotKey("lin", 3, 1)
	etag, err := l.PutIf(k, []byte("x"), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteIf(k, "wrong"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS, got %v", err)
	}
	if !dirExists(t, root, "data/lin/3") {
		t.Fatal("a failed DeleteIf removed the directory")
	}
	if err := l.DeleteIf(k, etag); err != nil {
		t.Fatal(err)
	}
	if dirExists(t, root, "data/lin/3") || dirExists(t, root, "data/lin") {
		t.Fatal("DeleteIf left the directories it emptied")
	}
}

// TestDeleteObjectsRemovesEmptiedDirectories: the batch path (GC's sweep)
// tidies too.
func TestDeleteObjectsRemovesEmptiedDirectories(t *testing.T) {
	l, root := newLocalAt(t)
	keys := []string{SnapshotKey("lin", 1, 1), SegmentKey("lin", 1, 2, 2), SnapshotKey("lin", 2, 3)}
	for _, k := range keys {
		if err := l.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := l.DeleteObjects(keys); err != nil {
		t.Fatal(err)
	}
	if dirExists(t, root, "data/lin") {
		t.Fatal("batch delete left the emptied lineage directory")
	}
}

// TestDeleteKeepsDirectoryHoldingALiveLock: a held per-key lock lives in
// the object's directory, so the directory is not empty and survives; once
// the lock is released, the next delete into it removes it.
func TestDeleteKeepsDirectoryHoldingALiveLock(t *testing.T) {
	l, root := newLocalAt(t)
	a := SnapshotKey("lin", 1, 1)
	if err := l.Put(a, []byte("x")); err != nil {
		t.Fatal(err)
	}
	p, err := l.path(SegmentKey("lin", 1, 2, 2))
	if err != nil {
		t.Fatal(err)
	}
	release, err := l.lock(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Delete(a); err != nil {
		release()
		t.Fatal(err)
	}
	if !dirExists(t, root, "data/lin/1") {
		release()
		t.Fatal("directory holding a live lock was removed")
	}
	release()
	b := SnapshotKey("lin", 1, 9)
	if err := l.Put(b, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.Delete(b); err != nil {
		t.Fatal(err)
	}
	if dirExists(t, root, "data/lin/1") {
		t.Fatal("directory survived the next delete after the lock was released")
	}
}

// TestPutIfRacingDirectoryRemovalSucceeds: a PutIf into the epoch directory
// that lands between a delete's unlink and its directory removal makes the
// directory non-empty, so the removal fails harmlessly and both succeed.
func TestPutIfRacingDirectoryRemovalSucceeds(t *testing.T) {
	l, root := newLocalAt(t)
	old := SnapshotKey("lin", 1, 1)
	racer := SegmentKey("lin", 1, 2, 2)
	if err := l.Put(old, []byte("x")); err != nil {
		t.Fatal(err)
	}
	fired := false
	var putErr error
	removeEmptyParentsHook = func() {
		if fired {
			return
		}
		fired = true
		_, putErr = l.PutIf(racer, []byte("y"), "")
	}
	t.Cleanup(func() { removeEmptyParentsHook = nil })
	if err := l.Delete(old); err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("removeEmptyParentsHook never ran")
	}
	if putErr != nil {
		t.Fatalf("racing PutIf: %v", putErr)
	}
	if !dirExists(t, root, "data/lin/1") {
		t.Fatal("directory removed under a racing PutIf")
	}
	keys, err := l.List("data/lin/")
	if err != nil || len(keys) != 1 || keys[0] != racer {
		t.Fatalf("List = %v, %v; want [%s]", keys, err, racer)
	}
	if data, _, err := l.Get(racer); err != nil || string(data) != "y" {
		t.Fatalf("Get racer = %q, %v", data, err)
	}
}

// TestPutRetriesOnceWhenItsDirectoryVanishes: a delete in another process
// can empty and remove a directory between a writer's MkdirAll and its
// create. Every writer re-creates the directory and retries once; a
// directory that keeps vanishing still errors.
func TestPutRetriesOnceWhenItsDirectoryVanishes(t *testing.T) {
	l, _ := newLocalAt(t)
	vanishOnce := func() {
		done := false
		afterMkdirHook = func(dir string) {
			if done {
				return
			}
			done = true
			if err := os.Remove(dir); err != nil {
				t.Errorf("hook remove %s: %v", dir, err)
			}
		}
	}
	t.Cleanup(func() { afterMkdirHook = nil })

	if err := l.Put("src", []byte("x")); err != nil {
		t.Fatal(err)
	}
	writers := []struct {
		name string
		put  func(key string) error
	}{
		{"Put", func(k string) error { return l.Put(k, []byte("x")) }},
		{"PutIf", func(k string) error {
			_, err := l.PutIf(k, []byte("x"), "")
			return err
		}},
		{"PutReader", func(k string) error { return l.PutReader(k, strings.NewReader("x"), 1) }},
		{"PutReaderIf", func(k string) error {
			_, err := l.PutReaderIf(k, strings.NewReader("x"), 1, "")
			return err
		}},
		{"CopyObject", func(k string) error { return l.CopyObject(k, "src") }},
	}
	for i, w := range writers {
		k := SnapshotKey("lin", uint64(i+1), 1)
		vanishOnce()
		if err := w.put(k); err != nil {
			t.Fatalf("%s after its directory vanished once: %v", w.name, err)
		}
		if data, _, err := l.Get(k); err != nil || string(data) != "x" {
			t.Fatalf("%s: Get = %q, %v", w.name, data, err)
		}
	}

	afterMkdirHook = func(dir string) { os.Remove(dir) }
	if err := l.Put(SnapshotKey("lin", 99, 1), []byte("x")); err == nil {
		t.Fatal("Put succeeded although its directory vanished on every attempt")
	}
	afterMkdirHook = nil
	if keys, err := l.List("data/lin/99/"); err != nil || len(keys) != 0 {
		t.Fatalf("List after failed put = %v, %v", keys, err)
	}
}

// TestListAfterDirectoryRemovalIsCorrect: List of a lineage, of data/ and
// of removed directories answer exactly the surviving keys.
func TestListAfterDirectoryRemovalIsCorrect(t *testing.T) {
	l, _ := newLocalAt(t)
	all := []string{
		SnapshotKey("a", 1, 1), SegmentKey("a", 1, 2, 3), SnapshotKey("a", 2, 4),
		SnapshotKey("a", 3, 5), SnapshotKey("b", 1, 1),
	}
	for _, k := range all {
		if err := l.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{all[0], all[1], all[3], all[4]} {
		if err := l.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if keys, err := l.List("data/a/"); err != nil || len(keys) != 1 || keys[0] != all[2] {
		t.Fatalf("List(data/a/) = %v, %v; want [%s]", keys, err, all[2])
	}
	if keys, err := l.List("data/"); err != nil || len(keys) != 1 || keys[0] != all[2] {
		t.Fatalf("List(data/) = %v, %v", keys, err)
	}
	if keys, err := l.List("data/b/"); err != nil || len(keys) != 0 {
		t.Fatalf("List(data/b/) = %v, %v; want empty", keys, err)
	}
	if keys, err := l.List("data/a/1/"); err != nil || len(keys) != 0 {
		t.Fatalf("List(data/a/1/) = %v, %v; want empty", keys, err)
	}
}
