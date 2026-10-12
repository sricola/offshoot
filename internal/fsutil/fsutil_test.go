package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWriteFileAtomicReplacesWholeFileAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.sum")
	if err := WriteFileAtomic(p, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(p, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(p)
	if err != nil || string(got) != "two" {
		t.Fatalf("content %q err %v", got, err)
	}
	fi, _ := os.Stat(p)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, want 0600", fi.Mode().Perm())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("temp file left behind: %v", entries)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir on a real directory: %v", err)
	}
	if err := SyncDir(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("SyncDir on a missing directory must report it")
	}
}

// TestSyncDirReportsARealFsyncFailureButNotAnUnsupportedOne: EIO from the
// directory fsync reaches the caller (a durable write must not be reported
// durable when the name may not be on disk), while an errno that means
// "this filesystem does not fsync directories" is not an error.
func TestSyncDirReportsARealFsyncFailureButNotAnUnsupportedOne(t *testing.T) {
	dir := t.TempDir()
	orig := fsync
	t.Cleanup(func() { fsync = orig })

	fsync = func(*os.File) error { return syscall.EIO }
	if err := SyncDir(dir); !errors.Is(err, syscall.EIO) {
		t.Fatalf("SyncDir with a failing fsync: err = %v, want EIO", err)
	}
	if err := WriteFileAtomic(filepath.Join(dir, "f"), []byte("x"), 0o600); !errors.Is(err, syscall.EIO) {
		t.Fatalf("WriteFileAtomic with a failing directory fsync: err = %v, want EIO", err)
	}
	if err := ReplaceFileAtomic(filepath.Join(dir, "g"), []byte("x"), 0o600); err != nil {
		t.Fatalf("ReplaceFileAtomic does not sync, so it must not see the failure: %v", err)
	}

	fsync = func(*os.File) error { return syscall.EINVAL }
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir on a filesystem that rejects directory fsync: %v", err)
	}
}
