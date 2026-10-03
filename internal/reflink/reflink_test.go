package reflink

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// writeSrc creates a source file of n pseudo-random bytes in dir and returns
// its path and content.
func writeSrc(t *testing.T, dir string, name string, n int) (path string, content []byte) {
	t.Helper()
	content = make([]byte, n)
	if n > 0 {
		if _, err := rand.Read(content); err != nil {
			t.Fatal(err)
		}
	}
	path = filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, content
}

// TestCopyFileCorrectness checks CopyFile produces byte-identical output at
// several sizes, including empty and larger than one page (4096 bytes) —
// the sizes most likely to expose an off-by-one in either the clone path or
// the plain-copy fallback.
func TestCopyFileCorrectness(t *testing.T) {
	for _, n := range []int{0, 1, 100, 4096, 4097, 65536, 1<<20 + 37} {
		n := n
		t.Run(sizeLabel(n), func(t *testing.T) {
			dir := t.TempDir()
			src, want := writeSrc(t, dir, "src", n)
			dst := filepath.Join(dir, "dst")
			if _, err := CopyFile(dst, src); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("copied content mismatch at size %d", n)
			}
		})
	}
}

func sizeLabel(n int) string {
	if n == 0 {
		return "empty"
	}
	return "n=" + strconv.Itoa(n)
}

// TestCopyFileClonedFlagOnClonableFS probes whether this machine's temp
// filesystem actually supports clones by attempting one; if it doesn't (CI
// runners on ext4, tmpfs, etc.), this test skips loudly rather than failing,
// since "cloned=true" is a filesystem capability, not something CopyFile can
// guarantee everywhere. Where clones ARE supported (APFS on darwin is the
// common case), it asserts cloned=true and the content still matches.
func TestCopyFileClonedFlagOnClonableFS(t *testing.T) {
	dir := t.TempDir()
	probeSrc, _ := writeSrc(t, dir, "probe-src", 16)
	probeDst := filepath.Join(dir, "probe-dst")
	if !cloneFile(probeDst, probeSrc) {
		t.Skip("temp filesystem does not support reflink/clonefile; skipping cloned-flag assertion")
	}
	os.Remove(probeDst)

	src, want := writeSrc(t, dir, "src", 1<<20)
	dst := filepath.Join(dir, "dst")
	cloned, err := CopyFile(dst, src)
	if err != nil {
		t.Fatal(err)
	}
	if !cloned {
		t.Fatal("filesystem supports clones (probe succeeded) but CopyFile reported cloned=false")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("cloned content mismatch")
	}
}

// TestCopyFileForcedFallbackCorrectness forces CopyFile down the plain-copy
// path via forceFallbackForTest, regardless of what the underlying
// filesystem supports, and checks both the reported cloned flag and the
// content.
func TestCopyFileForcedFallbackCorrectness(t *testing.T) {
	orig := forceFallbackForTest
	forceFallbackForTest = true
	defer func() { forceFallbackForTest = orig }()

	dir := t.TempDir()
	src, want := writeSrc(t, dir, "src", 1<<18+13)
	dst := filepath.Join(dir, "dst")
	cloned, err := CopyFile(dst, src)
	if err != nil {
		t.Fatal(err)
	}
	if cloned {
		t.Fatal("forceFallbackForTest set but CopyFile reported cloned=true")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("fallback copy content mismatch")
	}
}

// TestCopyPlainDirectly exercises the fallback function directly (not
// through CopyFile), the brief's "build the fallback as a separate function
// tested directly" option, at several sizes including empty.
func TestCopyPlainDirectly(t *testing.T) {
	for _, n := range []int{0, 1, 5000} {
		n := n
		t.Run(sizeLabel(n), func(t *testing.T) {
			dir := t.TempDir()
			src, want := writeSrc(t, dir, "src", n)
			dst := filepath.Join(dir, "dst")
			if err := copyPlain(dst, src); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("copyPlain content mismatch at size %d", n)
			}
		})
	}
}

// TestCopyFileMissingSrcErrors checks that a missing source surfaces as a
// real error (via the fallback, since a clone attempt against a missing
// source also fails and falls through) rather than silently producing an
// empty or absent destination.
func TestCopyFileMissingSrcErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := CopyFile(filepath.Join(dir, "dst"), filepath.Join(dir, "does-not-exist")); err == nil {
		t.Fatal("want an error copying a missing source")
	}
}

// TestCopyFileExistingDstErrors checks CopyFile's create-only contract: it
// must refuse to overwrite an existing destination, on every path (clone or
// fallback).
func TestCopyFileExistingDstErrors(t *testing.T) {
	dir := t.TempDir()
	src, _ := writeSrc(t, dir, "src", 10)
	dst := filepath.Join(dir, "dst")
	if err := os.WriteFile(dst, []byte("already here"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := CopyFile(dst, src); err == nil {
		t.Fatal("want an error copying onto an existing destination")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "already here" {
		t.Fatal("a rejected copy must not modify the existing destination")
	}
}

// TestCloneProducesIdenticalFile: on a clone-capable temp filesystem Clone
// succeeds and both files read identical; elsewhere it reports
// ErrUnsupported and leaves no dst.
//
// src is read-only (0444, like a by-chain cache entry): a clone that
// inherits that mode must still be fsynced and succeed.
func TestCloneProducesIdenticalFile(t *testing.T) {
	dir := t.TempDir()
	src, want := writeSrc(t, dir, "src", 1<<20+5)
	if err := os.Chmod(src, 0o444); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "dst")
	err := Clone(dst, src)
	if errors.Is(err, ErrUnsupported) {
		if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
			t.Fatalf("unsupported clone left dst behind: %v", statErr)
		}
		t.Skip("temp filesystem does not support reflink/clonefile")
	}
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("cloned content mismatch")
	}
}

// TestCloneForcedUnsupported: the test hook forces ErrUnsupported and no dst
// is created — Clone never falls back to a byte copy.
func TestCloneForcedUnsupported(t *testing.T) {
	orig := forceUnsupportedForTest
	forceUnsupportedForTest = true
	defer func() { forceUnsupportedForTest = orig }()

	dir := t.TempDir()
	src, _ := writeSrc(t, dir, "src", 100)
	dst := filepath.Join(dir, "dst")
	if err := Clone(dst, src); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Clone = %v, want ErrUnsupported", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("forced-unsupported Clone created dst: %v", err)
	}
}

// TestCloneMissingSrcIsNotUnsupported: a missing source is a real error, not
// a capability verdict a caller would silently degrade on.
func TestCloneMissingSrcIsNotUnsupported(t *testing.T) {
	dir := t.TempDir()
	err := Clone(filepath.Join(dir, "dst"), filepath.Join(dir, "missing"))
	if err == nil || errors.Is(err, ErrUnsupported) {
		t.Fatalf("Clone of a missing src = %v, want a non-ErrUnsupported error", err)
	}
}

// TestCloneFromLeavesSourceOpen: CloneFrom clones through the caller's
// descriptor and never closes it, the property that makes it safe on a
// file whose POSIX locks this process relies on (see Clone).
func TestCloneFromLeavesSourceOpen(t *testing.T) {
	dir := t.TempDir()
	srcPath, want := writeSrc(t, dir, "src", 1<<20+5)
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst := filepath.Join(dir, "dst")
	err = CloneFrom(dst, src)
	if _, statErr := src.Stat(); statErr != nil {
		t.Fatalf("CloneFrom closed its source descriptor: %v", statErr)
	}
	if errors.Is(err, ErrUnsupported) {
		if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
			t.Fatalf("unsupported clone left dst behind: %v", statErr)
		}
		t.Skip("temp filesystem does not support reflink/clonefile")
	}
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("cloned content mismatch")
	}
	if err := CloneFrom(dst, src); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CloneFrom onto an existing dst = %v, want ErrUnsupported (create-only)", err)
	}
}

func TestCloneFromForcedUnsupported(t *testing.T) {
	orig := forceUnsupportedForTest
	forceUnsupportedForTest = true
	defer func() { forceUnsupportedForTest = orig }()

	dir := t.TempDir()
	srcPath, _ := writeSrc(t, dir, "src", 100)
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst := filepath.Join(dir, "dst")
	if err := CloneFrom(dst, src); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CloneFrom = %v, want ErrUnsupported", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("forced-unsupported CloneFrom created dst: %v", err)
	}
}
