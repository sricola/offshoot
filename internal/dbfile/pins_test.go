package dbfile

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestReaderPinsUntilClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	writeFile(t, path, "content")
	s, err := Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := PinsAt(path); n != 1 {
		t.Fatalf("pins with one open Section = %d, want 1", n)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n, _ := PinsAt(path); n != 0 {
		t.Fatalf("pins after Close = %d, want 0", n)
	}
	if !isOpen(lookupLive(t, path)) {
		t.Fatal("Section.Close closed the cached descriptor; it must only unpin")
	}
}

// TestSectionCloseIsIdempotent: a deferred Close plus an explicit one must
// not consume a pin another Section holds on the same inode.
func TestSectionCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	writeFile(t, path, "content")
	a, _ := Reader(path)
	b, _ := Reader(path)
	a.Close()
	a.Close()
	if n, _ := PinsAt(path); n != 1 {
		t.Fatalf("pins after closing one of two Sections twice = %d, want 1", n)
	}
	b.Close()
	if n, _ := PinsAt(path); n != 0 {
		t.Fatalf("pins = %d, want 0", n)
	}
}

// TestReaderOrphansAReplacedFile: the old descriptor stays referenced (and
// open) as an orphan instead of being dropped to the *os.File finalizer,
// which would close it at an arbitrary GC whether or not anything relied on
// its inode's locks.
func TestReaderOrphansAReplacedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	writeFile(t, path, "first")
	s, _ := Reader(path)
	s.Close()
	old := lookupLive(t, path)

	replaceFile(t, path, "second-and-longer")
	s, err := Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, _ := io.ReadAll(s)
	if string(got) != "second-and-longer" {
		t.Fatalf("got %q", got)
	}
	if !isOrphan(old) || !isOpen(old) {
		t.Fatal("replaced file's descriptor was not kept as an open orphan")
	}
}

func TestReaderOrphansADeletedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	writeFile(t, path, "first")
	s, _ := Reader(path)
	s.Close()
	old := lookupLive(t, path)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Reader(path); !os.IsNotExist(err) {
		t.Fatalf("Reader on a deleted path = %v, want IsNotExist", err)
	}
	if !isOrphan(old) || !isOpen(old) {
		t.Fatal("deleted file's descriptor was not kept as an open orphan")
	}
}
