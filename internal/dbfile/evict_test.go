package dbfile

import (
	"os"
	"path/filepath"
	"testing"
)

// pinKinds gives each subtest the two ways an inode gets pinned.
var pinKinds = []struct {
	name string
	pin  func(t *testing.T, path string) (unpin func())
}{
	{"reader", func(t *testing.T, path string) func() {
		s, err := Reader(path)
		if err != nil {
			t.Fatal(err)
		}
		return func() { s.Close() }
	}},
	{"hold", func(t *testing.T, path string) func() {
		release, _, err := Hold(path)
		if err != nil {
			t.Fatal(err)
		}
		return release
	}},
}

func strandedPinnedUnder(dir string) int {
	in := under(dir)
	n := 0
	for _, e := range Entries() {
		if e.Orphan && e.Pins > 0 && in(e.Path) {
			n++
		}
	}
	return n
}

func TestEvictStrandedAfterRename(t *testing.T) {
	for _, k := range pinKinds {
		t.Run(k.name, func(t *testing.T) {
			dir := t.TempDir()
			in := under(dir)
			path := filepath.Join(dir, "c.db")
			writeFile(t, path, "old")
			s, _ := Reader(path)
			s.Close()
			e := lookupLive(t, path)
			unpin := k.pin(t, path)

			replaceFile(t, path, "new")
			if n := evictStranded(in); n != 0 {
				t.Fatalf("evictStranded closed %d descriptor(s) on a pinned inode", n)
			}
			if !isOrphan(e) || !isOpen(e) {
				t.Fatal("the sweep did not orphan the renamed-over descriptor, or closed it")
			}
			if n := strandedPinnedUnder(dir); n != 1 {
				t.Fatalf("pinned orphans = %d, want 1", n)
			}
			unpin()
			if n := evictStranded(in); n != 1 {
				t.Fatalf("evictStranded after unpin = %d, want 1", n)
			}
			assertClosed(t, e)
		})
	}
}

func TestEvictStrandedAfterDelete(t *testing.T) {
	for _, k := range pinKinds {
		t.Run(k.name, func(t *testing.T) {
			dir := t.TempDir()
			in := under(dir)
			path := filepath.Join(dir, "c.db")
			writeFile(t, path, "old")
			s, _ := Reader(path)
			s.Close()
			e := lookupLive(t, path)
			unpin := k.pin(t, path)

			// Nothing will ever call Reader on a destroyed checkout's path
			// again, so only the sweep can find this one.
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if n := evictStranded(in); n != 0 {
				t.Fatalf("closed %d pinned descriptor(s)", n)
			}
			if !isOrphan(e) || !isOpen(e) {
				t.Fatal("the sweep did not orphan the deleted path's descriptor, or closed it")
			}
			unpin()
			if n := evictStranded(in); n != 1 {
				t.Fatalf("evictStranded after unpin = %d, want 1", n)
			}
			assertClosed(t, e)
		})
	}
}

// TestPinnedOrphanIsReportedNotClosed: a Section its caller never closes is
// a pin leak. It must show up in ReadStats and never be closed.
func TestPinnedOrphanIsReportedNotClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.db")
	writeFile(t, path, "x")
	leaked, _ := Reader(path)
	e := lookupLive(t, path)
	os.Remove(path)
	for i := 0; i < 3; i++ {
		if n := evictStranded(under(dir)); n != 0 {
			t.Fatalf("pass %d closed a pinned orphan", i)
		}
	}
	if n := strandedPinnedUnder(dir); n != 1 {
		t.Fatalf("pinned orphans under the test dir = %d, want 1", n)
	}
	if ReadStats().StrandedPinned < 1 {
		t.Fatal("ReadStats().StrandedPinned does not count the leaked pin")
	}
	if !isOpen(e) {
		t.Fatal("pinned orphan was closed")
	}
	leaked.Close()
	if n := evictStranded(under(dir)); n != 1 {
		t.Fatalf("after Close: evictStranded = %d, want 1", n)
	}
}

func TestReadStatsCountsDescriptors(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
	writeFile(t, a, "a")
	writeFile(t, b, "b")
	before := ReadStats()
	sa, _ := Reader(a)
	sb, _ := Reader(b)
	defer sb.Close()
	sa.Close()
	os.Remove(a)
	sweep(under(dir))
	got := ReadStats()
	if got.Cached != before.Cached+1 || got.Orphaned != before.Orphaned+1 {
		t.Fatalf("Cached/Orphaned = %d/%d, want %d/%d", got.Cached, got.Orphaned, before.Cached+1, before.Orphaned+1)
	}
	if got.Descriptors() != got.Cached+got.Orphaned+got.Unidentified {
		t.Fatal("Descriptors() is not cached + orphaned + unidentified")
	}
	if got.Pins != before.Pins+1 {
		t.Fatalf("Pins = %d, want %d", got.Pins, before.Pins+1)
	}
	evictStranded(under(dir))
}
