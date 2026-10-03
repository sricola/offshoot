package dbfile

import (
	"os"
	"path/filepath"
	"strings"
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
		if e.Orphan && (e.Pins > 0 || e.Held > 0) && in(e.Path) {
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

// TestOrphanKeptByAHeldPathIsReported: while a Hold is active on a path,
// nothing cached under it is closed, whatever inode it names (see the
// package doc's Pins). So a path re-materialized twice under one Hold (a
// session open across both, with a read caching the middle file) keeps an
// orphan open on an inode nothing pins. It must be reported with the
// pinned ones, in ReadStats and Entries, or its descriptor and the
// unlinked file's disk would show up only in the aggregate count.
func TestOrphanKeptByAHeldPathIsReported(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	path := filepath.Join(dir, "c.db")
	writeFile(t, path, "a")
	release, _, err := Hold(path)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	replaceFile(t, path, "x")
	s, err := Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	e := lookupLive(t, path)
	before := ReadStats().StrandedPinned
	replaceFile(t, path, "y")
	if n := evictStranded(in); n != 0 {
		t.Fatalf("evictStranded closed %d descriptor(s) under a held path", n)
	}
	if !isOrphan(e) || !isOpen(e) || pinsOf(e.ino) != 0 {
		t.Fatal("precondition: want the middle file's descriptor an open, unpinned orphan")
	}
	abs, _ := filepath.Abs(path)
	var info *EntryInfo
	for _, ei := range Entries() {
		if ei.Path == abs && ei.Orphan && ei.Inode == e.ino {
			info = &ei
		}
	}
	if info == nil || info.Held != 1 {
		t.Fatalf("Entries() = %+v for the kept orphan, want Held 1", info)
	}
	if n := strandedPinnedUnder(dir); n != 1 {
		t.Fatalf("stranded and kept open under the test dir = %d, want 1", n)
	}
	if d := ReadStats().StrandedPinned - before; d != 1 {
		t.Fatalf("ReadStats().StrandedPinned rose by %d for the held path's orphan, want 1", d)
	}
	release()
	if n := evictStranded(in); n != 1 {
		t.Fatalf("evictStranded after release = %d, want 1", n)
	}
	assertClosed(t, e)
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

func TestEvictHonorsBudgetLRU(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	paths := map[string]string{}
	for _, n := range []string{"a", "b", "c", "d"} {
		paths[n] = filepath.Join(dir, n+".db")
		writeFile(t, paths[n], n)
	}
	touch := func(n string) {
		s, err := Reader(paths[n])
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	for _, n := range []string{"a", "b", "c", "d", "a"} { // a is touched again last
		touch(n)
	}
	es := map[string]*entry{}
	for n, p := range paths {
		es[n] = lookupLive(t, p)
	}
	if got := evict(2, in); got != 2 {
		t.Fatalf("evict(2) closed %d, want 2", got)
	}
	for _, n := range []string{"d", "a"} {
		if !isLive(es[n]) || !isOpen(es[n]) {
			t.Fatalf("%s (recently used) was evicted", n)
		}
	}
	for _, n := range []string{"b", "c"} {
		assertClosed(t, es[n])
	}
	evict(0, in)
}

func TestEvictSkipsInFlightReader(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	path := filepath.Join(dir, "big.db")
	writeFile(t, path, strings.Repeat("x", 1<<20))
	s, err := Reader(path)
	if err != nil {
		t.Fatal(err)
	}
	e := lookupLive(t, path)
	half := make([]byte, 512<<10)
	if _, err := s.ReadAt(half, 0); err != nil {
		t.Fatal(err)
	}
	if n := evict(0, in); n != 0 {
		t.Fatalf("evict(0) closed %d descriptor(s) under an in-flight read", n)
	}
	if _, err := s.ReadAt(half, 512<<10); err != nil {
		t.Fatalf("read after eviction pass: %v", err)
	}
	s.Close()
	if n := evict(0, in); n != 1 {
		t.Fatalf("evict(0) after Close = %d, want 1", n)
	}
	assertClosed(t, e)
}

// TestPinProtectsEveryPathToTheInode: pins belong to the inode. A Hold
// taken through a hard link must protect the descriptor cached under the
// original name.
func TestPinProtectsEveryPathToTheInode(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	path := filepath.Join(dir, "c.db")
	link := filepath.Join(dir, "alias.db")
	writeFile(t, path, "x")
	if err := os.Link(path, link); err != nil {
		t.Fatal(err)
	}
	s, _ := Reader(path)
	s.Close()
	e := lookupLive(t, path)
	release, _, err := Hold(link)
	if err != nil {
		t.Fatal(err)
	}
	if n := evict(0, in); n != 0 {
		t.Fatalf("evict(0) closed %d descriptor(s) on an inode held through another path", n)
	}
	if !isOpen(e) {
		t.Fatal("descriptor closed")
	}
	release()
	evict(0, in)
	assertClosed(t, e)
}

// TestEvictStopsAtPinnedEntries: a budget smaller than the number of pinned
// descriptors (more open sessions than -fd-budget) closes every unpinned
// one and no pinned one.
func TestEvictStopsAtPinnedEntries(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	var ps []string
	for _, n := range []string{"a", "b", "c"} {
		p := filepath.Join(dir, n+".db")
		writeFile(t, p, n)
		ps = append(ps, p)
	}
	pinned, _ := Reader(ps[0]) // oldest, and pinned
	for _, p := range ps[1:] {
		s, _ := Reader(p)
		s.Close()
	}
	a := lookupLive(t, ps[0])
	if n := evict(0, in); n != 2 {
		t.Fatalf("evict(0) = %d, want 2 (both unpinned)", n)
	}
	if !isLive(a) || !isOpen(a) {
		t.Fatal("pinned descriptor was evicted")
	}
	if n := evict(0, in); n != 0 {
		t.Fatalf("second pass closed %d", n)
	}
	pinned.Close()
	evict(0, in)
	assertClosed(t, a)
}

func TestEvictUnderScopesToDir(t *testing.T) {
	mine, theirs := t.TempDir(), t.TempDir()
	pm, pt := filepath.Join(mine, "m.db"), filepath.Join(theirs, "t.db")
	writeFile(t, pm, "m")
	writeFile(t, pt, "t")
	for _, p := range []string{pm, pt} {
		s, _ := Reader(p)
		s.Close()
	}
	et := lookupLive(t, pt)
	if n := EvictUnder(mine); n != 1 {
		t.Fatalf("EvictUnder = %d, want 1", n)
	}
	if !isLive(et) || !isOpen(et) {
		t.Fatal("EvictUnder closed a descriptor outside its directory")
	}
	EvictUnder(theirs)
}

// TestEvictStrandedAt: the re-check stats only the cached paths at or under
// the named files and directories, so its cost does not grow with every
// checkout the process has cached. Every unpinned orphan is still closed,
// wherever it is: one that was pinned when its own path was reclaimed is
// closed by whichever pass comes next.
func TestEvictStrandedAt(t *testing.T) {
	dir := t.TempDir()
	named := filepath.Join(dir, "named")
	other := filepath.Join(dir, "other")
	for _, d := range []string{named, other} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(dir, "file.db")
	inNamed := filepath.Join(named, "a.db")
	outside := filepath.Join(other, "b.db")
	sibling := filepath.Join(dir, "named-sibling.db") // named's prefix as a string, not as a directory
	early := filepath.Join(other, "early.db")
	entries := map[string]*entry{}
	for _, p := range []string{file, inNamed, outside, sibling, early} {
		writeFile(t, p, "x")
		s, err := Reader(p)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
		entries[p] = lookupLive(t, p)
	}
	t.Cleanup(func() { evictStranded(under(dir)) })

	// early is orphaned before the pass, outside the named paths.
	if err := os.Remove(early); err != nil {
		t.Fatal(err)
	}
	if _, err := Reader(early); !os.IsNotExist(err) {
		t.Fatalf("Reader on a removed path = %v, want IsNotExist", err)
	}
	if !isOrphan(entries[early]) {
		t.Fatal("precondition: Reader did not orphan the removed path's descriptor")
	}
	for _, p := range []string{file, inNamed, outside, sibling} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}

	if n := EvictStrandedAt(named, file); n < 3 {
		t.Fatalf("EvictStrandedAt closed %d descriptor(s), want at least 3", n)
	}
	for _, p := range []string{file, inNamed, early} {
		assertClosed(t, entries[p])
	}
	for _, p := range []string{outside, sibling} {
		if e := entries[p]; !isLive(e) || !isOpen(e) {
			t.Fatalf("%s is outside the named paths, so this pass must not have re-checked it", p)
		}
	}
}
