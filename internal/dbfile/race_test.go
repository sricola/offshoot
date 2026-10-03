package dbfile

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestEvictRacesHandle runs readers, re-materializations, holds and both
// evictors against one path at once. Under -race it checks the registry's
// locking. Without -race it still checks the property that matters: a read
// through a pinned Section never fails, which would happen if an evictor
// closed its descriptor mid-read.
func TestEvictRacesHandle(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	path := filepath.Join(dir, "r.db")
	const size = 64 << 10
	fill := func(b byte) []byte { return []byte(strings.Repeat(string(b), size)) }
	if err := os.WriteFile(path, fill('a'), 0o644); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	loop := func(f func(i int) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := f(i); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	loop(func(int) error { // reader: rename is atomic, so the path always exists
		s, err := Reader(path)
		if err != nil {
			return err
		}
		defer s.Close()
		b, err := io.ReadAll(s)
		if err != nil {
			return fmt.Errorf("read through a pinned Section: %w", err)
		}
		if len(b) != size || strings.Count(string(b), string(b[:1])) != size {
			return fmt.Errorf("torn read: %d bytes", len(b))
		}
		return nil
	})
	loop(func(i int) error { // re-materializer
		tmp := filepath.Join(dir, fmt.Sprintf("tmp-%d", i))
		if err := os.WriteFile(tmp, fill(byte('a'+i%26)), 0o644); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	})
	loop(func(int) error {
		release, _, err := Hold(path)
		if err != nil {
			return err
		}
		release()
		return nil
	})
	loop(func(int) error {
		evictStranded(in)
		evict(0, in)
		return nil
	})

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	evictStranded(in)
	evict(0, in)
	for _, e := range Entries() {
		if in(e.Path) {
			t.Errorf("still registered after a final eviction with nothing pinned: %+v", e)
		}
	}
}

// TestEveryCloseIsMarkedAndUnlocked checks the package doc's rule for
// closes: each is decided under mu, which drops the entry from the registry
// and marks its path and inode closing, and runs with mu released.
// TestEvictRacesHandle cannot see a violation: its holds open no
// connection. An unmarked close could land after a Hold, its sql.Open and
// that connection's first lock, and drop the lock; a close under mu stalls
// every Reader and Hold in the process (TestCloseRunsOutsideTheRegistryLock).
// Every close goes through closeMarked (the source check below), and at
// each one mu must be free and the marks in place. Nothing else in this
// binary runs concurrently with it.
func TestEveryCloseIsMarkedAndUnlocked(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
	for _, p := range []string{a, b} {
		writeFile(t, p, "x")
		s, err := Reader(p)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	replaceFile(t, a, "y") // the next sweep orphans a's descriptor
	var closes, locked, unmarked, registered int
	closeHookForTest = func(e *entry) {
		closes++
		if !mu.TryLock() {
			locked++
			return
		}
		if closingPath[e.path] == 0 || closingIno[e.ino] == 0 {
			unmarked++
		}
		if live[e.path] == e {
			registered++
		}
		for _, o := range orphans[e.ino] {
			if o == e {
				registered++
			}
		}
		mu.Unlock()
	}
	t.Cleanup(func() { closeHookForTest = nil })
	evictStranded(in) // closeOrphans closes a's old descriptor
	evict(0, in)      // evict closes b's
	if closes != 2 {
		t.Fatalf("closes seen = %d, want 2 (an orphan and a cached descriptor)", closes)
	}
	if locked != 0 {
		t.Fatalf("%d descriptor(s) closed with the registry lock held", locked)
	}
	if unmarked != 0 || registered != 0 {
		t.Fatalf("at close: %d not marked closing, %d still registered", unmarked, registered)
	}
	mu.Lock()
	leftover := len(closingPath) + len(closingIno)
	mu.Unlock()
	if leftover != 0 {
		t.Fatalf("%d closing mark(s) left after the closes returned", leftover)
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Name.Name == "closeMarked" {
				continue
			}
			ast.Inspect(fd, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Close" {
					if x, ok := sel.X.(*ast.SelectorExpr); ok && x.Sel.Name == "f" {
						t.Errorf("%s: %s closes a registry descriptor directly; go through closeMarked",
							fset.Position(call.Pos()), fd.Name.Name)
					}
				}
				return true
			})
		}
	}
}

// TestCloseRunsOutsideTheRegistryLock: the last close of an unlinked
// database frees its blocks inside close(2), tens of milliseconds per GiB on
// ext4. If that ran under the registry lock, every Reader, Hold, release and
// ReadStats in the process (session flushes, engine starts, the status op,
// /metrics) would wait behind it. Only a Hold on the closing descriptor's
// path must wait: its connection could otherwise take a lock on that inode
// just before the close drops it.
func TestCloseRunsOutsideTheRegistryLock(t *testing.T) {
	dir := t.TempDir()
	in := under(dir)
	victim, other := filepath.Join(dir, "victim.db"), filepath.Join(dir, "other.db")
	for _, p := range []string{victim, other} {
		writeFile(t, p, "x")
		s, err := Reader(p)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	replaceFile(t, victim, "y") // the next sweep orphans victim's descriptor
	stuck := lookupLive(t, victim)

	inClose, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(finish) }) }
	t.Cleanup(func() { unblock(); closeHookForTest = nil })
	closeHookForTest = func(e *entry) {
		if e == stuck {
			close(inClose)
			<-finish // a slow close of a large unlinked file
		}
	}
	evicted := make(chan int, 1)
	go func() { evicted <- evictStranded(in) }()
	<-inClose

	others := make(chan error, 1)
	go func() {
		s, err := Reader(other)
		if err == nil {
			s.Close()
			var release func()
			if release, _, err = Hold(other); err == nil {
				release()
			}
			ReadStats()
		}
		others <- err
	}()
	select {
	case err := <-others:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		unblock()
		t.Fatal("a Reader, Hold or ReadStats on another path waited for an unrelated close")
	}

	held := make(chan func(), 1)
	go func() {
		release, _, err := Hold(victim)
		if err != nil {
			t.Error(err)
			release = func() {}
		}
		held <- release
	}()
	select {
	case release := <-held:
		release()
		unblock()
		t.Fatal("Hold returned while a close of a descriptor under its path was still running")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	(<-held)()
	if n := <-evicted; n != 1 {
		t.Fatalf("evictStranded = %d, want 1", n)
	}
	assertClosed(t, stuck)
	evict(0, in)
}
