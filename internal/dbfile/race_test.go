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

// TestEveryCloseHoldsTheRegistryLock checks the package doc's rule that
// every close happens with mu held. TestEvictRacesHandle cannot see a
// violation: its holds open no connection. A close made after releasing mu
// could land after a Hold, its sql.Open and that connection's first lock,
// and drop the lock. Every close goes through closeLocked (the source check
// below), and at each one TryLock must fail because the closer holds mu.
// Nothing else in this binary runs concurrently with it.
func TestEveryCloseHoldsTheRegistryLock(t *testing.T) {
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
	var closes, unlocked int
	closeHookForTest = func() {
		closes++
		if mu.TryLock() {
			unlocked++
			mu.Unlock()
		}
	}
	t.Cleanup(func() { closeHookForTest = nil })
	evictStranded(in) // closeOrphans closes a's old descriptor
	evict(0, in)      // evict closes b's
	if closes != 2 {
		t.Fatalf("closes seen = %d, want 2 (an orphan and a cached descriptor)", closes)
	}
	if unlocked != 0 {
		t.Fatalf("%d descriptor(s) closed without the registry lock held", unlocked)
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
			if !ok || fd.Name.Name == "closeLocked" {
				continue
			}
			ast.Inspect(fd, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Close" {
					if x, ok := sel.X.(*ast.SelectorExpr); ok && x.Sel.Name == "f" {
						t.Errorf("%s: %s closes a registry descriptor directly; go through closeLocked",
							fset.Position(call.Pos()), fd.Name.Name)
					}
				}
				return true
			})
		}
	}
}
