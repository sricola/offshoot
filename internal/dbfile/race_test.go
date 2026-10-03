package dbfile

import (
	"fmt"
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
