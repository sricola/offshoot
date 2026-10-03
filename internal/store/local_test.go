package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLocalPutGetEtag(t *testing.T) {
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("data/x/1.ltx", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, etag, err := b.Get("data/x/1.ltx")
	if err != nil || string(data) != "hello" || etag == "" {
		t.Fatalf("got %q etag=%q err=%v", data, etag, err)
	}
	if _, _, err := b.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestLocalGetReader pins store.ReaderGetter's contract on the Local
// backend (perf audit H3 / task 9a): a streamed read must return the same
// content Get would, over the same key namespace, and a missing key must
// map to ErrNotFound exactly like Get does.
func TestLocalGetReader(t *testing.T) {
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("data/x/1.ltx", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	r, _, err := b.GetReader("data/x/1.ltx")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("streamed content = %q, want %q", got, "hello")
	}

	if _, _, err := b.GetReader("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound for a missing key, got %v", err)
	}
}

func TestLocalCreateOnly(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if _, err := b.PutIf("refs/a/main", []byte("v1"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/a/main", []byte("v2"), ""); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on create-only over existing, got %v", err)
	}
}

func TestLocalCASUpdate(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	etag1, err := b.PutIf("refs/a/main", []byte("v1"), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/a/main", []byte("bad"), "wrong-etag"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on stale etag, got %v", err)
	}
	etag2, err := b.PutIf("refs/a/main", []byte("v2"), etag1)
	if err != nil || etag2 == etag1 {
		t.Fatalf("CAS update failed: %v", err)
	}
	data, _, _ := b.Get("refs/a/main")
	if string(data) != "v2" {
		t.Fatalf("content = %q", data)
	}
}

func TestLocalCASRace(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	// Seed content must never equal any fmt.Sprint(n) produced below (n in
	// [0,32)); otherwise the goroutine that happens to win with n==0 writes
	// back identical content, leaving the etag unchanged and letting a
	// second goroutine legitimately CAS against the same original etag.
	etag, _ := b.PutIf("refs/a/main", []byte("seed"), "")
	var wg sync.WaitGroup
	wins := make(chan int, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := b.PutIf("refs/a/main", []byte(fmt.Sprint(n)), etag); err == nil {
				wins <- n
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	count := 0
	for range wins {
		count++
	}
	if count != 1 {
		t.Fatalf("exactly one CAS from the same etag must win, got %d", count)
	}
}

func TestLocalListAndDelete(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	for _, k := range []string{"data/l1/a", "data/l1/b", "data/l2/a"} {
		if err := b.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := b.List("data/l1/")
	if err != nil || len(keys) != 2 || keys[0] != "data/l1/a" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if err := b.Delete("data/l1/a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete("data/l1/a"); err != nil {
		t.Fatal("delete of absent key must not error")
	}
	if keys, _ = b.List("data/l1/"); len(keys) != 1 {
		t.Fatalf("keys=%v", keys)
	}
}

// TestLocalListWalksOnlyThePrefixDirectory pins that List reads only the
// directory its prefix names, never the whole store root: a local store's
// root also holds every checkout, shadow and by-chain cache file, and chain
// resolution Lists one lineage per hop, so a whole-root walk made every
// fork, checkout and checkpoint cost grow with the number of checkouts on
// disk. An unreadable sibling tree fails a whole-root walk and must not
// be touched here. A prefix that ends mid-name still matches every key
// under its directory that starts with it, and a prefix whose directory
// does not exist lists nothing without error.
func TestLocalListWalksOnlyThePrefixDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 directory anyway")
	}
	root := t.TempDir()
	b, _ := NewLocal(root)
	for _, k := range []string{"data/l1/a", "data/l1/b", "data/l2/a", "refs/db/main"} {
		if err := b.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	blocked := filepath.Join(root, "checkouts", "db")
	if err := os.MkdirAll(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(blocked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(blocked, 0o755) })

	keys, err := b.List("data/l1/")
	if err != nil || len(keys) != 2 || keys[0] != "data/l1/a" || keys[1] != "data/l1/b" {
		t.Fatalf("List(data/l1/) = %v, %v", keys, err)
	}
	keys, err = b.List("data/l")
	if err != nil || len(keys) != 3 {
		t.Fatalf("List(data/l) = %v, %v; want the three data/l* keys", keys, err)
	}
	keys, err = b.List("refs/")
	if err != nil || len(keys) != 1 || keys[0] != "refs/db/main" {
		t.Fatalf("List(refs/) = %v, %v", keys, err)
	}
	keys, err = b.List("data/missing/")
	if err != nil || len(keys) != 0 {
		t.Fatalf("List of an absent directory = %v, %v; want empty, no error", keys, err)
	}
}

// TestLocalDeleteObjects pins Local's BatchDeleter capability as an exact
// stand-in for a per-key Delete loop: every key it reports deleted is gone,
// a key that never existed still counts as deleted (matching Delete's
// os.Remove-IsNotExist-is-success idempotency), and empty input is (nil, nil).
func TestLocalDeleteObjects(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	var _ BatchDeleter = b // Local must implement the optional capability

	if deleted, err := b.DeleteObjects(nil); deleted != nil || err != nil {
		t.Fatalf("empty input must be (nil, nil), got (%v, %v)", deleted, err)
	}

	keys := []string{"data/l1/a", "data/l1/b", "data/l2/c"}
	for _, k := range keys {
		if err := b.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	req := append(append([]string(nil), keys...), "data/l1/never-existed")
	deleted, err := b.DeleteObjects(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != len(req) {
		t.Fatalf("deleted %d keys, want %d (absent key counts as deleted)", len(deleted), len(req))
	}
	for i, k := range req {
		if deleted[i] != k {
			t.Fatalf("deleted[%d] = %s, want %s", i, deleted[i], k)
		}
	}
	if left, _ := b.List("data/"); len(left) != 0 {
		t.Fatalf("objects left after batch delete: %v", left)
	}
}

// TestLocalDeleteIfConditionalDelete pins Local.DeleteIf as a TRUE
// compare-and-delete (Milestone 4 Task 6b), the local-backend half of
// ConditionalDeleter: a matching etag deletes, a stale etag or an absent key
// both fail with ErrCAS and leave whatever's there (if anything) untouched.
func TestLocalDeleteIfConditionalDelete(t *testing.T) {
	b, _ := NewLocal(t.TempDir())

	// Absent key: ErrCAS, not a silent no-op (unlike plain Delete).
	if err := b.DeleteIf("refs/a/main", "whatever"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS deleting an absent key, got %v", err)
	}

	etag, err := b.PutIf("refs/a/main", []byte("v1"), "")
	if err != nil {
		t.Fatal(err)
	}

	// Stale etag: refused, key still there.
	if err := b.DeleteIf("refs/a/main", "wrong-etag"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on stale etag, got %v", err)
	}
	if data, _, err := b.Get("refs/a/main"); err != nil || string(data) != "v1" {
		t.Fatalf("a failed DeleteIf must not remove the key: data=%q err=%v", data, err)
	}

	// Matching etag: deletes.
	if err := b.DeleteIf("refs/a/main", etag); err != nil {
		t.Fatalf("DeleteIf with a matching etag must succeed: %v", err)
	}
	if _, _, err := b.Get("refs/a/main"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key must be gone after DeleteIf, err=%v", err)
	}

	// A second DeleteIf with the SAME (now-stale, since the key is gone)
	// etag must fail rather than succeed as a no-op — a fresh Put'd key
	// under the same name racing this call must never be silently deleted
	// by a stale-etag DeleteIf that just happens to target an absent path.
	if err := b.DeleteIf("refs/a/main", etag); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS deleting an already-absent key with a stale etag, got %v", err)
	}
}

// TestStoreDeleteRefIfUsesConditionalDeleteOnLocal pins Store.DeleteRefIf's
// dispatch (Milestone 4 Task 6b): against a backend implementing
// ConditionalDeleter (Local), a stale etag must be refused with ErrCAS and
// the ref left in place — proving the store-level API actually reaches
// Local's true CAS delete, not just Local.DeleteIf in isolation.
func TestStoreDeleteRefIfUsesConditionalDeleteOnLocal(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	s := &Store{B: b}
	if err := s.InitManifest(); err != nil {
		t.Fatal(err)
	}
	etag, err := s.PutRef("app", "main", Ref{Lineage: "l1", Epoch: 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRefIf("app", "main", "stale-etag"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on a stale etag, got %v", err)
	}
	if _, _, err := s.GetRef("app", "main"); err != nil {
		t.Fatalf("a failed DeleteRefIf must not remove the ref: %v", err)
	}
	if err := s.DeleteRefIf("app", "main", etag); err != nil {
		t.Fatalf("DeleteRefIf with a matching etag must succeed: %v", err)
	}
	if _, _, err := s.GetRef("app", "main"); !errors.Is(err, ErrNotFound) {
		t.Fatal("ref must be gone after DeleteRefIf")
	}
}

func TestLocalBreaksStaleLock(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	p, err := b.path("refs/a/main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := p + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	stale := time.Now().Add(-1 * time.Minute)
	if err := os.Chtimes(lockPath, stale, stale); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if _, err := b.PutIf("refs/a/main", []byte("v1"), ""); err != nil {
		t.Fatalf("PutIf should break the stale lock and succeed, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("PutIf took too long to break stale lock: %v", elapsed)
	}
}

func TestLocalFreshLockNotBroken(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	p, err := b.path("refs/a/main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := p + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close() // freshly created: mtime is now

	done := make(chan struct{})
	go func() {
		b.PutIf("refs/a/main", []byte("v1"), "")
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("PutIf must not succeed while a fresh lock is held")
	case <-time.After(1 * time.Second):
	}

	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PutIf did not complete after the lock was removed")
	}
}

// TestLocalConcurrentPutSameKey guards against a regression found while
// hardening internal/ops's Checkpoint against concurrent callers: Put has no
// per-key lock (by design -- callers like session flush's overwrite of an
// orphan a crashed prior flush left, and GC's tombstone-list write, use it
// exactly because last-write-wins is intentional there; the at-rest
// checkpoint, which once overwrote too, now writes create-only under its
// own lease's epoch), so multiple goroutines can call
// Put on the identical key at the same time. The old write() used a fixed
// shared temp filename (key+".tmp"): one goroutine's os.Create (O_TRUNC) or
// os.Rename could clobber or disappear another's temp file mid-write,
// surfacing as a spurious "rename ... no such file or directory" instead of
// either goroutine cleanly succeeding. Every concurrent Put here must
// return a nil error, and the key must end up holding exactly one of the
// written payloads (not truncated, not torn).
func TestLocalConcurrentPutSameKey(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = b.Put("data/x/1.ltx", []byte(fmt.Sprintf("payload-%d", i)))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Put %d: %v", i, err)
		}
	}
	data, _, err := b.Get("data/x/1.ltx")
	if err != nil {
		t.Fatalf("Get after concurrent Put: %v", err)
	}
	matched := false
	for i := 0; i < n; i++ {
		if string(data) == fmt.Sprintf("payload-%d", i) {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatalf("final content %q is not any single writer's payload (torn write)", data)
	}
	// No leaked temp files should show up in List.
	keys, err := b.List("data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "data/x/1.ltx" {
		t.Fatalf("List after concurrent Put = %v, want exactly [data/x/1.ltx]", keys)
	}
}

func TestLocalRejectsTraversal(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if err := b.Put("../evil", []byte("x")); err == nil {
		t.Fatal("want error on path traversal")
	}
}

// TestLocalHeadRecordedEtag pins how Local.Head answers without re-reading
// the object: every write path (Put, PutIf, PutReader, PutReaderIf) leaves
// the content etag on the object as an extended attribute, Head returns
// that etag with the stat size, and anything that makes the attribute
// untrustworthy — an object written without one (an older version, or a
// filesystem without user xattrs), or one changed in place so its size no
// longer matches — makes Head hash the content instead, so it is never
// wrong, only slower.
func TestLocalHeadRecordedEtag(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	head := func(key string) (string, int64) {
		t.Helper()
		etag, size, err := b.Head(key)
		if err != nil {
			t.Fatalf("Head(%s): %v", key, err)
		}
		return etag, size
	}
	payload := []byte("recorded etag payload")
	want := etagOf(payload)

	// Each write path records the etag, and Head reports it with the size.
	if _, err := b.PutIf("data/putif", payload, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("data/put", payload); err != nil {
		t.Fatal(err)
	}
	if err := b.PutReader("data/putreader", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutReaderIf("data/putreaderif", bytes.NewReader(payload), int64(len(payload)), ""); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"data/putif", "data/put", "data/putreader", "data/putreaderif"} {
		if etag, size := head(key); etag != want || size != int64(len(payload)) {
			t.Fatalf("Head(%s) = (%s, %d), want (%s, %d)", key, etag, size, want, len(payload))
		}
	}

	// The attribute is really there (so the Head answers above came from
	// it, not from hashing) on a filesystem that supports user xattrs; on
	// one that does not, Head's fallback is what the assertions above
	// exercised, and the rest of this test has nothing more to pin.
	f, err := os.Open(filepath.Join(root, "data", "putif"))
	if err != nil {
		t.Fatal(err)
	}
	recorded, ok := readEtagAttr(f, int64(len(payload)))
	f.Close()
	if !ok {
		t.Skip("filesystem does not record user xattrs; Head hashed instead")
	}
	if recorded != want {
		t.Fatalf("recorded etag %s, want %s", recorded, want)
	}

	// An object with no attribute (written by something other than this
	// version of Local) is hashed.
	if err := os.WriteFile(filepath.Join(root, "data", "bare"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if etag, size := head("data/bare"); etag != want || size != int64(len(payload)) {
		t.Fatalf("Head(bare) = (%s, %d), want (%s, %d)", etag, size, want, len(payload))
	}

	// An object changed in place keeps a stale attribute, but its size no
	// longer matches, so Head hashes the current content.
	grown := append(append([]byte{}, payload...), " and more"...)
	if err := os.WriteFile(filepath.Join(root, "data", "putif"), grown, 0o644); err != nil {
		t.Fatal(err)
	}
	if etag, size := head("data/putif"); etag != etagOf(grown) || size != int64(len(grown)) {
		t.Fatalf("Head after in-place growth = (%s, %d), want (%s, %d)", etag, size, etagOf(grown), len(grown))
	}

	// A replace through Local lands the new etag with the new object.
	if err := b.Put("data/putif", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if etag, size := head("data/putif"); etag != etagOf([]byte("second")) || size != int64(len("second")) {
		t.Fatalf("Head after Put = (%s, %d), want (%s, %d)", etag, size, etagOf([]byte("second")), len("second"))
	}

	// A CopyObject's destination answers the source's etag whether or not
	// the copy carried the attribute (clonefile does, a byte copy does not).
	if err := b.CopyObject("data/copy", "data/put"); err != nil {
		t.Fatal(err)
	}
	if etag, size := head("data/copy"); etag != want || size != int64(len(payload)) {
		t.Fatalf("Head(copy) = (%s, %d), want (%s, %d)", etag, size, want, len(payload))
	}
}

// TestOnlyLocalSettlesItsWrites: a local store's conditional write that
// returns an error wrote nothing and never will, and it says so
// (SettledWriter); S3's can still land after it reported failure (a
// timeout, or the SDK's retry answered by its own first attempt), so it
// must not claim to settle them, or a caller would stop waiting for a
// write that is still on its way.
func TestOnlyLocalSettlesItsWrites(t *testing.T) {
	l, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := Backend(l).(SettledWriter); !ok || !s.WritesSettled() {
		t.Fatal("a local store does not say it settles its writes")
	}
	if _, ok := Backend((*S3)(nil)).(SettledWriter); ok {
		t.Fatal("S3 says it settles its writes")
	}
}
