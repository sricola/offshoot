package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops/reflink"
)

// Local is a directory-backed Backend. CAS is implemented with a per-key
// O_CREAT|O_EXCL lock file: acquire lock -> read+verify etag -> write temp,
// fsync, rename -> release lock. A bare rename alone is atomic REPLACE, not
// compare-and-swap; the lock provides the compare step.
type Local struct {
	root string
}

func NewLocal(root string) (*Local, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Local{root: root}, nil
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (l *Local) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

func (l *Local) Get(key string) ([]byte, string, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return data, etagOf(data), nil
}

// write does a write-to-temp-then-rename. The temp file gets a unique name
// per call (via os.CreateTemp) rather than a fixed p+".tmp": Put has no
// per-key lock (PutIf's lock guards its own read-then-write, but Put is used
// standalone wherever last-write-wins is intentional, e.g. Checkpoint's
// orphan-snapshot overwrite and GC's tombstone-list write), so two
// goroutines can legitimately call write() on the same p concurrently. A
// shared fixed temp name means one goroutine's os.Create (O_TRUNC) or
// os.Rename can clobber or disappear out from under the other mid-write,
// surfacing as a spurious "rename ... no such file or directory" — a real
// bug, not a benign race, since it aborts a write that should have quietly
// lost or won. A unique temp name per call makes each writer self-contained;
// the final os.Rename is still atomic, so the last one to rename wins
// cleanly with no torn or missing file in between.
func (l *Local) write(p string, data []byte) error {
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, filepath.Base(p)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func (l *Local) Put(key string, data []byte) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	return l.write(p, data)
}

func (l *Local) lock(p string) (release func(), err error) {
	lockPath := p + ".lock"
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			f.Close()
			return func() { os.Remove(lockPath) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		// A healthy CAS holds the lock for milliseconds. If it's been sitting
		// here for a while, assume the process that created it was killed and
		// break it. Stat/remove races are benign: the file may vanish between
		// stat and remove (fine, someone else broke it), or another process
		// may concurrently break the same stale lock (also fine — both then
		// race O_EXCL normally).
		if info, statErr := os.Stat(lockPath); statErr == nil {
			if time.Since(info.ModTime()) > 30*time.Second {
				os.Remove(lockPath)
				continue
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("store: lock timeout on %s (if no offshoot process is running, delete this file)", lockPath)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (l *Local) PutIf(key string, data []byte, ifMatch string) (string, error) {
	p, err := l.path(key)
	if err != nil {
		return "", err
	}
	release, err := l.lock(p)
	if err != nil {
		return "", err
	}
	defer release()

	cur, err := os.ReadFile(p)
	switch {
	case os.IsNotExist(err):
		if ifMatch != "" {
			return "", fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch)
		}
	case err != nil:
		return "", err
	default:
		if ifMatch == "" {
			return "", fmt.Errorf("%w: key exists", ErrCAS)
		}
		if etagOf(cur) != ifMatch {
			return "", fmt.Errorf("%w: etag mismatch", ErrCAS)
		}
	}
	if err := l.write(p, data); err != nil {
		return "", err
	}
	return etagOf(data), nil
}

func (l *Local) List(prefix string) ([]string, error) {
	var keys []string
	root := l.root
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if strings.HasSuffix(p, ".lock") || strings.Contains(filepath.Base(p), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

// tempName returns a unique path in dir (using base as the filename
// pattern's stem) that is guaranteed to have been free at least momentarily,
// without leaving a file behind. CopyObject needs this rather than write()'s
// os.CreateTemp-and-keep-the-handle approach: reflink.CopyFile (like the
// clonefile(2)/FICLONE syscalls it wraps) requires its destination to not
// already exist, so the temp path itself must be absent, not merely unique.
// The same benign race write()'s doc comment accepts for its own per-call
// temp names applies here identically.
func tempName(dir, base string) (string, error) {
	f, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	if err := os.Remove(name); err != nil {
		return "", err
	}
	return name, nil
}

// CopyObject makes dst a byte-identical copy of src, using reflink.CopyFile
// (a filesystem clone on APFS/btrfs/xfs-with-reflink, a plain byte copy
// otherwise) so the copy is near-constant-time when the filesystem supports
// it. Like write(), it copies to a uniquely-named temp file first and
// renames into place, so a reader of dst never observes a partial file and
// a failure partway through leaves dst untouched.
func (l *Local) CopyObject(dst, src string) error {
	dstPath, err := l.path(dst)
	if err != nil {
		return err
	}
	srcPath, err := l.path(src)
	if err != nil {
		return err
	}
	if _, err := os.Stat(srcPath); err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	dir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := tempName(dir, filepath.Base(dstPath))
	if err != nil {
		return err
	}
	if _, err := reflink.CopyFile(tmp, srcPath); err != nil {
		os.Remove(tmp)
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	if err := os.Rename(tmp, dstPath); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func (l *Local) Delete(key string) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
