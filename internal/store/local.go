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

func (l *Local) write(p string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
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
	return os.Rename(tmp, p)
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
		if strings.HasSuffix(p, ".lock") || strings.HasSuffix(p, ".tmp") {
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
