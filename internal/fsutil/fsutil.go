// Package fsutil holds the two small filesystem disciplines every durable
// write in offshoot shares: a directory fsync after a rename, and an atomic
// whole-file write. It is a leaf package (no offshoot imports) so store,
// ltxio, ops and capture can all use it.
package fsutil

import (
	"os"
	"path/filepath"
)

// SyncDir fsyncs the directory at dir so a rename into it survives a crash,
// not only the process's death. A rename is atomic the moment it returns,
// but on ext4 and XFS the directory entry can still sit in the page cache
// until the directory itself is synced; without this a crash can keep the
// fully synced new file yet lose the name pointing at it.
//
// This is fsync(2), deliberately not Go's (*os.File).Sync: on macOS that
// issues F_FULLFSYNC, a full disk-cache flush that costs tens of
// milliseconds per call, and a directory needs only its metadata ordered
// behind the file's own sync. Best effort: platforms and filesystems that
// reject fsync on a directory are not an error, since the rename itself
// already happened and nothing more can be done for it there.
func SyncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = fsync(d)
	d.Close()
}

// WriteFileAtomic writes data to path through a uniquely named temp file in
// the same directory, syncs it, renames it into place and syncs the
// directory, so a reader never sees a partial file and a crash leaves
// either the old content or the new one. For content whose loss is only a
// cache miss, use ReplaceFileAtomic instead: it keeps the atomic rename
// and skips the syncs.
func WriteFileAtomic(path string, data []byte, mode os.FileMode) error {
	return writeAtomic(path, data, mode, true)
}

// ReplaceFileAtomic is WriteFileAtomic without the fsyncs: readers still
// never see a torn file, but after power loss the file may be missing or
// hold the previous content. Right for derived state that a reader can
// rebuild (a checkout's .sum sidecar is re-derived by hashing), where a
// disk-cache flush per write would cost more than the cache saves.
func ReplaceFileAtomic(path string, data []byte, mode os.FileMode) error {
	return writeAtomic(path, data, mode, false)
}

func writeAtomic(path string, data []byte, mode os.FileMode, durable bool) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if _, err := f.Write(data); err != nil {
		return fail(err)
	}
	if err := f.Chmod(mode); err != nil {
		return fail(err)
	}
	if durable {
		if err := f.Sync(); err != nil {
			return fail(err)
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if durable {
		SyncDir(dir)
	}
	return nil
}
