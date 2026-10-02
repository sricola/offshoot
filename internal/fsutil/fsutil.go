// Package fsutil holds the two small filesystem disciplines every durable
// write in offshoot shares: a directory fsync after a rename, and an atomic
// whole-file write. It is a leaf package (no offshoot imports) so store,
// ltxio, ops and capture can all use it.
package fsutil

import (
	"os"
	"path/filepath"
)

// SyncDir fsyncs the directory at dir so a rename into it survives power
// loss, not only process death. A rename is atomic in the kernel's view
// the moment it returns, but on ext4 and XFS the directory entry can still
// sit in the page cache until the directory itself is synced; without this
// a crash can keep the fully fsynced new file yet lose the name pointing at
// it. Best effort: filesystems and platforms that reject fsync on a
// directory (some network and FUSE mounts; older macOS versions answer
// EINVAL) are not an error, since the rename itself already happened and
// nothing more can be done for it there.
func SyncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	d.Close()
}

// WriteFileAtomic writes data to path through a uniquely named temp file in
// the same directory, fsyncs it, renames it into place and syncs the
// directory, so a reader never sees a partial file and a crash leaves
// either the old content or the new one. mode is applied to the temp file
// before the rename.
func WriteFileAtomic(path string, data []byte, mode os.FileMode) error {
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
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	SyncDir(dir)
	return nil
}
