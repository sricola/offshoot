//go:build linux || darwin

package fsutil

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// fsync is fsync(2) on the file's descriptor: on Linux exactly what a
// directory sync needs; on macOS the metadata flush without the
// F_FULLFSYNC disk-cache flush that (*os.File).Sync adds. A variable so a
// test can make it fail.
var fsync = func(f *os.File) error { return unix.Fsync(int(f.Fd())) }

// fsyncUnsupported reports an errno a filesystem uses to say directories
// cannot be fsynced here, as opposed to a sync that failed.
func fsyncUnsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY)
}
