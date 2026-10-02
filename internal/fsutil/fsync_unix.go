//go:build linux || darwin

package fsutil

import (
	"os"

	"golang.org/x/sys/unix"
)

// fsync is fsync(2) on the file's descriptor: on Linux exactly what a
// directory sync needs; on macOS the metadata flush without the
// F_FULLFSYNC disk-cache flush that (*os.File).Sync adds.
func fsync(f *os.File) error { return unix.Fsync(int(f.Fd())) }
