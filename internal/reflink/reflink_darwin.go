//go:build darwin

package reflink

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile attempts a clonefile(2) copy-on-write clone of src to dst — the
// mechanism APFS supports (the only darwin filesystem that does, in
// practice; HFS+ and network/FUSE mounts do not). clonefile(2) creates dst
// itself and requires it not already exist, matching CopyFile's create-only
// contract directly — unlike Linux's FICLONE, there is no separately opened
// destination fd to clean up on failure. Any failure (dst already exists,
// cross-device, a filesystem that doesn't support cloning, or any other
// error) returns false, never an error — CopyFile's caller falls back to a
// plain copy.
func cloneFile(dst, src string) bool {
	return unix.Clonefile(src, dst, 0) == nil
}

// cloneFileFrom is cloneFile from an already open source, through
// fclonefileat(2). It opens no descriptor and never closes src.
func cloneFileFrom(dst string, src *os.File) bool {
	rc, err := src.SyscallConn()
	if err != nil {
		return false
	}
	var cloneErr error
	if err := rc.Control(func(fd uintptr) {
		cloneErr = unix.Fclonefileat(int(fd), unix.AT_FDCWD, dst, 0)
	}); err != nil {
		return false
	}
	return cloneErr == nil
}
