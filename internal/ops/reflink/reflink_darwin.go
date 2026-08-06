//go:build darwin

package reflink

import "golang.org/x/sys/unix"

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
