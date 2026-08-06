//go:build linux

package reflink

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile attempts a reflink clone of src to dst via the FICLONE ioctl —
// Linux's mechanism for filesystems that support copy-on-write extent
// sharing (btrfs, xfs with reflink=1, overlayfs with a supporting backing
// fs). It returns false, never an error, for anything short of a clean
// clone: most filesystems (notably ext4) don't support FICLONE at all
// (ENOTSUP/EOPNOTSUPP), a cross-device request can't be a clone (EXDEV), and
// any other ioctl failure is treated the same way — CopyFile's caller falls
// back to a plain copy in every one of those cases.
//
// dst is created here (O_CREAT|O_EXCL, matching CopyFile's create-only
// contract) so the ioctl has an open destination fd to clone into; unlike
// darwin's clonefile(2), FICLONE operates on already-open descriptors, not
// paths. If the clone does not succeed, any dst file this call created is
// removed before returning, so the plain-copy fallback's own O_EXCL create
// still sees an absent path rather than tripping over a leftover empty file.
func cloneFile(dst, src string) bool {
	in, err := os.Open(src)
	if err != nil {
		return false
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return false
	}
	cloned := false
	defer func() {
		out.Close()
		if !cloned {
			os.Remove(dst)
		}
	}()

	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err != nil {
		return false
	}
	cloned = true
	return true
}
