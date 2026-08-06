// Package reflink copies a file using a filesystem-level clone
// (copy-on-write reflink) when the platform and filesystem support it, and
// falls back to a plain byte copy otherwise. It exists so ops.Fork's fast
// path (Task 6a) can seed a child lineage's snapshot object without paying
// for a full read+decode+re-encode: on a clone-capable filesystem (APFS,
// btrfs, xfs with reflink=1), CopyFile is near-constant-time regardless of
// file size.
//
// This package works standalone — it knows nothing about offshoot's object
// keys, lineages, or the store.Backend interface; it is a pure local
// filesystem primitive that internal/store/local.go's CopyObject builds on.
package reflink

import (
	"io"
	"os"
)

// forceFallbackForTest, when true, makes CopyFile skip the platform clone
// attempt entirely and go straight to the plain-copy fallback. Test-only:
// never set outside reflink_test.go. It exists so the fallback path
// (copyPlain and CopyFile's wiring to it) can be exercised deterministically
// on any machine/filesystem, regardless of whether the test's temp dir
// actually supports clones (CI runners, tmpfs, ext4 without reflink=1, ...).
var forceFallbackForTest bool

// CopyFile copies src to dst, using a filesystem clone (reflink via the
// FICLONE ioctl on Linux, clonefile(2) on darwin) when the platform and
// filesystem support it; cloned reports whether a clone happened. Any
// failure to clone — unsupported filesystem, cross-device, or any other
// clone-specific error — falls back silently to a plain byte copy; only a
// failure in the fallback itself (e.g. src does not exist, dst already
// exists, permission denied) is returned as err.
//
// dst is created O_EXCL-equivalent: both the clone syscalls and the plain-
// copy fallback require dst to not already exist, and CopyFile writes it
// directly rather than through a temp file. A caller that wants dst to land
// atomically at a final path (e.g. so a reader never observes a partial
// file) must call CopyFile against its own temporary path and rename the
// result into place itself — see internal/store/local.go's CopyObject for
// that convention.
func CopyFile(dst, src string) (cloned bool, err error) {
	if !forceFallbackForTest && cloneFile(dst, src) {
		return true, nil
	}
	if err := copyPlain(dst, src); err != nil {
		return false, err
	}
	return false, nil
}

// copyPlain copies src to dst by reading and writing bytes, with no cloning
// involved. This is CopyFile's fallback, and is also tested directly (see
// reflink_test.go) so its correctness is verified independent of whether
// any given test machine's filesystem supports clones at all.
//
// dst is opened create-only (O_EXCL) to match the clone syscalls' own
// create-only semantics, so a caller sees the same "dst already exists"
// failure mode on every platform regardless of which path CopyFile actually
// took. The written data is fsynced before close so a completed plain copy
// is exactly as durable as a completed clone.
func copyPlain(dst, src string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			out.Close()
			os.Remove(dst)
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	return out.Close()
}
