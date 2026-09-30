// Package reflink copies a file using a filesystem-level clone
// (copy-on-write reflink) when the platform and filesystem support it, and
// falls back to a plain byte copy otherwise. It exists so ops.Fork's fast
// path (Task 6a) can seed a child lineage's snapshot object without paying
// for a full read+decode+re-encode: on a clone-capable filesystem (APFS,
// btrfs, xfs with reflink=1), CopyFile is near-constant-time regardless of
// file size. Clone is the strict variant — clone or ErrUnsupported, never a
// byte copy — for callers (ops' by-chain checkout cache) whose fast path is
// only worth taking when it is actually copy-on-write.
//
// This package works standalone — it knows nothing about offshoot's object
// keys, lineages, or the store.Backend interface; it is a pure local
// filesystem primitive that internal/store/local.go's CopyObject builds on.
package reflink

import (
	"errors"
	"io"
	"os"
)

// ErrUnsupported is Clone's verdict that this platform or filesystem cannot
// make a copy-on-write clone of src at dst. Callers treat it as "no fast
// path" and take their ordinary route; it is never a data error.
var ErrUnsupported = errors.New("reflink: clone unsupported on this filesystem")

// forceUnsupportedForTest, when true, makes Clone report ErrUnsupported
// without attempting a clone. Test-only, mirroring forceFallbackForTest.
var forceUnsupportedForTest bool

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
// dst is durable on disk (fsynced) before CopyFile returns success on
// EITHER path — see syncFile's doc comment for why the clone path needs an
// explicit step for this and copyPlain doesn't. A caller relying on dst
// surviving a crash immediately after CopyFile returns (e.g. before a
// rename that publishes it at a final path) does not need to fsync again
// itself.
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
		if err := syncFile(dst); err != nil {
			return true, err
		}
		return true, nil
	}
	if err := copyPlain(dst, src); err != nil {
		return false, err
	}
	return false, nil
}

// Clone makes dst a copy-on-write clone of src, or returns ErrUnsupported
// without creating dst when the platform or filesystem cannot clone. Unlike
// CopyFile it never falls back to a byte copy, so a caller on a non-CoW
// filesystem never pays for a second full write it did not ask for.
//
// A missing src is reported as that error, not ErrUnsupported: the clone
// syscalls only say "no", so src is checked first to keep "the source
// vanished" (e.g. a concurrent cache eviction) distinguishable from "this
// filesystem cannot clone". dst must not exist (create-only, as CopyFile),
// and is fsynced before a successful return, as CopyFile's clone path is.
func Clone(dst, src string) error {
	if _, err := os.Stat(src); err != nil {
		return err
	}
	if forceUnsupportedForTest || !cloneFile(dst, src) {
		return ErrUnsupported
	}
	if err := syncFile(dst); err != nil {
		os.Remove(dst)
		return err
	}
	return nil
}

// syncFile opens path (which CopyFile or Clone has just populated via a
// successful clone) and fsyncs it, then closes it.
//
// Neither clone mechanism hands CopyFile an open descriptor it could fsync
// directly: clonefile(2) (darwin) only ever takes paths, never returning an
// fd, and FICLONE (linux)'s destination fd is opened and closed entirely
// inside cloneFile, private to that platform file. Without this, a
// completed clone would be LESS durable than copyPlain's fallback (which
// does fsync its own handle before closing — see copyPlain's doc comment)
// even though both are meant to give CopyFile's caller the same guarantee:
// a power loss immediately after CopyFile returns must never leave dst
// missing or truncated once fsynced metadata (e.g. a ref written after a
// rename of dst into place) claims it exists.
//
// It opens read-only: fsync needs no write access, and a clone carries its
// source's mode (clonefile(2)), so a clone of a read-only 0444 file — a
// by-chain cache entry — is itself 0444 and could not be opened for writing.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// copyPlain copies src to dst by reading and writing bytes, with no cloning
// involved. This is CopyFile's fallback, and is also tested directly (see
// reflink_test.go) so its correctness is verified independent of whether
// any given test machine's filesystem supports clones at all.
//
// dst is opened create-only (O_EXCL) to match the clone syscalls' own
// create-only semantics, so a caller sees the same "dst already exists"
// failure mode on every platform regardless of which path CopyFile actually
// took. The written data is fsynced before close here directly (unlike the
// clone path, which has no open handle to fsync and uses syncFile instead —
// see CopyFile's doc comment) so a completed plain copy is exactly as
// durable as a completed clone.
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
