//go:build linux || darwin

package store

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// etagAttr is the extended attribute Local stamps on every object it writes,
// holding the object's content etag (etagOf) and byte length as
// "<hex sha256> <size>". It is set on the temp file before the rename into
// place, so it travels with the inode: an object and its etag can never be
// observed out of step, even by a Put racing a PutIf on the same key with
// no lock between them (each writer's rename carries its own etag). Head
// reads it back from the same open descriptor it stats, so the etag and
// size it returns describe one inode. The "user." prefix is what Linux
// requires for an unprivileged attribute; darwin accepts any name.
const etagAttr = "user.offshoot.etag"

// setEtagAttr records etag and size on f. Best effort: a filesystem without
// user xattrs (some tmpfs, NFS and FUSE mounts) or any other failure leaves
// the object without one, and Head then hashes the content as it always
// could. The write itself is never failed over it.
func setEtagAttr(f *os.File, etag string, size int64) {
	_ = unix.Fsetxattr(int(f.Fd()), etagAttr, []byte(etag+" "+strconv.FormatInt(size, 10)), 0)
}

// readEtagAttr returns the etag recorded on f when there is one and it
// describes a file of exactly size bytes (the size f's own stat reports).
// A missing, malformed or size-mismatched attribute answers ok=false and
// the caller hashes instead; the size check is what catches an object
// truncated or appended to in place by something other than Local, which
// only ever replaces whole files by rename.
func readEtagAttr(f *os.File, size int64) (etag string, ok bool) {
	buf := make([]byte, 96)
	n, err := unix.Fgetxattr(int(f.Fd()), etagAttr, buf)
	if err != nil || n <= 0 || n > len(buf) {
		return "", false
	}
	fields := strings.Fields(string(buf[:n]))
	if len(fields) != 2 || len(fields[0]) != 64 {
		return "", false
	}
	for _, c := range fields[0] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	recorded, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || recorded != size {
		return "", false
	}
	return fields[0], true
}
