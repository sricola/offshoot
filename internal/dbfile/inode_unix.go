//go:build unix

package dbfile

import (
	"fmt"
	"os"
	"syscall"
)

// inodeOf is the (device, inode) pair fi describes: what POSIX advisory
// locks, and therefore pins, are keyed by. A path is not enough: two paths
// can name one inode, and a rename gives one path a new inode.
func inodeOf(fi os.FileInfo) (Inode, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return Inode{}, fmt.Errorf("dbfile: %s: no inode in %T", fi.Name(), fi.Sys())
	}
	return Inode{Dev: uint64(st.Dev), Ino: uint64(st.Ino)}, nil
}
