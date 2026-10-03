//go:build !unix

package dbfile

import (
	"errors"
	"os"
)

// inodeOf has nothing to report off unix. offshoot supports linux and darwin
// only (docs/status.md, Platform); this keeps the package building elsewhere,
// where every Reader and Hold fails rather than guessing at file identity.
func inodeOf(os.FileInfo) (Inode, error) { return Inode{}, errors.ErrUnsupported }
