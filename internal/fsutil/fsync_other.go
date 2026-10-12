//go:build !linux && !darwin

package fsutil

import "os"

var fsync = func(f *os.File) error { return f.Sync() }

// fsyncUnsupported: no platform-specific errnos are known here; every
// failure counts.
func fsyncUnsupported(error) bool { return false }
