//go:build !linux && !darwin

package fsutil

import "os"

func fsync(f *os.File) error { return f.Sync() }
