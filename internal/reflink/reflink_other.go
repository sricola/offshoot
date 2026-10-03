//go:build !linux && !darwin

package reflink

import "os"

// cloneFile always reports no clone happened on any platform other than
// linux/darwin (the only two offshoot supports — see docs/status.md's
// Platform section). CopyFile's fallback (copyPlain) is fully portable, so
// the package still builds and works correctly here, just without the fast
// path.
func cloneFile(dst, src string) bool { return false }

// cloneFileFrom: no clone outside linux/darwin, as cloneFile.
func cloneFileFrom(dst string, src *os.File) bool { return false }
