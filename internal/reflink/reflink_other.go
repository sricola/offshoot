//go:build !linux && !darwin

package reflink

// cloneFile always reports no clone happened on any platform other than
// linux/darwin (the only two offshoot supports — see docs/status.md's
// Platform section). CopyFile's fallback (copyPlain) is fully portable, so
// the package still builds and works correctly here, just without the fast
// path.
func cloneFile(dst, src string) bool { return false }
