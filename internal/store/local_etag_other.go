//go:build !linux && !darwin

package store

import "os"

// Without extended attributes Local records no etag beside its objects and
// Head hashes the content, exactly as Get does.
func setEtagAttr(*os.File, string, int64) {}

func readEtagAttr(*os.File, int64) (string, bool) { return "", false }
