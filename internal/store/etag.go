package store

import "strings"

// NormalizeETag reduces an etag to the form two provider responses for the
// same object agree on: surrounding whitespace is trimmed, a weak
// validator prefix ("W/" or "w/") is dropped, one pair of surrounding
// double quotes is removed, and the result is lower-cased.
//
// It exists for comparisons between two etags a provider returned, never
// for what is sent back to one. S3 defines the ETag header as a quoted
// string and that is what PutIf, Get and Head return, but an S3-compatible
// provider (MinIO, RustFS, R2, B2, Ceph RGW, a proxy in front of any of
// them) may hand back the same object's etag quoted on PUT and bare on
// HEAD, with a weak "W/" prefix on one of them, or with the hex in a
// different case. ops.verifyOwnObject compares the etag its create-only
// put returned with the etag a Head returns after the ref CAS, and trusts
// the object with that one request when they agree; without this
// normalization a provider that reformats between the two cost a full Get
// of the object it had just written, for nothing.
//
// A conditional write's If-Match header must carry the etag exactly as the
// provider returned it, so the S3 backend does not normalize ifMatch. The
// local backend's own "<sha256> <size>" etags pass through unchanged apart
// from case (they are already lower-case hex) and are compared raw in its
// CAS paths, which this function does not touch.
func NormalizeETag(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == 'W' || s[0] == 'w') && s[1] == '/' {
		s = s[2:]
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	return strings.ToLower(s)
}
