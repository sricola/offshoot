// Package store implements offshoot's object-storage layout: a minimal
// conditional-write Backend interface, the local-directory implementation,
// and the typed manifest/ref schema on top.
package store

import "errors"

var (
	ErrNotFound = errors.New("store: not found")
	ErrCAS      = errors.New("store: compare-and-swap conflict")
	// ErrCopyUnsupported is returned by CopyObject when a backend cannot
	// perform a server-side or filesystem-level copy at all (e.g. S3 in
	// Task 6a, before Task 6b's server-side CopyObject lands). Callers
	// (ops.Fork's fast path) must treat this as "fall back to the slow,
	// materialize-and-re-encode path" — it is a capability signal, not a
	// hard failure.
	ErrCopyUnsupported = errors.New("store: CopyObject not supported by this backend")
)

type Backend interface {
	Get(key string) (data []byte, etag string, err error)
	Put(key string, data []byte) error
	PutIf(key string, data []byte, ifMatch string) (string, error)
	List(prefix string) ([]string, error)
	Delete(key string) error
	// CopyObject makes dst a byte-identical copy of src within the same
	// backend. Returns ErrNotFound if src does not exist, or
	// ErrCopyUnsupported if this backend cannot perform the copy at all —
	// see ErrCopyUnsupported's doc comment for what callers must do with
	// that.
	//
	// CopyObject OVERWRITES an existing dst — like Put, not like the
	// create-only PutIf every snapshot/segment write otherwise uses. Today's
	// only caller (ops.Fork's fast path) always mints a fresh destination
	// key (a brand-new lineage's snapshot key) that nothing else can be
	// racing to write, so this is never observably different from
	// create-only in practice; it is specified as overwrite because that is
	// what a rename-into-place (the local backend) and a single-request
	// server-side copy (S3, Task 6b) both do natively, and requiring
	// create-only here would mean an extra existence check or conditional
	// request for no caller that needs it. A future caller that DOES need
	// create-only-or-fail must check for an existing dst itself (e.g. via
	// Get) before calling CopyObject.
	CopyObject(dst, src string) error
}
