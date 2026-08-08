// Package store implements offshoot's object-storage layout: a minimal
// conditional-write Backend interface, the local-directory implementation,
// and the typed manifest/ref schema on top.
package store

import "errors"

var (
	ErrNotFound = errors.New("store: not found")
	ErrCAS      = errors.New("store: compare-and-swap conflict")
	// ErrCopyUnsupported is returned by CopyObject when a backend cannot
	// perform a server-side or filesystem-level copy of THIS particular
	// object — every backend offshoot ships supports CopyObject in general
	// (local: a filesystem clone or plain-copy fallback; S3: a real
	// server-side CopyObject call), but S3's is gated to objects at or
	// under its 5GB single-request CopyObject limit, so the sentinel still
	// fires for anything larger (see store.S3.CopyObject's doc comment;
	// multipart UploadPartCopy is not implemented, out of scope). Callers
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

// BatchDeleter is an optional Backend capability: delete many keys in as few
// round trips as the backend allows (S3: the DeleteObjects API, 1000 keys
// per request — perf audit H2; Local: a plain loop, no RPC to save but the
// same contract so callers stay uniform). It is deliberately NOT part of
// Backend itself: test wrappers and future backends keep working unchanged,
// and callers (ops' GC sweep) type-assert and fall back to per-key Delete.
//
// DeleteObjects returns the keys it SUCCESSFULLY deleted — so a caller
// pruning per-key state (GC tombstones) prunes exactly those — plus an
// error describing any failures. A key that did not exist counts as
// successfully deleted (idempotent), matching Delete. Empty input is a
// no-op: (nil, nil).
type BatchDeleter interface {
	DeleteObjects(keys []string) (deleted []string, err error)
}
