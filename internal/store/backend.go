// Package store implements offshoot's object-storage layout: a minimal
// conditional-write Backend interface, the local-directory implementation,
// and the typed manifest/ref schema on top.
package store

import (
	"errors"
	"io"
)

var (
	ErrNotFound = errors.New("store: not found")
	ErrCAS      = errors.New("store: compare-and-swap conflict")
	// ErrCopyUnsupported is returned by CopyObject when a backend cannot
	// perform a server-side or filesystem-level copy of THIS particular
	// object — every backend offshoot ships supports CopyObject in general
	// (local: a filesystem clone or plain-copy fallback; S3: a real
	// server-side CopyObject call, single-request under 5GB and multipart
	// UploadPartCopy above it), but S3 still cannot copy a source over its
	// own 5TiB per-object ceiling under any strategy, so the sentinel fires
	// for that case (see store.S3.CopyObject's doc comment). Callers
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

// BatchDeleter, ReaderGetter, and ReaderPutter (below) are all discovered by
// TYPE ASSERTION against a caller's store.Backend value (b.(store.BatchDeleter),
// etc.), never through the Backend interface itself. That means a wrapper
// type that implements Backend by embedding another Backend (e.g. for
// instrumentation, caching, or retry logic) SILENTLY HIDES whichever of
// these three the embedded value actually implements: the embedding type's
// own method set does not include them unless it redeclares each one, so
// the type assertion fails and the caller falls back to the buffered/
// per-key path. That fallback stays CORRECT — nothing breaks — but it
// silently loses the batching/streaming benefit the capability exists for
// (perf audits H2/H3). A wrapper that wants to preserve a capability must
// forward that method explicitly, not just embed and hope. The one
// in-repo wrapper today, ops.markCache (internal/ops/gc.go, used only in
// the GC mark walk), implements store.Backend directly rather than
// embedding one, and needs none of these three capabilities for what it
// does (Get/List only) — so nothing is affected today.
//
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

// ReaderGetter is an optional Backend capability: fetch an object as a
// stream instead of a fully-buffered []byte, so a caller applying a large
// object (e.g. ops' chain materialization — perf audit H3) need not hold it
// all in memory at once. Like BatchDeleter, it is deliberately NOT part of
// Backend itself: test wrappers and backends that only ever handle small
// objects keep working unchanged, and callers type-assert and fall back to
// the buffered Get.
//
// The caller MUST Close the returned reader on every path, including a
// mid-read error — the underlying stream (an S3 response body, an open
// local file descriptor) is not otherwise released. etag follows Get's
// contract where the backend can supply it without reading the body (S3:
// the GetObject response's ETag header, free); a backend for which
// computing a real content etag would require reading the whole object
// (defeating the point of streaming) MAY return "" instead — a caller that
// needs a content etag should use Get, not GetReader.
type ReaderGetter interface {
	GetReader(key string) (r io.ReadCloser, etag string, err error)
}

// ReaderPutter is an optional Backend capability: upload an object by
// STREAMING from r (exactly size bytes) instead of a fully-buffered []byte,
// so a caller uploading a large object (e.g. a session flush's snapshot
// encode — perf audit H3, write side) need not hold it all in memory at
// once. Like BatchDeleter/ReaderGetter, it is deliberately NOT part of
// Backend itself: test wrappers and backends that only ever handle small
// objects keep working unchanged, and callers type-assert and fall back to
// the buffered Put/PutIf.
//
// PutReaderIf mirrors PutIf's CAS contract exactly: ifMatch == "" means
// create-only (fails with store.ErrCAS if the key already exists),
// otherwise it is a compare-and-swap against the given etag, failing with
// store.ErrCAS on a mismatch or on a missing key. PutReader mirrors Put: an
// unconditional overwrite.
//
// r must yield exactly size bytes. A conforming implementation MUST NOT
// buffer the whole object in memory to satisfy either method — S3's streams
// the SDK's PutObject body directly (ContentLength: size); Local's streams
// to a temp file and renames into place, matching Put/PutIf's existing
// write-then-rename discipline.
type ReaderPutter interface {
	PutReaderIf(key string, r io.Reader, size int64, ifMatch string) (etag string, err error)
	PutReader(key string, r io.Reader, size int64) error
}
