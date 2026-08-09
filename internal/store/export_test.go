package store

// This file exists only in test binaries (export_test.go is never compiled
// into non-test builds). It exposes S3's unexported multipartThreshold and
// defaultPartSize test knobs (see s3.go's doc comments on those vars) to the
// EXTERNAL store_test package, following the same pattern
// internal/ops/export_test.go uses for forkSlowPathForTest.

// SetMultipartThresholdForTest overrides the object-size threshold above
// which store.S3.PutReader/PutReaderIf switch to a multipart upload,
// returning a restore func that puts back the previous value (call it via
// t.Cleanup). A real multi-gigabyte upload can't be exercised in a test, so
// tests set this low (e.g. 1 KiB) to drive small payloads through the
// multipart path. Test-only: never call this outside a test.
func SetMultipartThresholdForTest(v int64) (restore func()) {
	prev := multipartThreshold
	multipartThreshold = v
	return func() { multipartThreshold = prev }
}

// SetPartSizeForTest overrides the part size store.S3's multipart path uses
// (defaultPartSize in partSizeFor), returning a restore func that puts back
// the previous value (call it via t.Cleanup). Exercising "several real
// parts" against production's 64 MiB default would need a several-hundred-
// MiB test payload; tests instead shrink this (down to minPartSize, S3's
// real 5 MiB floor — partSizeFor's own clamp prevents going lower) so a
// modest payload still splits into multiple genuine parts. Test-only: never
// call this outside a test.
func SetPartSizeForTest(v int64) (restore func()) {
	prev := defaultPartSize
	defaultPartSize = v
	return func() { defaultPartSize = prev }
}

// SetMultipartConcurrencyForTest overrides the worker count store.S3's
// putMultipart uses on the io.ReaderAt path (multipartConcurrency in
// s3.go), returning a restore func that puts back the previous value (call
// it via t.Cleanup). Tests force this to 1 for a deterministic sequential
// run, or leave/raise it above 1 (paired with a fake that can observe
// overlap, e.g. blocking each part until N have arrived) to prove parts
// really do upload in parallel. Test-only: never call this outside a test.
func SetMultipartConcurrencyForTest(v int) (restore func()) {
	prev := multipartConcurrency
	multipartConcurrency = v
	return func() { multipartConcurrency = prev }
}

// SetCopyObjectMaxBytesForTest overrides the source-size threshold above
// which store.S3.CopyObject switches from a single-request CopyObject to a
// multipart UploadPartCopy sequence (copyObjectMaxBytes in s3.go),
// returning a restore func that puts back the previous value (call it via
// t.Cleanup). A real >5GiB source can't be exercised in a test, so tests
// lower this (paired with SetPartSizeForTest) to drive a modest real
// payload through the multipart-copy path. Test-only: never call this
// outside a test.
func SetCopyObjectMaxBytesForTest(v int64) (restore func()) {
	prev := copyObjectMaxBytes
	copyObjectMaxBytes = v
	return func() { copyObjectMaxBytes = prev }
}
