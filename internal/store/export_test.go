package store

// This file exists only in test binaries (export_test.go is never compiled
// into non-test builds). It exposes S3's unexported multipartThreshold test
// knob (see s3.go's doc comment on that var) to the EXTERNAL store_test
// package, following the same pattern internal/ops/export_test.go uses for
// forkSlowPathForTest.

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
