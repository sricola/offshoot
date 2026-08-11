package store

import "time"

// This file exists only in test binaries (export_test.go is never compiled
// into non-test builds). It exposes S3's seven unexported test knobs —
// multipartThreshold, defaultPartSize, multipartConcurrency, and
// multipartRPCTimeout (s3_multipart.go), and copyObjectMaxBytes,
// singleShotRPCTimeout, and readProgressTimeout (s3.go); see the doc
// comments on those vars — to the EXTERNAL store_test package via
// SetMultipartThresholdForTest, SetPartSizeForTest,
// SetMultipartConcurrencyForTest, SetMultipartRPCTimeoutForTest,
// SetCopyObjectMaxBytesForTest, SetSingleShotRPCTimeoutForTest, and
// SetReadProgressTimeoutForTest, following the same pattern
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
// s3_multipart.go), returning a restore func that puts back the previous
// value (call it via t.Cleanup). Tests force this to 1 for a deterministic sequential
// run, or leave/raise it above 1 (paired with a fake that can observe
// overlap, e.g. blocking each part until N have arrived) to prove parts
// really do upload in parallel. Test-only: never call this outside a test.
func SetMultipartConcurrencyForTest(v int) (restore func()) {
	prev := multipartConcurrency
	multipartConcurrency = v
	return func() { multipartConcurrency = prev }
}

// SetMultipartRPCTimeoutForTest overrides the per-call deadline every
// individual multipart RPC runs under (multipartRPCTimeout in
// s3_multipart.go), returning a restore func that puts back the previous
// value (call it via t.Cleanup). A test proving the deadline actually
// fires — and that the upload then fails with a context-deadline error and
// still aborts — cannot wait out the production value of 15 minutes, so it
// shrinks this instead. Test-only: never call this outside a test.
func SetMultipartRPCTimeoutForTest(d time.Duration) (restore func()) {
	prev := multipartRPCTimeout
	multipartRPCTimeout = d
	return func() { multipartRPCTimeout = prev }
}

// SetSingleShotRPCTimeoutForTest overrides the BASE component of every
// buffered single-shot S3 RPC's deadline (singleShotRPCTimeout in s3.go —
// the whole deadline for size-0 calls, and the flat component of
// singleShotDeadline's size-scaled one for calls with a known payload),
// returning a restore func that puts back the previous value (call it via
// t.Cleanup). A test proving the deadline actually fires cannot wait out
// the production value of 15 minutes, so it shrinks this instead; test
// payloads are far under singleShotFloorBytesPerSecond, so their size
// component is 0s and the hook value is effectively the whole deadline —
// the hook does not (and does not need to) scale the throughput floor.
// Test-only: never call this outside a test.
func SetSingleShotRPCTimeoutForTest(d time.Duration) (restore func()) {
	prev := singleShotRPCTimeout
	singleShotRPCTimeout = d
	return func() { singleShotRPCTimeout = prev }
}

// SingleShotDeadlineForTest exposes singleShotDeadline (s3.go) so a test
// can pin its size-scaling arithmetic directly — that a known multi-GiB
// payload earns transfer time at the 1 MiB/s floor on top of the flat
// base, instead of the flat base silently imposing a throughput floor on
// large single-request transfers. Test-only: never call this outside a
// test.
func SingleShotDeadlineForTest(size int64) time.Duration {
	return singleShotDeadline(size)
}

// SetReadProgressTimeoutForTest overrides GetReader's per-Read progress
// watchdog window (readProgressTimeout in s3.go), returning a restore func
// that puts back the previous value (call it via t.Cleanup). A test proving
// the watchdog fires — or that a slow consumer with pauses LONGER than the
// window is never killed — cannot wait out the production 60 seconds, so it
// shrinks this instead. Test-only: never call this outside a test.
func SetReadProgressTimeoutForTest(d time.Duration) (restore func()) {
	prev := readProgressTimeout
	readProgressTimeout = d
	return func() { readProgressTimeout = prev }
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
