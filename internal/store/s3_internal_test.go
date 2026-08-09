package store

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestPartSizeFor pins partSizeFor's contract: at least minPartSize and
// defaultPartSize, and never so small that size splits into more than
// maxParts parts — the two S3 hard limits it exists to satisfy (see s3.go's
// doc comment on partSizeFor).
func TestPartSizeFor(t *testing.T) {
	cases := []struct {
		name string
		size int64
		want int64
	}{
		{"tiny", 1, defaultPartSize},
		{"exactly defaultPartSize", defaultPartSize, defaultPartSize},
		{"a few GiB, well under the maxParts pressure point", 4 * 1024 * 1024 * 1024, defaultPartSize},
		{
			"huge enough that maxParts forces a bigger part",
			// At defaultPartSize per part this would need far more than
			// maxParts parts, so partSizeFor must scale the part size up.
			int64(maxParts) * defaultPartSize * 4,
			// ceil(size / maxParts)
			(int64(maxParts)*defaultPartSize*4 + maxParts - 1) / maxParts,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := partSizeFor(c.size)
			if got != c.want {
				t.Fatalf("partSizeFor(%d) = %d, want %d", c.size, got, c.want)
			}
			if got < minPartSize {
				t.Fatalf("partSizeFor(%d) = %d, below minPartSize %d", c.size, got, int64(minPartSize))
			}
			parts := (c.size + got - 1) / got
			if parts > maxParts {
				t.Fatalf("partSizeFor(%d) = %d implies %d parts, exceeds maxParts %d", c.size, got, parts, maxParts)
			}
		})
	}
}

// TestMultipartThresholdForTestHook pins SetMultipartThresholdForTest's
// restore behavior: it must put back the exact previous value, including
// the package default, so one test's override can never leak into another.
func TestMultipartThresholdForTestHook(t *testing.T) {
	orig := multipartThreshold
	restore := SetMultipartThresholdForTest(1234)
	if multipartThreshold != 1234 {
		t.Fatalf("multipartThreshold = %d, want 1234", multipartThreshold)
	}
	restore()
	if multipartThreshold != orig {
		t.Fatalf("multipartThreshold after restore = %d, want original %d", multipartThreshold, orig)
	}
}

// TestConcurrentPartUploadReportsParentCancellation pins a defensive check
// added after review: concurrentPartUpload's producer goroutine has only
// ONE early exit, <-cctx.Done(), so "the producer stopped before feeding
// every part" and "firstErr != nil" are supposed to be the same event —
// but that equivalence held only because putMultipart is the sole caller
// and always passes context.Background() (never cancellable) in. Nothing
// in concurrentPartUpload itself enforced that; its own signature takes a
// ctx and derives cctx from it, exactly the ordinary, correct-looking shape
// that invites a future caller to thread a real request-scoped context
// through. If that ever happens and the PARENT ctx is canceled externally
// (not by a part failure), the old code would return a nil error alongside
// a parts slice containing zero-value (unfilled) CompletedPart entries for
// whatever parts the producer never got to hand out — silent corruption,
// not a reported failure.
//
// numParts=0 makes this deterministic without needing any real S3 call
// (s.cl is never touched: zero workers are spawned, and the producer's
// `for partNum := 1; partNum <= 0; ...` loop body never executes even
// once, so it can't race the also-zero number of receivers) — a pure unit
// test of the ctx.Err() check itself, not a timing-dependent proof.
func TestConcurrentPartUploadReportsParentCancellation(t *testing.T) {
	s := &S3{bucket: "unused"} // s.cl deliberately nil/unused — see doc comment above
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // parent already canceled before concurrentPartUpload even starts

	ra := bytes.NewReader(nil)
	parts, err := s.concurrentPartUpload(ctx, "key", "fk", aws.String("upload-id"), ra, 0, 0, defaultPartSize, 0)
	if err == nil {
		t.Fatal("concurrentPartUpload must report an error when the parent context is already canceled, not silently return zero-value parts")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled (or wrapping it)", err)
	}
	if parts != nil {
		t.Fatalf("parts = %v, want nil alongside a reported cancellation error", parts)
	}
}
