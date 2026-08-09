package store

import "testing"

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
