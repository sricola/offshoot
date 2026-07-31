package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// OpenBackend resolves a store spec to a Backend and verifies that it
// enforces conditional writes.
//
// Specs:
//
//	/path or ./path     local directory
//	file:///abs/path    local directory
//	s3://bucket/prefix  S3-compatible bucket (AWS S3, R2, Tigris, MinIO)
//
// S3 endpoint, region and addressing style come from OFFSHOOT_S3_ENDPOINT,
// OFFSHOOT_S3_REGION and OFFSHOOT_S3_PATH_STYLE; credentials come from the
// AWS SDK default chain.
func OpenBackend(ctx context.Context, spec string) (Backend, error) {
	b, err := newBackend(ctx, spec)
	if err != nil {
		return nil, err
	}
	if err := ProbeCAS(b); err != nil {
		return nil, fmt.Errorf("store %s cannot be used: %w", spec, err)
	}
	return b, nil
}

func newBackend(ctx context.Context, spec string) (Backend, error) {
	if spec == "" {
		return nil, fmt.Errorf("store: empty store spec")
	}
	if !strings.Contains(spec, "://") {
		return NewLocal(spec)
	}
	u, err := url.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("store: invalid store spec %q: %w", spec, err)
	}
	switch u.Scheme {
	case "file":
		return NewLocal(u.Path)
	case "s3":
		if u.Host == "" {
			return nil, fmt.Errorf("store: s3 spec needs a bucket: s3://bucket[/prefix]")
		}
		return NewS3(ctx, S3Config{
			Bucket:       u.Host,
			Prefix:       strings.Trim(u.Path, "/"),
			Endpoint:     os.Getenv("OFFSHOOT_S3_ENDPOINT"),
			Region:       os.Getenv("OFFSHOOT_S3_REGION"),
			UsePathStyle: isTrue(os.Getenv("OFFSHOOT_S3_PATH_STYLE")),
		})
	default:
		return nil, fmt.Errorf("store: unsupported store scheme %q (use a path, file://, or s3://)", u.Scheme)
	}
}

func isTrue(v string) bool {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
