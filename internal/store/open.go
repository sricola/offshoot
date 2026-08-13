package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// OpenBackend resolves a store spec to a Backend and verifies that it
// enforces conditional writes.
//
// Specs:
//
//	/path or ./path     local directory
//	file:///abs/path    local directory
//	s3://bucket/prefix  S3-compatible bucket (AWS S3, R2, MinIO)
//
// S3 endpoint, region and addressing style come from OFFSHOOT_S3_ENDPOINT,
// OFFSHOOT_S3_REGION and OFFSHOOT_S3_PATH_STYLE; credentials come from the
// AWS SDK default chain.
//
// OpenBackend runs unconditionally on every call, i.e. every CLI invocation:
// ProbeCAS's ~8 sequential round trips (see probe.go) are re-paid each time
// against a remote store, not cached across invocations. That is deliberate
// fail-closed behavior, not an oversight — a long-lived daemon (Plan 4) is
// the intended way to amortize the probe across many operations instead of
// weakening or skipping it here.
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
		cfg, err := s3ConfigFromURL(u)
		if err != nil {
			return nil, err
		}
		return NewS3(ctx, cfg)
	default:
		return nil, fmt.Errorf("store: unsupported store scheme %q (use a path, file://, or s3://)", u.Scheme)
	}
}

// s3ConfigFromURL builds an S3Config from a parsed s3:// spec URL, resolving
// endpoint/region/path-style from the OFFSHOOT_S3_* env vars. This is the
// single place that resolution happens: both newBackend (which opens the
// actual backend) and StoreIdentity (which derives a cache key for local
// state like checkout directories) call it, so the two cannot drift apart —
// a spec that StoreIdentity treats as pointing at one endpoint always opens
// against that same endpoint, and vice versa.
func s3ConfigFromURL(u *url.URL) (S3Config, error) {
	if u.Host == "" {
		return S3Config{}, fmt.Errorf("store: s3 spec needs a bucket: s3://bucket[/prefix]")
	}
	return S3Config{
		Bucket:       u.Host,
		Prefix:       strings.Trim(u.Path, "/"),
		Endpoint:     os.Getenv("OFFSHOOT_S3_ENDPOINT"),
		Region:       os.Getenv("OFFSHOOT_S3_REGION"),
		UsePathStyle: isTrue(os.Getenv("OFFSHOOT_S3_PATH_STYLE")),
	}, nil
}

// StoreIdentity returns a canonical identity string for spec: it captures
// the fully resolved backend configuration (including env-derived S3
// settings), not just the literal spec text. Two spec strings that resolve
// to the same backend (e.g. "s3://b/p" and "s3://b/p/") yield the same
// identity; the same spec string resolving to different backends across
// sessions (e.g. OFFSHOOT_S3_ENDPOINT pointed at MinIO one session and real
// AWS the next) yields different identities.
//
// This exists so callers that need a stable, collision-safe cache key for
// per-store local state (e.g. ops.checkoutRoot's checkout cache directory)
// can key off what the store actually IS rather than the raw spec string,
// which can be ambiguous.
func StoreIdentity(spec string) (string, error) {
	if spec == "" {
		return "", fmt.Errorf("store: empty store spec")
	}
	if !strings.Contains(spec, "://") {
		return canonicalPath(spec)
	}
	u, err := url.Parse(spec)
	if err != nil {
		return "", fmt.Errorf("store: invalid store spec %q: %w", spec, err)
	}
	switch u.Scheme {
	case "file":
		return canonicalPath(u.Path)
	case "s3":
		cfg, err := s3ConfigFromURL(u)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("s3|%s|%s|%s|%s|%t", cfg.Bucket, cfg.Prefix, cfg.Endpoint, cfg.Region, cfg.UsePathStyle), nil
	default:
		return "", fmt.Errorf("store: unsupported store scheme %q (use a path, file://, or s3://)", u.Scheme)
	}
}

// canonicalPath returns the cleaned absolute form of a local filesystem
// path, so equivalent spellings (relative vs. absolute, trailing slash,
// "..") resolve to the same identity.
func canonicalPath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("store: resolving path %q: %w", p, err)
	}
	return filepath.Clean(abs), nil
}

func isTrue(v string) bool {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
