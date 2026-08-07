package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Config describes an S3-compatible bucket. Credentials come from the AWS
// SDK default chain (env, shared config, IAM role).
type S3Config struct {
	Bucket       string
	Prefix       string // optional key prefix, no leading slash; "" allowed
	Endpoint     string // optional custom endpoint (R2/Tigris/MinIO/fake)
	Region       string // defaults to "auto" when Endpoint is set, else SDK default chain
	UsePathStyle bool   // required for MinIO and the fake
}

// S3 is a Backend over S3-compatible object storage using conditional writes
// for compare-and-swap. Etags are provider-issued opaque tokens: they are
// returned and replayed verbatim, never parsed or compared to a hash.
type S3 struct {
	cl     *s3.Client
	bucket string
	prefix string
}

func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("store: s3 bucket is required")
	}
	region := cfg.Region
	if region == "" && cfg.Endpoint != "" {
		region = "auto"
	}
	var loadOpts []func(*awsconfig.LoadOptions) error
	if region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("store: load aws config: %w", err)
	}
	cl := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		if cfg.UsePathStyle {
			o.UsePathStyle = true
		}
	})
	prefix := strings.Trim(cfg.Prefix, "/")
	return &S3{cl: cl, bucket: cfg.Bucket, prefix: prefix}, nil
}

// full maps a backend key to a bucket key.
func (s *S3) full(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	if s.prefix == "" {
		return key, nil
	}
	return s.prefix + "/" + key, nil
}

func (s *S3) strip(full string) string {
	if s.prefix == "" {
		return full
	}
	return strings.TrimPrefix(full, s.prefix+"/")
}

// statusOf extracts the HTTP status from an SDK error, or 0.
func statusOf(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// isNotFound reports whether err means "the key is absent" — never "the
// bucket is absent" (NoSuchBucket must surface loud, not look like an empty
// store). The typed API error code is authoritative when present; the raw
// HTTP status is only a fallback for errors that carry no API error code at
// all (e.g. transport-level responses the SDK couldn't parse into one).
func isNotFound(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
		return false
	}
	return statusOf(err) == http.StatusNotFound
}

// isPreconditionFailed reports whether err is a conditional-write rejection.
// S3 returns 412 PreconditionFailed for a failed condition and 409
// ConditionalRequestConflict when a concurrent write raced ours; both mean
// "your compare failed, retry". The typed API error code is authoritative
// when present; a bare HTTP 409 can also mean an unrelated conflict (Object
// Lock/retention, OperationAborted), so status is only a fallback when no
// API error code is available.
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
		return false
	}
	switch statusOf(err) {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return true
	}
	return false
}

func (s *S3) Get(key string) ([]byte, string, error) {
	fk, err := s.full(key)
	if err != nil {
		return nil, "", err
	}
	out, err := s.cl.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("store: s3 get %s: %w", key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("store: s3 read %s: %w", key, err)
	}
	return data, aws.ToString(out.ETag), nil
}

func (s *S3) Put(key string, data []byte) error {
	fk, err := s.full(key)
	if err != nil {
		return err
	}
	_, err = s.cl.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		Body: bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("store: s3 put %s: %w", key, err)
	}
	return nil
}

func (s *S3) PutIf(key string, data []byte, ifMatch string) (string, error) {
	fk, err := s.full(key)
	if err != nil {
		return "", err
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		Body: bytes.NewReader(data),
	}
	if ifMatch == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(ifMatch)
	}
	out, err := s.cl.PutObject(context.Background(), in)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", fmt.Errorf("%w: %s", ErrCAS, key)
		}
		// An If-Match against a missing key is a failed compare, not an error.
		if ifMatch != "" && isNotFound(err) {
			return "", fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch)
		}
		return "", fmt.Errorf("store: s3 conditional put %s: %w", key, err)
	}
	return aws.ToString(out.ETag), nil
}

func (s *S3) List(prefix string) ([]string, error) {
	fp, err := s.full(prefix + "x") // validate; the sentinel char is discarded
	if err != nil {
		return nil, err
	}
	fp = strings.TrimSuffix(fp, "x")
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.cl, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(fp),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("store: s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, s.strip(aws.ToString(obj.Key)))
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// copyObjectMaxBytes is the largest object store.S3.CopyObject will copy
// server-side. S3's single-request "PUT Object - Copy" (what CopyObject
// below issues) supports source objects up to 5 GiB; anything larger
// requires the multipart UploadPartCopy API, which this backend does not
// implement — that's out of scope for Task 6b (server-side CopyObject),
// same as it was for Task 6a (reflink locally). Rather than let an
// oversized copy fail loudly with an opaque SDK/API error, CopyObject
// checks the source's size first (a HEAD request) and returns the
// ErrCopyUnsupported sentinel for anything over the limit — the same
// signal Task 6a already wired ops.Fork's fast path to treat as "fall back
// to the slow, materialize-and-re-encode path", which handles objects of
// any size (at a real per-byte cost, but correctly).
const copyObjectMaxBytes = 5 * 1024 * 1024 * 1024 // 5 GiB

// s3CopySource builds the value CopyObjectInput.CopySource requires:
// "bucket/key", with the bucket and each key path segment percent-encoded
// per S3's contract (the API models this as a single opaque, URL-encoded
// string, not separate bucket/key fields, even though every other
// operation here takes them separately). offshoot's own key alphabet
// ([a-z0-9-_.] plus '/' separators — see store.ValidateName and
// store.SnapshotKey/SegmentKey) never actually needs escaping, but building
// this correctly for an arbitrary key costs nothing and avoids depending on
// that alphabet staying that way forever.
func s3CopySource(bucket, key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return url.PathEscape(bucket) + "/" + strings.Join(segs, "/")
}

// CopyObject makes dst a server-side copy of src, without downloading or
// re-uploading the bytes through this process — see copyObjectMaxBytes for
// the one case (objects over S3's 5GB single-request CopyObject limit)
// where it declines and returns ErrCopyUnsupported instead, and
// store.Backend's doc comment for the overwrite-on-existing-dst contract
// this honors (S3's CopyObject overwrites dst natively; there is nothing
// extra to do here for that).
//
// The size check is a HEAD request before the copy, not a check against
// whatever CopyObject itself returns on failure: S3's actual behavior for
// an over-limit single-request copy is an EntityTooLarge error, which this
// backend could also detect and translate, but checking first means a
// caller never pays for (and never has to unwind after) an API call that
// was always going to fail — and it is the same HEAD this method needs
// anyway to translate a missing source into store.ErrNotFound, since a 404
// from CopyObject itself is not reliably distinguishable between "source
// missing" and "destination bucket misconfigured" the way isNotFound's
// typed-error path is for Get.
func (s *S3) CopyObject(dst, src string) error {
	fsrc, err := s.full(src)
	if err != nil {
		return err
	}
	fdst, err := s.full(dst)
	if err != nil {
		return err
	}

	head, err := s.cl.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fsrc),
	})
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("store: s3 head %s (for copy): %w", src, err)
	}
	if size := aws.ToInt64(head.ContentLength); size > copyObjectMaxBytes {
		return ErrCopyUnsupported
	}

	_, err = s.cl.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(fdst),
		CopySource: aws.String(s3CopySource(s.bucket, fsrc)),
	})
	if err != nil {
		if isNotFound(err) {
			// The source existed at the HEAD above but is gone now (deleted
			// concurrently — offshoot's own objects are immutable once
			// written, but this backend doesn't get to assume every caller
			// is offshoot). Same "source absent" case the HEAD check exists
			// to catch, so report it the same way.
			return ErrNotFound
		}
		return fmt.Errorf("store: s3 copy %s -> %s: %w", src, dst, err)
	}
	return nil
}

// Delete removes key unconditionally. S3 deliberately does NOT implement
// store.ConditionalDeleter here: DeleteObject has no compare-and-delete
// precondition in the S3 API (If-Match/If-None-Match only apply to
// GetObject/PutObject; DeleteObject accepts no conditional headers at all),
// so there is no honest way to make this a true CAS delete the way Local's
// DeleteIf is. Callers that need delete-time safety on this backend (Task
// 6b's Destroy claim-guard, in particular) get it from the claim-marker
// pattern instead — a CAS-written Ref.Deleting claim that a concurrent
// AcquireLease is taught to refuse — not from this method; see
// store.DeleteRefIf's doc comment for the full reasoning.
func (s *S3) Delete(key string) error {
	fk, err := s.full(key)
	if err != nil {
		return err
	}
	_, err = s.cl.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("store: s3 delete %s: %w", key, err)
	}
	return nil
}
