package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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

func isNotFound(err error) bool {
	if statusOf(err) == http.StatusNotFound {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

// isPreconditionFailed reports whether err is a conditional-write rejection.
// S3 returns 412 PreconditionFailed for a failed condition and 409
// ConditionalRequestConflict when a concurrent write raced ours; both mean
// "your compare failed, retry".
func isPreconditionFailed(err error) bool {
	switch statusOf(err) {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
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
