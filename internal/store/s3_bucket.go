package store

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// EnsureBucket creates cfg.Bucket if it does not already exist. It exists
// for disposable local S3-compatible servers that boot with no buckets at
// all — CI's RustFS container, `make bench-s3` — so the conformance suite
// and benchmarks need no separate client image to make one. A bucket that
// already exists (owned by this account or anyone else) is left alone.
// Nothing in offshoot's own operations ever calls this: a store is always
// attached to a bucket the operator already provisioned, and offshoot never
// creates buckets on a real provider on its own.
func EnsureBucket(ctx context.Context, cfg S3Config) error {
	s, err := NewS3(ctx, cfg)
	if err != nil {
		return err
	}
	_, err = s.cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return err
}
