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
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// GetReader implements store.ReaderGetter: it returns the GetObject
// response's Body directly, without buffering it into memory first (unlike
// Get, which io.ReadAlls it) — so a caller applying a large object (e.g. a
// snapshot/segment during chain materialization) holds only the current
// object's stream open, not its full bytes. Same key namespacing and
// ErrNotFound mapping as Get. The caller MUST Close the returned reader;
// leaving it open leaks the underlying HTTP connection.
func (s *S3) GetReader(key string) (io.ReadCloser, string, error) {
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
	return out.Body, aws.ToString(out.ETag), nil
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

// minPartSize is S3's minimum multipart part size: every part except the
// last one must be at least this large (S3 API constraint).
const minPartSize = 5 * 1024 * 1024 // 5 MiB

// defaultPartSize is the part size store.S3 uses for a multipart upload
// when the object isn't large enough to force a bigger size via maxParts
// (see partSizeFor) — 64 MiB. Part size sets the RPC count and therefore
// (bounded by multipartConcurrency's fan-out) the wall-clock cost of a
// large upload: at 64 MiB and multipartConcurrency==4, a 5 GiB snapshot is
// ~80 UploadPart round trips, 4 in flight at a time — the AWS CLI's smaller
// 8 MiB default is only cheap there because it's paired with 10 concurrent
// part uploads; this backend deliberately uses fewer, larger parts with
// fewer workers (see multipartConcurrency's doc comment for why). Note this
// concurrency only applies on the io.ReaderAt path (putMultipart's doc
// comment); the non-ReaderAt fallback is still strictly sequential, so a
// smaller part size there is still strictly worse, never safer.
//
// This also sets the non-io.ReaderAt (or non-io.Seeker) fallback's
// per-part buffer size, which scales with it — up to ~524 MiB for a
// single part of a 5 TiB object once maxParts forces partSizeFor above
// this default. That's a real memory cost, but production never pays it:
// this backend's only caller (flush.go's snapshot upload) always passes an
// *os.File, which satisfies both io.ReaderAt and io.Seeker and so always
// takes the zero-buffer io.NewSectionReader path (see putMultipart's doc
// comment). Only a caller passing some other reader shape — not exercised
// in production today — would actually allocate a partSize-sized buffer.
//
// This is a package var, not a const, ONLY so tests can shrink it —
// exercising "several real parts" against a real ~1 GiB+ payload isn't a
// reasonable ask of a unit test. Never set it outside a test; production
// code must never assign to it. See SetPartSizeForTest (export_test.go).
var defaultPartSize int64 = 64 * 1024 * 1024 // 64 MiB

// maxParts is S3's hard limit on the number of parts a single multipart
// upload may have (S3 API constraint).
const maxParts = 10000

// multipartConcurrency is the number of parts store.S3.putMultipart
// (and store.S3.copyObjectMultipart, for its own client-side bookkeeping —
// though that path's UploadPartCopy calls are server-side and copy
// sequentially, see copyObjectMultipart's doc comment) uploads in parallel
// on the io.ReaderAt path — see putMultipart's doc comment for why the
// non-ReaderAt fallback can NEVER use this and stays at effective
// concurrency 1 regardless of this value.
//
// 4 is a deliberately small default, not a "use all available parallelism"
// number: parts are already large (partSizeFor keeps them at least 5 MiB,
// 64 MiB by default — see defaultPartSize), so a handful of concurrent
// large-part transfers already saturates a typical link; more workers
// mostly adds goroutine/connection overhead and widens the blast radius of
// a burst of S3 503 SlowDown responses, without a proportional throughput
// gain the way many-small-part concurrency (e.g. the AWS CLI's 10 workers
// over 8 MiB parts) needs to get the same effect.
//
// This is a package var, not a const, ONLY so tests can override it — down
// to 1 for a deterministic sequential run (TestS3PutReaderIfMultipartConcurrency1),
// or left/raised above 1 to prove parts really do upload in parallel
// (TestS3PutReaderIfMultipartConcurrencyParallel). Never set it outside a
// test; production code must never assign to it. See
// SetMultipartConcurrencyForTest (export_test.go).
var multipartConcurrency = 4

// multipartThreshold is the object size above which PutReader/PutReaderIf
// switch from a single PutObject to a multipart upload (CreateMultipartUpload
// + UploadPart* + CompleteMultipartUpload). Its default is S3's single-
// PutObject ceiling of 5 GiB — the same limit copyObjectMaxBytes documents
// for CopyObject's separate, still-unimplemented multipart gap (CopyObject
// is untouched by this).
//
// This is a package var, not a const, ONLY so tests can override it — a
// real >5 GiB upload can't be exercised in a test. Never set it outside a
// test; production code must never assign to it. See
// SetMultipartThresholdForTest (export_test.go).
var multipartThreshold int64 = 5 * 1024 * 1024 * 1024 // 5 GiB

// partSizeFor returns the per-part size store.S3 uses to multipart-upload
// an object of the given total size: at least minPartSize and defaultPartSize,
// and large enough that size splits into at most maxParts parts — S3's two
// hard limits on part size and part count. This keeps ANY object size up to
// S3's 5 TiB per-object limit within the 10,000-part ceiling. The final part
// is always whatever remains (<= the computed part size), never padded.
func partSizeFor(size int64) int64 {
	ps := defaultPartSize
	if need := (size + maxParts - 1) / maxParts; need > ps {
		ps = need
	}
	if ps < minPartSize {
		ps = minPartSize
	}
	return ps
}

// PutReader implements store.ReaderPutter's unconditional overwrite. For
// size <= multipartThreshold it issues a single PutObject with Body: r and
// ContentLength: size, so the SDK never needs to buffer r's content to
// determine its length (unlike Put, which wraps an already-in-memory []byte
// in bytes.NewReader). The SDK's own payload-hash-for-signing step (SigV4)
// does not require buffering either: over HTTPS (the normal case) S3 uses
// UNSIGNED-PAYLOAD and skips hashing the body at all; over plain HTTP it
// streams the body through a SHA256 hasher and rewinds (r must support
// io.Seeker, true of the *os.File this backend's only caller — flush.go's
// snapshot upload — passes) rather than holding it in memory.
//
// For size > multipartThreshold (lifting S3's 5 GiB single-PutObject
// ceiling) it instead uses a multipart upload — see putMultipart's doc
// comment for the mechanics, part sizing, and the abort-on-every-error-path
// guarantee that makes this safe to call. PutReader sets no conditions on
// the multipart Complete (unconditional, matching the single-PUT path).
func (s *S3) PutReader(key string, r io.Reader, size int64) error {
	if size > multipartThreshold {
		_, err := s.putMultipart(key, r, size, "", false)
		return err
	}
	fk, err := s.full(key)
	if err != nil {
		return err
	}
	_, err = s.cl.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		Body: r, ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("store: s3 put %s: %w", key, err)
	}
	return nil
}

// PutReaderIf implements store.ReaderPutter's CAS write. For size <=
// multipartThreshold it issues identical ifMatch-to-precondition-header
// translation as PutIf (create-only via IfNoneMatch: "*", CAS via IfMatch)
// on a single PutObject, with Body: r/ContentLength: size in place of a
// buffered []byte — see PutReader's doc comment for why that does not
// require buffering the object.
//
// For size > multipartThreshold it uses a multipart upload instead — see
// putMultipart's doc comment. The condition (IfNoneMatch/IfMatch) is placed
// on CompleteMultipartUpload, not on CreateMultipartUpload or any UploadPart
// call (the SDK's CompleteMultipartUploadInput supports both fields), and a
// precondition rejection there maps to store.ErrCAS via the same
// isPreconditionFailed/isNotFound helpers and the same error wording as the
// single-PUT path below, so CAS semantics are indistinguishable to a
// caller regardless of which path an object's size took.
//
// One observable difference: a multipart object's ETag is NOT its MD5 the
// way a single-PUT object's is — S3 returns "<md5-of-the-part-md5s>-<part
// count>" instead (e.g. "d41d8cd9...-3"), a valid opaque etag for future
// If-Match calls but not a content hash. offshoot never parses or hashes
// etags (see S3's type doc comment), and this backend's only production
// caller (flush.go's snapshot upload) discards PutReaderIf's returned etag
// entirely, so this is harmless in practice — noted here for any future
// caller that might assume otherwise.
func (s *S3) PutReaderIf(key string, r io.Reader, size int64, ifMatch string) (string, error) {
	if size > multipartThreshold {
		return s.putMultipart(key, r, size, ifMatch, true)
	}
	fk, err := s.full(key)
	if err != nil {
		return "", err
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		Body: r, ContentLength: aws.Int64(size),
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

// putMultipart uploads r (exactly size bytes) to key via S3's multipart
// upload API: CreateMultipartUpload, then a sequential loop of UploadPart
// calls (PartNumber 1-based, ascending), then CompleteMultipartUpload with
// the collected parts in PartNumber order. It is the shared implementation
// behind PutReader (conditional == false) and PutReaderIf (conditional ==
// true, ifMatch translated to IfNoneMatch/IfMatch on the Complete call
// exactly as PutReaderIf's doc comment describes).
//
// COST-CRITICAL: an abandoned multipart upload leaves its already-uploaded
// parts stored (and billed) on S3 indefinitely — S3 does not garbage-collect
// them on its own (outside of a bucket lifecycle rule this backend does not
// configure). So once CreateMultipartUpload succeeds, a deferred cleanup
// issues AbortMultipartUpload on EVERY exit path that is not a successful
// Complete: a part-upload error, a part-read error (non-ReaderAt fallback),
// a Complete error (including a CAS/precondition rejection), all funnel
// through the same defer via the named `completed` flag, which is only set
// true immediately after CompleteMultipartUpload succeeds. If the abort
// itself also fails, that failure is appended to (not substituted for) the
// original error, so the original failure is never masked — the caller
// still needs to know the write failed, and the abort failure is surfaced
// too so an operator can investigate the now-possibly-lingering upload.
//
// Part sourcing avoids buffering the whole object: when r is BOTH an
// io.ReaderAt and an io.Seeker (true for the *os.File the production
// caller — flush.go's snapshot upload — passes), each part is an
// io.NewSectionReader(ra, base+offset, n), where base is r's position at
// entry (via Seek(0, SeekCurrent)) — zero buffering, the SDK reads directly
// from the file at each part's offset. The Seeker requirement (not just
// ReaderAt) matters: ReadAt always takes an absolute offset from the start
// of the underlying data, so without capturing base a seeked-then-passed
// reader would silently upload the wrong bytes above multipartThreshold
// while the single-PUT path below it (which reads from r's current
// position via Body: r) would not — see the base offset comment in the
// loop below. When r is not both, each part is instead read fully into a
// single reused buffer sized to one part (bounded memory: at most one
// part's worth, reused across parts, never the whole object).
//
// CONCURRENCY IS ONLY SAFE ON THE io.ReaderAt PATH. When useReaderAt, parts
// upload in parallel (bounded to multipartConcurrency workers — see
// concurrentPartUpload) because io.ReaderAt is documented safe for parallel
// ReadAt calls on the same underlying data: each worker reads its own part
// via its own io.NewSectionReader, no shared mutable state. The non-
// ReaderAt fallback reads every part sequentially from r into ONE REUSED
// buffer — a second worker there would race the shared buffer AND consume r
// out of order (r has no seek/random-access story at all in this branch,
// that's exactly why it's the fallback). So the fallback stays strictly
// sequential, deliberately: giving it a buffer per worker would "fix" the
// race but multiply memory by the worker count for precisely the callers
// who couldn't afford a single partSize-sized buffer in the first place —
// worse, not better. See putMultipart's abort/ordering/checksum guarantees
// below; concurrentPartUpload preserves all three under parallelism.
func (s *S3) putMultipart(key string, r io.Reader, size int64, ifMatch string, conditional bool) (etag string, err error) {
	fk, err := s.full(key)
	if err != nil {
		return "", err
	}
	ctx := context.Background()

	created, err := s.cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		// Explicit, not inferred: the SDK's default RequestChecksumCalculation
		// is WhenSupported (not WhenRequired), so every UploadPart call below
		// gets a CRC32 checksum attached whether or not this field is set —
		// S3 then infers the whole multipart upload's checksum algorithm from
		// the first part it sees. Declaring it here up front makes that
		// contract explicit rather than accidental, and matches what
		// CompleteMultipartUpload below must now supply per part (see the
		// CompletedPart construction in the loop) — S3 rejects Complete with
		// "InvalidRequest: ... must include the checksum for each part" if a
		// part's checksum is dropped, which types.CompletedPart{ETag,
		// PartNumber} alone (the pre-fix shape) silently did.
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return "", fmt.Errorf("store: s3 create multipart upload %s: %w", key, err)
	}
	uploadID := created.UploadId

	completed := false
	defer func() {
		if completed {
			return
		}
		_, aerr := s.cl.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket: aws.String(s.bucket), Key: aws.String(fk), UploadId: uploadID,
		})
		if aerr != nil {
			err = fmt.Errorf("%w (additionally, aborting the multipart upload also failed, so its parts may be left billed on S3: %v)", err, aerr)
		}
	}()

	partSize := partSizeFor(size)
	// The zero-buffer path needs BOTH io.ReaderAt (to read a part without
	// consuming r) AND io.Seeker (to learn r's CURRENT position, since
	// io.ReaderAt.ReadAt always takes an ABSOLUTE offset from the start of
	// the underlying data, not from wherever r happens to be positioned
	// now). Using ReaderAt alone would silently read from absolute offset 0
	// regardless of where the caller had already Seek'd r to — correct for
	// the common case (a freshly opened file) but wrong, silently, for any
	// caller that seeks first and expects the single-PUT path's semantics
	// (Body: r, which always reads from r's current position — see
	// PutReader's doc comment) to carry over. A reader that fails Seek(0,
	// SeekCurrent) (satisfies the interface but errors, e.g. a pipe) also
	// falls back rather than risk reading from the wrong place.
	ra, isReaderAt := r.(io.ReaderAt)
	seeker, isSeeker := r.(io.Seeker)
	var base int64
	useReaderAt := isReaderAt && isSeeker
	if useReaderAt {
		var serr error
		base, serr = seeker.Seek(0, io.SeekCurrent)
		if serr != nil {
			useReaderAt = false
		}
	}
	var buf []byte
	if !useReaderAt {
		buf = make([]byte, partSize)
	}

	var parts []types.CompletedPart
	if useReaderAt {
		// Safe to parallelize — see the CONCURRENCY paragraph in
		// putMultipart's doc comment above and concurrentPartUpload's own
		// doc comment for the worker/ordering/error-handling design.
		numParts := int32((size + partSize - 1) / partSize)
		parts, err = s.concurrentPartUpload(ctx, key, fk, uploadID, ra, base, size, partSize, numParts)
		if err != nil {
			return "", err
		}
	} else {
		// NOT safe to parallelize — see the CONCURRENCY paragraph above.
		// Strictly sequential: one reused buffer, r read in order.
		partNum := int32(1)
		for offset := int64(0); offset < size; offset += partSize {
			n := partSize
			if remain := size - offset; remain < n {
				n = remain
			}

			if _, rerr := io.ReadFull(io.LimitReader(r, n), buf[:n]); rerr != nil {
				return "", fmt.Errorf("store: s3 read part %d for %s: %w", partNum, key, rerr)
			}

			up, uerr := s.cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(s.bucket), Key: aws.String(fk),
				UploadId: uploadID, PartNumber: aws.Int32(partNum),
				Body: bytes.NewReader(buf[:n]), ContentLength: aws.Int64(n),
				// Explicit here too, not just on CreateMultipartUpload above —
				// DO NOT simplify this back out. The SDK only defaults a part's
				// checksum algorithm when RequestChecksumCalculation ==
				// WhenSupported (the SDK default); a caller running with
				// when_required (the common workaround third-party S3-compatible
				// stores need after the Jan-2025 default-checksum change — and
				// this backend explicitly targets R2/Tigris/MinIO via S3Config)
				// would otherwise send this UploadPart with NO checksum while
				// CreateMultipartUpload already declared the upload CRC32,
				// making every CompletedPart's checksum silently absent and
				// CompleteMultipartUpload's declared-vs-supplied check fail on
				// real S3. Setting it here makes the SDK compute CRC32
				// unconditionally, so declared and supplied always agree
				// regardless of RequestChecksumCalculation.
				ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
			})
			if uerr != nil {
				return "", fmt.Errorf("store: s3 upload part %d for %s: %w", partNum, key, uerr)
			}
			// Carry every checksum field UploadPart returned (whichever the
			// SDK actually computed — see CreateMultipartUploadInput's
			// ChecksumAlgorithm comment above for why one is always present
			// in practice) forward into CompletedPart. Dropping these (ETag/
			// PartNumber alone) is what makes CompleteMultipartUpload below
			// fail against real S3 once a checksum was attached to the part.
			parts = append(parts, types.CompletedPart{
				ETag:              up.ETag,
				PartNumber:        aws.Int32(partNum),
				ChecksumCRC32:     up.ChecksumCRC32,
				ChecksumCRC32C:    up.ChecksumCRC32C,
				ChecksumCRC64NVME: up.ChecksumCRC64NVME,
				ChecksumSHA1:      up.ChecksumSHA1,
				ChecksumSHA256:    up.ChecksumSHA256,
			})
			partNum++
		}
	}

	completeIn := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		// Free defense-in-depth against this backend's own offset-arithmetic
		// bugs: S3 rejects Complete with a 400 if the parts' total length
		// doesn't match, instead of silently storing a wrong-length object.
		MpuObjectSize: aws.Int64(size),
	}
	if conditional {
		if ifMatch == "" {
			completeIn.IfNoneMatch = aws.String("*")
		} else {
			completeIn.IfMatch = aws.String(ifMatch)
		}
	}
	out, cerr := s.cl.CompleteMultipartUpload(ctx, completeIn)
	if cerr != nil {
		if conditional && isPreconditionFailed(cerr) {
			return "", fmt.Errorf("%w: %s", ErrCAS, key)
		}
		if conditional && ifMatch != "" && isNotFound(cerr) {
			return "", fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch)
		}
		return "", fmt.Errorf("store: s3 complete multipart upload %s: %w", key, cerr)
	}
	completed = true
	return aws.ToString(out.ETag), nil
}

// concurrentPartUpload uploads all parts of a putMultipart call in
// parallel, bounded to multipartConcurrency simultaneous UploadPart calls.
// Callers MUST only use this on the io.ReaderAt path (see putMultipart's
// CONCURRENCY paragraph) — it reads each part via its own
// io.NewSectionReader(ra, ...), which is what makes concurrent reads of the
// same underlying source (ra) safe with no shared mutable state at all.
//
// Work distribution: part numbers 1..numParts are fed one at a time over an
// UNBUFFERED channel to a small pool of worker goroutines, so a worker
// blocks for its next assignment rather than a producer racing ahead of
// the pool — there is never more than one part's worth of "assigned but not
// yet started" work outstanding per idle worker.
//
// Ordering with no locking: parts is pre-sized to numParts up front, and
// each worker writes its result to parts[partNum-1] — its own, permanently
// distinct index. Go permits concurrent writes to DIFFERENT elements of the
// same slice with no synchronization (only same-element access races), so
// results land in ascending PartNumber order for free, regardless of which
// order the underlying UploadPart calls actually complete in — no sort
// step, no mutex, no risk of the eventual CompleteMultipartUpload seeing
// gaps or duplicates.
//
// First-error-wins, cancel-the-rest: the first UploadPart failure is
// captured via sync.Once (so a second failure — including one caused by the
// cancellation below — can never overwrite the actually-first error) and
// immediately cancels a context.WithCancel derived from ctx. Every
// in-flight and not-yet-started UploadPart call observes that cancellation
// promptly (the producer goroutine also stops handing out new part numbers
// once it sees Done()), so a large upload fails fast on the first bad part
// instead of paying for every remaining one. The function still waits
// (sync.WaitGroup) for every worker to actually return before it returns
// itself, so putMultipart's caller never issues AbortMultipartUpload while
// one of this upload's UploadPart calls might still be in flight.
func (s *S3) concurrentPartUpload(ctx context.Context, key, fk string, uploadID *string, ra io.ReaderAt, base, size, partSize int64, numParts int32) ([]types.CompletedPart, error) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	parts := make([]types.CompletedPart, numParts)
	jobs := make(chan int32)

	var firstErrOnce sync.Once
	var firstErr error
	recordErr := func(e error) {
		firstErrOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	workers := multipartConcurrency
	if workers < 1 {
		workers = 1
	}
	if int32(workers) > numParts {
		workers = int(numParts)
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for partNum := range jobs {
				offset := int64(partNum-1) * partSize
				n := partSize
				if remain := size - offset; remain < n {
					n = remain
				}
				up, uerr := s.cl.UploadPart(cctx, &s3.UploadPartInput{
					Bucket: aws.String(s.bucket), Key: aws.String(fk),
					UploadId: uploadID, PartNumber: aws.Int32(partNum),
					Body: io.NewSectionReader(ra, base+offset, n), ContentLength: aws.Int64(n),
					// See the identical ChecksumAlgorithm comment on the
					// sequential-fallback UploadPart call in putMultipart —
					// the same "must be explicit, not inferred" reasoning
					// applies here unchanged.
					ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
				})
				if uerr != nil {
					recordErr(fmt.Errorf("store: s3 upload part %d for %s: %w", partNum, key, uerr))
					continue
				}
				// Same checksum-field carry-forward as the sequential path;
				// see putMultipart's CompletedPart construction comment.
				parts[partNum-1] = types.CompletedPart{
					ETag:              up.ETag,
					PartNumber:        aws.Int32(partNum),
					ChecksumCRC32:     up.ChecksumCRC32,
					ChecksumCRC32C:    up.ChecksumCRC32C,
					ChecksumCRC64NVME: up.ChecksumCRC64NVME,
					ChecksumSHA1:      up.ChecksumSHA1,
					ChecksumSHA256:    up.ChecksumSHA256,
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for partNum := int32(1); partNum <= numParts; partNum++ {
			select {
			case <-cctx.Done():
				return
			case jobs <- partNum:
			}
		}
	}()

	wg.Wait()
	return parts, firstErr
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
// via a single-request "PUT Object - Copy" (what CopyObject issues below).
// S3 caps that API at 5 GiB source objects; above it, CopyObject switches
// to a multipart server-side copy instead (CreateMultipartUpload + a
// sequence of UploadPartCopy calls + CompleteMultipartUpload — see
// copyObjectMultipart), which is NOT limited by this constant — only by
// s3MaxObjectBytes, S3's actual per-object ceiling. This constant is
// therefore a strategy-selection threshold, not a capability limit: it
// picks the cheaper single-request path when the source is small enough
// for it, not the point past which a copy becomes unsupported.
//
// This is a package var, not a const, ONLY so tests can shrink it — a real
// multi-gigabyte source object isn't a reasonable ask of a unit test, so
// tests lower this (paired with SetPartSizeForTest) to drive a modest real
// payload through the multipart-copy path instead. Never set it outside a
// test; production code must never assign to it. See
// SetCopyObjectMaxBytesForTest (export_test.go).
var copyObjectMaxBytes int64 = 5 * 1024 * 1024 * 1024 // 5 GiB

// s3MaxObjectBytes is S3's hard ceiling on any single object's size, under
// any upload/copy strategy (single PUT, multipart upload, or multipart
// copy) — an S3 API constraint, not something this backend chooses.
// CopyObject returns ErrCopyUnsupported for a source over this size because
// there is genuinely no S3 mechanism that could copy it, unlike the
// copyObjectMaxBytes threshold above, which this backend routes around via
// multipart copy rather than declining.
const s3MaxObjectBytes = 5 * 1024 * 1024 * 1024 * 1024 // 5 TiB

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
// re-uploading the bytes through this process — for sources at or under
// copyObjectMaxBytes (S3's 5 GiB single-request CopyObject limit) via one
// CopyObject call below; for larger sources, up to s3MaxObjectBytes (S3's
// 5 TiB per-object ceiling), via copyObjectMultipart's multipart
// UploadPartCopy sequence instead. Only a source over s3MaxObjectBytes — a
// size no S3 mechanism can copy at all — returns ErrCopyUnsupported; see
// store.Backend's doc comment for the overwrite-on-existing-dst contract
// this honors either way (S3's Copy/CompleteMultipartUpload overwrite dst
// natively; there is nothing extra to do here for that).
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
	size := aws.ToInt64(head.ContentLength)
	if size > s3MaxObjectBytes {
		return ErrCopyUnsupported
	}
	if size > copyObjectMaxBytes {
		return s.copyObjectMultipart(dst, fsrc, fdst, size)
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

// copyObjectMultipart implements CopyObject for a source over
// copyObjectMaxBytes (S3's 5 GiB single-request PUT-Copy ceiling), via
// CreateMultipartUpload + a sequence of UploadPartCopy calls (one per byte
// range, sized the same way putMultipart sizes its parts — see
// partSizeFor, which already keeps every part within UploadPartCopy's own
// 5 GiB per-part maximum) + CompleteMultipartUpload. fsrc and fdst are
// already-prefixed bucket keys (CopyObject has already validated and
// mapped both); size is the source's HEAD-reported length.
//
// Same abort discipline as putMultipart: once CreateMultipartUpload
// succeeds, a deferred AbortMultipartUpload runs on every exit that is not
// a successful Complete (a part-copy failure, an empty/malformed
// CopyPartResult, or a Complete failure), via the same named-`completed`-
// flag pattern — see putMultipart's doc comment for the full reasoning
// (an abandoned multipart upload bills its uploaded parts on S3 forever).
// CompleteMultipartUpload here sets no conditions, matching CopyObject's
// documented OVERWRITE semantics for dst (same as the single-request path
// above).
//
// CopySourceRange arithmetic: UploadPartCopy's CopySourceRange header is
// "bytes=<start>-<end>" with BOTH ends INCLUSIVE — unlike Go slice
// bounds, which are half-open. For a part covering n bytes starting at
// offset, the correct end is offset+n-1, NOT offset+n: using offset+n
// would ask S3 to copy one byte beyond this part (silently pulling in the
// first byte of the next part, or erroring on the source's last part),
// and using offset+n-2 (or any other short end) would silently drop bytes
// from the copy. This is computed once below and worth restating loudly
// because getting it wrong doesn't fail loudly — it produces a copy that
// looks superficially fine (right total length, if the error is
// off-by-one in a way that still sums correctly) but is not actually
// byte-identical to the source.
//
// Parts are copied sequentially, not concurrently like putMultipart's
// client-uploaded parts: UploadPartCopy is a server-side operation (S3
// reads the source and writes the destination itself; no object bytes
// flow through this process or this backend's network link at all), so
// there is no client-side round-trip-bandwidth motivation to parallelize
// it the way client-uploaded parts have. A future improvement, not
// required here.
func (s *S3) copyObjectMultipart(dst, fsrc, fdst string, size int64) (err error) {
	ctx := context.Background()

	created, cerr := s.cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fdst),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if cerr != nil {
		return fmt.Errorf("store: s3 create multipart copy %s: %w", dst, cerr)
	}
	uploadID := created.UploadId

	completed := false
	defer func() {
		if completed {
			return
		}
		_, aerr := s.cl.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket: aws.String(s.bucket), Key: aws.String(fdst), UploadId: uploadID,
		})
		if aerr != nil {
			err = fmt.Errorf("%w (additionally, aborting the multipart copy also failed, so its parts may be left billed on S3: %v)", err, aerr)
		}
	}()

	copySource := s3CopySource(s.bucket, fsrc)
	partSize := partSizeFor(size)
	var parts []types.CompletedPart
	partNum := int32(1)
	for offset := int64(0); offset < size; offset += partSize {
		n := partSize
		if remain := size - offset; remain < n {
			n = remain
		}
		end := offset + n - 1 // INCLUSIVE end — see the doc comment above.

		up, uerr := s.cl.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: aws.String(s.bucket), Key: aws.String(fdst),
			UploadId: uploadID, PartNumber: aws.Int32(partNum),
			CopySource:      aws.String(copySource),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
		})
		if uerr != nil {
			return fmt.Errorf("store: s3 upload part copy %d for %s: %w", partNum, dst, uerr)
		}
		if up.CopyPartResult == nil {
			return fmt.Errorf("store: s3 upload part copy %d for %s: empty CopyPartResult", partNum, dst)
		}
		// Carry every checksum field UploadPartCopy returned, same as
		// putMultipart's UploadPart results — see that function's
		// CompletedPart construction comment for why dropping these makes
		// CompleteMultipartUpload fail against real S3.
		parts = append(parts, types.CompletedPart{
			ETag:              up.CopyPartResult.ETag,
			PartNumber:        aws.Int32(partNum),
			ChecksumCRC32:     up.CopyPartResult.ChecksumCRC32,
			ChecksumCRC32C:    up.CopyPartResult.ChecksumCRC32C,
			ChecksumCRC64NVME: up.CopyPartResult.ChecksumCRC64NVME,
			ChecksumSHA1:      up.CopyPartResult.ChecksumSHA1,
			ChecksumSHA256:    up.CopyPartResult.ChecksumSHA256,
		})
		partNum++
	}

	_, cerr2 := s.cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fdst), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		// Same free defense-in-depth as putMultipart: S3 rejects Complete
		// with a 400 if the parts' total length doesn't match size.
		MpuObjectSize: aws.Int64(size),
	})
	if cerr2 != nil {
		return fmt.Errorf("store: s3 complete multipart copy %s: %w", dst, cerr2)
	}
	completed = true
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

// s3DeleteObjectsMaxKeys is the S3 DeleteObjects API's hard per-request
// limit on the number of keys.
const s3DeleteObjectsMaxKeys = 1000

// DeleteObjects implements store.BatchDeleter: keys are chunked into
// batches of at most 1000 (the API's limit) and each batch is one
// DeleteObjects round trip — 80k objects cost ~80 RPCs instead of 80k
// serial DeleteObject calls (perf audit H2). Batches are issued
// sequentially; that already collapses the sweep's wall time by three
// orders of magnitude, and issuing them concurrently would need a rate/
// error story S3's 503-slowdown behavior makes nontrivial, so concurrency
// is deliberately left for later.
//
// Per the interface contract it returns the keys (in the caller's
// un-prefixed form) that were actually deleted, plus an error naming any
// keys the API reported per-key Errors for. A key that did not exist counts
// as deleted (S3's DeleteObjects reports absent keys under Deleted), same
// as Delete. A transport-level batch failure returns the keys deleted by
// the batches that DID complete plus the error.
func (s *S3) DeleteObjects(keys []string) (deleted []string, err error) {
	if len(keys) == 0 {
		return nil, nil
	}
	// Validate every key up front so an invalid key fails the call before
	// any RPC, instead of surfacing mid-sweep with half the work done.
	orig := make(map[string]string, len(keys)) // bucket key -> caller's key
	full := make([]string, len(keys))
	for i, k := range keys {
		fk, ferr := s.full(k)
		if ferr != nil {
			return nil, ferr
		}
		full[i] = fk
		orig[fk] = k
	}
	var failed []string
	for start := 0; start < len(full); start += s3DeleteObjectsMaxKeys {
		end := min(start+s3DeleteObjectsMaxKeys, len(full))
		ids := make([]types.ObjectIdentifier, 0, end-start)
		for _, fk := range full[start:end] {
			ids = append(ids, types.ObjectIdentifier{Key: aws.String(fk)})
		}
		out, cerr := s.cl.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			// Quiet=false so the response lists every Deleted key — the
			// contract's "which keys actually succeeded" needs them.
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)},
		})
		if cerr != nil {
			return deleted, fmt.Errorf("store: s3 batch delete: %w", cerr)
		}
		for _, d := range out.Deleted {
			if k, ok := orig[aws.ToString(d.Key)]; ok {
				deleted = append(deleted, k)
			}
		}
		for _, e := range out.Errors {
			k := aws.ToString(e.Key)
			if o, ok := orig[k]; ok {
				k = o
			}
			// Key + API error code only — never the raw error Message, which
			// providers have been known to stuff request-signing detail into.
			failed = append(failed, fmt.Sprintf("%s (%s)", k, aws.ToString(e.Code)))
		}
	}
	if len(failed) > 0 {
		return deleted, fmt.Errorf("store: s3 batch delete failed for %d key(s): %s",
			len(failed), strings.Join(failed, ", "))
	}
	return deleted, nil
}
