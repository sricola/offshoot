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
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
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

// s3ResponseHeaderTimeout bounds how long any S3 call waits for its
// response headers to BEGIN arriving once the request has been sent. It
// deliberately does NOT bound the body transfer — a slow-but-progressing
// multi-GiB download or upload can never be killed by it — which is
// exactly why it is safe (and intended) to apply to EVERY call this
// client makes, Get/Put/List included, not just the multipart path: a
// healthy backend sends response headers within seconds regardless of
// object size, while a stalled or hostile endpoint that accepts the TCP
// connection and never responds would otherwise block a call forever (the
// SDK's default HTTP client sets dial and TLS-handshake timeouts but no
// response-header timeout at all — security audit 2, LOW-1: one such
// stall in an UploadPart wedged flush AND Session.Close for good). 60s is
// deliberately generous — the value only needs to be finite, not tight,
// and must ride out an S3 brown-out without tripping. No TOTAL request
// timeout is set at this layer, equally deliberately: that WOULD kill
// legitimate large transfers mid-body.
const s3ResponseHeaderTimeout = 60 * time.Second

// singleShotRPCTimeout is the per-call context.WithTimeout deadline every
// buffered single-shot S3 RPC runs under: Get (including its io.ReadAll of
// the body), Put, PutIf, each List page, Delete, each DeleteObjects batch,
// CopyObject's HeadObject and single-request copy, and the below-threshold
// single PutObject inside PutReader/PutReaderIf. It closes the residual the
// multipart-only deadlines (multipartRPCTimeout, s3_multipart.go) left
// open: s3ResponseHeaderTimeout only bounds waiting for response HEADERS to
// begin (and only starts once the request body has been fully written), so
// a backend that accepts a request and then stalls mid-body — or simply
// stops reading an upload — could still wedge any of these calls forever,
// several of which sit on the daemon's flush path under flushMu (so
// Session.Close hung too, the same shape as security audit 2, LOW-1).
//
// This flat value is the WHOLE deadline only for calls with no meaningful
// payload or no size known at call time — List pages, Delete, each
// DeleteObjects batch, HeadObject, and Get (on this backend the buffered
// Get carries refs/manifests/tombstones, small metadata objects; chain
// members stream via GetReader, which has its own watchdog instead) — all
// of which a healthy backend answers in seconds, so 15 minutes is already
// absurdly generous headroom, matching multipartRPCTimeout's flat value
// for its own metadata RPCs (Create/Complete). For calls whose payload
// size IS known (Put/PutIf, the below-threshold single PutObject in
// PutReader/PutReaderIf, and CopyObject's single-request copy),
// singleShotDeadline additionally scales this base by the size — see its
// doc comment: a flat 15 minutes alone would impose an effective ~5.7
// MiB/s sustained-throughput floor on a just-under-5-GiB transfer, killing
// exactly the legitimate slow transfer this layer must never kill.
//
// This is a package var, not a const, ONLY so tests can shrink it — a test
// proving the deadline fires cannot wait 15 real minutes. It is the BASE
// component of singleShotDeadline, so shrinking it shrinks every sized
// deadline too (test payloads are small, so their size component is ~0 and
// the hook value is effectively the whole deadline). Never set it outside
// a test; production code must never assign to it. See
// SetSingleShotRPCTimeoutForTest (export_test.go).
var singleShotRPCTimeout = 15 * time.Minute

// singleShotFloorBytesPerSecond is the pessimistic sustained-throughput
// floor singleShotDeadline sizes a known payload's transfer allowance to —
// the SAME 1 MiB/s floor multipartRPCTimeout's 15 minutes is derived from
// (~550 MiB worst-case part / 1 MiB/s ≈ 9.2 min, plus headroom; see its
// doc comment). Kept as a named constant so the two layers' sizing can
// never silently diverge: if one floor is ever revisited, this comment and
// that one point at each other.
const singleShotFloorBytesPerSecond = 1 << 20 // 1 MiB/s

// singleShotDeadline returns the per-call deadline for a buffered
// single-shot RPC carrying a payload of the given size (0 for calls with
// no meaningful payload or unknown size): the flat singleShotRPCTimeout
// base plus the time the payload needs at singleShotFloorBytesPerSecond.
// Mirroring multipartRPCTimeout's per-part math at whole-object scale is
// what keeps this layer's promise honest: a single-shot call may carry up
// to the full 5 GiB multipartThreshold in one request, which at the 1
// MiB/s floor needs ~85 minutes — a flat 15 minutes would kill it on
// every attempt, permanently (e.g. a ~4 GiB below-threshold snapshot
// flush over a ~20 Mbps uplink legitimately takes ~27 minutes). The point
// remains "eventually unwedges", never "fails fast".
func singleShotDeadline(size int64) time.Duration {
	d := singleShotRPCTimeout
	if size > 0 {
		d += time.Duration(size/singleShotFloorBytesPerSecond) * time.Second
	}
	return d
}

// singleShotCtx returns the context every buffered single-shot S3 RPC runs
// under: deadline singleShotDeadline(size), with size 0 for calls with no
// meaningful payload or no size known at call time (see
// singleShotRPCTimeout's doc comment for which calls those are and why the
// flat base suffices for them). Every call site follows the same shape:
// obtain the pair, make the SDK call, cancel as soon as the call's result —
// INCLUDING any body read the method itself performs (Get's io.ReadAll) —
// is fully consumed. Loops (List's pagination, DeleteObjects' batches) get
// a fresh pair per iteration and cancel it before the next, never a
// deferred cancel that would accumulate.
func singleShotCtx(size int64) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), singleShotDeadline(size))
}

// readProgressTimeout is GetReader's per-Read progress watchdog window: the
// returned stream is killed only if a single blocking Read makes no
// progress at all for this long (see watchdogReader). Unlike
// singleShotRPCTimeout it never bounds a stream's TOTAL lifetime — a
// legitimate hours-long read of a huge object is fine as long as bytes keep
// arriving — so it can be tight: a healthy backend mid-body delivers SOME
// bytes well within 60 seconds, the same order of patience as
// s3ResponseHeaderTimeout's wait for headers.
//
// This is a package var, not a const, ONLY so tests can shrink it. Never
// set it outside a test; production code must never assign to it. See
// SetReadProgressTimeoutForTest (export_test.go).
var readProgressTimeout = 60 * time.Second

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
	// Bound every call's wait for response headers to begin — see
	// s3ResponseHeaderTimeout's doc comment. BuildableClient is the SDK's
	// own default client type, so its standard dial/TLS/connection-pool
	// settings are preserved; only ResponseHeaderTimeout is added on top.
	loadOpts = append(loadOpts, awsconfig.WithHTTPClient(
		awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			tr.ResponseHeaderTimeout = s3ResponseHeaderTimeout
		})))
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
	// Deferred, not called right after GetObject returns: the SDK call
	// returns once headers arrive, but the body is read by the io.ReadAll
	// below, which must still run under this deadline — canceling earlier
	// would kill every Get's body read; canceling later than function exit
	// is impossible. The deferred cancel covers every return path. Size 0
	// (flat deadline): the response size isn't known until headers arrive,
	// and buffered Gets on this backend carry small metadata objects —
	// large chain members stream via GetReader instead (see
	// singleShotRPCTimeout's doc comment).
	ctx, cancel := singleShotCtx(0)
	defer cancel()
	out, err := s.cl.GetObject(ctx, &s3.GetObjectInput{
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
//
// Stall protection: the returned stream outlives this call, so it CANNOT
// run under a singleShotRPCTimeout-style total deadline the way Get does —
// that would kill a legitimate long read of a large object. Instead the
// request runs under a cancelable context and the Body is wrapped in a
// watchdogReader: a single Read that blocks for readProgressTimeout with no
// bytes at all cancels the request, failing the Read with a recognizable
// "read stalled" error, while any progress at all re-arms the window and a
// slow CONSUMER (long pauses BETWEEN Reads) is never affected — see
// watchdogReader's doc comment. The stream's only production consumer
// (ops' lazyReader, chain materialization) treats any Read error as fatal
// and closes everything, so the watchdog error needs no special handling.
func (s *S3) GetReader(key string) (io.ReadCloser, string, error) {
	fk, err := s.full(key)
	if err != nil {
		return nil, "", err
	}
	ctx, cancel := context.WithCancel(context.Background())
	out, err := s.cl.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
	})
	if err != nil {
		cancel()
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("store: s3 get %s: %w", key, err)
	}
	return newWatchdogReader(out.Body, cancel, key), aws.ToString(out.ETag), nil
}

// watchdogReader wraps a GetObject response Body with a per-Read progress
// watchdog: each Read arms a timer for readProgressTimeout on entry and
// stops it on exit, so the timer can ONLY fire while a Read is actually
// blocked inside the underlying body. That asymmetry is the whole design:
// a stalled PRODUCER (the backend delivering no bytes) leaves Read blocked
// until the timer fires and cancels the request context, which unblocks the
// Read with an error; a slow CONSUMER (arbitrarily long pauses BETWEEN
// Reads) never has an armed timer at all and is never killed. Any Read that
// returns — even one byte — disarms the timer, so a slow-but-progressing
// transfer re-earns a full window on every call.
//
// Concurrency/race notes:
//   - The timer is created ONCE (already stopped) in newWatchdogReader, so
//     Read and Close never race on the field itself; Timer.Reset/Stop are
//     safe for concurrent use.
//   - Timer-fire racing Close is harmless by construction: both just call
//     cancel (idempotent) and Timer.Stop (idempotent).
//   - A timer that fires just as its Read returns successfully has canceled
//     the request context, so the NEXT Read fails — spurious in principle,
//     but only reachable when a Read consumed the entire window anyway,
//     i.e. the stream was already at the edge of the stall definition.
//
// The watchdog-fire error wraps the underlying transport error (which
// carries context.Canceled) in a recognizable "read stalled" message.
// Callers already treat any Read error from this stream as fatal — the only
// production consumer is ops' lazyReader, whose error path closes every
// stream and propagates — so no caller needs to distinguish it.
type watchdogReader struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	key     string
	timer   *time.Timer
	stalled atomic.Bool
}

func newWatchdogReader(body io.ReadCloser, cancel context.CancelFunc, key string) *watchdogReader {
	w := &watchdogReader{body: body, cancel: cancel, key: key}
	w.timer = time.AfterFunc(readProgressTimeout, func() {
		w.stalled.Store(true)
		w.cancel()
	})
	w.timer.Stop() // created disarmed; armed only for the duration of each Read
	return w
}

func (w *watchdogReader) Read(p []byte) (int, error) {
	w.timer.Reset(readProgressTimeout)
	n, err := w.body.Read(p)
	w.timer.Stop()
	if err != nil && err != io.EOF && w.stalled.Load() {
		err = fmt.Errorf("store: s3 read %s: read stalled for %v with no progress: %w",
			w.key, readProgressTimeout, err)
	}
	return n, err
}

func (w *watchdogReader) Close() error {
	w.timer.Stop()
	w.cancel()
	return w.body.Close()
}

func (s *S3) Put(key string, data []byte) error {
	fk, err := s.full(key)
	if err != nil {
		return err
	}
	ctx, cancel := singleShotCtx(int64(len(data)))
	_, err = s.cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		Body: bytes.NewReader(data),
	})
	cancel()
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
	ctx, cancel := singleShotCtx(int64(len(data)))
	out, err := s.cl.PutObject(ctx, in)
	cancel()
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
	ctx, cancel := singleShotCtx(size)
	_, err = s.cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
		Body: r, ContentLength: aws.Int64(size),
	})
	cancel()
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
	ctx, cancel := singleShotCtx(size)
	out, err := s.cl.PutObject(ctx, in)
	cancel()
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
		// A fresh deadline per page, canceled before the next iteration —
		// deliberately NOT a deferred cancel, which would accumulate one
		// live context per page across the whole pagination.
		ctx, cancel := singleShotCtx(0)
		page, err := p.NextPage(ctx)
		cancel()
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

	hctx, hcancel := singleShotCtx(0)
	head, err := s.cl.HeadObject(hctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fsrc),
	})
	hcancel()
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
		// Background here, per-RPC multipartRPCTimeout deadlines inside —
		// see copyObjectMultipart's doc comment.
		return s.copyObjectMultipart(context.Background(), dst, fsrc, fdst, size)
	}

	// Sized by the HEAD-reported source size: the copy is server-side (no
	// bytes flow through this process), so it is normally far faster than a
	// client upload of the same size, but a 5 GiB copy still deserves the
	// same transfer headroom rather than betting the flat base covers it.
	ctx, cancel := singleShotCtx(size)
	_, err = s.cl.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(fdst),
		CopySource: aws.String(s3CopySource(s.bucket, fsrc)),
	})
	cancel()
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
	ctx, cancel := singleShotCtx(0)
	_, err = s.cl.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk),
	})
	cancel()
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
		// A fresh deadline per batch, canceled before the next iteration —
		// same no-deferred-cancel-in-a-loop discipline as List's pagination.
		ctx, cancel := singleShotCtx(0)
		out, cerr := s.cl.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			// Quiet=false so the response lists every Deleted key — the
			// contract's "which keys actually succeeded" needs them.
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)},
		})
		cancel()
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
