package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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
// PutObject ceiling of 5 GiB — the same 5 GiB boundary copyObjectMaxBytes
// (s3.go) uses as CopyObject's own strategy-selection threshold between a
// single-request copy and copyObjectMultipart's server-side multipart copy.
//
// This is a package var, not a const, ONLY so tests can override it — a
// real >5 GiB upload can't be exercised in a test. Never set it outside a
// test; production code must never assign to it. See
// SetMultipartThresholdForTest (export_test.go).
var multipartThreshold int64 = 5 * 1024 * 1024 * 1024 // 5 GiB

// multipartRPCTimeout is the per-call context.WithTimeout deadline every
// individual multipart RPC (CreateMultipartUpload, each UploadPart /
// UploadPartCopy, CompleteMultipartUpload, and putMultipart's HeadObject
// preflight) runs under. It exists so a single stalled backend response
// can only ever delay a multipart operation, never wedge it forever
// (security audit 2, LOW-1: putMultipart runs on the flush path holding
// flushMu, so an UploadPart that never returned hung every future Flush
// AND Session.Close) — the transport-level s3ResponseHeaderTimeout (s3.go)
// already catches a response that never BEGINS, and this layer addition-
// ally bounds a response/transfer that begins but then stalls mid-body.
//
// 15 minutes is sized to the WORST-case single RPC, not the common one:
// the largest part partSizeFor can produce is ~550 MiB (a 5 TiB object
// split into maxParts parts), which at a deliberately pessimistic 1 MiB/s
// floor throughput needs ~9.2 minutes to transfer; 15 minutes covers that
// with headroom, and equally covers UploadPartCopy's server-side copy of a
// same-sized part and CompleteMultipartUpload's assembly pause. The point
// is "eventually unwedges", not "fails fast" — a deadline that is too
// tight would kill legitimate slow transfers, which is precisely what the
// transport layer's response-header-only timeout was chosen to avoid.
//
// This is a package var, not a const, ONLY so tests can shrink it — a
// test proving the deadline fires cannot wait 15 real minutes. Never set
// it outside a test; production code must never assign to it. See
// SetMultipartRPCTimeoutForTest (export_test.go).
var multipartRPCTimeout = 15 * time.Minute

// multipartAbortTimeout bounds the deferred AbortMultipartUpload cleanup
// (abortUnlessCompleted): abort is a small metadata-only RPC, so it gets a
// much tighter bound than multipartRPCTimeout — the failure it cleans up
// after may itself have been a stall, and cleanup hanging on the same
// stalled backend would reintroduce exactly the wedge the per-RPC
// deadlines exist to prevent. If the abort times out, its error is
// appended to the original failure (never substituted for it), same as
// any other abort failure.
const multipartAbortTimeout = time.Minute

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

// partLen returns the byte length of the part starting at offset within an
// object of the given total size: partSize for every part except the final
// one, which is whatever remains (<= partSize, never padded — see
// partSizeFor's doc comment).
func partLen(size, offset, partSize int64) int64 {
	if remain := size - offset; remain < partSize {
		return remain
	}
	return partSize
}

// abortUnlessCompleted is the shared deferred cleanup behind putMultipart's
// and copyObjectMultipart's COST-CRITICAL abort guarantee (see
// putMultipart's doc comment: an abandoned multipart upload bills its parts
// on S3 indefinitely): unless *completed was set true — which the callers
// only ever do immediately after a successful CompleteMultipartUpload — it
// issues AbortMultipartUpload for the upload, and if the abort itself also
// fails, appends that failure to (never substitutes it for) *err, so the
// original failure is never masked. what names the operation in the
// appended error ("upload" or "copy"). Use it as
// `defer s.abortUnlessCompleted(&completed, fk, uploadID, &err, "upload")`,
// where completed is the caller's named flag and err its named error
// return.
func (s *S3) abortUnlessCompleted(completed *bool, fk string, uploadID *string, err *error, what string) {
	if *completed {
		return
	}
	// Fresh Background-derived context, deliberately not the (possibly
	// already expired) context the failed upload ran under — the abort must
	// get its own chance to run — but bounded by multipartAbortTimeout so
	// cleanup against the very backend that just stalled cannot hang either.
	actx, cancel := context.WithTimeout(context.Background(), multipartAbortTimeout)
	defer cancel()
	_, aerr := s.cl.AbortMultipartUpload(actx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fk), UploadId: uploadID,
	})
	if aerr != nil {
		*err = fmt.Errorf("%w (additionally, aborting the multipart %s also failed, so its parts may be left billed on S3: %v)", *err, what, aerr)
	}
}

// partChecksums is the ETag-plus-checksum field set a finished part carries
// forward into its types.CompletedPart entry. *s3.UploadPartOutput and
// *types.CopyPartResult expose these same fields under the same names but
// are unrelated SDK types, so each gets a thin adaptor
// (uploadPartChecksums / copyPartChecksums) rather than an interface or
// generics machinery — three call sites don't justify more.
type partChecksums struct {
	etag, crc32, crc32c, crc64nvme, sha1, sha256 *string
}

// Both adaptors use field-keyed (not positional) literals on purpose: all
// six fields are *string, so a future reorder of partChecksums would
// silently cross-wire positional literals — field keys make the compiler
// guard the pairing.
func uploadPartChecksums(up *s3.UploadPartOutput) partChecksums {
	return partChecksums{
		etag:      up.ETag,
		crc32:     up.ChecksumCRC32,
		crc32c:    up.ChecksumCRC32C,
		crc64nvme: up.ChecksumCRC64NVME,
		sha1:      up.ChecksumSHA1,
		sha256:    up.ChecksumSHA256,
	}
}

func copyPartChecksums(cp *types.CopyPartResult) partChecksums {
	return partChecksums{
		etag:      cp.ETag,
		crc32:     cp.ChecksumCRC32,
		crc32c:    cp.ChecksumCRC32C,
		crc64nvme: cp.ChecksumCRC64NVME,
		sha1:      cp.ChecksumSHA1,
		sha256:    cp.ChecksumSHA256,
	}
}

// completedPart builds the types.CompletedPart entry for partNum, carrying
// EVERY checksum field the part's response actually returned. This is the
// one place the next SDK checksum algorithm must be added: dropping a field
// here is what makes CompleteMultipartUpload fail against real S3 once a
// checksum was attached to the part — see putMultipart's CompletedPart
// comments at the call sites for the per-path reasoning (an UploadPart
// result always carries one in practice; a CopyPartResult's may
// legitimately all be nil).
func completedPart(partNum int32, c partChecksums) types.CompletedPart {
	return types.CompletedPart{
		ETag:              c.etag,
		PartNumber:        aws.Int32(partNum),
		ChecksumCRC32:     c.crc32,
		ChecksumCRC32C:    c.crc32c,
		ChecksumCRC64NVME: c.crc64nvme,
		ChecksumSHA1:      c.sha1,
		ChecksumSHA256:    c.sha256,
	}
}

// putMultipart uploads r (exactly size bytes) to key via S3's multipart
// upload API: CreateMultipartUpload, then a sequential loop of UploadPart
// calls (PartNumber 1-based, ascending), then CompleteMultipartUpload with
// the collected parts in PartNumber order. It is the shared implementation
// behind PutReader (conditional == false) and PutReaderIf (conditional ==
// true, ifMatch translated to IfNoneMatch/IfMatch on the Complete call
// exactly as PutReaderIf's doc comment describes). The conditional
// create-only case additionally issues a best-effort HeadObject preflight
// before creating the upload, to fail an already-exists conflict fast
// instead of after the full transfer — see the preflight comment in the
// body for its exact (deliberately non-load-bearing) semantics.
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
	// Derived internally, not taken as a parameter: PutReader/PutReaderIf
	// are interface methods whose signatures carry no context. Every RPC
	// below runs under its own multipartRPCTimeout-bounded child of this
	// context, so no single stalled response can wedge the upload (security
	// audit 2, LOW-1 — see multipartRPCTimeout's doc comment).
	ctx := context.Background()

	// Fail-fast preflight for the create-only case (security audit 2,
	// INFO-1): a create-only (ifMatch == "") multipart write whose key
	// already exists — flush.go's crashed-prior-attempt orphan is the
	// production case — would otherwise upload EVERY part (>5 GiB of
	// transfer, since only above-threshold objects reach this method)
	// before CompleteMultipartUpload's IfNoneMatch condition rejects it.
	// A cheap HeadObject first returns the same ErrCAS with zero bytes
	// uploaded. Strictly an optimization, never load-bearing:
	//   - Complete's condition below remains authoritative — a key created
	//     between this Head and the Complete (the unavoidable race window)
	//     still ends in ErrCAS there, exactly as before.
	//   - ANY Head error falls through to the normal upload: 404 is the
	//     expected "key absent" answer, and a transient/permission error
	//     (network, 403) must not fail a write that Complete's condition
	//     would have adjudicated correctly anyway — best-effort only.
	//   - The ifMatch != "" CAS path is deliberately NOT preflighted: it
	//     wants the real etag comparison at Complete, which a Head
	//     existence check cannot stand in for.
	if conditional && ifMatch == "" {
		hctx, hcancel := context.WithTimeout(ctx, multipartRPCTimeout)
		_, herr := s.cl.HeadObject(hctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(fk),
		})
		hcancel()
		if herr == nil {
			// Same error shape as Complete's precondition rejection below,
			// so callers cannot tell (and must not care) which layer failed
			// the compare.
			return "", fmt.Errorf("%w: %s", ErrCAS, key)
		}
	}

	crctx, crcancel := context.WithTimeout(ctx, multipartRPCTimeout)
	created, err := s.cl.CreateMultipartUpload(crctx, &s3.CreateMultipartUploadInput{
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
	crcancel()
	if err != nil {
		return "", fmt.Errorf("store: s3 create multipart upload %s: %w", key, err)
	}
	uploadID := created.UploadId

	completed := false
	defer s.abortUnlessCompleted(&completed, fk, uploadID, &err, "upload")

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
			n := partLen(size, offset, partSize)

			if _, rerr := io.ReadFull(io.LimitReader(r, n), buf[:n]); rerr != nil {
				return "", fmt.Errorf("store: s3 read part %d for %s: %w", partNum, key, rerr)
			}

			pctx, pcancel := context.WithTimeout(ctx, multipartRPCTimeout)
			up, uerr := s.cl.UploadPart(pctx, &s3.UploadPartInput{
				Bucket: aws.String(s.bucket), Key: aws.String(fk),
				UploadId: uploadID, PartNumber: aws.Int32(partNum),
				Body: bytes.NewReader(buf[:n]), ContentLength: aws.Int64(n),
				// Explicit here too, not just on CreateMultipartUpload above —
				// DO NOT simplify this back out. The SDK only defaults a part's
				// checksum algorithm when RequestChecksumCalculation ==
				// WhenSupported (the SDK default); a caller running with
				// when_required (the common workaround third-party S3-compatible
				// stores need after the Jan-2025 default-checksum change — and
				// this backend explicitly targets R2/MinIO via S3Config)
				// would otherwise send this UploadPart with NO checksum while
				// CreateMultipartUpload already declared the upload CRC32,
				// making every CompletedPart's checksum silently absent and
				// CompleteMultipartUpload's declared-vs-supplied check fail on
				// real S3. Setting it here makes the SDK compute CRC32
				// unconditionally, so declared and supplied always agree
				// regardless of RequestChecksumCalculation.
				ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
			})
			pcancel()
			if uerr != nil {
				return "", fmt.Errorf("store: s3 upload part %d for %s: %w", partNum, key, uerr)
			}
			// Carry every checksum field UploadPart returned (whichever the
			// SDK actually computed — see CreateMultipartUploadInput's
			// ChecksumAlgorithm comment above for why one is always present
			// in practice) forward into CompletedPart, via completedPart —
			// dropping them (ETag/PartNumber alone) is what makes
			// CompleteMultipartUpload below fail against real S3 once a
			// checksum was attached to the part.
			parts = append(parts, completedPart(partNum, uploadPartChecksums(up)))
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
	cmctx, cmcancel := context.WithTimeout(ctx, multipartRPCTimeout)
	out, cerr := s.cl.CompleteMultipartUpload(cmctx, completeIn)
	cmcancel()
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
// this GO PROCESS still has an UploadPart call outstanding — that is a
// client-side guarantee only. S3 itself may already be finishing a part
// whose request context we just canceled: canceling the client-side call
// doesn't retroactively un-happen the write S3 was partway through
// executing, so a part can still materialize on S3's side after the
// client-side call "returned" canceled and after AbortMultipartUpload
// lands (AWS's own docs note an abort can need repeating for exactly this
// reason). The prior strictly-sequential code never canceled anything
// in-flight, so this is a genuine, new-to-concurrency possibility, not a
// pre-existing one — but it is also the ordinary shape of ANY concurrent
// multipart failure against real S3, not specific to this implementation,
// and every offshoot caller already tolerates an abort that doesn't
// instantly free 100% of a failed upload's storage (see
// docs/operations.md's note on configuring a bucket lifecycle rule for
// incomplete multipart uploads — the standard operational answer to this
// class of race, not a gap this function needs to close itself). This
// function deliberately does not follow up with a ListParts sweep to
// verify the abort actually caught everything.
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
				n := partLen(size, offset, partSize)
				// Per-RPC deadline layered on the shared cancel context:
				// a part whose response stalls fails THIS part with
				// context.DeadlineExceeded, which recordErr below then
				// turns into cancellation of every other in-flight part —
				// so one stalled response fails the upload promptly
				// instead of wedging it (see multipartRPCTimeout).
				pctx, pcancel := context.WithTimeout(cctx, multipartRPCTimeout)
				up, uerr := s.cl.UploadPart(pctx, &s3.UploadPartInput{
					Bucket: aws.String(s.bucket), Key: aws.String(fk),
					UploadId: uploadID, PartNumber: aws.Int32(partNum),
					Body: io.NewSectionReader(ra, base+offset, n), ContentLength: aws.Int64(n),
					// See the identical ChecksumAlgorithm comment on the
					// sequential-fallback UploadPart call in putMultipart —
					// the same "must be explicit, not inferred" reasoning
					// applies here unchanged.
					ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
				})
				pcancel()
				if uerr != nil {
					recordErr(fmt.Errorf("store: s3 upload part %d for %s: %w", partNum, key, uerr))
					continue
				}
				// Same checksum-field carry-forward as the sequential path;
				// see putMultipart's CompletedPart construction comment.
				parts[partNum-1] = completedPart(partNum, uploadPartChecksums(up))
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
	if firstErr == nil {
		// The producer's ONLY early exit is <-cctx.Done(), so "the producer
		// bailed before feeding every part" and "firstErr != nil" are
		// supposed to be the same event — but that's only true because
		// cancel() is called from exactly one place, recordErr, and
		// putMultipart always passes context.Background() in, which itself
		// is never Done(). Nothing here enforces that invariant: this
		// function's signature takes a ctx parameter and does the (correct,
		// ordinary) thing of deriving cctx from it, which is exactly what
		// invites a future caller to thread a real request-scoped context
		// through — at which point an external cancellation would stop the
		// producer via <-cctx.Done() with firstErr still nil, and this
		// would silently return a parts slice with zero-value
		// (unfilled) CompletedPart entries for the parts that never ran.
		// Checking ctx.Err() directly (not cctx.Err(), which is always
		// non-nil once ANY cancellation — including our own on a part
		// failure — has fired) catches exactly that case without
		// depending on the single-call-site invariant holding forever.
		if e := ctx.Err(); e != nil {
			return nil, e
		}
	}
	return parts, firstErr
}

// copyObjectMultipart implements CopyObject for a source over
// copyObjectMaxBytes (S3's 5 GiB single-request PUT-Copy ceiling), via
// CreateMultipartUpload + a sequence of UploadPartCopy calls (one per byte
// range, sized the same way putMultipart sizes its parts — see
// partSizeFor, which already keeps every part within UploadPartCopy's own
// 5 GiB per-part maximum) + CompleteMultipartUpload. fsrc and fdst are
// already-prefixed bucket keys (CopyObject has already validated and
// mapped both); size is the source's HEAD-reported length. Every RPC runs
// under its own multipartRPCTimeout-bounded child of ctx, same as
// putMultipart (security audit 2, LOW-1 — see multipartRPCTimeout's doc
// comment).
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
//
// Checksum declaration: unlike putMultipart, CreateMultipartUpload here
// deliberately does NOT declare a ChecksumAlgorithm — see the comment on
// that call below for why declaring one would break copies of exactly the
// sources this method exists to handle.
//
// Undocumented-until-now divergence from the single-request path above:
// this method does not set ContentType or any other user metadata on
// CreateMultipartUpload, so dst ends up with none, whereas the
// single-request CopyObject call preserves the source's Content-Type and
// metadata by default (S3's MetadataDirective defaults to COPY). Harmless
// today — this backend never sets metadata on any write, single-request
// or multipart, upload or copy — but a real divergence between this
// method's two branches if that ever changes; a future caller that starts
// relying on metadata surviving a copy would need to explicitly read the
// source's HeadObjectOutput and pass it through here too.
func (s *S3) copyObjectMultipart(ctx context.Context, dst, fsrc, fdst string, size int64) (err error) {
	crctx, crcancel := context.WithTimeout(ctx, multipartRPCTimeout)
	created, cerr := s.cl.CreateMultipartUpload(crctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fdst),
		// DELIBERATELY no ChecksumAlgorithm here — the opposite choice from
		// putMultipart's CreateMultipartUpload, and not an oversight to be
		// "fixed" into consistency later. putMultipart declares CRC32
		// because the SDK computes it CLIENT-SIDE from the body it is
		// uploading, so a value is always available to satisfy whatever
		// algorithm Create declares. UploadPartCopy has NO client-side body
		// at all — S3 copies server-side, and a part's checksum (if any)
		// comes back on CopyPartResult only when the SOURCE object's own
		// checksum state allows it to be derived: an object written by an
		// older offshoot, by a third-party writer, with a different
		// algorithm (e.g. SHA256), or whose stored checksum doesn't
		// decompose cleanly at THIS copy's part boundaries can all
		// legitimately yield an all-nil CopyPartResult. Declaring CRC32 on
		// Create here would then make CompleteMultipartUpload reject every
		// such copy with "InvalidRequest: ... must include the checksum for
		// each part" — exactly the failure putMultipart's own explicit
		// declaration exists to PREVENT, inflicted here on precisely the
		// sources this method was written to handle. So: declare nothing,
		// and forward whatever checksum fields UploadPartCopy actually
		// returns (see the CompletedPart construction below) — harmless and
		// useful when present, never required.
	})
	crcancel()
	if cerr != nil {
		return fmt.Errorf("store: s3 create multipart copy %s: %w", dst, cerr)
	}
	uploadID := created.UploadId

	completed := false
	defer s.abortUnlessCompleted(&completed, fdst, uploadID, &err, "copy")

	copySource := s3CopySource(s.bucket, fsrc)
	partSize := partSizeFor(size)
	var parts []types.CompletedPart
	partNum := int32(1)
	for offset := int64(0); offset < size; offset += partSize {
		n := partLen(size, offset, partSize)
		end := offset + n - 1 // INCLUSIVE end — see the doc comment above.

		pctx, pcancel := context.WithTimeout(ctx, multipartRPCTimeout)
		up, uerr := s.cl.UploadPartCopy(pctx, &s3.UploadPartCopyInput{
			Bucket: aws.String(s.bucket), Key: aws.String(fdst),
			UploadId: uploadID, PartNumber: aws.Int32(partNum),
			CopySource:      aws.String(copySource),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
		})
		pcancel()
		if uerr != nil {
			return fmt.Errorf("store: s3 upload part copy %d for %s: %w", partNum, dst, uerr)
		}
		if up.CopyPartResult == nil {
			return fmt.Errorf("store: s3 upload part copy %d for %s: empty CopyPartResult", partNum, dst)
		}
		// Carry whatever checksum fields UploadPartCopy actually returned —
		// unlike putMultipart's UploadPart results, these may legitimately
		// all be nil (see the "DELIBERATELY no ChecksumAlgorithm" comment on
		// CreateMultipartUpload above for why that's fine here and MUST NOT
		// be "fixed" by declaring one): forwarding them when present costs
		// nothing and helps S3 verify integrity when it can.
		parts = append(parts, completedPart(partNum, copyPartChecksums(up.CopyPartResult)))
		partNum++
	}

	cmctx, cmcancel := context.WithTimeout(ctx, multipartRPCTimeout)
	_, cerr2 := s.cl.CompleteMultipartUpload(cmctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(fdst), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		// Same free defense-in-depth as putMultipart: S3 rejects Complete
		// with a 400 if the parts' total length doesn't match size.
		MpuObjectSize: aws.Int64(size),
	})
	cmcancel()
	if cerr2 != nil {
		return fmt.Errorf("store: s3 complete multipart copy %s: %w", dst, cerr2)
	}
	completed = true
	return nil
}
