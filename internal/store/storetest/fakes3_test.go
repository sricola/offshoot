package storetest

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func put(t *testing.T, f *FakeS3, key, body string, hdrs map[string]string) *http.Response {
	t.Helper()
	return do(t, http.MethodPut, f, key, body, hdrs)
}

// post issues a raw POST against the fake, mirroring put — used to drive
// CreateMultipartUpload/CompleteMultipartUpload directly, bypassing the AWS
// SDK/store.S3 entirely. This fake never checks SigV4 signatures (see
// handle()), so a bare net/http request reaches the same code path a
// signed SDK request would.
func post(t *testing.T, f *FakeS3, key, body string, hdrs map[string]string) *http.Response {
	t.Helper()
	return do(t, http.MethodPost, f, key, body, hdrs)
}

func do(t *testing.T, method string, f *FakeS3, key, body string, hdrs map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, f.URL()+"/"+f.Bucket()+"/"+key, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	resp.Body = io.NopCloser(strings.NewReader(string(b)))
	return resp
}

// uploadID extracts <UploadId>...</UploadId> from a CreateMultipartUpload
// response body — just enough XML "parsing" for these tests to drive the
// fake's multipart endpoints directly without pulling in the SDK.
func uploadID(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(b), "<UploadId>")
	if !ok {
		t.Fatalf("no <UploadId> in response body: %s", b)
	}
	id, _, ok := strings.Cut(rest, "</UploadId>")
	if !ok {
		t.Fatalf("unterminated <UploadId> in response body: %s", b)
	}
	return id
}

func TestFakeS3IfNoneMatchStar(t *testing.T) {
	f := NewFakeS3(t)
	if got := put(t, f, "k", "v1", map[string]string{"If-None-Match": "*"}).StatusCode; got != 200 {
		t.Fatalf("first create-only put = %d, want 200", got)
	}
	if got := put(t, f, "k", "v2", map[string]string{"If-None-Match": "*"}).StatusCode; got != 412 {
		t.Fatalf("second create-only put = %d, want 412", got)
	}
}

func TestFakeS3IfMatch(t *testing.T) {
	f := NewFakeS3(t)
	resp := put(t, f, "k", "v1", nil)
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("PUT must return an ETag")
	}
	if got := put(t, f, "k", "v2", map[string]string{"If-Match": `"deadbeef"`}).StatusCode; got != 412 {
		t.Fatalf("stale If-Match = %d, want 412", got)
	}
	if got := put(t, f, "k", "v2", map[string]string{"If-Match": etag}).StatusCode; got != 200 {
		t.Fatalf("matching If-Match = %d, want 200", got)
	}
	if got := put(t, f, "absent", "v", map[string]string{"If-Match": etag}).StatusCode; got != 412 {
		t.Fatalf("If-Match on absent key = %d, want 412", got)
	}
}

func TestFakeS3IgnorePreconditionsMode(t *testing.T) {
	f := NewFakeS3(t)
	f.IgnorePreconditions(true)
	put(t, f, "k", "v1", nil)
	if got := put(t, f, "k", "v2", map[string]string{"If-None-Match": "*"}).StatusCode; got != 200 {
		t.Fatalf("ignore mode must accept the write, got %d", got)
	}
}

// TestFakeS3CompleteRejectsMissingDeclaredChecksum is the regression guard
// for the bug class this file was written to make catchable automatically:
// store.S3.copyObjectMultipart, at review time, declared
// ChecksumAlgorithm: CRC32 on CreateMultipartUpload the same way
// putMultipart correctly does — but UploadPartCopy has no client-side body
// to checksum, so a part copied from a source with no derivable checksum
// (an older writer, a third-party object, SHA256, a non-decomposing
// full-object checksum) legitimately supplies no checksum at all, and real
// S3 rejects Complete for such an upload with InvalidRequest. This test
// drives the fake directly with raw HTTP (bypassing store.S3 and the SDK
// entirely — this fake never checks SigV4 signatures, see handle()) to
// prove the fake itself now enforces that contract: a declared algorithm
// with a part missing that algorithm's checksum must be rejected. If
// someone reintroduces a declared ChecksumAlgorithm on
// copyObjectMultipart's Create (see that function's doc comment for why
// not to), this is what would start failing store's own tests instead of
// only breaking silently against real S3.
func TestFakeS3CompleteRejectsMissingDeclaredChecksum(t *testing.T) {
	f := NewFakeS3(t)

	createResp := post(t, f, "k?uploads", "", map[string]string{"X-Amz-Checksum-Algorithm": "CRC32"})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateMultipartUpload = %d, want 200", createResp.StatusCode)
	}
	id := uploadID(t, createResp)

	// UploadPart with NO checksum header — exactly what UploadPartCopy
	// gives the fake for a source with no derivable checksum.
	partResp := put(t, f, fmt.Sprintf("k?partNumber=1&uploadId=%s", id), "part one body", nil)
	if partResp.StatusCode != http.StatusOK {
		t.Fatalf("UploadPart = %d, want 200", partResp.StatusCode)
	}
	etag := partResp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("UploadPart must return an ETag")
	}

	completeBody := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag)
	completeResp := post(t, f, "k?uploadId="+id, completeBody, nil)
	if completeResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(completeResp.Body)
		t.Fatalf("CompleteMultipartUpload with a declared algorithm but a part missing its checksum = %d, want 400 (InvalidRequest); body: %s",
			completeResp.StatusCode, body)
	}
}

// TestFakeS3CompleteAcceptsSuppliedDeclaredChecksum is the positive
// counterpart: a declared algorithm with every part carrying that
// algorithm's checksum (what store.S3.putMultipart always produces, since
// the SDK computes CRC32 client-side from the body it uploads) must still
// succeed under the enforcement above — this is what pins that enforcement
// didn't just start rejecting everything.
func TestFakeS3CompleteAcceptsSuppliedDeclaredChecksum(t *testing.T) {
	f := NewFakeS3(t)

	createResp := post(t, f, "k?uploads", "", map[string]string{"X-Amz-Checksum-Algorithm": "CRC32"})
	id := uploadID(t, createResp)

	partResp := put(t, f, fmt.Sprintf("k?partNumber=1&uploadId=%s", id), "part one body",
		map[string]string{"X-Amz-Checksum-Crc32": "deadbeef=="})
	etag := partResp.Header.Get("ETag")
	if got := partResp.Header.Get("X-Amz-Checksum-Crc32"); got != "deadbeef==" {
		t.Fatalf("UploadPart response must echo the request's checksum header, got %q", got)
	}

	completeBody := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag><ChecksumCRC32>deadbeef==</ChecksumCRC32></Part></CompleteMultipartUpload>`,
		etag)
	completeResp := post(t, f, "k?uploadId="+id, completeBody, nil)
	if completeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(completeResp.Body)
		t.Fatalf("CompleteMultipartUpload with the declared checksum supplied = %d, want 200; body: %s",
			completeResp.StatusCode, body)
	}
}

// TestFakeS3CompleteAllowsNoChecksumWhenNoneDeclared pins the OTHER half
// of the contract: an upload created with NO declared ChecksumAlgorithm —
// exactly store.S3.copyObjectMultipart's choice — must accept parts with
// no checksum at all, since there is nothing to enforce.
func TestFakeS3CompleteAllowsNoChecksumWhenNoneDeclared(t *testing.T) {
	f := NewFakeS3(t)

	createResp := post(t, f, "k?uploads", "", nil) // no X-Amz-Checksum-Algorithm
	id := uploadID(t, createResp)

	partResp := put(t, f, fmt.Sprintf("k?partNumber=1&uploadId=%s", id), "part one body", nil)
	etag := partResp.Header.Get("ETag")

	completeBody := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag)
	completeResp := post(t, f, "k?uploadId="+id, completeBody, nil)
	if completeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(completeResp.Body)
		t.Fatalf("CompleteMultipartUpload with no declared algorithm and no part checksums = %d, want 200; body: %s",
			completeResp.StatusCode, body)
	}
}
