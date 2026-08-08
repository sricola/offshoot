package storetest

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const fakeBucket = "offshoot-test"

type FakeS3 struct {
	srv    *httptest.Server
	mu     sync.Mutex
	objs   map[string][]byte
	ignore bool

	// fault, when set and returning ok, replaces the response with status.
	fault func(method, key string) (int, bool)

	// sizeOverrides, when a key is present, is the Content-Length this fake
	// reports for that key's HEAD response instead of len(objs[key]). Exists
	// so a test can exercise store.S3.CopyObject's 5GB size gate
	// (copyObjectMaxBytes) without actually allocating and uploading a
	// multi-gigabyte object — the gate only ever inspects the HEAD response,
	// never the real bytes, so a small stored object with a lied-about size
	// drives the same code path a real oversized object would.
	sizeOverrides map[string]int64

	// batchDeleteErrs marks keys the batch DeleteObjects handler reports a
	// per-key <Error> for instead of deleting — the partial-failure shape
	// real S3 uses (a 200 response carrying Error entries), which
	// store.S3.DeleteObjects must map to "not in deleted, named in err".
	batchDeleteErrs map[string]bool
}

func NewFakeS3(t *testing.T) *FakeS3 {
	t.Helper()
	f := &FakeS3{objs: map[string][]byte{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *FakeS3) URL() string    { return f.srv.URL }
func (f *FakeS3) Bucket() string { return fakeBucket }

func (f *FakeS3) IgnorePreconditions(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ignore = v
}

// SetFault installs a fault-injection hook. fn is called for every request;
// returning ok replaces the response with the given HTTP status.
func (f *FakeS3) SetFault(fn func(method, key string) (int, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fn
}

// SetSizeOverride makes this fake report size for key's Content-Length on
// HEAD, regardless of how much data is actually stored under it. See the
// sizeOverrides field's doc comment for why this exists.
func (f *FakeS3) SetSizeOverride(key string, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sizeOverrides == nil {
		f.sizeOverrides = map[string]int64{}
	}
	f.sizeOverrides[key] = size
}

// SetBatchDeleteError makes the batch DeleteObjects handler report a
// per-key <Error> for key (in a 200 response, real S3's partial-failure
// shape) instead of deleting it. See the batchDeleteErrs field.
func (f *FakeS3) SetBatchDeleteError(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.batchDeleteErrs == nil {
		f.batchDeleteErrs = map[string]bool{}
	}
	f.batchDeleteErrs[key] = true
}

// Seed stores data at key directly, bypassing HTTP. Exists so a test that
// needs thousands of objects in place (the DeleteObjects chunking test)
// doesn't pay thousands of signed SDK Puts just to arrange its fixture.
func (f *FakeS3) Seed(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.objs[key] = cp
}

func etagOf(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// bucketOf and keyOf split a request path on its first path-segment
// boundary — the bucket, then the key — rather than stripping a bare string
// prefix, so a bucket name that happens to prefix another string can't be
// confused for a segment match.
func bucketOf(p string) string {
	bucket, _, _ := strings.Cut(strings.TrimPrefix(p, "/"), "/")
	return bucket
}

func keyOf(p string) string {
	_, key, _ := strings.Cut(strings.TrimPrefix(p, "/"), "/")
	return key
}

func (f *FakeS3) handle(w http.ResponseWriter, r *http.Request) {
	if bucketOf(r.URL.Path) != fakeBucket {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `<Error><Code>NoSuchBucket</Code></Error>`)
		return
	}
	key := keyOf(r.URL.Path)
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fault != nil {
		if status, ok := f.fault(r.Method, key); ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(status)
			io.WriteString(w, `<Error><Code>InternalError</Code></Error>`)
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		if key == "" || r.URL.Query().Get("list-type") == "2" {
			f.list(w, r)
			return
		}
		data, ok := f.objs[key]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		w.Header().Set("ETag", etagOf(data))
		w.Write(data)

	case http.MethodPut:
		// store.S3.CopyObject issues a PUT carrying X-Amz-Copy-Source
		// instead of a body — S3's server-side copy contract. Route it
		// separately: the ordinary PUT path below would otherwise treat
		// this as a normal (bodyless) write and silently truncate dst to
		// empty, which is exactly backwards from what the client asked for.
		if copySrc := r.Header.Get("X-Amz-Copy-Source"); copySrc != "" {
			f.copyObject(w, key, copySrc)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !f.ignore {
			cur, exists := f.objs[key]
			if inm := r.Header.Get("If-None-Match"); inm == "*" && exists {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
				return
			}
			if im := r.Header.Get("If-Match"); im != "" {
				if !exists || etagOf(cur) != im {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusPreconditionFailed)
					io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
					return
				}
			}
		}
		f.objs[key] = body
		w.Header().Set("ETag", etagOf(body))
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		delete(f.objs, key)
		w.WriteHeader(http.StatusNoContent)

	case http.MethodPost:
		// Batch delete: POST /{bucket}?delete with an XML key manifest —
		// what store.S3.DeleteObjects issues (perf audit H2). No other POST
		// operation is faked.
		if _, ok := r.URL.Query()["delete"]; ok && key == "" {
			f.deleteObjects(w, r)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case http.MethodHead:
		data, ok := f.objs[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", etagOf(data))
		// store.S3.CopyObject's 5GB size gate reads HeadObjectOutput.
		// ContentLength; without this header the SDK reports it as 0/nil
		// regardless of the object's real size, which would make that gate
		// untestable against this fake. sizeOverrides lets a test lie about
		// it deliberately — see that field's doc comment.
		size := int64(len(data))
		if override, ok := f.sizeOverrides[key]; ok {
			size = override
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// copyObject serves a CopyObject request (a PUT carrying X-Amz-Copy-Source
// instead of a body): looks up the source named by that header — "bucket/
// key", percent-encoded per S3's contract, see store.S3's s3CopySource —
// and stores a copy of its bytes at dstKey, overwriting any existing
// content there (store.Backend.CopyObject's documented contract; nothing
// extra to enforce here since a plain map write already overwrites).
//
// Precondition headers (CopySourceIfMatch etc.) are not simulated: store.
// S3.CopyObject never sets them, so there is nothing exercising that
// behavior to fake.
func (f *FakeS3) copyObject(w http.ResponseWriter, dstKey, copySource string) {
	srcKey, err := decodeCopySource(copySource)
	if err != nil {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `<Error><Code>InvalidArgument</Code></Error>`)
		return
	}
	data, ok := f.objs[srcKey]
	if !ok {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.objs[dstKey] = cp
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<CopyObjectResult><ETag>%s</ETag><LastModified>%s</LastModified></CopyObjectResult>`,
		etagOf(cp), time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
}

// decodeCopySource extracts the object key from an X-Amz-Copy-Source header
// value: "[/]bucket/key" (percent-encoded), optionally carrying a
// "?versionId=..." suffix this fake ignores (store.S3.CopyObject never sets
// a version). The bucket component is checked against fakeBucket — this
// fake serves exactly one bucket, same as the rest of its handlers.
func decodeCopySource(v string) (string, error) {
	v = strings.TrimPrefix(v, "/")
	v, _, _ = strings.Cut(v, "?")
	bucket, key, ok := strings.Cut(v, "/")
	if !ok || bucket != fakeBucket {
		return "", fmt.Errorf("unexpected copy source %q", v)
	}
	// url.PathUnescape (not QueryUnescape) to match store.S3's s3CopySource,
	// which encodes with url.PathEscape — QueryUnescape would additionally
	// (and wrongly) turn a literal '+' into a space.
	dec, err := url.PathUnescape(key)
	if err != nil {
		return "", err
	}
	return dec, nil
}

// deleteObjectsRequest mirrors the S3 DeleteObjects request body:
// <Delete><Object><Key>k</Key></Object>...</Delete>.
type deleteObjectsRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
	Quiet bool `xml:"Quiet"`
}

// deleteObjectsResult mirrors the DeleteObjects response:
// <DeleteResult><Deleted><Key>k</Key></Deleted><Error>...</Error>...</DeleteResult>.
type deleteObjectsResult struct {
	XMLName xml.Name             `xml:"DeleteResult"`
	Deleted []deleteObjectsKey   `xml:"Deleted"`
	Errors  []deleteObjectsError `xml:"Error"`
}

type deleteObjectsKey struct {
	Key string `xml:"Key"`
}

type deleteObjectsError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// deleteObjects serves the batch DeleteObjects API (POST ?delete). Real S3
// semantics this fake reproduces because store.S3.DeleteObjects depends on
// them: at most 1000 keys per request (over-limit is a MalformedXML 400 —
// enforcing it here is what makes a chunking test against this fake
// honest), an absent key still counts as Deleted (idempotent), and per-key
// failures come back as <Error> entries in a 200 response, not an HTTP
// error. batchDeleteErrs (SetBatchDeleteError) drives that last case.
func (f *FakeS3) deleteObjects(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var req deleteObjectsRequest
	if xml.Unmarshal(body, &req) != nil || len(req.Objects) > 1000 {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `<Error><Code>MalformedXML</Code></Error>`)
		return
	}
	var res deleteObjectsResult
	for _, o := range req.Objects {
		if f.batchDeleteErrs[o.Key] {
			res.Errors = append(res.Errors, deleteObjectsError{
				Key: o.Key, Code: "InternalError", Message: "injected by SetBatchDeleteError",
			})
			continue
		}
		delete(f.objs, o.Key)
		if !req.Quiet {
			res.Deleted = append(res.Deleted, deleteObjectsKey{Key: o.Key})
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}

type listResult struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

func (f *FakeS3) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	token := q.Get("continuation-token")

	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if token != "" {
		i := sort.SearchStrings(keys, token)
		keys = keys[i:]
	}
	const maxKeys = 1000
	truncated := false
	var next string
	if len(keys) > maxKeys {
		next = keys[maxKeys]
		keys = keys[:maxKeys]
		truncated = true
	}

	var res listResult
	for _, k := range keys {
		res.Contents = append(res.Contents, struct {
			Key string `xml:"Key"`
		}{Key: k})
	}
	res.IsTruncated = truncated
	res.NextContinuationToken = next
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}
