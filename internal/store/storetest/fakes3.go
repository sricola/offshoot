package storetest

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

const fakeBucket = "offshoot-test"

type FakeS3 struct {
	srv    *httptest.Server
	mu     sync.Mutex
	objs   map[string][]byte
	ignore bool
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

func etagOf(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// keyOf strips the leading "/bucket/" from the request path.
func keyOf(p string) string {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimPrefix(p, fakeBucket)
	return strings.TrimPrefix(p, "/")
}

func (f *FakeS3) handle(w http.ResponseWriter, r *http.Request) {
	key := keyOf(r.URL.Path)
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.Method {
	case http.MethodGet:
		if key == "" || r.URL.Query().Get("list-type") == "2" {
			f.list(w, r)
			return
		}
		data, ok := f.objs[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		w.Header().Set("ETag", etagOf(data))
		w.Write(data)

	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !f.ignore {
			cur, exists := f.objs[key]
			if inm := r.Header.Get("If-None-Match"); inm == "*" && exists {
				w.WriteHeader(http.StatusPreconditionFailed)
				io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
				return
			}
			if im := r.Header.Get("If-Match"); im != "" {
				if !exists || etagOf(cur) != im {
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

	case http.MethodHead:
		data, ok := f.objs[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", etagOf(data))
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

type listResult struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
	IsTruncated bool `xml:"IsTruncated"`
}

func (f *FakeS3) list(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var res listResult
	for _, k := range keys {
		res.Contents = append(res.Contents, struct {
			Key string `xml:"Key"`
		}{Key: k})
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}
