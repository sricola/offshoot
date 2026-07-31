package storetest

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func put(t *testing.T, f *FakeS3, key, body string, hdrs map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, f.URL()+"/"+f.Bucket()+"/"+key, strings.NewReader(body))
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
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
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
