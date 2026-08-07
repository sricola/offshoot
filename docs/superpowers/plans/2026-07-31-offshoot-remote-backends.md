# offshoot Plan 3: S3-Compatible Remote Backends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run every existing offshoot operation against an S3-compatible bucket — `offshoot -store s3://bucket/prefix init` and everything downstream — with compare-and-swap safety verified at attach time rather than assumed.

**Architecture:** The `Backend` interface from Plan 2 is the seam; this plan adds a second implementation over S3 conditional writes (`If-None-Match: *` for create-only, `If-Match: <etag>` for CAS) plus a capability probe that refuses stores whose provider silently ignores those preconditions. A shared conformance suite runs the identical contract against every backend, an in-process fake S3 server makes that suite hermetic in CI, and an env-gated integration test runs it against real providers before we claim support.

**Tech Stack:** Go 1.24+, `github.com/aws/aws-sdk-go-v2` (Apache-2.0), stdlib `net/http/httptest` for the fake, existing `internal/store` package.

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md` § Storage layout, § Supported stores. **Plan sequence:** Plan 1 (capture spike, merged, GO) → Plan 2 (LTX storage + lifecycle, local mode, merged) → **this plan** → Plan 4 (daemon mode: live WAL capture wiring, leases + epoch bumping, incremental segments, TTL) → Plan 5 (MCP server, SDKs, LangGraph adapter, launch demo).

## Global Constraints

- Module `github.com/sricola/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only
- `Backend` contract is fixed by Plan 2 and MUST NOT change: `Get(key) (data []byte, etag string, err error)`, `Put(key, data) error`, `PutIf(key, data, ifMatch string) (etag string, err error)` where `ifMatch==""` means create-only, `List(prefix) ([]string, error)` sorted, `Delete(key) error` idempotent; sentinels `store.ErrNotFound`, `store.ErrCAS`
- Every ref mutation in `internal/ops` already goes through `PutIf`; this plan changes no `ops` logic
- **Supported stores are only those the probe passes** — the spec names AWS S3, Cloudflare R2, Tigris, MinIO, and explicitly excludes GCS's S3-interop API (no CAS on writes). Never claim "any S3-compatible"
- Probe failure is a hard refusal at attach, never a silent downgrade
- Fail closed: a provider that accepts a conditional write it should have rejected must fail the probe
- Tests must be hermetic by default (no network); real-provider tests are env-gated and skip cleanly when unset
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/store/storetest/conformance.go   Shared Backend contract suite (non-_test so any package can run it)
internal/store/storetest/fakes3.go        In-process S3 subset server (httptest) with conditional-write semantics
internal/store/storetest/fakes3_test.go   Tests that the fake itself honors preconditions
internal/store/local_test.go              (modify) run the conformance suite; keep local-only tests
internal/store/s3.go                      S3Backend over aws-sdk-go-v2
internal/store/s3_test.go                 S3Backend against the fake, via the conformance suite
internal/store/probe.go                   CAS capability probe (backend-agnostic)
internal/store/probe_test.go
internal/store/open.go                    URL/path → Backend selection (`s3://`, `file://`, bare path)
internal/store/open_test.go
internal/store/s3_integration_test.go     Env-gated real-provider run of the conformance suite + probe
internal/ops/ops.go                       (modify) Init/Open take a store spec string, call the probe
cmd/offshoot/main.go                      (modify) -store accepts s3:// URLs; usage/help text
README.md                                 (modify) supported-store matrix, credentials, honesty note
```

---

### Task 1: Backend conformance suite

**Files:**
- Create: `internal/store/storetest/conformance.go`
- Modify: `internal/store/local_test.go`

**Interfaces:**
- Consumes: `store.Backend`, `store.ErrNotFound`, `store.ErrCAS` (Plan 2)
- Produces:

```go
package storetest

// RunConformance exercises the full Backend contract. newBackend must return
// a fresh, empty backend on every call; keyPrefix is prepended to every key
// the suite uses so that runs against a shared real bucket cannot collide.
func RunConformance(t *testing.T, keyPrefix string, newBackend func(t *testing.T) store.Backend)
```

The suite covers: Put/Get round trip with a non-empty etag; `Get` of a missing key returning `ErrNotFound`; create-only `PutIf` succeeding once and returning `ErrCAS` on the second attempt; `PutIf` with a stale etag returning `ErrCAS`; `PutIf` with the current etag succeeding and changing the etag; concurrent `PutIf` from one etag having exactly one winner; `List` returning sorted, prefix-filtered keys and omitting deleted ones; idempotent `Delete`. Note it must never assume etags are content hashes (real S3 multipart etags are not), only that they change when content changes and can be round-tripped.

- [ ] **Step 1: Write the suite**

`internal/store/storetest/conformance.go`:

```go
// Package storetest provides the shared Backend contract suite and an
// in-process S3 subset server, so every backend is verified against exactly
// the same expectations.
package storetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/sricola/offshoot/internal/store"
)

// RunConformance exercises the full Backend contract.
func RunConformance(t *testing.T, keyPrefix string, newBackend func(t *testing.T) store.Backend) {
	t.Helper()
	k := func(s string) string { return keyPrefix + s }

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		b := newBackend(t)
		if err := b.Put(k("data/x/1.ltx"), []byte("hello")); err != nil {
			t.Fatal(err)
		}
		data, etag, err := b.Get(k("data/x/1.ltx"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "hello" {
			t.Errorf("data = %q, want %q", data, "hello")
		}
		if etag == "" {
			t.Error("etag must not be empty")
		}
	})

	t.Run("GetMissingIsErrNotFound", func(t *testing.T) {
		b := newBackend(t)
		if _, _, err := b.Get(k("nope")); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("CreateOnlyRejectsExisting", func(t *testing.T) {
		b := newBackend(t)
		if _, err := b.PutIf(k("refs/a/main"), []byte("v1"), ""); err != nil {
			t.Fatal(err)
		}
		if _, err := b.PutIf(k("refs/a/main"), []byte("v2"), ""); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS on create-only over existing, got %v", err)
		}
		data, _, err := b.Get(k("refs/a/main"))
		if err != nil || string(data) != "v1" {
			t.Fatalf("rejected create-only must not modify content: %q %v", data, err)
		}
	})

	t.Run("CASRejectsStaleEtag", func(t *testing.T) {
		b := newBackend(t)
		etag1, err := b.PutIf(k("refs/a/main"), []byte("v1"), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.PutIf(k("refs/a/main"), []byte("bad"), "sha256-of-nothing"); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS on stale etag, got %v", err)
		}
		etag2, err := b.PutIf(k("refs/a/main"), []byte("v2"), etag1)
		if err != nil {
			t.Fatal(err)
		}
		if etag2 == etag1 {
			t.Error("etag must change when content changes")
		}
		data, _, _ := b.Get(k("refs/a/main"))
		if string(data) != "v2" {
			t.Fatalf("content = %q, want v2", data)
		}
	})

	t.Run("CASOnMissingKeyFails", func(t *testing.T) {
		b := newBackend(t)
		if _, err := b.PutIf(k("refs/absent"), []byte("v"), "some-etag"); !errors.Is(err, store.ErrCAS) {
			t.Fatalf("want ErrCAS for CAS against absent key, got %v", err)
		}
	})

	t.Run("ConcurrentCASHasOneWinner", func(t *testing.T) {
		b := newBackend(t)
		etag, err := b.PutIf(k("refs/race"), []byte("seed"), "")
		if err != nil {
			t.Fatal(err)
		}
		const n = 16
		var wg sync.WaitGroup
		wins := make(chan int, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if _, err := b.PutIf(k("refs/race"), []byte(fmt.Sprintf("w%d", idx)), etag); err == nil {
					wins <- idx
				}
			}(i)
		}
		wg.Wait()
		close(wins)
		count := 0
		for range wins {
			count++
		}
		if count != 1 {
			t.Fatalf("exactly one CAS from the same etag must win, got %d", count)
		}
	})

	t.Run("ListSortedFilteredAndDeleted", func(t *testing.T) {
		b := newBackend(t)
		for _, key := range []string{"data/l1/b", "data/l1/a", "data/l2/a"} {
			if err := b.Put(k(key), []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		keys, err := b.List(k("data/l1/"))
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 2 || keys[0] != k("data/l1/a") || keys[1] != k("data/l1/b") {
			t.Fatalf("keys = %v, want sorted [a b] under the prefix", keys)
		}
		if err := b.Delete(k("data/l1/a")); err != nil {
			t.Fatal(err)
		}
		if keys, _ = b.List(k("data/l1/")); len(keys) != 1 || keys[0] != k("data/l1/b") {
			t.Fatalf("after delete keys = %v", keys)
		}
	})

	t.Run("DeleteIsIdempotent", func(t *testing.T) {
		b := newBackend(t)
		if err := b.Delete(k("data/never-existed")); err != nil {
			t.Fatalf("delete of absent key must not error: %v", err)
		}
	})
}
```

- [ ] **Step 2: Run the suite against Local**

Append to `internal/store/local_test.go` (keep every existing local-only test — stale-lock breaking, path traversal, concurrent same-key Put — they test Local internals the contract does not cover):

```go
func TestLocalConformance(t *testing.T) {
	storetest.RunConformance(t, "", func(t *testing.T) store.Backend {
		b, err := NewLocal(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}
```

This creates an import cycle risk: `local_test.go` is in package `store`, and `storetest` imports `store`. Fix by making the conformance test an external test file instead — create `internal/store/conformance_local_test.go` with `package store_test`:

```go
package store_test

import (
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

func TestLocalConformance(t *testing.T) {
	storetest.RunConformance(t, "", func(t *testing.T) store.Backend {
		b, err := store.NewLocal(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}
```

Do NOT add the helper to `local_test.go`; use the external test file above.

- [ ] **Step 3: Run and verify**

Run: `go test ./internal/store/... -v -race`
Expected: PASS. If `ConcurrentCASHasOneWinner` fails against Local, that is a real Local bug (Plan 2 has a passing equivalent — compare before touching the suite).

- [ ] **Step 4: Commit**

```bash
git add internal/store/storetest internal/store/conformance_local_test.go
git commit -m "test: shared Backend conformance suite, run against the local backend"
```

---

### Task 2: In-process S3 subset server

**Files:**
- Create: `internal/store/storetest/fakes3.go`, `internal/store/storetest/fakes3_test.go`

**Interfaces:**
- Consumes: stdlib only
- Produces:

```go
package storetest

// FakeS3 is an in-process S3 subset implementing exactly the operations
// offshoot uses: GetObject, PutObject (with If-Match / If-None-Match
// preconditions), DeleteObject, and ListObjectsV2. It is a development and
// CI convenience — passing against it is NOT evidence a real provider
// honors preconditions. See s3_integration_test.go.
type FakeS3 struct{ ... }

func NewFakeS3(t *testing.T) *FakeS3 // starts an httptest.Server, closed via t.Cleanup
func (f *FakeS3) URL() string        // endpoint URL
func (f *FakeS3) Bucket() string     // pre-created bucket name ("offshoot-test")

// IgnorePreconditions makes the fake accept conditional writes without
// evaluating them — used to prove the CAS probe (Task 4) catches such a
// provider.
func (f *FakeS3) IgnorePreconditions(v bool)
```

Semantics to implement: etag = `"` + hex MD5 + `"` (quoted, matching S3); `If-None-Match: *` fails with 412 when the key exists; `If-Match: <etag>` fails with 412 when the key is absent or the etag differs; concurrent requests serialized by a mutex; ListObjectsV2 returns `<Contents><Key>` entries sorted, honoring `prefix`, no pagination (tests stay under 1000 keys).

- [ ] **Step 1: Write the fake's own tests first**

`internal/store/storetest/fakes3_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/store/storetest -run TestFakeS3 -v`
Expected: FAIL — `NewFakeS3` undefined.

- [ ] **Step 3: Implement the fake**

`internal/store/storetest/fakes3.go`:

```go
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
```

- [ ] **Step 4: Run and verify**

Run: `go test ./internal/store/storetest -v -race`
Expected: PASS ×3.

- [ ] **Step 5: Commit**

```bash
git add internal/store/storetest
git commit -m "test: in-process S3 subset server with conditional-write semantics"
```

---

### Task 3: S3Backend

**Files:**
- Create: `internal/store/s3.go`, `internal/store/s3_test.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `Backend`, `ErrNotFound`, `ErrCAS` (Plan 2); `storetest.NewFakeS3`, `storetest.RunConformance` (Tasks 1-2)
- Produces:

```go
package store

type S3Config struct {
	Bucket       string
	Prefix       string // optional key prefix, no leading slash; "" allowed
	Endpoint     string // optional custom endpoint (R2/Tigris/MinIO/fake)
	Region       string // defaults to "auto" when Endpoint is set, else SDK default chain
	UsePathStyle bool   // required for MinIO and the fake
}

func NewS3(ctx context.Context, cfg S3Config) (*S3, error) // loads credentials from the SDK default chain
```

`*S3` implements `Backend`. Mapping: `Get` → GetObject (NoSuchKey/404 → `ErrNotFound`); `Put` → PutObject unconditional; `PutIf` with `ifMatch==""` → PutObject with `IfNoneMatch: "*"`, with an etag → PutObject with `IfMatch: <etag>`; HTTP 412 (`PreconditionFailed`) **or** 409 (`ConditionalRequestConflict`, which S3 returns for concurrent conditional writes) → `ErrCAS`; `List` → ListObjectsV2 paginated, sorted, prefix-joined; `Delete` → DeleteObject (404 is not an error). Etags are returned verbatim from the provider (quoted) and passed back verbatim — never parsed or compared to a content hash.

**API-adaptation authorization:** the code below is written against `aws-sdk-go-v2`'s expected surface (`config.LoadDefaultConfig`, `s3.NewFromConfig` with `o.BaseEndpoint`/`o.UsePathStyle`, `PutObjectInput.IfMatch`/`IfNoneMatch`, `ListObjectsV2Paginator`, `smithy.APIError`). Run `go doc github.com/aws/aws-sdk-go-v2/service/s3 PutObjectInput` and adapt names/signatures to the real API. The exported contract (`S3Config`, `NewS3`, Backend methods) and the conformance suite are fixed; internal adaptation is pre-authorized and must be listed in your report. If `PutObjectInput` lacks `IfMatch` in the pinned SDK version, upgrade the SDK — do NOT emulate CAS with a read-then-write.

- [ ] **Step 1: Add the dependency and read the real API**

```bash
cd /Users/sray/gits/sql
go get github.com/aws/aws-sdk-go-v2/config@latest github.com/aws/aws-sdk-go-v2/service/s3@latest
go doc github.com/aws/aws-sdk-go-v2/service/s3 PutObjectInput | head -40
```

Confirm `IfMatch` and `IfNoneMatch` exist on `PutObjectInput` before writing code.

- [ ] **Step 2: Write the failing test**

`internal/store/s3_test.go` — external test package to avoid the storetest import cycle:

```go
package store_test

import (
	"context"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

// newFakeBacked returns an S3 backend pointed at a fresh in-process fake.
func newFakeBacked(t *testing.T) store.Backend {
	t.Helper()
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket:       f.Bucket(),
		Endpoint:     f.URL(),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestS3Conformance(t *testing.T) {
	storetest.RunConformance(t, "", newFakeBacked)
}

func TestS3PrefixIsolatesKeys(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	mk := func(prefix string) store.Backend {
		b, err := store.NewS3(context.Background(), store.S3Config{
			Bucket: f.Bucket(), Prefix: prefix, Endpoint: f.URL(),
			Region: "us-east-1", UsePathStyle: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, bb := mk("tenant-a"), mk("tenant-b")
	if err := a.Put("refs/x", []byte("A")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bb.Get("refs/x"); err == nil {
		t.Fatal("prefixes must isolate keys")
	}
	keys, err := a.List("refs/")
	if err != nil || len(keys) != 1 || keys[0] != "refs/x" {
		t.Fatalf("List must return prefix-stripped keys: %v %v", keys, err)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `go test ./internal/store -run TestS3 -v`
Expected: FAIL — `store.NewS3` undefined.

- [ ] **Step 4: Implement**

`internal/store/s3.go`:

```go
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
	Prefix       string
	Endpoint     string
	Region       string
	UsePathStyle bool
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
	loadOpts := []func(*awsconfig.LoadOptions) error{}
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
```

- [ ] **Step 5: Run and verify**

Run: `go test ./internal/store/... -v -race`
Expected: PASS — the full conformance suite against the fake, plus the prefix test. If `ConcurrentCASHasOneWinner` fails, check that the fake serializes on its mutex AND that 409 maps to `ErrCAS`.

- [ ] **Step 6: Commit**

```bash
git add internal/store go.mod go.sum
git commit -m "feat: S3-compatible backend using conditional writes for CAS"
```

---

### Task 4: CAS capability probe

**Files:**
- Create: `internal/store/probe.go`, `internal/store/probe_test.go`

**Interfaces:**
- Consumes: `Backend`, `ErrCAS`, `ErrNotFound`; `storetest.NewFakeS3` + `IgnorePreconditions` (Task 2)
- Produces:

```go
package store

// ErrNoCAS reports a store whose backend accepted a conditional write it
// should have rejected — offshoot's safety depends on compare-and-swap, so
// such a store is refused rather than used with weaker guarantees.
var ErrNoCAS = errors.New("store: backend does not enforce conditional writes")

// ProbeCAS verifies that b enforces create-only and compare-and-swap
// semantics, using a temporary key under probe/. It cleans up after itself.
// Returns ErrNoCAS (wrapped, with detail) when a precondition is ignored.
func ProbeCAS(b Backend) error
```

Probe steps: pick key `probe/cas-<random hex>`; create-only put must succeed; second create-only put must return `ErrCAS` **and** leave content unchanged; CAS with a bogus etag must return `ErrCAS` and leave content unchanged; CAS with the real etag must succeed; delete the key. Content checks matter because a provider can return an error yet still write, or accept and overwrite.

- [ ] **Step 1: Write the failing test**

`internal/store/probe_test.go` — external test package:

```go
package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

func TestProbeCASPassesLocal(t *testing.T) {
	b, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ProbeCAS(b); err != nil {
		t.Fatalf("local backend must pass the probe: %v", err)
	}
	keys, _ := b.List("probe/")
	if len(keys) != 0 {
		t.Fatalf("probe must clean up after itself, left: %v", keys)
	}
}

func TestProbeCASPassesFakeS3(t *testing.T) {
	if err := store.ProbeCAS(newFakeBacked(t)); err != nil {
		t.Fatalf("fake honoring preconditions must pass: %v", err)
	}
}

func TestProbeCASRejectsProviderIgnoringPreconditions(t *testing.T) {
	f := storetest.NewFakeS3(t)
	f.IgnorePreconditions(true)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = store.ProbeCAS(b)
	if !errors.Is(err, store.ErrNoCAS) {
		t.Fatalf("want ErrNoCAS for a provider ignoring preconditions, got %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/store -run TestProbe -v`
Expected: FAIL — `store.ProbeCAS` undefined.

- [ ] **Step 3: Implement**

`internal/store/probe.go`:

```go
package store

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrNoCAS reports a backend that does not enforce conditional writes.
var ErrNoCAS = errors.New("store: backend does not enforce conditional writes")

// ProbeCAS verifies create-only and compare-and-swap enforcement. offshoot's
// single-writer-per-ref guarantee rests entirely on these semantics, so a
// store that fails the probe is refused outright rather than used with
// silently weaker safety.
func ProbeCAS(b Backend) error {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return err
	}
	key := "probe/cas-" + hex.EncodeToString(buf)
	defer b.Delete(key)

	first := []byte("offshoot-probe-1")
	etag, err := b.PutIf(key, first, "")
	if err != nil {
		return fmt.Errorf("store: probe create-only put failed: %w", err)
	}

	// A second create-only put must be rejected AND must not modify content.
	second := []byte("offshoot-probe-2")
	if _, err := b.PutIf(key, second, ""); err == nil {
		return fmt.Errorf("%w: create-only put over an existing key was accepted", ErrNoCAS)
	} else if !errors.Is(err, ErrCAS) {
		return fmt.Errorf("store: probe create-only put returned an unexpected error: %w", err)
	}
	if err := probeContentIs(b, key, first, "after a rejected create-only put"); err != nil {
		return err
	}

	// A CAS with a bogus etag must be rejected AND must not modify content.
	if _, err := b.PutIf(key, second, "offshoot-probe-not-a-real-etag"); err == nil {
		return fmt.Errorf("%w: compare-and-swap with a stale etag was accepted", ErrNoCAS)
	} else if !errors.Is(err, ErrCAS) {
		return fmt.Errorf("store: probe stale-etag put returned an unexpected error: %w", err)
	}
	if err := probeContentIs(b, key, first, "after a rejected compare-and-swap"); err != nil {
		return err
	}

	// A CAS with the current etag must succeed.
	if _, err := b.PutIf(key, second, etag); err != nil {
		return fmt.Errorf("store: probe compare-and-swap with a valid etag failed: %w", err)
	}
	return probeContentIs(b, key, second, "after a successful compare-and-swap")
}

func probeContentIs(b Backend, key string, want []byte, when string) error {
	got, _, err := b.Get(key)
	if err != nil {
		return fmt.Errorf("store: probe read %s: %w", when, err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%w: content changed %s", ErrNoCAS, when)
	}
	return nil
}
```

- [ ] **Step 4: Run and verify**

Run: `go test ./internal/store/... -v -race`
Expected: PASS — all three probe tests plus everything prior.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat: CAS capability probe that refuses stores ignoring preconditions"
```

---

### Task 5: Store URL selection and CLI wiring

**Files:**
- Create: `internal/store/open.go`, `internal/store/open_test.go`
- Modify: `internal/ops/ops.go` (Init/Open), `cmd/offshoot/main.go` (usage text)
- Test: `internal/ops/ops_test.go` (append one test)

**Interfaces:**
- Consumes: `NewLocal`, `NewS3`, `S3Config`, `ProbeCAS` (Tasks 3-4)
- Produces:

```go
package store

// OpenBackend resolves a store spec to a Backend and verifies CAS support.
//   /path/or/relative  → local directory
//   file:///abs/path   → local directory
//   s3://bucket/prefix → S3-compatible; endpoint/region/path-style come from
//                        OFFSHOOT_S3_ENDPOINT, OFFSHOOT_S3_REGION and
//                        OFFSHOOT_S3_PATH_STYLE ("1"/"true")
// The probe runs for every backend; a store that fails it is refused.
func OpenBackend(ctx context.Context, spec string) (Backend, error)
```

`internal/ops` changes: `Init(spec string)` and `Open(spec string)` call `store.OpenBackend` instead of `store.NewLocal`, and `Workspace.Root` becomes the checkout root — for remote stores checkouts must live on local disk, so `Root` becomes `OFFSHOOT_CHECKOUTS` if set, else `<os.UserCacheDir()>/offshoot/<sha256(spec)[:16]>`, and for local stores it stays the store directory itself (unchanged behavior, so Plan-2 tests keep passing). Add `Workspace.Spec string` for diagnostics.

- [ ] **Step 1: Write the failing tests**

`internal/store/open_test.go` — external test package:

```go
package store_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

func TestOpenBackendLocalPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	b, err := store.OpenBackend(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBackendFileURL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	if _, err := store.OpenBackend(context.Background(), "file://"+dir); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBackendS3URL(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")

	b, err := store.OpenBackend(context.Background(), "s3://"+f.Bucket()+"/tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/x", []byte("v"), ""); err != nil {
		t.Fatal(err)
	}
	keys, err := b.List("refs/")
	if err != nil || len(keys) != 1 || keys[0] != "refs/x" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}

func TestOpenBackendRefusesStoreWithoutCAS(t *testing.T) {
	f := storetest.NewFakeS3(t)
	f.IgnorePreconditions(true)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")

	_, err := store.OpenBackend(context.Background(), "s3://"+f.Bucket())
	if err == nil || !strings.Contains(err.Error(), "conditional writes") {
		t.Fatalf("want a refusal naming conditional writes, got %v", err)
	}
}

func TestOpenBackendRejectsUnknownScheme(t *testing.T) {
	if _, err := store.OpenBackend(context.Background(), "gs://bucket/x"); err == nil {
		t.Fatal("unknown scheme must be refused")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/store -run TestOpenBackend -v`
Expected: FAIL — `store.OpenBackend` undefined.

- [ ] **Step 3: Implement OpenBackend**

`internal/store/open.go`:

```go
package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// OpenBackend resolves a store spec to a Backend and verifies that it
// enforces conditional writes.
//
// Specs:
//
//	/path or ./path     local directory
//	file:///abs/path    local directory
//	s3://bucket/prefix  S3-compatible bucket (AWS S3, R2, Tigris, MinIO)
//
// S3 endpoint, region and addressing style come from OFFSHOOT_S3_ENDPOINT,
// OFFSHOOT_S3_REGION and OFFSHOOT_S3_PATH_STYLE; credentials come from the
// AWS SDK default chain.
func OpenBackend(ctx context.Context, spec string) (Backend, error) {
	b, err := newBackend(ctx, spec)
	if err != nil {
		return nil, err
	}
	if err := ProbeCAS(b); err != nil {
		return nil, fmt.Errorf("store %s cannot be used: %w", spec, err)
	}
	return b, nil
}

func newBackend(ctx context.Context, spec string) (Backend, error) {
	if spec == "" {
		return nil, fmt.Errorf("store: empty store spec")
	}
	if !strings.Contains(spec, "://") {
		return NewLocal(spec)
	}
	u, err := url.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("store: invalid store spec %q: %w", spec, err)
	}
	switch u.Scheme {
	case "file":
		return NewLocal(u.Path)
	case "s3":
		if u.Host == "" {
			return nil, fmt.Errorf("store: s3 spec needs a bucket: s3://bucket[/prefix]")
		}
		return NewS3(ctx, S3Config{
			Bucket:       u.Host,
			Prefix:       strings.Trim(u.Path, "/"),
			Endpoint:     os.Getenv("OFFSHOOT_S3_ENDPOINT"),
			Region:       os.Getenv("OFFSHOOT_S3_REGION"),
			UsePathStyle: isTrue(os.Getenv("OFFSHOOT_S3_PATH_STYLE")),
		})
	default:
		return nil, fmt.Errorf("store: unsupported store scheme %q (use a path, file://, or s3://)", u.Scheme)
	}
}

func isTrue(v string) bool {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
```

- [ ] **Step 4: Wire ops.Init/Open**

In `internal/ops/ops.go`, replace the bodies of `Init` and `Open` (keep their names and `(*Workspace, error)` signatures; the parameter is now a store spec, not necessarily a directory):

```go
// Init creates a new store at spec and returns a workspace for it.
func Init(spec string) (*Workspace, error) {
	b, err := store.OpenBackend(context.Background(), spec)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.InitManifest(); err != nil {
		return nil, err
	}
	root, err := checkoutRoot(spec)
	if err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root, Spec: spec}, nil
}

// Open attaches to an existing store at spec.
func Open(spec string) (*Workspace, error) {
	b, err := store.OpenBackend(context.Background(), spec)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.CheckManifest(); err != nil {
		return nil, err
	}
	root, err := checkoutRoot(spec)
	if err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root, Spec: spec}, nil
}

// checkoutRoot decides where materialized checkouts live. For a local store
// they sit inside the store directory (unchanged from local mode). For a
// remote store they go to OFFSHOOT_CHECKOUTS, or a per-store directory under
// the user cache dir — checkouts are real SQLite files and must be local.
func checkoutRoot(spec string) (string, error) {
	if !strings.Contains(spec, "://") {
		return spec, nil
	}
	if u, err := url.Parse(spec); err == nil && u.Scheme == "file" {
		return u.Path, nil
	}
	if dir := os.Getenv("OFFSHOOT_CHECKOUTS"); dir != "" {
		return dir, nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("ops: no checkout directory (set OFFSHOOT_CHECKOUTS): %w", err)
	}
	sum := sha256.Sum256([]byte(spec))
	return filepath.Join(cache, "offshoot", hex.EncodeToString(sum[:8])), nil
}
```

Add `Spec string` to the `Workspace` struct and the imports it needs (`context`, `net/url`, `crypto/sha256`, `encoding/hex`, `strings` — several may already be present).

- [ ] **Step 5: Add the ops-level test**

Append to `internal/ops/ops_test.go`:

```go
func TestInitAndOpenAgainstS3Spec(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("OFFSHOOT_S3_ENDPOINT", f.URL())
	t.Setenv("OFFSHOOT_S3_REGION", "us-east-1")
	t.Setenv("OFFSHOOT_S3_PATH_STYLE", "1")
	t.Setenv("OFFSHOOT_CHECKOUTS", t.TempDir())

	spec := "s3://" + f.Bucket() + "/p"
	w, err := Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(path, os.Getenv("OFFSHOOT_CHECKOUTS")) {
		t.Fatalf("remote-store checkout must live under OFFSHOOT_CHECKOUTS, got %s", path)
	}
	// Re-open the same store and see the database.
	w2, err := Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	sts, err := w2.Status()
	if err != nil || len(sts) != 1 || sts[0].DB != "app" {
		t.Fatalf("status=%v err=%v", sts, err)
	}
}
```

(add `"github.com/sricola/offshoot/internal/store/storetest"`, `"os"`, `"strings"` to the test imports if missing — note `internal/ops` tests are in package `ops`, and `storetest` imports only `store`, so there is no cycle)

- [ ] **Step 6: Update CLI usage text**

In `cmd/offshoot/main.go`, update the usage constant's store line to:

```
Store location: -store SPEC or OFFSHOOT_STORE, default ./.offshoot
  SPEC is a directory path, file:///abs/path, or s3://bucket/prefix
  S3: OFFSHOOT_S3_ENDPOINT, OFFSHOOT_S3_REGION, OFFSHOOT_S3_PATH_STYLE;
      credentials from the AWS SDK default chain (env, shared config, IAM role)
  Remote stores keep checkouts in OFFSHOOT_CHECKOUTS (default: user cache dir)
```

No other CLI change is needed — `-store` is already passed straight to `ops.Init`/`ops.Open`.

- [ ] **Step 7: Run everything**

Run: `go test ./... -count=1 -race && go vet ./...`
Expected: PASS. All Plan-2 tests must still pass unchanged — local specs keep their old behavior (checkouts inside the store dir).

- [ ] **Step 8: Commit**

```bash
git add internal/store internal/ops cmd/offshoot
git commit -m "feat: s3:// store specs with CAS probe at attach and local checkout roots"
```

---

### Task 6: Real-provider integration test and support matrix

**Files:**
- Create: `internal/store/s3_integration_test.go`
- Modify: `README.md`, `Makefile`

**Interfaces:**
- Consumes: `NewS3`, `ProbeCAS`, `storetest.RunConformance`
- Produces: `make test-s3` target; a documented, honest support matrix

The test is env-gated: it runs only when `OFFSHOOT_S3_TEST_BUCKET` is set, uses a unique key prefix per run so it cannot collide with other runs or real data, and cleans up. Passing against the fake is explicitly *not* evidence about a provider; this test is how a provider earns a row in the matrix.

- [ ] **Step 1: Write the integration test**

`internal/store/s3_integration_test.go`:

```go
package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

// TestS3RealProvider runs the full Backend conformance suite and the CAS
// probe against a real S3-compatible provider. Set OFFSHOOT_S3_TEST_BUCKET
// (plus OFFSHOOT_S3_ENDPOINT / OFFSHOOT_S3_REGION / OFFSHOOT_S3_PATH_STYLE
// and credentials as needed) to enable it. This is the only evidence that
// justifies listing a provider as supported — the in-process fake proves
// nothing about a real provider's precondition handling.
func TestS3RealProvider(t *testing.T) {
	bucket := os.Getenv("OFFSHOOT_S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("set OFFSHOOT_S3_TEST_BUCKET to run real-provider tests")
	}
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	prefix := "offshoot-itest/" + hex.EncodeToString(buf)

	newBackend := func(t *testing.T) store.Backend {
		t.Helper()
		b, err := store.NewS3(context.Background(), store.S3Config{
			Bucket:       bucket,
			Prefix:       prefix + "/" + freshSegment(t),
			Endpoint:     os.Getenv("OFFSHOOT_S3_ENDPOINT"),
			Region:       os.Getenv("OFFSHOOT_S3_REGION"),
			UsePathStyle: os.Getenv("OFFSHOOT_S3_PATH_STYLE") == "1",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { deleteAll(t, b) })
		return b
	}

	t.Run("Probe", func(t *testing.T) {
		if err := store.ProbeCAS(newBackend(t)); err != nil {
			t.Fatalf("provider failed the CAS probe: %v", err)
		}
	})
	t.Run("Conformance", func(t *testing.T) {
		storetest.RunConformance(t, "", newBackend)
	})
}

// freshSegment returns a unique path segment so sub-tests never share keys.
func freshSegment(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(buf)
}

func deleteAll(t *testing.T, b store.Backend) {
	t.Helper()
	keys, err := b.List("")
	if err != nil {
		t.Logf("cleanup list failed: %v", err)
		return
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			t.Logf("cleanup delete %s: %v", k, err)
		}
	}
}
```

- [ ] **Step 2: Verify it skips cleanly and compiles**

Run: `go test ./internal/store -run TestS3RealProvider -v`
Expected: `--- SKIP` with the "set OFFSHOOT_S3_TEST_BUCKET" message.

- [ ] **Step 3: Run it against a real provider**

Start MinIO locally (it is the cheapest real provider to verify, and the spec names it as supported):

```bash
docker run -d --rm -p 9000:9000 --name offshoot-minio \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
docker exec offshoot-minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec offshoot-minio mc mb local/offshoot-itest 2>/dev/null || true
```

Then:

```bash
OFFSHOOT_S3_TEST_BUCKET=offshoot-itest \
OFFSHOOT_S3_ENDPOINT=http://localhost:9000 \
OFFSHOOT_S3_REGION=us-east-1 \
OFFSHOOT_S3_PATH_STYLE=1 \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
go test ./internal/store -run TestS3RealProvider -v
```

Expected: PASS for both sub-tests. Record the exact MinIO image tag in your report. If Docker is unavailable in this environment, say so plainly in the report and record the matrix row as unverified — do NOT claim a provider passes without running this.

Stop MinIO when done: `docker stop offshoot-minio`.

- [ ] **Step 4: Add the Makefile target**

Append to `Makefile`:

```make
.PHONY: test-s3
test-s3:
	go test ./internal/store -run TestS3RealProvider -count=1 -v
```

- [ ] **Step 5: Update the README**

Replace the README's status line and add a storage section after the quickstart:

```markdown
**Status: pre-alpha — local and S3-compatible stores working (Plans 2-3); capture spike GO (Plan 1).**
```

```markdown
## Storage

    offshoot -store ./.offshoot init                 # local directory (default)
    offshoot -store s3://my-bucket/offshoot init     # S3-compatible bucket

offshoot's safety rests on compare-and-swap: every branch ref update is a
conditional write. At attach time it **probes the store** and refuses to run
if conditional writes are not enforced, rather than silently degrading.

Configuration for `s3://` specs — credentials come from the AWS SDK default
chain (environment, shared config, IAM role):

| Variable | Meaning |
|---|---|
| `OFFSHOOT_S3_ENDPOINT` | Custom endpoint (R2, Tigris, MinIO) |
| `OFFSHOOT_S3_REGION` | Region; defaults to `auto` when an endpoint is set |
| `OFFSHOOT_S3_PATH_STYLE` | `1` for path-style addressing (MinIO) |
| `OFFSHOOT_CHECKOUTS` | Where checkouts are materialized (remote stores) |

### Provider support

A provider is listed as supported only after the conformance suite and CAS
probe pass against it for real (`make test-s3`) — the in-process fake used in
unit tests proves nothing about a real provider.

| Provider | Status |
|---|---|
| MinIO | verified — see `make test-s3` |
| AWS S3 | expected to pass (conditional writes GA since Nov 2024); not yet run |
| Cloudflare R2 | expected to pass; not yet run |
| Tigris | expected to pass; not yet run |
| Google Cloud Storage (S3 interop) | **unsupported** — no conditional writes on the S3 API; the probe refuses it |

Checkouts are always real local SQLite files; only the snapshots and refs
live in the store.
```

Adjust the MinIO row to match what you actually observed in Step 3 (if Docker was unavailable, mark it "not yet run" and say so in your report — do not write "verified" without evidence).

- [ ] **Step 6: Full suite and commit**

Run: `go test ./... -count=1 && go vet ./...`
Expected: PASS (the integration test skips without the env var).

```bash
git add internal/store Makefile README.md
git commit -m "test: env-gated real-provider conformance run; document the support matrix"
```

---

### Task 7: Adversarial pass — remote-store failure modes

**Files:**
- Modify: `internal/store/storetest/fakes3.go` (fault injection), `internal/store/s3_test.go` (append)
- Modify: only if the tests expose real bugs

**Interfaces:**
- Produces: `func (f *FakeS3) SetFault(fn func(method, key string) (status int, ok bool))` — when `fn` returns `ok`, the request is answered with `status` instead of being served.

Failure modes to pin: a 500 on PutObject surfaces as an error (never a silent success, never `ErrCAS`); a 500 on GetObject is not mistaken for `ErrNotFound`; a 429/503 surfaces as an error rather than being swallowed; `List` over more than one page returns every key (drive with 1500 keys against the fake — add pagination support to the fake first, capping page size at 1000 like S3 and honoring `continuation-token`).

- [ ] **Step 1: Add fault injection and pagination to the fake**

In `internal/store/storetest/fakes3.go`, add to the struct and constructor path:

```go
// fault, when set and returning ok, replaces the response with status.
fault func(method, key string) (int, bool)
```

```go
// SetFault installs a fault-injection hook. fn is called for every request;
// returning ok replaces the response with the given HTTP status.
func (f *FakeS3) SetFault(fn func(method, key string) (int, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fn
}
```

At the top of `handle`, after computing `key` and taking the lock:

```go
	if f.fault != nil {
		if status, ok := f.fault(r.Method, key); ok {
			w.WriteHeader(status)
			io.WriteString(w, `<Error><Code>InternalError</Code></Error>`)
			return
		}
	}
```

Replace `list` with a paginating version:

```go
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
```

and extend `listResult`:

```go
type listResult struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}
```

- [ ] **Step 2: Write the adversarial tests**

Append to `internal/store/s3_test.go`:

```go
func TestS3ServerErrorsAreNotSwallowed(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	f.SetFault(func(method, key string) (int, bool) { return 500, true })

	if err := b.Put("k2", []byte("v")); err == nil {
		t.Error("a 500 on Put must surface as an error")
	}
	if _, err := b.PutIf("k3", []byte("v"), ""); err == nil {
		t.Error("a 500 on conditional Put must surface as an error")
	} else if errors.Is(err, store.ErrCAS) {
		t.Error("a 500 must not be reported as a CAS conflict")
	}
	if _, _, err := b.Get("k"); err == nil {
		t.Error("a 500 on Get must surface as an error")
	} else if errors.Is(err, store.ErrNotFound) {
		t.Error("a 500 must not be reported as ErrNotFound")
	}
	if _, err := b.List("k"); err == nil {
		t.Error("a 500 on List must surface as an error")
	}
}

func TestS3ListPaginates(t *testing.T) {
	f := storetest.NewFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	b, err := store.NewS3(context.Background(), store.S3Config{
		Bucket: f.Bucket(), Endpoint: f.URL(), Region: "us-east-1", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 1500
	for i := 0; i < n; i++ {
		if err := b.Put(fmt.Sprintf("data/lin/%05d.ltx", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := b.List("data/lin/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != n {
		t.Fatalf("got %d keys across pages, want %d", len(keys), n)
	}
	if keys[0] != "data/lin/00000.ltx" || keys[n-1] != fmt.Sprintf("data/lin/%05d.ltx", n-1) {
		t.Fatalf("pagination lost ordering: first=%s last=%s", keys[0], keys[n-1])
	}
}
```

(add `"errors"` and `"fmt"` to the test file's imports)

- [ ] **Step 3: Run, investigate, fix**

Run: `go test ./internal/store/... -v -race -count=2`
Expected: the pagination test may expose a real bug if `List`'s prefix handling or the paginator is wrong; the error tests may expose 500s being mapped to `ErrCAS`/`ErrNotFound` through an over-broad matcher. Fix whatever surfaces in `internal/store/s3.go` — the invariants are: only genuine precondition failures map to `ErrCAS`, only genuine absence maps to `ErrNotFound`, everything else is a loud error. Document each fix in your report.

- [ ] **Step 4: Full suite**

Run: `go test ./... -count=1 -race && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "test: remote-store fault injection and pagination coverage"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** This plan implements spec § Storage layout's remote half (S3-compatible backends behind the existing key layout) and § Supported stores (the named provider list plus the CAS capability probe and the GCS exclusion). Deferred by design and stated in the header: daemon mode with live capture, leases and epoch bumping, incremental segments, TTL (Plan 4); MCP/SDKs/demo (Plan 5). No `internal/ops` lifecycle logic changes here — the Backend seam is the whole point.
2. **Placeholder scan:** none; Task 3 carries a bounded, explicit API-adaptation authorization for the AWS SDK surface (contract and conformance suite fixed), and Task 6 Step 3 prescribes exactly what to record if Docker is unavailable rather than allowing an unverified "supported" claim.
3. **Type consistency:** `Backend` methods match Plan 2 exactly and are unchanged; `S3Config` fields are used identically in Tasks 3, 5, and 6; `storetest.RunConformance(t, keyPrefix, newBackend)` has the same signature in Tasks 1, 3, and 6; `ProbeCAS(b Backend) error` and `ErrNoCAS` are consistent across Tasks 4-5; `OpenBackend(ctx, spec)` is used identically in `open.go` and `ops.Init/Open`; the fake's `IgnorePreconditions`/`SetFault` signatures match their call sites in Tasks 4 and 7.
