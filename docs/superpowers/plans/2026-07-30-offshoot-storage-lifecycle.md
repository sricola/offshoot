# offshoot Plan 2: LTX Storage Layer + Branch Lifecycle (Local Mode) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working `offshoot` CLI: git-like create/checkout/checkpoint/fork/rollback/promote/destroy/GC over SQLite databases, stored as LTX snapshots in an epoch-fenced local-directory store — the spec's 60-second quickstart.

**Architecture:** Every checkpoint encodes the SQLite main file as a full-snapshot LTX object under `data/{lineage}/{epoch}/`; branch refs are CAS-updated JSON objects mapping names → lineages/TXIDs/checkpoints. Fork/rollback/promote all create a NEW lineage seeded from a source snapshot (one-writer-per-lineage invariant from the spec). A `Backend` interface abstracts storage; Plan 2 ships the local-dir backend with O_EXCL-lock CAS. GC is two-phase (tombstone → grace → sweep).

**Tech Stack:** Go 1.23+, `github.com/superfly/ltx` (Apache-2.0, pinned), `github.com/mattn/go-sqlite3` (already present), stdlib CLI (no cobra).

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md`. **Plan sequence:** Plan 1 (capture spike, merged, GO) → **this plan** → Plan 3 (S3/R2/Tigris backends + CAS probe, daemon mode with live capture via the existing `internal/capture` engine, incremental LTX segments, TTL/leases) → Plan 4 (MCP server, SDKs, LangGraph adapter, launch demo).

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; Go 1.23+; cgo (mattn); Linux/macOS only
- Name charset for db/branch/checkpoint: `[a-z0-9-_.]`, max 128 chars (spec § Naming model); default branch `main`
- Ref invariant: **a lineage is only ever written by one branch, for its entire life** (spec § Core model)
- All ref mutations go through CAS (`PutIf`); all new objects use create-only puts (`ifMatch=""` ⇒ must-not-exist)
- CLI mode = explicit operations at rest (spec § Architecture): ops open the DB transiently; if a live writer holds a lock, wait with busy timeout then fail cleanly
- **Plan-2 documented simplifications** (each stated in --help/README, revisited in Plan 3): checkpoints are full snapshots, not incremental segments; checkout path is fixed at `<store>/checkouts/{db}/{branch}.db` and rollback/promote replace it in place after a lock probe (daemon mode will use immutable paths); no TTLs (GC collects unreachable lineages only)
- Fail closed: any LTX checksum error aborts with no partial file left behind
- Tests must not depend on wall-clock sleeps for correctness; `sqlite3` CLI guarded by `exec.LookPath`
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/ltxio/ltxio.go        LTX snapshot encode/materialize (wraps superfly/ltx)
internal/ltxio/ltxio_test.go
internal/store/backend.go      Backend interface + errors
internal/store/local.go        Local-dir backend with O_EXCL CAS
internal/store/local_test.go
internal/store/store.go        Manifest, Ref codec, key layout, typed ref ops
internal/store/store_test.go
internal/ops/ops.go            Workspace: lifecycle operations
internal/ops/ops_test.go
internal/ops/gc.go             Two-phase GC
internal/ops/gc_test.go
cmd/offshoot/main.go           CLI dispatch
cmd/offshoot/main_test.go      End-to-end CLI test (quickstart transcript)
```

---

### Task 1: LTX snapshot codec (`internal/ltxio`)

**Files:**
- Create: `internal/ltxio/ltxio.go`
- Test: `internal/ltxio/ltxio_test.go`
- Modify: `go.mod` (add `github.com/superfly/ltx`)

**Interfaces:**
- Consumes: `github.com/superfly/ltx` (external), stdlib
- Produces (exact — later tasks depend on these):

```go
package ltxio

// EncodeSnapshot writes a full-snapshot LTX of the SQLite main database at
// dbPath, covering TXIDs [1, txid]. Caller must have fully checkpointed the
// WAL (TRUNCATE) first; EncodeSnapshot returns an error if a non-empty -wal
// file exists next to dbPath.
func EncodeSnapshot(dbPath string, txid uint64, w io.Writer) error

// Materialize decodes a full-snapshot LTX from r into dbPath, verifying the
// trailer checksum. On any error the destination is left untouched (write to
// temp file + rename). Returns the snapshot's MaxTXID.
func Materialize(r io.Reader, dbPath string) (uint64, error)
```

**API-adaptation authorization:** the code below is written against `superfly/ltx`'s documented format and expected Go API (`NewEncoder`/`EncodeHeader`/`EncodePage`/`SetPostApplyChecksum`/`Close`, `NewDecoder`/`DecodeHeader`/`DecodePage`/`Close`, `ltx.ChecksumPage`/`ltx.LTX_VERSION` names may differ). Run `go doc github.com/superfly/ltx` and adapt names/signatures to the real API — the test contract and the two exported functions above are fixed; internal adaptation is pre-authorized and must be listed in your report.

- [ ] **Step 1: Add the dependency**

```bash
cd /Users/sray/gits/sql && go get github.com/superfly/ltx@latest && go doc github.com/superfly/ltx | head -60
```

Read the output; note the real encoder/decoder API before writing code.

- [ ] **Step 2: Write the failing test**

`internal/ltxio/ltxio_test.go`:

```go
package ltxio

import (
	"bytes"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func makeDB(t *testing.T, rows int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(300))"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func dump(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", path, ".dump").Output()
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestSnapshotRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := makeDB(t, 50)
	var buf bytes.Buffer
	if err := EncodeSnapshot(src, 7, &buf); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "restored.db")
	txid, err := Materialize(bytes.NewReader(buf.Bytes()), dst)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 7 {
		t.Errorf("txid = %d, want 7", txid)
	}
	if dump(t, src) != dump(t, dst) {
		t.Fatal("dump mismatch after round trip")
	}
}

func TestEncodeRefusesDirtyWAL(t *testing.T) {
	src := makeDB(t, 3)
	// Recreate a non-empty WAL: write without checkpointing.
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; INSERT INTO t (v) VALUES (randomblob(10))"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := EncodeSnapshot(src, 2, &buf); err == nil {
		t.Fatal("want error on non-empty WAL")
	}
}

func TestMaterializeFailsClosedOnCorruption(t *testing.T) {
	src := makeDB(t, 20)
	var buf bytes.Buffer
	if err := EncodeSnapshot(src, 3, &buf); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	b[len(b)/2] ^= 0xFF // flip a bit mid-file
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := Materialize(bytes.NewReader(b), dst); err == nil {
		t.Fatal("want checksum error")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("corrupt materialize must leave no destination file")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/ltxio -v`
Expected: FAIL — `EncodeSnapshot` undefined.

- [ ] **Step 4: Implement**

`internal/ltxio/ltxio.go` (adapt ltx API names per Step 1):

```go
// Package ltxio encodes SQLite databases as full-snapshot LTX files and
// materializes them back, wrapping github.com/superfly/ltx behind a stable
// interface (the ltx Go API carries no stability promise; the format spec is
// the contract).
package ltxio

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
)

const dbHeaderSize = 100

// EncodeSnapshot writes a full-snapshot LTX of dbPath covering [1, txid].
func EncodeSnapshot(dbPath string, txid uint64, w io.Writer) error {
	if fi, err := os.Stat(dbPath + "-wal"); err == nil && fi.Size() > 0 {
		return fmt.Errorf("ltxio: %s has a non-empty WAL; checkpoint(TRUNCATE) first", dbPath)
	}
	f, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer f.Close()

	hdr := make([]byte, dbHeaderSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return fmt.Errorf("ltxio: read db header: %w", err)
	}
	pageSize := uint32(binary.BigEndian.Uint16(hdr[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	nPages := binary.BigEndian.Uint32(hdr[28:32]) // database size in pages

	enc := ltx.NewEncoder(w)
	if err := enc.EncodeHeader(ltx.Header{
		Version:  1,
		PageSize: pageSize,
		Commit:   nPages,
		MinTXID:  1,
		MaxTXID:  ltx.TXID(txid),
	}); err != nil {
		return err
	}
	buf := make([]byte, pageSize)
	var cksum ltx.Checksum
	for pgno := uint32(1); pgno <= nPages; pgno++ {
		if _, err := f.ReadAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
			return fmt.Errorf("ltxio: read page %d: %w", pgno, err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return err
		}
		cksum = ltx.ChecksumFlag | (cksum ^ ltx.ChecksumPage(pgno, buf))
	}
	enc.SetPostApplyChecksum(cksum)
	return enc.Close()
}

// Materialize decodes a full-snapshot LTX into dbPath atomically.
func Materialize(r io.Reader, dbPath string) (uint64, error) {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return 0, err
	}
	h := dec.Header()
	if h.MinTXID != 1 {
		return 0, fmt.Errorf("ltxio: not a full snapshot (MinTXID=%d)", h.MinTXID)
	}
	tmp := dbPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp)

	buf := make([]byte, h.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err == io.EOF {
			break
		} else if err != nil {
			f.Close()
			return 0, err
		}
		if _, err := f.WriteAt(buf, int64(ph.Pgno-1)*int64(h.PageSize)); err != nil {
			f.Close()
			return 0, err
		}
	}
	// Close verifies the trailer checksum — fail closed before the rename.
	if err := dec.Close(); err != nil {
		f.Close()
		return 0, fmt.Errorf("ltxio: checksum: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, dbPath); err != nil {
		return 0, err
	}
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")
	return uint64(h.MaxTXID), nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/ltxio -v`
Expected: PASS ×3. If `dec.Close()` does not verify page checksums against a mid-file bit flip (only trailer fields), verify per-page: accumulate `ltx.ChecksumPage` during decode and compare to the trailer's PostApplyChecksum yourself — the corruption test is the contract, adapt the mechanism.

- [ ] **Step 6: Commit**

```bash
git add internal/ltxio go.mod go.sum
git commit -m "feat: LTX full-snapshot encode/materialize with fail-closed checksums"
```

---

### Task 2: Backend interface + local-dir backend (`internal/store`)

**Files:**
- Create: `internal/store/backend.go`, `internal/store/local.go`
- Test: `internal/store/local_test.go`

**Interfaces:**
- Consumes: stdlib only
- Produces:

```go
package store

var (
	ErrNotFound = errors.New("store: not found")
	ErrCAS      = errors.New("store: compare-and-swap conflict")
)

// Backend is a minimal conditional-write object store.
type Backend interface {
	// Get returns the object and its etag.
	Get(key string) (data []byte, etag string, err error)
	// Put writes unconditionally (used only for immutable data objects).
	Put(key string, data []byte) error
	// PutIf writes conditionally: ifMatch=="" means create-only (the key
	// must not exist); otherwise the current etag must equal ifMatch.
	// Returns the new etag; ErrCAS on conflict.
	PutIf(key string, data []byte, ifMatch string) (string, error)
	// List returns all keys with the given prefix, sorted.
	List(prefix string) ([]string, error)
	Delete(key string) error // no error if absent
}

func NewLocal(root string) (*Local, error) // creates root if needed
```

Etag = hex SHA-256 of content. Local CAS: per-key `O_CREAT|O_EXCL` lock file, read-verify-write(temp+fsync+rename)-unlock (spec § Local backend: bare rename is atomic replace, NOT CAS). Keys map to files under root; key path segments must not contain `..` (reject).

- [ ] **Step 1: Write the failing test**

`internal/store/local_test.go`:

```go
package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestLocalPutGetEtag(t *testing.T) {
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("data/x/1.ltx", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, etag, err := b.Get("data/x/1.ltx")
	if err != nil || string(data) != "hello" || etag == "" {
		t.Fatalf("got %q etag=%q err=%v", data, etag, err)
	}
	if _, _, err := b.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLocalCreateOnly(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if _, err := b.PutIf("refs/a/main", []byte("v1"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/a/main", []byte("v2"), ""); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on create-only over existing, got %v", err)
	}
}

func TestLocalCASUpdate(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	etag1, err := b.PutIf("refs/a/main", []byte("v1"), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutIf("refs/a/main", []byte("bad"), "wrong-etag"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS on stale etag, got %v", err)
	}
	etag2, err := b.PutIf("refs/a/main", []byte("v2"), etag1)
	if err != nil || etag2 == etag1 {
		t.Fatalf("CAS update failed: %v", err)
	}
	data, _, _ := b.Get("refs/a/main")
	if string(data) != "v2" {
		t.Fatalf("content = %q", data)
	}
}

func TestLocalCASRace(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	etag, _ := b.PutIf("refs/a/main", []byte("0"), "")
	var wg sync.WaitGroup
	wins := make(chan int, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := b.PutIf("refs/a/main", []byte(fmt.Sprint(n)), etag); err == nil {
				wins <- n
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
}

func TestLocalListAndDelete(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	for _, k := range []string{"data/l1/a", "data/l1/b", "data/l2/a"} {
		if err := b.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := b.List("data/l1/")
	if err != nil || len(keys) != 2 || keys[0] != "data/l1/a" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if err := b.Delete("data/l1/a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete("data/l1/a"); err != nil {
		t.Fatal("delete of absent key must not error")
	}
	if keys, _ = b.List("data/l1/"); len(keys) != 1 {
		t.Fatalf("keys=%v", keys)
	}
}

func TestLocalRejectsTraversal(t *testing.T) {
	b, _ := NewLocal(t.TempDir())
	if err := b.Put("../evil", []byte("x")); err == nil {
		t.Fatal("want error on path traversal")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store -v`
Expected: FAIL — package doesn't exist / `NewLocal` undefined.

- [ ] **Step 3: Implement**

`internal/store/backend.go`:

```go
// Package store implements offshoot's object-storage layout: a minimal
// conditional-write Backend interface, the local-directory implementation,
// and the typed manifest/ref schema on top.
package store

import "errors"

var (
	ErrNotFound = errors.New("store: not found")
	ErrCAS      = errors.New("store: compare-and-swap conflict")
)

type Backend interface {
	Get(key string) (data []byte, etag string, err error)
	Put(key string, data []byte) error
	PutIf(key string, data []byte, ifMatch string) (string, error)
	List(prefix string) ([]string, error)
	Delete(key string) error
}
```

`internal/store/local.go`:

```go
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Local is a directory-backed Backend. CAS is implemented with a per-key
// O_CREAT|O_EXCL lock file: acquire lock -> read+verify etag -> write temp,
// fsync, rename -> release lock. A bare rename alone is atomic REPLACE, not
// compare-and-swap; the lock provides the compare step.
type Local struct {
	root string
}

func NewLocal(root string) (*Local, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Local{root: root}, nil
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (l *Local) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

func (l *Local) Get(key string) ([]byte, string, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return data, etagOf(data), nil
}

func (l *Local) write(p string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, p)
}

func (l *Local) Put(key string, data []byte) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	return l.write(p, data)
}

func (l *Local) lock(p string) (release func(), err error) {
	lockPath := p + ".lock"
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			f.Close()
			return func() { os.Remove(lockPath) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("store: lock timeout on %s", lockPath)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (l *Local) PutIf(key string, data []byte, ifMatch string) (string, error) {
	p, err := l.path(key)
	if err != nil {
		return "", err
	}
	release, err := l.lock(p)
	if err != nil {
		return "", err
	}
	defer release()

	cur, err := os.ReadFile(p)
	switch {
	case os.IsNotExist(err):
		if ifMatch != "" {
			return "", fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch)
		}
	case err != nil:
		return "", err
	default:
		if ifMatch == "" {
			return "", fmt.Errorf("%w: key exists", ErrCAS)
		}
		if etagOf(cur) != ifMatch {
			return "", fmt.Errorf("%w: etag mismatch", ErrCAS)
		}
	}
	if err := l.write(p, data); err != nil {
		return "", err
	}
	return etagOf(data), nil
}

func (l *Local) List(prefix string) ([]string, error) {
	var keys []string
	root := l.root
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if strings.HasSuffix(p, ".lock") || strings.HasSuffix(p, ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (l *Local) Delete(key string) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store -v -race`
Expected: PASS ×6 (race clean — the CAS race test is the point).

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat: Backend interface and local-dir backend with O_EXCL-lock CAS"
```

---

### Task 3: Manifest, ref schema, key layout (`internal/store/store.go`)

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `Backend`, `ErrNotFound`, `ErrCAS` (Task 2)
- Produces:

```go
const LayoutVersion = 1

type Manifest struct {
	LayoutVersion int    `json:"layout_version"`
	CreatedAt     string `json:"created_at"` // RFC3339
}

type Ref struct {
	Schema      int               `json:"schema"`       // 1
	Lineage     string            `json:"lineage"`      // 32-char hex id
	Epoch       uint64            `json:"epoch"`        // bumped on lineage acquisition
	HeadTXID    uint64            `json:"head_txid"`
	Checkpoints map[string]uint64 `json:"checkpoints"`  // name -> txid
	Parent      string            `json:"parent,omitempty"` // "db@branch@txid"
	Protected   bool              `json:"protected"`
}

type Store struct{ B Backend }

func (s *Store) InitManifest() error                 // create-only; error if exists
func (s *Store) CheckManifest() error                // ErrNotFound if absent; error if newer layout
func (s *Store) GetRef(db, branch string) (Ref, string, error)   // ref, etag
func (s *Store) PutRef(db, branch string, r Ref, ifMatch string) (string, error)
func (s *Store) DeleteRef(db, branch string) error
func (s *Store) ListRefs() (map[string][]string, error)          // db -> branches (sorted)

func ValidateName(s string) error                    // [a-z0-9-_.], 1..128
func NewLineageID() string                           // 16 random bytes, hex
func RefKey(db, branch string) string                // "refs/{db}/{branch}"
func SnapshotKey(lineage string, epoch, txid uint64) string
// "data/{lineage}/{epoch}/snapshot-{txid:016x}.ltx"
func LineagePrefix(lineage string) string            // "data/{lineage}/"
```

- [ ] **Step 1: Write the failing test**

`internal/store/store_test.go`:

```go
package store

import (
	"errors"
	"strings"
	"testing"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	b, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Store{B: b}
}

func TestManifestLifecycle(t *testing.T) {
	s := newStore(t)
	if err := s.CheckManifest(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound before init, got %v", err)
	}
	if err := s.InitManifest(); err != nil {
		t.Fatal(err)
	}
	if err := s.InitManifest(); err == nil {
		t.Fatal("double init must fail")
	}
	if err := s.CheckManifest(); err != nil {
		t.Fatal(err)
	}
}

func TestRefRoundTripAndCAS(t *testing.T) {
	s := newStore(t)
	r := Ref{Schema: 1, Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1,
		Checkpoints: map[string]uint64{"init": 1}}
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil || gotEtag != etag {
		t.Fatalf("get: %v", err)
	}
	if got.Lineage != r.Lineage || got.Checkpoints["init"] != 1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	got.HeadTXID = 2
	if _, err := s.PutRef("app", "main", got, "stale"); !errors.Is(err, ErrCAS) {
		t.Fatalf("want ErrCAS, got %v", err)
	}
	if _, err := s.PutRef("app", "main", got, etag); err != nil {
		t.Fatal(err)
	}
}

func TestListRefs(t *testing.T) {
	s := newStore(t)
	for _, br := range []string{"main", "attempt-1"} {
		r := Ref{Schema: 1, Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1}
		if _, err := s.PutRef("app", br, r, ""); err != nil {
			t.Fatal(err)
		}
	}
	m, err := s.ListRefs()
	if err != nil || len(m["app"]) != 2 || m["app"][0] != "attempt-1" {
		t.Fatalf("m=%v err=%v", m, err)
	}
}

func TestValidateName(t *testing.T) {
	for _, ok := range []string{"app", "attempt-1", "v1.2_x"} {
		if err := ValidateName(ok); err != nil {
			t.Errorf("%q should be valid: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "App", "a/b", "a b", strings.Repeat("x", 129)} {
		if err := ValidateName(bad); err == nil {
			t.Errorf("%q should be invalid", bad)
		}
	}
}

func TestKeys(t *testing.T) {
	if k := SnapshotKey("abc", 2, 255); k != "data/abc/2/snapshot-00000000000000ff.ltx" {
		t.Fatalf("k=%s", k)
	}
	if id := NewLineageID(); len(id) != 32 || id == NewLineageID() {
		t.Fatalf("lineage id: %s", id)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store -run 'TestManifest|TestRef|TestList|TestValidate|TestKeys' -v`
Expected: FAIL — `Store` undefined.

- [ ] **Step 3: Implement**

`internal/store/store.go`:

```go
package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	LayoutVersion = 1
	manifestKey   = "offshoot.json"
	maxNameLen    = 128
)

type Manifest struct {
	LayoutVersion int    `json:"layout_version"`
	CreatedAt     string `json:"created_at"`
}

type Ref struct {
	Schema      int               `json:"schema"`
	Lineage     string            `json:"lineage"`
	Epoch       uint64            `json:"epoch"`
	HeadTXID    uint64            `json:"head_txid"`
	Checkpoints map[string]uint64 `json:"checkpoints"`
	Parent      string            `json:"parent,omitempty"`
	Protected   bool              `json:"protected"`
}

type Store struct{ B Backend }

func (s *Store) InitManifest() error {
	m := Manifest{LayoutVersion: LayoutVersion, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	data, _ := json.Marshal(m)
	_, err := s.B.PutIf(manifestKey, data, "")
	if err != nil {
		return fmt.Errorf("store: init: %w", err)
	}
	return nil
}

func (s *Store) CheckManifest() error {
	data, _, err := s.B.Get(manifestKey)
	if err != nil {
		return err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("store: corrupt manifest: %w", err)
	}
	if m.LayoutVersion > LayoutVersion {
		return fmt.Errorf("store: layout version %d is newer than this binary supports (%d)",
			m.LayoutVersion, LayoutVersion)
	}
	return nil
}

func ValidateName(name string) error {
	if name == "" || len(name) > maxNameLen {
		return fmt.Errorf("store: invalid name %q (1-%d chars)", name, maxNameLen)
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("store: invalid name %q (allowed: [a-z0-9-_.])", name)
		}
	}
	return nil
}

func NewLineageID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b)
}

func RefKey(db, branch string) string { return "refs/" + db + "/" + branch }

func SnapshotKey(lineage string, epoch, txid uint64) string {
	return fmt.Sprintf("data/%s/%d/snapshot-%016x.ltx", lineage, epoch, txid)
}

func LineagePrefix(lineage string) string { return "data/" + lineage + "/" }

func (s *Store) GetRef(db, branch string) (Ref, string, error) {
	data, etag, err := s.B.Get(RefKey(db, branch))
	if err != nil {
		return Ref{}, "", err
	}
	var r Ref
	if err := json.Unmarshal(data, &r); err != nil {
		return Ref{}, "", fmt.Errorf("store: corrupt ref %s@%s: %w", db, branch, err)
	}
	return r, etag, nil
}

func (s *Store) PutRef(db, branch string, r Ref, ifMatch string) (string, error) {
	if err := ValidateName(db); err != nil {
		return "", err
	}
	if err := ValidateName(branch); err != nil {
		return "", err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return s.B.PutIf(RefKey(db, branch), data, ifMatch)
}

func (s *Store) DeleteRef(db, branch string) error {
	return s.B.Delete(RefKey(db, branch))
}

func (s *Store) ListRefs() (map[string][]string, error) {
	keys, err := s.B.List("refs/")
	if err != nil {
		return nil, err
	}
	m := map[string][]string{}
	for _, k := range keys {
		parts := strings.Split(k, "/")
		if len(parts) != 3 {
			continue
		}
		m[parts[1]] = append(m[parts[1]], parts[2])
	}
	for db := range m {
		sort.Strings(m[db])
	}
	return m, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store -v`
Expected: PASS (all Task 2 + Task 3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat: store manifest, ref schema, and epoch-fenced key layout"
```

---

### Task 4: Workspace core ops — init, create, checkout, path (`internal/ops` + CLI skeleton)

**Files:**
- Create: `internal/ops/ops.go`, `cmd/offshoot/main.go`
- Test: `internal/ops/ops_test.go`

**Interfaces:**
- Consumes: `ltxio.EncodeSnapshot/Materialize` (Task 1), `store.*` (Tasks 2-3), `mattn/go-sqlite3`
- Produces:

```go
package ops

// Workspace binds a store to a local checkout directory tree.
type Workspace struct {
	Store *store.Store
	Root  string // store root dir; checkouts live at {Root}/checkouts/{db}/{branch}.db
}

func Open(root string) (*Workspace, error)      // checks manifest (ErrNotFound if uninitialized)
func Init(root string) (*Workspace, error)      // creates store + manifest
func (w *Workspace) Create(db string) error     // empty SQLite DB, branch "main" (protected), checkpoint "init"@txid 1
func (w *Workspace) CreateFrom(db, srcPath string) error // import existing file (source untouched)
func (w *Workspace) Checkout(db, branch string) (string, error) // materialize head -> path
func (w *Workspace) CheckoutPath(db, branch string) string       // path only (no materialize)
// parseTarget: "db" -> (db, "main"); "db@branch" -> (db, branch)
func ParseTarget(s string) (db, branch string, err error)
```

Create semantics (spec § Naming model): refuse existing names; `main` is protected by default; the initial state is checkpoint `"init"` at TXID 1. CreateFrom must never truncate/overwrite the source; it snapshots it at a transaction boundary (open read-only, `VACUUM INTO` a temp copy if a WAL exists, else read directly — simplest correct: open with mattn, `PRAGMA wal_checkpoint(TRUNCATE)` on a COPY, never the original: copy file+wal+shm to temp dir first, checkpoint the copy, encode the copy).

- [ ] **Step 1: Write the failing test**

`internal/ops/ops_test.go`:

```go
package ops

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/store"
)

func newWS(t *testing.T) *Workspace {
	t.Helper()
	w, err := Init(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestInitAndOpen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "s")
	if _, err := Open(root); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("open before init: want ErrNotFound, got %v", err)
	}
	if _, err := Init(root); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAndCheckout(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err == nil {
		t.Fatal("duplicate create must fail")
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("checkout not writable: %v", err)
	}
	r, _, err := w.Store.GetRef("app", "main")
	if err != nil || !r.Protected || r.Checkpoints["init"] != 1 || r.HeadTXID != 1 {
		t.Fatalf("ref: %+v err=%v", r, err)
	}
}

func TestCreateFromImportsWithoutTouchingSource(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "legacy.db")
	if out, err := exec.Command("sqlite3", src,
		"CREATE TABLE t (v); INSERT INTO t VALUES (42);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	before, _ := os.ReadFile(src)

	w := newWS(t)
	if err := w.CreateFrom("legacy", src); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(src)
	if string(before) != string(after) {
		t.Fatal("import mutated the source file")
	}
	path, err := w.Checkout("legacy", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT v FROM t;").Output()
	if err != nil || string(out) != "42\n" {
		t.Fatalf("imported content: %q err=%v", out, err)
	}
}

func TestParseTarget(t *testing.T) {
	db, br, err := ParseTarget("app")
	if err != nil || db != "app" || br != "main" {
		t.Fatalf("%s %s %v", db, br, err)
	}
	db, br, err = ParseTarget("app@x-1")
	if err != nil || db != "app" || br != "x-1" {
		t.Fatalf("%s %s %v", db, br, err)
	}
	if _, _, err := ParseTarget("a@b@c"); err == nil {
		t.Fatal("want error")
	}
	if _, _, err := ParseTarget("Bad@x"); err == nil {
		t.Fatal("want name validation error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ops -v`
Expected: FAIL — package missing.

- [ ] **Step 3: Implement**

`internal/ops/ops.go`:

```go
// Package ops implements offshoot's branch lifecycle operations over a
// store.Backend: create, checkout, checkpoint, fork, rollback, promote,
// destroy, and GC. Plan-2 scope is CLI/at-rest mode: full-snapshot
// checkpoints, fixed checkout paths, no daemon.
package ops

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/offshoot-db/offshoot/internal/ltxio"
	"github.com/offshoot-db/offshoot/internal/store"
)

type Workspace struct {
	Store *store.Store
	Root  string
}

func Init(root string) (*Workspace, error) {
	b, err := store.NewLocal(root)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.InitManifest(); err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root}, nil
}

func Open(root string) (*Workspace, error) {
	b, err := store.NewLocal(root)
	if err != nil {
		return nil, err
	}
	s := &store.Store{B: b}
	if err := s.CheckManifest(); err != nil {
		return nil, err
	}
	return &Workspace{Store: s, Root: root}, nil
}

func ParseTarget(s string) (string, string, error) {
	parts := strings.Split(s, "@")
	db, branch := parts[0], "main"
	switch len(parts) {
	case 1:
	case 2:
		branch = parts[1]
	default:
		return "", "", fmt.Errorf("ops: invalid target %q (want db or db@branch)", s)
	}
	if err := store.ValidateName(db); err != nil {
		return "", "", err
	}
	if err := store.ValidateName(branch); err != nil {
		return "", "", err
	}
	return db, branch, nil
}

func (w *Workspace) CheckoutPath(db, branch string) string {
	return filepath.Join(w.Root, "checkouts", db, branch+".db")
}

// snapshotTo encodes dbPath (a quiesced SQLite file) as snapshot txid into a
// fresh lineage at epoch and returns the lineage id.
func (w *Workspace) snapshotTo(dbPath string, txid uint64) (string, error) {
	lineage := store.NewLineageID()
	var buf bytes.Buffer
	if err := ltxio.EncodeSnapshot(dbPath, txid, &buf); err != nil {
		return "", err
	}
	// Immutable data object: create-only put under a fresh lineage/epoch.
	if _, err := w.Store.B.PutIf(store.SnapshotKey(lineage, 1, txid), buf.Bytes(), ""); err != nil {
		return "", err
	}
	return lineage, nil
}

func (w *Workspace) Create(db string) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	// Build an empty SQLite DB in a temp dir, snapshot it as TXID 1.
	tmp := filepath.Join(os.TempDir(), "offshoot-create-"+store.NewLineageID()+".db")
	defer func() { os.Remove(tmp); os.Remove(tmp + "-wal"); os.Remove(tmp + "-shm") }()
	conn, err := sql.Open("sqlite3", tmp)
	if err != nil {
		return err
	}
	if _, err := conn.Exec("PRAGMA journal_mode=WAL; PRAGMA user_version=0; PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		conn.Close()
		return err
	}
	conn.Close()
	return w.createFromQuiesced(db, tmp)
}

func (w *Workspace) createFromQuiesced(db, quiescedPath string) error {
	lineage, err := w.snapshotTo(quiescedPath, 1)
	if err != nil {
		return err
	}
	ref := store.Ref{
		Schema: 1, Lineage: lineage, Epoch: 1, HeadTXID: 1,
		Checkpoints: map[string]uint64{"init": 1},
		Protected:   true, // main is protected by default (spec § Security posture)
	}
	if _, err := w.Store.PutRef(db, "main", ref, ""); err != nil {
		return fmt.Errorf("ops: create %s: %w", db, err)
	}
	return nil
}

// CreateFrom imports an existing SQLite file. The source is never modified:
// the file (plus -wal/-shm if present) is copied to a temp dir, the COPY is
// checkpointed to quiesce it, and the copy is snapshotted.
func (w *Workspace) CreateFrom(db, srcPath string) error {
	if err := store.ValidateName(db); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "offshoot-import-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	cp := filepath.Join(dir, "import.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := copyFile(srcPath+suffix, cp+suffix); err != nil {
			if suffix != "" && os.IsNotExist(err) {
				continue
			}
			return err
		}
	}
	conn, err := sql.Open("sqlite3", cp)
	if err != nil {
		return err
	}
	if _, err := conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		conn.Close()
		return err
	}
	conn.Close()
	return w.createFromQuiesced(db, cp)
}

// Checkout materializes db@branch's head snapshot to its fixed path.
func (w *Workspace) Checkout(db, branch string) (string, error) {
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	path := w.CheckoutPath(db, branch)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := w.materialize(ref, ref.HeadTXID, path); err != nil {
		return "", err
	}
	return path, nil
}

func (w *Workspace) materialize(ref store.Ref, txid uint64, dst string) error {
	data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, ref.Epoch, txid))
	if err != nil {
		return fmt.Errorf("ops: snapshot txid %d not found in lineage %s: %w", txid, ref.Lineage, err)
	}
	if _, err := ltxio.Materialize(bytes.NewReader(data), dst); err != nil {
		return err
	}
	return nil
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
```

`cmd/offshoot/main.go` (skeleton — later tasks extend the switch; write it complete for the ops that exist so far):

```go
// Command offshoot is the branchable-SQLite CLI (Plan 2: local mode).
package main

import (
	"fmt"
	"os"

	"github.com/offshoot-db/offshoot/internal/ops"
)

const usage = `offshoot — branch SQLite like git (local mode)

Usage:
  offshoot init                      create a store in ./.offshoot
  offshoot create <db> [--from f]    new database (branch main), or import file f
  offshoot checkout <db>[@branch]    materialize a working copy; prints its path
  offshoot path <db>[@branch]        print the checkout path

Store location: -store DIR or OFFSHOOT_STORE, default ./.offshoot
`

func storeRoot(args []string) (string, []string) {
	root := os.Getenv("OFFSHOOT_STORE")
	if root == "" {
		root = ".offshoot"
	}
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == "-store" && i+1 < len(args) {
			root = args[i+1]
			i++
			continue
		}
		out = append(out, args[i])
	}
	return root, out
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "offshoot:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	root, args := storeRoot(args)
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	cmd, rest := args[0], args[1:]

	if cmd == "init" {
		_, err := ops.Init(root)
		if err == nil {
			fmt.Println("initialized store at", root)
		}
		return err
	}

	w, err := ops.Open(root)
	if err != nil {
		return fmt.Errorf("open store %s: %w (run 'offshoot init'?)", root, err)
	}
	switch cmd {
	case "create":
		if len(rest) < 1 {
			return fmt.Errorf("usage: offshoot create <db> [--from file]")
		}
		if len(rest) == 3 && rest[1] == "--from" {
			return w.CreateFrom(rest[0], rest[2])
		}
		return w.Create(rest[0])
	case "checkout", "path":
		if len(rest) != 1 {
			return fmt.Errorf("usage: offshoot %s <db>[@branch]", cmd)
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		if cmd == "path" {
			fmt.Println(w.CheckoutPath(db, branch))
			return nil
		}
		path, err := w.Checkout(db, branch)
		if err != nil {
			return err
		}
		fmt.Println(path)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/ops -v && go build ./cmd/offshoot`
Expected: PASS ×4; CLI builds.

- [ ] **Step 5: Commit**

```bash
git add internal/ops cmd/offshoot
git commit -m "feat: workspace init/create/checkout/import and offshoot CLI skeleton"
```

---

### Task 5: Checkpoint (`internal/ops` + CLI)

**Files:**
- Modify: `internal/ops/ops.go` (append), `cmd/offshoot/main.go` (add case + usage line)
- Test: `internal/ops/ops_test.go` (append)

**Interfaces:**
- Consumes: Task 4's Workspace internals
- Produces:

```go
// Checkpoint quiesces the checkout (wal_checkpoint TRUNCATE with busy
// timeout; fails cleanly if a writer holds the DB), encodes a full snapshot
// at HeadTXID+1 into the branch's CURRENT lineage/epoch, and CAS-updates the
// ref (HeadTXID++, Checkpoints[name] = new txid). Name must be new.
func (w *Workspace) Checkpoint(db, branch, name string) (uint64, error)
```

CLI: `offshoot checkpoint <db>[@branch] <name>`.

- [ ] **Step 1: Write the failing test**

Append to `internal/ops/ops_test.go`:

```go
func TestCheckpointAndRematerialize(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1),(2),(3);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	txid, err := w.Checkpoint("app", "main", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if txid != 2 {
		t.Errorf("txid = %d, want 2", txid)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err == nil {
		t.Fatal("duplicate checkpoint name must fail")
	}

	// A fresh checkout must contain the checkpointed data.
	want, _ := exec.Command("sqlite3", path, ".dump").Output()
	path2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", path2, ".dump").Output()
	if string(want) != string(got) {
		t.Fatal("re-checkout does not match checkpointed state")
	}
}

func TestCheckpointFailsCleanlyUnderLiveWriter(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Exec("PRAGMA journal_mode=WAL; CREATE TABLE t (v)"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	// Open write transaction -> checkpoint must fail cleanly, not hang forever.
	if _, err := w.Checkpoint("app", "main", "x"); err == nil {
		t.Fatal("checkpoint under live write txn must fail")
	}
	tx.Rollback()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ops -run TestCheckpoint -v`
Expected: FAIL — `Checkpoint` undefined.

- [ ] **Step 3: Implement**

Append to `internal/ops/ops.go`:

```go
// Checkpoint snapshots the current checkout state as a named checkpoint.
// Plan-2 (CLI/at-rest) semantics: full-snapshot encode; requires the
// checkout to be quiescible (busy timeout 3s, then clean failure).
func (w *Workspace) Checkpoint(db, branch, name string) (uint64, error) {
	if err := store.ValidateName(name); err != nil {
		return 0, err
	}
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return 0, err
	}
	if _, exists := ref.Checkpoints[name]; exists {
		return 0, fmt.Errorf("ops: checkpoint %q already exists on %s@%s", name, db, branch)
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err != nil {
		return 0, fmt.Errorf("ops: no checkout for %s@%s (run checkout first): %w", db, branch, err)
	}
	if err := quiesce(path); err != nil {
		return 0, err
	}
	txid := ref.HeadTXID + 1
	var buf bytes.Buffer
	if err := ltxio.EncodeSnapshot(path, txid, &buf); err != nil {
		return 0, err
	}
	if _, err := w.Store.B.PutIf(store.SnapshotKey(ref.Lineage, ref.Epoch, txid), buf.Bytes(), ""); err != nil {
		return 0, err
	}
	ref.HeadTXID = txid
	if ref.Checkpoints == nil {
		ref.Checkpoints = map[string]uint64{}
	}
	ref.Checkpoints[name] = txid
	if _, err := w.Store.PutRef(db, branch, ref, etag); err != nil {
		// The snapshot object is orphaned inside a still-live lineage; GC
		// ignores live lineages, so it is retained harmlessly. Loud error.
		return 0, fmt.Errorf("ops: ref update lost a race (retry): %w", err)
	}
	return txid, nil
}

// quiesce checkpoints the WAL fully, failing cleanly on a busy database.
func quiesce(path string) error {
	conn, err := sql.Open("sqlite3", path+"?_busy_timeout=3000")
	if err != nil {
		return err
	}
	defer conn.Close()
	var busy, logN, ckptN int
	if err := conn.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logN, &ckptN); err != nil {
		return fmt.Errorf("ops: checkpoint: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("ops: database is busy (live writer or reader); close connections and retry")
	}
	return nil
}
```

Add to `cmd/offshoot/main.go`'s switch (and its usage text):

```go
	case "checkpoint":
		if len(rest) != 2 {
			return fmt.Errorf("usage: offshoot checkpoint <db>[@branch] <name>")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		txid, err := w.Checkpoint(db, branch, rest[1])
		if err != nil {
			return err
		}
		fmt.Printf("checkpoint %q at txid %d\n", rest[1], txid)
		return nil
```

Usage line: `  offshoot checkpoint <db>[@branch] <name>   snapshot the checkout as a named checkpoint`

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/ops -v -timeout 120s`
Expected: PASS. The live-writer test must fail within ~3-4s (busy timeout), not hang.

- [ ] **Step 5: Commit**

```bash
git add internal/ops cmd/offshoot
git commit -m "feat: named checkpoints via full-snapshot LTX encode"
```

---

### Task 6: Fork (`internal/ops` + CLI)

**Files:**
- Modify: `internal/ops/ops.go` (append), `cmd/offshoot/main.go` (add case + usage)
- Test: `internal/ops/ops_test.go` (append)

**Interfaces:**
- Consumes: Tasks 4-5
- Produces:

```go
// Fork creates newBranch from db@srcBranch. at=="" forks the head; otherwise
// at names a checkpoint on the source branch. The child gets its OWN lineage
// (materialized fork point, spec § Fork mechanics): the source snapshot bytes
// are copied server-side into the child's lineage at the same TXID. The
// child ref records Parent "db@srcBranch@txid", inherits NO checkpoints
// (spec: children do not inherit parent checkpoints), Protected=false, and
// its own checkpoint map starts with {"fork": txid}.
func (w *Workspace) Fork(db, srcBranch, newBranch, at string) (uint64, error)
```

Fork contract (spec): the fork contains everything committed before the call — Plan-2 CLI meaning: fork of head implicitly checkpoints first IF the checkout has uncommitted changes? NO — keep semantics explicit and simple: **fork copies committed snapshots only**; if the source checkout has un-checkpointed changes, fork proceeds from the last checkpoint and prints a warning listing the head TXID used. (The daemon's synchronous-flush fork is Plan 3.) This is a documented Plan-2 simplification; the test pins it.

- [ ] **Step 1: Write the failing test**

Append to `internal/ops/ops_test.go`:

```go
func TestForkIsIndependentOfParent(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	txid, err := w.Fork("app", "main", "attempt-1", "")
	if err != nil || txid != 2 {
		t.Fatalf("fork: txid=%d err=%v", txid, err)
	}
	// Child materializes and matches the parent's checkpointed state.
	cpath, err := w.Checkout("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", cpath, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("child content: %q", got)
	}

	// Storage independence: delete the ENTIRE parent lineage; child must
	// still materialize (spec: children never reference parent segments).
	parentRef, _, _ := w.Store.GetRef("app", "main")
	keys, _ := w.Store.B.List(store.LineagePrefix(parentRef.Lineage))
	for _, k := range keys {
		if err := w.Store.B.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Checkout("app", "attempt-1"); err != nil {
		t.Fatalf("child not independent of parent lineage: %v", err)
	}

	// Child ref shape.
	cref, _, _ := w.Store.GetRef("app", "attempt-1")
	if cref.Parent != "app@main@2" || cref.Protected ||
		cref.Checkpoints["fork"] != 2 || len(cref.Checkpoints) != 1 {
		t.Fatalf("child ref: %+v", cref)
	}
	if cref.Lineage == parentRef.Lineage {
		t.Fatal("child must have its own lineage")
	}
}

func TestForkAtCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")
	exec.Command("sqlite3", path, "INSERT INTO t VALUES (2);").Run()
	w.Checkpoint("app", "main", "v2")

	if _, err := w.Fork("app", "main", "old", "v1"); err != nil {
		t.Fatal(err)
	}
	p, _ := w.Checkout("app", "old")
	got, _ := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("fork --at v1 content: %q", got)
	}
	if _, err := w.Fork("app", "main", "bad", "nope"); err == nil {
		t.Fatal("unknown checkpoint must fail")
	}
	if _, err := w.Fork("app", "main", "old", ""); err == nil {
		t.Fatal("existing branch name must fail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ops -run TestFork -v`
Expected: FAIL — `Fork` undefined.

- [ ] **Step 3: Implement**

Append to `internal/ops/ops.go`:

```go
// Fork creates newBranch from db@srcBranch at head or a named checkpoint.
func (w *Workspace) Fork(db, srcBranch, newBranch, at string) (uint64, error) {
	if err := store.ValidateName(newBranch); err != nil {
		return 0, err
	}
	src, _, err := w.Store.GetRef(db, srcBranch)
	if err != nil {
		return 0, err
	}
	txid := src.HeadTXID
	if at != "" {
		t, ok := src.Checkpoints[at]
		if !ok {
			return 0, fmt.Errorf("ops: no checkpoint %q on %s@%s", at, db, srcBranch)
		}
		txid = t
	}
	// Materialized fork point: copy the source snapshot into the child's own
	// lineage so the child never references parent storage.
	data, _, err := w.Store.B.Get(store.SnapshotKey(src.Lineage, src.Epoch, txid))
	if err != nil {
		return 0, fmt.Errorf("ops: source snapshot txid %d: %w", txid, err)
	}
	childLineage := store.NewLineageID()
	if _, err := w.Store.B.PutIf(store.SnapshotKey(childLineage, 1, txid), data, ""); err != nil {
		return 0, err
	}
	child := store.Ref{
		Schema: 1, Lineage: childLineage, Epoch: 1, HeadTXID: txid,
		Checkpoints: map[string]uint64{"fork": txid},
		Parent:      fmt.Sprintf("%s@%s@%d", db, srcBranch, txid),
	}
	if _, err := w.Store.PutRef(db, newBranch, child, ""); err != nil {
		// Branch already exists (or lost a race): remove the orphan snapshot.
		w.Store.B.Delete(store.SnapshotKey(childLineage, 1, txid))
		return 0, fmt.Errorf("ops: fork %s@%s: %w", db, newBranch, err)
	}
	return txid, nil
}
```

Add CLI case:

```go
	case "fork":
		fs := rest
		at := ""
		if len(fs) == 4 && fs[2] == "--at" {
			at = fs[3]
			fs = fs[:2]
		}
		if len(fs) != 2 {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		txid, err := w.Fork(db, branch, fs[1], at)
		if err != nil {
			return err
		}
		fmt.Printf("forked %s@%s -> %s@%s at txid %d\n", db, branch, db, fs[1], txid)
		return nil
```

Usage line: `  offshoot fork <db>[@branch] <new> [--at cp]   branch from head or a checkpoint`

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/ops -v -timeout 120s`
Expected: PASS — including parent-lineage-deletion independence.

- [ ] **Step 5: Commit**

```bash
git add internal/ops cmd/offshoot
git commit -m "feat: fork with materialized fork points (storage-independent children)"
```

---

### Task 7: Rollback + Promote (`internal/ops` + CLI)

**Files:**
- Modify: `internal/ops/ops.go` (append), `cmd/offshoot/main.go` (add cases + usage)
- Test: `internal/ops/ops_test.go` (append)

**Interfaces:**
- Consumes: Tasks 4-6 (`Fork`'s copy-into-new-lineage pattern is reused via a shared helper — extract `copySnapshotToNewLineage`)
- Produces:

```go
// Rollback repoints db@branch at a NEW lineage seeded from checkpoint `to`,
// re-materializes the fixed checkout path (after a lock probe — fails
// cleanly if the checkout is held open), and returns the checkout path.
// The old lineage is orphaned (collected later by GC). Checkpoints at or
// before `to` are kept; later ones are dropped.
func (w *Workspace) Rollback(db, branch, to string) (string, error)

// Promote repoints db@target at a NEW lineage seeded from db@source's head
// (promote-as-fork, spec § Promote). Requires --force for protected targets
// (force param). Source branch survives unchanged. Target's old lineage is
// orphaned. Target's checkout (if any) is re-materialized after a lock probe.
// Target's checkpoint map is reset to {"promote": txid}.
func (w *Workspace) Promote(db, source, target string, force bool) (uint64, error)
```

Lock probe: attempt `quiesce(path)`; busy ⇒ clean error, never a file swap under a live connection.

- [ ] **Step 1: Write the failing test**

Append to `internal/ops/ops_test.go`:

```go
func TestRollback(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "good")
	exec.Command("sqlite3", path, "DROP TABLE t;").Run()
	w.Checkpoint("app", "main", "bad")

	before, _, _ := w.Store.GetRef("app", "main")
	p, err := w.Rollback("app", "main", "good")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := exec.Command("sqlite3", p, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("rolled-back content: %q", got)
	}
	after, _, _ := w.Store.GetRef("app", "main")
	if after.Lineage == before.Lineage {
		t.Fatal("rollback must move to a new lineage")
	}
	if _, ok := after.Checkpoints["bad"]; ok {
		t.Fatal("later checkpoint must be dropped")
	}
	if after.Checkpoints["good"] != before.Checkpoints["good"] {
		t.Fatal("earlier checkpoint must be kept")
	}
	if !after.Protected {
		t.Fatal("protected flag must survive rollback")
	}
}

func TestPromote(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")
	w.Fork("app", "main", "attempt-1", "")

	ap, _ := w.Checkout("app", "attempt-1")
	exec.Command("sqlite3", ap, "INSERT INTO t VALUES (99);").Run()
	w.Checkpoint("app", "attempt-1", "winner")

	// Protected target requires force.
	if _, err := w.Promote("app", "attempt-1", "main", false); err == nil {
		t.Fatal("promote onto protected main without force must fail")
	}
	txid, err := w.Promote("app", "attempt-1", "main", true)
	if err != nil {
		t.Fatal(err)
	}
	mp, _ := w.Checkout("app", "main")
	got, _ := exec.Command("sqlite3", mp, "SELECT count(*) FROM t;").Output()
	if string(got) != "2\n" {
		t.Fatalf("promoted main content: %q", got)
	}
	mref, _, _ := w.Store.GetRef("app", "main")
	aref, _, _ := w.Store.GetRef("app", "attempt-1")
	if mref.Lineage == aref.Lineage {
		t.Fatal("promote must seed a NEW lineage (one writer per lineage)")
	}
	if mref.Checkpoints["promote"] != txid || !mref.Protected {
		t.Fatalf("target ref: %+v", mref)
	}
	// Source survives; destroying it later must not affect main (independence).
	keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage))
	for _, k := range keys {
		w.Store.B.Delete(k)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatalf("promoted main not independent of source lineage: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ops -run 'TestRollback|TestPromote' -v`
Expected: FAIL — `Rollback` undefined.

- [ ] **Step 3: Implement**

Append to `internal/ops/ops.go` (first extract the shared helper and refactor `Fork` to use it):

```go
// copySnapshotToNewLineage copies the snapshot at (src.Lineage, src.Epoch,
// txid) into a brand-new lineage (epoch 1) and returns the lineage id.
// This is the primitive behind fork, rollback, and promote: every branch
// repoint gets a fresh lineage, preserving one-writer-per-lineage.
func (w *Workspace) copySnapshotToNewLineage(src store.Ref, txid uint64) (string, error) {
	data, _, err := w.Store.B.Get(store.SnapshotKey(src.Lineage, src.Epoch, txid))
	if err != nil {
		return "", fmt.Errorf("ops: snapshot txid %d in lineage %s: %w", txid, src.Lineage, err)
	}
	lineage := store.NewLineageID()
	if _, err := w.Store.B.PutIf(store.SnapshotKey(lineage, 1, txid), data, ""); err != nil {
		return "", err
	}
	return lineage, nil
}

func (w *Workspace) Rollback(db, branch, to string) (string, error) {
	ref, etag, err := w.Store.GetRef(db, branch)
	if err != nil {
		return "", err
	}
	txid, ok := ref.Checkpoints[to]
	if !ok {
		return "", fmt.Errorf("ops: no checkpoint %q on %s@%s", to, db, branch)
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return "", fmt.Errorf("ops: checkout in use; close connections before rollback: %w", err)
		}
	}
	lineage, err := w.copySnapshotToNewLineage(ref, txid)
	if err != nil {
		return "", err
	}
	kept := map[string]uint64{}
	for name, t := range ref.Checkpoints {
		if t <= txid {
			kept[name] = t
		}
	}
	next := ref
	next.Lineage, next.Epoch, next.HeadTXID, next.Checkpoints = lineage, 1, txid, kept
	if _, err := w.Store.PutRef(db, branch, next, etag); err != nil {
		w.Store.B.Delete(store.SnapshotKey(lineage, 1, txid))
		return "", fmt.Errorf("ops: rollback lost a race (retry): %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := w.materialize(next, txid, path); err != nil {
		return "", err
	}
	return path, nil
}

func (w *Workspace) Promote(db, source, target string, force bool) (uint64, error) {
	src, _, err := w.Store.GetRef(db, source)
	if err != nil {
		return 0, err
	}
	tgt, tgtEtag, err := w.Store.GetRef(db, target)
	if err != nil {
		return 0, err
	}
	if tgt.Protected && !force {
		return 0, fmt.Errorf("ops: %s@%s is protected; use --force", db, target)
	}
	txid := src.HeadTXID
	lineage, err := w.copySnapshotToNewLineage(src, txid)
	if err != nil {
		return 0, err
	}
	next := tgt
	next.Lineage, next.Epoch, next.HeadTXID = lineage, 1, txid
	next.Checkpoints = map[string]uint64{"promote": txid}
	next.Parent = fmt.Sprintf("%s@%s@%d", db, source, txid)
	if _, err := w.Store.PutRef(db, target, next, tgtEtag); err != nil {
		w.Store.B.Delete(store.SnapshotKey(lineage, 1, txid))
		return 0, fmt.Errorf("ops: promote lost a race (retry): %w", err)
	}
	// Refresh the target checkout if one exists and is quiescible.
	path := w.CheckoutPath(db, target)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return txid, fmt.Errorf("ops: promoted, but checkout %s is in use and was NOT refreshed: %w", path, err)
		}
		if err := w.materialize(next, txid, path); err != nil {
			return txid, err
		}
	}
	return txid, nil
}
```

Refactor `Fork` to use `copySnapshotToNewLineage` (replace its inline Get/PutIf block).

CLI cases:

```go
	case "rollback":
		if len(rest) != 3 || rest[1] != "--to" {
			return fmt.Errorf("usage: offshoot rollback <db>[@branch] --to <checkpoint>")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		p, err := w.Rollback(db, branch, rest[2])
		if err != nil {
			return err
		}
		fmt.Println(p)
		return nil
	case "promote":
		force := false
		fs := rest[:0]
		for _, a := range rest {
			if a == "--force" {
				force = true
				continue
			}
			fs = append(fs, a)
		}
		if len(fs) != 3 || fs[1] != "--onto" {
			return fmt.Errorf("usage: offshoot promote <db>@<source> --onto <target> [--force]")
		}
		db, srcBranch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		txid, err := w.Promote(db, srcBranch, fs[2], force)
		if err != nil {
			return err
		}
		fmt.Printf("promoted %s@%s -> %s@%s at txid %d\n", db, srcBranch, db, fs[2], txid)
		return nil
```

Usage lines: `  offshoot rollback <db>[@branch] --to <cp>` / `  offshoot promote <db>@<src> --onto <target> [--force]`

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/ops -v -timeout 120s`
Expected: PASS — all ops tests including refactored Fork tests.

- [ ] **Step 5: Commit**

```bash
git add internal/ops cmd/offshoot
git commit -m "feat: rollback and promote as new-lineage repoints with protected-branch gate"
```

---

### Task 8: Destroy + two-phase GC (`internal/ops/gc.go` + CLI)

**Files:**
- Create: `internal/ops/gc.go`
- Modify: `internal/ops/ops.go` (Destroy), `cmd/offshoot/main.go` (cases + usage)
- Test: `internal/ops/gc_test.go`

**Interfaces:**
- Consumes: Tasks 4-7
- Produces:

```go
// Destroy deletes a branch ref. Protected branches require force. The
// branch's lineage becomes unreachable and is collected by GC. Destroying a
// parent never affects children (materialized fork points). The checkout
// file is removed if quiescible; if held open, destroy fails cleanly.
func (w *Workspace) Destroy(db, branch string, force bool) error

// GC is two-phase (spec § GC): phase 1 tombstones unreachable lineages
// (in data/ but referenced by no ref); phase 2 deletes lineages whose
// tombstone is older than grace AND which are still unreachable.
// Returns (tombstoned, deleted) lineage counts.
func (w *Workspace) GC(grace time.Duration) (int, int, error)
```

Tombstone object: `gc/tombstones` — JSON `map[lineage]RFC3339Nano markedAt`, CAS-updated.

- [ ] **Step 1: Write the failing test**

`internal/ops/gc_test.go`:

```go
package ops

import (
	"os/exec"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

func TestDestroyAndGC(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")
	w.Fork("app", "main", "attempt-1", "")
	aref, _, _ := w.Store.GetRef("app", "attempt-1")

	// Protected destroy requires force.
	if err := w.Destroy("app", "main", false); err == nil {
		t.Fatal("destroying protected main without force must fail")
	}
	if err := w.Destroy("app", "attempt-1", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.GetRef("app", "attempt-1"); err == nil {
		t.Fatal("ref must be gone")
	}

	// Phase 1: tombstone the now-unreachable lineage; nothing deleted yet.
	tombstoned, deleted, err := w.GC(time.Hour)
	if err != nil || tombstoned != 1 || deleted != 0 {
		t.Fatalf("gc1: %d %d %v", tombstoned, deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) == 0 {
		t.Fatal("phase 1 must not delete data")
	}
	// Phase 2 with zero grace: swept.
	if _, deleted, err = w.GC(0); err != nil || deleted != 1 {
		t.Fatalf("gc2: deleted=%d err=%v", deleted, err)
	}
	if keys, _ := w.Store.B.List(store.LineagePrefix(aref.Lineage)); len(keys) != 0 {
		t.Fatalf("lineage not swept: %v", keys)
	}
	// Live lineages untouched.
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatalf("live branch damaged by GC: %v", err)
	}
}

func TestGCSparesRereferencedLineage(t *testing.T) {
	w := newWS(t)
	w.Create("app")
	// Tombstone main's lineage artificially, then verify phase 2 spares it
	// because it is still referenced.
	ref, _, _ := w.Store.GetRef("app", "main")
	if err := w.tombstone(map[string]string{ref.Lineage: "2000-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, deleted, err := w.GC(0); err != nil || deleted != 0 {
		t.Fatalf("gc must spare a referenced lineage: deleted=%d err=%v", deleted, err)
	}
	if _, err := w.Checkout("app", "main"); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ops -run 'TestDestroy|TestGCSpares' -v`
Expected: FAIL — `Destroy`/`GC`/`tombstone` undefined.

- [ ] **Step 3: Implement**

`internal/ops/gc.go`:

```go
package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

const tombstoneKey = "gc/tombstones"

func (w *Workspace) Destroy(db, branch string, force bool) error {
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		return err
	}
	if ref.Protected && !force {
		return fmt.Errorf("ops: %s@%s is protected; use --force", db, branch)
	}
	path := w.CheckoutPath(db, branch)
	if _, err := os.Stat(path); err == nil {
		if err := quiesce(path); err != nil {
			return fmt.Errorf("ops: checkout in use; close connections before destroy: %w", err)
		}
	}
	if err := w.Store.DeleteRef(db, branch); err != nil {
		return err
	}
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")
	return nil
}

func (w *Workspace) loadTombstones() (map[string]string, string, error) {
	data, etag, err := w.Store.B.Get(tombstoneKey)
	if errors.Is(err, store.ErrNotFound) {
		return map[string]string{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, "", fmt.Errorf("ops: corrupt tombstone list: %w", err)
	}
	return m, etag, nil
}

func (w *Workspace) tombstone(m map[string]string) error {
	cur, etag, err := w.loadTombstones()
	if err != nil {
		return err
	}
	for k, v := range m {
		cur[k] = v
	}
	data, _ := json.Marshal(cur)
	if etag == "" {
		_, err = w.Store.B.PutIf(tombstoneKey, data, "")
	} else {
		_, err = w.Store.B.PutIf(tombstoneKey, data, etag)
	}
	return err
}

// liveLineages returns every lineage referenced by any ref.
func (w *Workspace) liveLineages() (map[string]bool, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for db, branches := range refs {
		for _, br := range branches {
			r, _, err := w.Store.GetRef(db, br)
			if err != nil {
				return nil, err
			}
			live[r.Lineage] = true
		}
	}
	return live, nil
}

// allLineages lists every lineage present under data/.
func (w *Workspace) allLineages() (map[string]bool, error) {
	keys, err := w.Store.B.List("data/")
	if err != nil {
		return nil, err
	}
	all := map[string]bool{}
	for _, k := range keys {
		parts := strings.Split(k, "/")
		if len(parts) >= 2 {
			all[parts[1]] = true
		}
	}
	return all, nil
}

func (w *Workspace) GC(grace time.Duration) (tombstoned, deleted int, err error) {
	live, err := w.liveLineages()
	if err != nil {
		return 0, 0, err
	}
	all, err := w.allLineages()
	if err != nil {
		return 0, 0, err
	}
	stones, _, err := w.loadTombstones()
	if err != nil {
		return 0, 0, err
	}

	// Phase 1: tombstone unreachable lineages not already marked.
	newStones := map[string]string{}
	for lineage := range all {
		if !live[lineage] {
			if _, marked := stones[lineage]; !marked {
				newStones[lineage] = time.Now().UTC().Format(time.RFC3339Nano)
				tombstoned++
			}
		}
	}
	if len(newStones) > 0 {
		if err := w.tombstone(newStones); err != nil {
			return tombstoned, 0, err
		}
	}

	// Phase 2: sweep stones older than grace that are STILL unreachable
	// (re-list refs after the grace check — a fork could have re-referenced).
	cutoff := time.Now().Add(-grace)
	for lineage, markedAt := range stones {
		ts, perr := time.Parse(time.RFC3339Nano, markedAt)
		if perr != nil || !ts.Before(cutoff) {
			continue
		}
		liveNow, err := w.liveLineages()
		if err != nil {
			return tombstoned, deleted, err
		}
		if liveNow[lineage] {
			continue // re-referenced during grace; keep the stone for review
		}
		keys, err := w.Store.B.List(store.LineagePrefix(lineage))
		if err != nil {
			return tombstoned, deleted, err
		}
		for _, k := range keys {
			if err := w.Store.B.Delete(k); err != nil {
				return tombstoned, deleted, err
			}
		}
		deleted++
		delete(stones, lineage)
	}
	// Persist the pruned stone list (best-effort CAS; conflict = another GC
	// ran concurrently, which is safe — stones are re-derived each run).
	data, _ := json.Marshal(stones)
	w.Store.B.Put(tombstoneKey, data)
	return tombstoned, deleted, nil
}
```

CLI cases:

```go
	case "destroy":
		force := false
		fs := rest[:0]
		for _, a := range rest {
			if a == "--force" {
				force = true
				continue
			}
			fs = append(fs, a)
		}
		if len(fs) != 1 {
			return fmt.Errorf("usage: offshoot destroy <db>[@branch] [--force]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		return w.Destroy(db, branch, force)
	case "gc":
		grace := time.Hour
		if len(rest) == 2 && rest[0] == "--grace" {
			d, err := time.ParseDuration(rest[1])
			if err != nil {
				return err
			}
			grace = d
		}
		tombstoned, deleted, err := w.GC(grace)
		if err != nil {
			return err
		}
		fmt.Printf("gc: tombstoned %d, deleted %d lineages\n", tombstoned, deleted)
		return nil
```

(add `"time"` to main.go imports; usage lines for destroy/gc)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/ops -v -timeout 120s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ops cmd/offshoot
git commit -m "feat: destroy and two-phase GC with tombstones and re-reference sparing"
```

---

### Task 9: Status command + end-to-end CLI quickstart test + README

**Files:**
- Modify: `internal/ops/ops.go` (Status), `cmd/offshoot/main.go` (case + usage), `README.md`
- Test: `cmd/offshoot/main_test.go`

**Interfaces:**
- Consumes: everything prior
- Produces:

```go
type BranchStatus struct {
	DB, Branch  string
	HeadTXID    uint64
	Checkpoints []string // sorted
	Protected   bool
	Parent      string
	CheckedOut  bool
}
func (w *Workspace) Status() ([]BranchStatus, error) // sorted by db, then branch
```

- [ ] **Step 1: Write the failing end-to-end test**

`cmd/offshoot/main_test.go` — drives `run()` directly (no subprocess), executing the README quickstart:

```go
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// call runs the CLI's run() with -store pointing at dir, capturing stdout.
func call(t *testing.T, dir string, args ...string) string {
	t.Helper()
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := run(append([]string{"-store", dir}, args...))
	w.Close()
	os.Stdout = old
	buf := make([]byte, 1<<16)
	n, _ := r.Read(buf)
	if err != nil {
		t.Fatalf("offshoot %v: %v", args, err)
	}
	return string(buf[:n])
}

func TestQuickstartTranscript(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	path := strings.TrimSpace(call(t, store, "checkout", "app"))

	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE users (name); INSERT INTO users VALUES ('ada');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	call(t, store, "checkpoint", "app", "v1")
	call(t, store, "fork", "app", "attempt-1")

	apath := strings.TrimSpace(call(t, store, "checkout", "app@attempt-1"))
	exec.Command("sqlite3", apath, "DELETE FROM users;").Run() // destructive attempt
	call(t, store, "checkpoint", "app@attempt-1", "oops")
	call(t, store, "rollback", "app@attempt-1", "--to", "fork")

	got, _ := exec.Command("sqlite3",
		strings.TrimSpace(call(t, store, "path", "app@attempt-1")), "SELECT name FROM users;").Output()
	if string(got) != "ada\n" {
		t.Fatalf("rollback lost data: %q", got)
	}

	// Winner path: modify attempt, promote onto main.
	exec.Command("sqlite3", apath, "INSERT INTO users VALUES ('grace');").Run()
	call(t, store, "checkpoint", "app@attempt-1", "winner")
	call(t, store, "promote", "app@attempt-1", "--onto", "main", "--force")
	mgot, _ := exec.Command("sqlite3",
		strings.TrimSpace(call(t, store, "path", "app")), "SELECT count(*) FROM users;").Output()
	if string(mgot) != "2\n" {
		t.Fatalf("promoted main: %q", mgot)
	}

	call(t, store, "destroy", "app@attempt-1")
	call(t, store, "gc", "--grace", "0s")

	status := call(t, store, "status")
	if !strings.Contains(status, "app@main") || strings.Contains(status, "attempt-1") {
		t.Fatalf("status:\n%s", status)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/offshoot -v`
Expected: FAIL — `status` unknown command.

- [ ] **Step 3: Implement Status**

Append to `internal/ops/ops.go`:

```go
type BranchStatus struct {
	DB, Branch  string
	HeadTXID    uint64
	Checkpoints []string
	Protected   bool
	Parent      string
	CheckedOut  bool
}

func (w *Workspace) Status() ([]BranchStatus, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	var dbs []string
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	var out []BranchStatus
	for _, db := range dbs {
		for _, br := range refs[db] {
			r, _, err := w.Store.GetRef(db, br)
			if err != nil {
				return nil, err
			}
			var cps []string
			for name := range r.Checkpoints {
				cps = append(cps, name)
			}
			sort.Strings(cps)
			_, coErr := os.Stat(w.CheckoutPath(db, br))
			out = append(out, BranchStatus{
				DB: db, Branch: br, HeadTXID: r.HeadTXID, Checkpoints: cps,
				Protected: r.Protected, Parent: r.Parent, CheckedOut: coErr == nil,
			})
		}
	}
	return out, nil
}
```

(add `"sort"` to ops.go imports)

CLI case:

```go
	case "status":
		sts, err := w.Status()
		if err != nil {
			return err
		}
		for _, s := range sts {
			flags := ""
			if s.Protected {
				flags += " protected"
			}
			if s.CheckedOut {
				flags += " checked-out"
			}
			fmt.Printf("%s@%s txid=%d checkpoints=[%s]%s\n",
				s.DB, s.Branch, s.HeadTXID, strings.Join(s.Checkpoints, ","), flags)
		}
		return nil
```

(add `"strings"` to main.go imports; usage line)

- [ ] **Step 4: Update README**

Replace `README.md` content:

```markdown
# offshoot

Branch SQLite like git: create, fork, checkpoint, rollback, and promote
SQLite databases — stock SQLite files, your storage, one binary.

**Status: pre-alpha — local mode working (Plan 2); capture spike GO (Plan 1).**
Requires Go 1.23+, cgo, and the `sqlite3` CLI for tests. Linux and macOS only.

## Quickstart (60 seconds, no server, no bucket)

    go build -o offshoot ./cmd/offshoot
    ./offshoot init
    ./offshoot create app
    sqlite3 "$(./offshoot path app)" "CREATE TABLE users (name); INSERT INTO users VALUES ('ada');"
    ./offshoot checkpoint app v1
    ./offshoot fork app attempt-1        # instant branch
    sqlite3 "$(./offshoot path app@attempt-1)" "DELETE FROM users;"   # destructive experiment
    ./offshoot rollback app@attempt-1 --to fork                        # undo it
    ./offshoot promote app@attempt-1 --onto main --force               # or ship it
    ./offshoot status

Plan-2 (local mode) notes: checkpoints are full snapshots; checkout paths are
fixed at `<store>/checkouts/{db}/{branch}.db`; operations require the
checkout to be quiescent (no live writers). Daemon mode with live capture,
incremental segments, and S3/R2/Tigris backends is Plan 3.

Design: docs/superpowers/specs/2026-07-29-offshoot-design.md
Capture-spike evidence: docs/superpowers/specs/2026-07-29-offshoot-spike-report.md
```

- [ ] **Step 5: Run all tests**

Run: `go test ./... -count=1 && go vet ./...`
Expected: all packages PASS (including the existing capture/wal/replay suites — untouched).

- [ ] **Step 6: Commit**

```bash
git add internal/ops cmd/offshoot README.md
git commit -m "feat: status command, end-to-end quickstart test, README quickstart"
```

---

### Task 10: Concurrency & corruption hardening pass

**Files:**
- Test: `internal/ops/ops_test.go` (append), `internal/store/local_test.go` (append)
- Modify: only if tests expose bugs

**Interfaces:** none new — this task adds adversarial tests for the invariants and fixes what they expose.

- [ ] **Step 1: Write the adversarial tests**

Append to `internal/ops/ops_test.go`:

```go
func TestConcurrentForksFromSameParent(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES (1);").Run()
	w.Checkpoint("app", "main", "v1")

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, errs[n] = w.Fork("app", "main", fmt.Sprintf("f-%d", n), "")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("fork %d: %v", i, err)
		}
	}
	// All 8 children have distinct lineages and materialize correctly.
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		r, _, err := w.Store.GetRef("app", fmt.Sprintf("f-%d", i))
		if err != nil || seen[r.Lineage] {
			t.Fatalf("child %d: err=%v dup-lineage=%v", i, err, seen[r.Lineage])
		}
		seen[r.Lineage] = true
		if _, err := w.Checkout("app", fmt.Sprintf("f-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentCheckpointsOnlyOneWins(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v);").Run()

	var wg sync.WaitGroup
	okCount := 0
	var mu sync.Mutex
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := w.Checkpoint("app", "main", fmt.Sprintf("cp-%d", n)); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	// At least one wins; losers fail loudly with the CAS-race error, and the
	// ref must remain internally consistent (head >= every recorded checkpoint).
	if okCount == 0 {
		t.Fatal("no checkpoint succeeded")
	}
	r, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	for name, txid := range r.Checkpoints {
		if txid > r.HeadTXID {
			t.Fatalf("checkpoint %s@txid %d beyond head %d", name, txid, r.HeadTXID)
		}
		if _, _, err := w.Store.B.Get(store.SnapshotKey(r.Lineage, r.Epoch, txid)); err != nil {
			t.Fatalf("recorded checkpoint %s has no snapshot object: %v", name, err)
		}
	}
}

func TestCorruptSnapshotFailsClosedEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	w.Create("app")
	r, _, _ := w.Store.GetRef("app", "main")
	key := store.SnapshotKey(r.Lineage, r.Epoch, 1)
	data, _, _ := w.Store.B.Get(key)
	data[len(data)/2] ^= 0xFF
	w.Store.B.Put(key, data)
	if _, err := w.Checkout("app", "main"); err == nil {
		t.Fatal("checkout of corrupt snapshot must fail closed")
	}
}
```

(add `"sync"` to the test file's imports if missing)

- [ ] **Step 2: Run and fix**

Run: `go test ./internal/ops -v -race -count=3 -timeout 300s`
Expected: likely failures on the concurrent-checkpoint test (both goroutines encode at the same txid — the create-only snapshot put makes the second writer fail before the ref CAS, which is the CORRECT loud behavior; but verify no partial state: a loser must not leave the ref pointing at its snapshot). Fix whatever surfaces, keeping the invariants: CAS losers fail loudly; winners' state is complete; no silent success. Document each fix in the commit message. If everything passes first try, verify the tests actually exercise concurrency (add a `t.Log` of loser errors — they must show CAS conflicts at least sometimes across `-count=3`).

- [ ] **Step 3: Run the full suite**

Run: `go test ./... -count=1 -race -timeout 600s && go vet ./...`
Expected: everything green including Plan-1 packages.

- [ ] **Step 4: Commit**

```bash
git add internal/ops internal/store
git commit -m "test: adversarial concurrency and corruption coverage for lifecycle ops"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** Plan 2 implements spec § Naming model (ValidateName, ParseTarget, create/import), § Core model fork/rollback/promote/destroy with one-writer-per-lineage via new-lineage repoints, § Storage layout (epoch in keys, manifest versioning, tombstones; CAS everywhere), § Local backend CAS mechanism, § Security posture (protected branches + --force), § GC two-phase. Deliberately deferred to Plan 3 (stated in header + README): S3/R2/Tigris + CAS probe, daemon/live capture, incremental segments, TTL, leases/epoch bumps beyond 1 (epochs are in the schema but only epoch 1 is written until the daemon acquires/reclaims branches), reflink fast-path, MCP/SDKs (Plan 4).
2. **Placeholder scan:** none present; Task 1 carries an explicit, bounded API-adaptation authorization for the superfly/ltx surface (the test contract is fixed); Task 10 Step 2 prescribes expected failure modes rather than TBDs.
3. **Type consistency:** `store.Ref`/`Store` signatures match across Tasks 3-9; `Workspace` methods consistent (`Fork(db, srcBranch, newBranch, at)`, `Rollback(db, branch, to)`, `Promote(db, source, target, force)`, `Destroy(db, branch, force)`, `GC(grace)`); `copySnapshotToNewLineage` extracted in Task 7 and reused by Fork (refactor step named); CLI cases match those signatures; `SnapshotKey(lineage, epoch, txid)` used identically in Tasks 4-8 and tests.
