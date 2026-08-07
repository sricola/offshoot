# offshoot Plan 1: WAL Capture Risk Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove (or disprove) that a Go process can capture every committed transaction from a SQLite database written by foreign connections it does not own — surviving `kill -9` of both writer and capturer — with zero *undetected* divergence.

**Architecture:** A capture engine holds a long-running read transaction on its own SQLite connection (Litestream's lock dance), reads committed frames directly from the `-wal` file, and emits them to a sink. A replayer applies captured transactions onto a base snapshot; equivalence is verified by comparing `sqlite3 .dump` output. A torture harness runs stock `sqlite3` CLI writers under random `kill -9` and verifies after every round. Crash of the capturer must be *detected* (salt/state mismatch → rebase), never silently absorbed.

**Tech Stack:** Go 1.23+, `github.com/mattn/go-sqlite3` (cgo — same driver Litestream uses), stock `sqlite3` CLI as the foreign writer, no other dependencies.

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md` (§ WAL capture and the connection contract, § Testing strategy). This plan is the spec's "prototype #1"; a negative result invalidates the product design and stops the plan sequence.

## Global Constraints

- Module path: `github.com/sricola/offshoot`
- Go 1.23+; cgo required (mattn/go-sqlite3)
- Platforms: Linux and macOS only; no Windows code paths
- License: Apache-2.0 (LICENSE file in Task 1)
- Tests must not depend on wall-clock sleeps for correctness — poll with deadlines
- `sqlite3` CLI must be on PATH for integration tests (guard with `exec.LookPath`, skip with message if absent)
- Every capture divergence must be either **equivalent** or **explicitly detected** — an undetected mismatch is a failed test, full stop
- Commit messages: conventional commits (`feat:`, `test:`, `chore:`), ending with the session trailers used in this repo's history

## Plan sequence (context for the implementer)

1. **This plan** — capture spike: WAL parser, reader, replayer, capture engine, torture harness, go/no-go report.
2. Plan 2 (future): storage backends (local dir + S3/CAS probe), LTX encoding, epoch fencing, lifecycle ops (create/fork/checkpoint/rollback/promote/destroy), GC.
3. Plan 3 (future): daemon mode, lifecycle API, leases, TTL, observability.
4. Plan 4 (future): MCP server, Python/TS SDKs, LangGraph adapter, launch demo.

## File Structure

```
LICENSE, README.md, Makefile, .gitignore, go.mod
internal/wal/wal.go            WAL binary format: header/frame parsing, checksums
internal/wal/wal_test.go
internal/wal/reader.go         Incremental committed-transaction reader over a -wal file
internal/wal/reader_test.go
internal/replay/replay.go      Apply captured transactions onto a base DB copy; dump equivalence
internal/replay/replay_test.go
internal/capture/engine.go     Capture engine: lock dance, poll loop, checkpoint takeover, rebase
internal/capture/state.go      Persistent sidecar state (offset/salts) for crash detection
internal/capture/engine_test.go
cmd/torture/main.go            Torture harness binary (writer spawn, killer, verifier)
internal/capture/torture_test.go  Long-running torture test (tagged)
docs/superpowers/specs/2026-07-29-offshoot-spike-report.md  (Task 8 output)
```

---

### Task 1: Repository scaffold

**Files:**
- Create: `go.mod`, `Makefile`, `.gitignore`, `LICENSE`, `README.md`

**Interfaces:**
- Consumes: nothing
- Produces: buildable module `github.com/sricola/offshoot`; `make test` runs `go test ./...`

- [ ] **Step 1: Initialize module and files**

```bash
cd /Users/sray/gits/sql
go mod init github.com/sricola/offshoot
go get github.com/mattn/go-sqlite3@latest
```

`.gitignore`:
```
*.db
*.db-wal
*.db-shm
/bin/
```

`Makefile`:
```make
.PHONY: test test-torture build
test:
	go test ./... -count=1
test-torture:
	go test ./internal/capture -tags=torture -run TestTorture -timeout 30m -v
build:
	go build -o bin/torture ./cmd/torture
```

`LICENSE`: the Apache-2.0 text (https://www.apache.org/licenses/LICENSE-2.0.txt), copyright "2026 the offshoot authors".

`README.md`:
```markdown
# offshoot

Branch SQLite like git. **Status: pre-alpha — risk-spike phase.**
See docs/superpowers/specs/2026-07-29-offshoot-design.md for the design.
This tree currently contains the WAL-capture risk spike (plan 1 of 4).
Requires Go 1.23+, cgo, and the `sqlite3` CLI on PATH for tests.
Linux and macOS only.
```

- [ ] **Step 2: Verify the module builds**

Run: `go build ./... && go vet ./...`
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "chore: scaffold offshoot module for capture spike"
```

---

### Task 2: WAL binary format parser

**Files:**
- Create: `internal/wal/wal.go`
- Test: `internal/wal/wal_test.go`

**Interfaces:**
- Consumes: nothing
- Produces (exact, later tasks depend on these):

```go
package wal

const (
	HeaderSize      = 32
	FrameHeaderSize = 24
)

type Header struct {
	Magic         uint32 // 0x377f0682 (LE checksums) or 0x377f0683 (BE checksums)
	Version       uint32 // 3007000
	PageSize      uint32
	CheckpointSeq uint32
	Salt1, Salt2  uint32
	Cksum1, Cksum2 uint32
}

type FrameHeader struct {
	Pgno         uint32
	CommitSize   uint32 // nonzero ⇒ commit frame; value = DB size in pages after txn
	Salt1, Salt2 uint32
	Cksum1, Cksum2 uint32
}

type Frame struct {
	Header FrameHeader
	Data   []byte // page content, len == Header.PageSize of the WAL
}

func ParseHeader(b []byte) (Header, error)          // validates magic, version, checksum
func ParseFrameHeader(b []byte) FrameHeader          // fixed big-endian field decode
func (h Header) FrameSize() int                      // FrameHeaderSize + int(h.PageSize)
func (h Header) ChecksumByteOrder() binary.ByteOrder // per magic low bit
// Checksum computes the SQLite WAL cumulative checksum over b (len%8==0),
// seeded with s1,s2, reading 32-bit words in bo. Returns updated s1,s2.
func Checksum(bo binary.ByteOrder, s1, s2 uint32, b []byte) (uint32, uint32)
```

All WAL header/frame *fields* are big-endian (SQLite file format docs); only the *checksum word reads* vary by magic. `ParseHeader` errors: `ErrBadMagic`, `ErrBadVersion`, `ErrChecksum` (define as `errors.New` sentinels).

- [ ] **Step 1: Write the failing test**

`internal/wal/wal_test.go` — generate a real WAL with mattn and parse it:

```go
package wal

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// makeWAL creates a DB in WAL mode, writes n transactions, and returns the
// raw -wal bytes while the connection is still open (close would checkpoint).
func makeWAL(t *testing.T, n int) []byte {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, p := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0", "PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseHeader(t *testing.T) {
	b := makeWAL(t, 3)
	h, err := ParseHeader(b[:HeaderSize])
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.Version != 3007000 {
		t.Errorf("version = %d", h.Version)
	}
	if h.PageSize != 4096 {
		t.Errorf("page size = %d", h.PageSize)
	}
}

func TestFrameChainValid(t *testing.T) {
	b := makeWAL(t, 3)
	h, err := ParseHeader(b[:HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	bo := h.ChecksumByteOrder()
	s1, s2 := h.Cksum1, h.Cksum2
	commits := 0
	for off := HeaderSize; off+h.FrameSize() <= len(b); off += h.FrameSize() {
		fh := ParseFrameHeader(b[off : off+FrameHeaderSize])
		if fh.Salt1 != h.Salt1 || fh.Salt2 != h.Salt2 {
			t.Fatalf("salt mismatch at %d", off)
		}
		s1, s2 = Checksum(bo, s1, s2, b[off:off+8])
		s1, s2 = Checksum(bo, s1, s2, b[off+FrameHeaderSize:off+h.FrameSize()])
		if s1 != fh.Cksum1 || s2 != fh.Cksum2 {
			t.Fatalf("checksum mismatch at %d", off)
		}
		if fh.CommitSize != 0 {
			commits++
		}
	}
	// CREATE TABLE + 3 INSERTs, each autocommitted
	if commits != 4 {
		t.Errorf("commit frames = %d, want 4", commits)
	}
}

func TestParseHeaderRejectsGarbage(t *testing.T) {
	if _, err := ParseHeader(make([]byte, HeaderSize)); err == nil {
		t.Fatal("want error on zero header")
	}
}

func TestChecksumKnownProperty(t *testing.T) {
	// checksum must be order-sensitive and seed-sensitive
	b := []byte{1, 0, 0, 0, 2, 0, 0, 0}
	a1, a2 := Checksum(binary.LittleEndian, 0, 0, b)
	b1, b2 := Checksum(binary.LittleEndian, 1, 1, b)
	if a1 == b1 && a2 == b2 {
		t.Fatal("seed ignored")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wal -run 'TestParse|TestFrame|TestChecksum' -v`
Expected: FAIL — compile error, `ParseHeader` undefined.

- [ ] **Step 3: Write the implementation**

`internal/wal/wal.go`:

```go
// Package wal parses the SQLite write-ahead-log binary format.
// Reference: https://www.sqlite.org/fileformat2.html#walformat
package wal

import (
	"encoding/binary"
	"errors"
)

const (
	HeaderSize      = 32
	FrameHeaderSize = 24

	magicLE = 0x377f0682 // checksum words read little-endian
	magicBE = 0x377f0683 // checksum words read big-endian
	version = 3007000
)

var (
	ErrBadMagic   = errors.New("wal: bad magic")
	ErrBadVersion = errors.New("wal: unsupported version")
	ErrChecksum   = errors.New("wal: header checksum mismatch")
)

type Header struct {
	Magic          uint32
	Version        uint32
	PageSize       uint32
	CheckpointSeq  uint32
	Salt1, Salt2   uint32
	Cksum1, Cksum2 uint32
}

type FrameHeader struct {
	Pgno           uint32
	CommitSize     uint32
	Salt1, Salt2   uint32
	Cksum1, Cksum2 uint32
}

type Frame struct {
	Header FrameHeader
	Data   []byte
}

func ParseHeader(b []byte) (Header, error) {
	h := Header{
		Magic:         binary.BigEndian.Uint32(b[0:4]),
		Version:       binary.BigEndian.Uint32(b[4:8]),
		PageSize:      binary.BigEndian.Uint32(b[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(b[12:16]),
		Salt1:         binary.BigEndian.Uint32(b[16:20]),
		Salt2:         binary.BigEndian.Uint32(b[20:24]),
		Cksum1:        binary.BigEndian.Uint32(b[24:28]),
		Cksum2:        binary.BigEndian.Uint32(b[28:32]),
	}
	if h.Magic != magicLE && h.Magic != magicBE {
		return h, ErrBadMagic
	}
	if h.Version != version {
		return h, ErrBadVersion
	}
	s1, s2 := Checksum(h.ChecksumByteOrder(), 0, 0, b[:24])
	if s1 != h.Cksum1 || s2 != h.Cksum2 {
		return h, ErrChecksum
	}
	return h, nil
}

func ParseFrameHeader(b []byte) FrameHeader {
	return FrameHeader{
		Pgno:       binary.BigEndian.Uint32(b[0:4]),
		CommitSize: binary.BigEndian.Uint32(b[4:8]),
		Salt1:      binary.BigEndian.Uint32(b[8:12]),
		Salt2:      binary.BigEndian.Uint32(b[12:16]),
		Cksum1:     binary.BigEndian.Uint32(b[16:20]),
		Cksum2:     binary.BigEndian.Uint32(b[20:24]),
	}
}

func (h Header) FrameSize() int { return FrameHeaderSize + int(h.PageSize) }

func (h Header) ChecksumByteOrder() binary.ByteOrder {
	if h.Magic == magicBE {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

// Checksum implements SQLite's cumulative WAL checksum. len(b) must be a
// multiple of 8.
func Checksum(bo binary.ByteOrder, s1, s2 uint32, b []byte) (uint32, uint32) {
	for i := 0; i+8 <= len(b); i += 8 {
		s1 += bo.Uint32(b[i:i+4]) + s2
		s2 += bo.Uint32(b[i+4:i+8]) + s1
	}
	return s1, s2
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/wal -v`
Expected: PASS (all four tests). If `TestFrameChainValid` fails on commit count only, print the count — internal b-tree page splits don't add commit frames, but if SQLite versions differ in autocommit batching, relax the assertion to `commits >= 4` and note it in the test comment.

- [ ] **Step 5: Commit**

```bash
git add internal/wal go.mod go.sum
git commit -m "feat: SQLite WAL format parser with checksum chain validation"
```

---

### Task 3: Incremental committed-transaction reader

**Files:**
- Create: `internal/wal/reader.go`
- Test: `internal/wal/reader_test.go`

**Interfaces:**
- Consumes: `wal.ParseHeader`, `wal.ParseFrameHeader`, `wal.Checksum`, `wal.Frame` (Task 2)
- Produces:

```go
package wal

// Reader incrementally extracts committed transactions from a -wal file that
// a live writer is appending to. It never returns a partially committed txn.
type Reader struct { /* unexported fields */ }

var ErrWALRestarted = errors.New("wal: salt changed — WAL was restarted")

func NewReader(walPath string) *Reader
// Next returns the next committed transaction's frames (last frame has
// CommitSize != 0), or (nil, nil) when no complete txn is available yet.
// Returns ErrWALRestarted when the file's header salts no longer match the
// salts it started with (caller must rebase), and ErrChecksum on chain break.
func (r *Reader) Next() ([]Frame, error)
// Offset returns the byte offset of the first unconsumed frame,
// and the salts the reader is bound to. Used for persistent state.
func (r *Reader) Offset() (off int64, salt1, salt2 uint32)
// Bind resumes a reader at a known offset/salt (crash recovery).
func (r *Reader) Bind(off int64, salt1, salt2 uint32)
```

Semantics: `Next` opens/stats the file on each call (no cached fd across calls — the file can be truncated under us); reads the 32-byte header; if the reader has salts bound and they differ → `ErrWALRestarted`; if unbound, binds to the current salts at offset 32. Frames are validated (salt match + cumulative checksum) and buffered until a commit frame is seen; only then is the transaction returned and the offset advanced. Frames after the last commit (a torn/in-flight txn at crash) are never surfaced.

- [ ] **Step 1: Write the failing test**

`internal/wal/reader_test.go` (reuses `makeWAL`'s pattern with a live DB):

```go
package wal

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openWALDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, p := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestReaderSeesEachCommit(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")

	drain := func() int {
		n := 0
		for {
			tx, err := r.Next()
			if err != nil {
				t.Fatal(err)
			}
			if tx == nil {
				return n
			}
			if tx[len(tx)-1].Header.CommitSize == 0 {
				t.Fatal("returned txn does not end in commit frame")
			}
			n++
		}
	}
	drain() // consume CREATE TABLE

	for i := 0; i < 5; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
			t.Fatal(err)
		}
	}
	if got := drain(); got != 5 {
		t.Errorf("committed txns = %d, want 5", got)
	}
}

func TestReaderIgnoresTornTail(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}
	// Simulate a torn write: append half a frame of garbage to the WAL.
	f, err := os.OpenFile(path+"-wal", os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	tx, err := r.Next()
	if err != nil {
		t.Fatalf("torn tail must not error, got %v", err)
	}
	if tx != nil {
		t.Fatal("torn tail must not produce a transaction")
	}
	_ = db
}

func TestReaderDetectsRestart(t *testing.T) {
	db, path := openWALDB(t)
	r := NewReader(path + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
	}
	// RESTART the WAL (new salts), then write again.
	if _, err := db.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(64))"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != ErrWALRestarted {
		t.Fatalf("want ErrWALRestarted, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wal -run TestReader -v`
Expected: FAIL — `NewReader` undefined.

- [ ] **Step 3: Write the implementation**

`internal/wal/reader.go`:

```go
package wal

import (
	"errors"
	"io"
	"os"
)

var ErrWALRestarted = errors.New("wal: salt changed — WAL was restarted")

type Reader struct {
	path       string
	bound      bool
	off        int64
	salt1      uint32
	salt2      uint32
	s1, s2     uint32 // running checksum at off
	hdr        Header
}

func NewReader(walPath string) *Reader { return &Reader{path: walPath} }

func (r *Reader) Offset() (int64, uint32, uint32) { return r.off, r.salt1, r.salt2 }

func (r *Reader) Bind(off int64, salt1, salt2 uint32) {
	// Rebinding at an offset requires the checksum seed at that offset; for
	// crash recovery we only ever Bind at HeaderSize with the header seed —
	// the engine rebases in all other cases (see capture.State).
	r.bound, r.off, r.salt1, r.salt2 = true, off, salt1, salt2
}

func (r *Reader) Next() ([]Frame, error) {
	f, err := os.Open(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no WAL yet
		}
		return nil, err
	}
	defer f.Close()

	hb := make([]byte, HeaderSize)
	if _, err := io.ReadFull(f, hb); err != nil {
		return nil, nil // header not fully written yet
	}
	hdr, err := ParseHeader(hb)
	if err != nil {
		return nil, nil // header torn mid-write; try again later
	}
	if !r.bound {
		r.bound = true
		r.off = HeaderSize
		r.salt1, r.salt2 = hdr.Salt1, hdr.Salt2
		r.s1, r.s2 = hdr.Cksum1, hdr.Cksum2
		r.hdr = hdr
	} else if hdr.Salt1 != r.salt1 || hdr.Salt2 != r.salt2 {
		return nil, ErrWALRestarted
	}
	r.hdr = hdr

	fsz := int64(hdr.FrameSize())
	var frames []Frame
	off, s1, s2 := r.off, r.s1, r.s2
	buf := make([]byte, fsz)
	for {
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, nil // incomplete frame at tail — wait
		}
		fh := ParseFrameHeader(buf[:FrameHeaderSize])
		if fh.Salt1 != hdr.Salt1 || fh.Salt2 != hdr.Salt2 {
			return nil, nil // stale/unwritten region past valid frames
		}
		s1, s2 = Checksum(hdr.ChecksumByteOrder(), s1, s2, buf[:8])
		s1, s2 = Checksum(hdr.ChecksumByteOrder(), s1, s2, buf[FrameHeaderSize:])
		if s1 != fh.Cksum1 || s2 != fh.Cksum2 {
			return nil, nil // torn frame at tail — wait
		}
		data := make([]byte, hdr.PageSize)
		copy(data, buf[FrameHeaderSize:])
		frames = append(frames, Frame{Header: fh, Data: data})
		off += fsz
		if fh.CommitSize != 0 {
			r.off, r.s1, r.s2 = off, s1, s2
			return frames, nil
		}
	}
}
```

Note the deliberate choice: any invalid frame at the tail returns "no txn yet" rather than an error — SQLite itself treats invalid tail frames as end-of-log. Only a *salt change in the header* is a hard signal (restart).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/wal -v`
Expected: PASS (Task 2 + Task 3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/wal
git commit -m "feat: incremental committed-transaction WAL reader with restart detection"
```

---

### Task 4: Replayer and dump-equivalence checker

**Files:**
- Create: `internal/replay/replay.go`
- Test: `internal/replay/replay_test.go`

**Interfaces:**
- Consumes: `wal.Frame` (Task 2)
- Produces:

```go
package replay

// Replica maintains a SQLite database file reconstructed from a base
// snapshot plus applied WAL transactions.
type Replica struct { /* unexported */ }

func New(path string) *Replica
// Rebase replaces the replica's contents with a copy of snapshotPath.
func (r *Replica) Rebase(snapshotPath string) error
// Apply writes one committed transaction's frames. The final frame must be a
// commit frame; the file is truncated/extended to CommitSize pages.
func (r *Replica) Apply(pageSize uint32, frames []wal.Frame) error
// Path returns the replica DB file path.
func (r *Replica) Path() string

// Dump returns `sqlite3 <path> .dump` output; both sides of an equivalence
// check must be quiesced (no -wal) or opened read-only.
func Dump(dbPath string) (string, error)
```

- [ ] **Step 1: Write the failing test**

`internal/replay/replay_test.go`:

```go
package replay

import (
	"database/sql"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/wal"
)

func TestReplayMatchesSource(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")

	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0"} {
		if _, err := db.Exec(p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}
	// Checkpoint so the base snapshot contains the schema, then copy it.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "base.db")
	copyFile(t, src, base)

	// Write 20 transactions into the WAL (not checkpointed).
	for i := 0; i < 20; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(256))"); err != nil {
			t.Fatal(err)
		}
	}

	// Capture all committed txns from the WAL and apply to a replica.
	rep := New(filepath.Join(dir, "replica.db"))
	if err := rep.Rebase(base); err != nil {
		t.Fatal(err)
	}
	r := wal.NewReader(src + "-wal")
	for {
		tx, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx == nil {
			break
		}
		if err := rep.Apply(4096, tx); err != nil {
			t.Fatal(err)
		}
	}

	// Quiesce source and compare dumps.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	sd, err := Dump(src)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := Dump(rep.Path())
	if err != nil {
		t.Fatal(err)
	}
	if sd != rd {
		t.Fatalf("dump mismatch:\n--- source ---\n%.2000s\n--- replica ---\n%.2000s", sd, rd)
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
```

(add `"os"` to imports)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/replay -v`
Expected: FAIL — package doesn't compile, `New` undefined.

- [ ] **Step 3: Write the implementation**

`internal/replay/replay.go`:

```go
// Package replay reconstructs a SQLite database from a base snapshot plus
// captured WAL transactions.
package replay

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/sricola/offshoot/internal/wal"
)

type Replica struct {
	path string
}

func New(path string) *Replica       { return &Replica{path: path} }
func (r *Replica) Path() string      { return r.path }

func (r *Replica) Rebase(snapshotPath string) error {
	in, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(r.path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	// A rebased replica must not carry stale sidecar files.
	os.Remove(r.path + "-wal")
	os.Remove(r.path + "-shm")
	return out.Close()
}

func (r *Replica) Apply(pageSize uint32, frames []wal.Frame) error {
	if len(frames) == 0 {
		return fmt.Errorf("replay: empty transaction")
	}
	last := frames[len(frames)-1]
	if last.Header.CommitSize == 0 {
		return fmt.Errorf("replay: transaction does not end in a commit frame")
	}
	f, err := os.OpenFile(r.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, fr := range frames {
		off := int64(fr.Header.Pgno-1) * int64(pageSize)
		if _, err := f.WriteAt(fr.Data, off); err != nil {
			return err
		}
	}
	if err := f.Truncate(int64(last.Header.CommitSize) * int64(pageSize)); err != nil {
		return err
	}
	return f.Sync()
}

func Dump(dbPath string) (string, error) {
	out, err := exec.Command("sqlite3", dbPath, ".dump").Output()
	if err != nil {
		return "", fmt.Errorf("sqlite3 .dump %s: %w", dbPath, err)
	}
	return string(out), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/replay -v`
Expected: PASS. If the dump differs, debug before proceeding — this equivalence check is the oracle every later task trusts. (Likely causes: page-1 header fields updated only on checkpoint — compare with `.dump` which is content-level and ignores file-header drift; if `.dump` itself differs, the capture is genuinely wrong.)

- [ ] **Step 5: Commit**

```bash
git add internal/replay
git commit -m "feat: WAL transaction replayer with sqlite3-dump equivalence oracle"
```

---

### Task 5: Capture engine — lock dance, poll loop, checkpoint takeover, rebase

**Files:**
- Create: `internal/capture/engine.go`, `internal/capture/state.go`
- Test: `internal/capture/engine_test.go`

**Interfaces:**
- Consumes: `wal.Reader`, `wal.Frame`, `wal.ErrWALRestarted` (Task 3), `replay.Replica` (Task 4)
- Produces:

```go
package capture

// Sink receives capture output. Implementations: replay.Replica satisfies
// this via a thin adapter in the tests; plan 2 adds an LTX sink.
type Sink interface {
	Rebase(snapshotPath string) error
	Apply(pageSize uint32, frames []wal.Frame) error
}

type Engine struct { /* unexported */ }

type Options struct {
	DBPath   string
	StateDir string        // sidecar state + snapshots live here
	Sink     Sink
	Poll     time.Duration // default 10ms
}

func NewEngine(o Options) *Engine
// Run blocks, capturing until ctx is cancelled. It performs the initial
// rebase (checkpoint + snapshot copy), holds the read-lock dance, polls for
// committed transactions, and periodically performs checkpoint takeover.
func (e *Engine) Run(ctx context.Context) error
// Rebased reports how many times the engine rebased (1 = initial only).
// Every rebase beyond the first means continuity was lost and re-established
// — detected, never silent.
func (e *Engine) Rebased() int
```

`state.go` — persistent sidecar for crash detection:

```go
package capture

type State struct {
	Off   int64  `json:"off"`
	Salt1 uint32 `json:"salt1"`
	Salt2 uint32 `json:"salt2"`
}

func LoadState(path string) (State, bool, error) // ok=false when absent
func SaveState(path string, s State) error       // atomic: temp + rename
```

Engine algorithm (implement exactly this, in this order):

1. Open own connection (mattn) with `PRAGMA busy_timeout=5000; PRAGMA journal_mode=wal; PRAGMA wal_autocheckpoint=0` (autocheckpoint off applies to *our* connection only — foreign connections may still checkpoint passively; that is safe because passive checkpoints cannot restart the WAL past our read mark).
2. **Rebase**: run `PRAGMA wal_checkpoint(TRUNCATE)` (retry on busy up to 5s; fall back to `RESTART`, then to snapshotting via `VACUUM INTO` if both stay busy — `VACUUM INTO` needs no exclusive lock). Copy the main DB file (or the `VACUUM INTO` output) to `StateDir/snapshot.db`; call `Sink.Rebase`. Reset `wal.Reader`, save `State{Off: 32, salts from new WAL header}` — or `Off: 0` sentinel when no WAL exists yet (reader binds on first frame).
3. **Hold the read lock**: `BEGIN; SELECT count(*) FROM sqlite_master;` on a dedicated `sql.Conn`, kept open. This is what prevents any foreign checkpoint from *restarting* the WAL under us.
4. **Poll loop**: every `Poll`, call `reader.Next()` repeatedly until nil; for each txn: `Sink.Apply`, then `SaveState` with the reader's new offset. On `ErrWALRestarted` (our lock lapsed — daemon paused, connection dropped): increment rebase counter, go to 2.
5. **Checkpoint takeover** every 64 captured transactions or 5s of idle: `COMMIT` the read tx, `PRAGMA wal_checkpoint(RESTART)` (busy-retry, best-effort — skip on sustained busy), re-`BEGIN` the read tx, rebind the reader (restart ⇒ new salts ⇒ reader must re-bind from offset 32; the frames before the restart were already captured because takeover only runs when the reader is fully drained — assert this invariant in code).
6. On `ctx` cancel: final drain, `COMMIT` read tx, return.

- [ ] **Step 1: Write the failing test**

`internal/capture/engine_test.go`:

```go
package capture

import (
	"context"
	"database/sql"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/wal"
)

type replicaSink struct{ r *replay.Replica }

func (s replicaSink) Rebase(p string) error                          { return s.r.Rebase(p) }
func (s replicaSink) Apply(ps uint32, fr []wal.Frame) error          { return s.r.Apply(ps, fr) }

func startEngine(t *testing.T, dbPath string) (*Engine, *replay.Replica, context.CancelFunc, chan error) {
	t.Helper()
	dir := t.TempDir()
	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: dbPath, StateDir: dir, Sink: replicaSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	return e, rep, cancel, done
}

// waitEqual polls until the replica's dump matches the (quiesced) source.
func waitEqual(t *testing.T, src string, rep *replay.Replica, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var sd, rd string
	for time.Now().Before(end) {
		var err1, err2 error
		sd, err1 = replay.Dump(src)
		rd, err2 = replay.Dump(rep.Path())
		if err1 == nil && err2 == nil && sd == rd {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica never converged.\n--- source ---\n%.2000s\n--- replica ---\n%.2000s", sd, rd)
}

func TestEngineCapturesForeignGoWriter(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite3", src)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		t.Fatal(err)
	}

	_, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	for i := 0; i < 50; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (randomblob(128))"); err != nil {
			t.Fatal(err)
		}
	}
	waitEqual(t, src, rep, 10*time.Second)
}

func TestEngineCapturesStockCLIWriter(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	// Create + WAL mode via the CLI itself — the engine never owns the writer.
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3: %v: %s", err, out)
	}

	_, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	for i := 0; i < 20; i++ {
		if out, err := exec.Command("sqlite3", src,
			"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(128));").CombinedOutput(); err != nil {
			t.Fatalf("sqlite3 insert: %v: %s", err, out)
		}
	}
	waitEqual(t, src, rep, 10*time.Second)
}

func TestEngineSurvivesForeignPassiveCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	src := filepath.Join(t.TempDir(), "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("sqlite3: %v: %s", err, out)
	}
	e, rep, cancel, done := startEngine(t, src)
	defer func() { cancel(); <-done }()

	for i := 0; i < 10; i++ {
		if out, err := exec.Command("sqlite3", src, "PRAGMA busy_timeout=5000;"+
			"INSERT INTO t (v) VALUES (randomblob(128)); PRAGMA wal_checkpoint(PASSIVE);").CombinedOutput(); err != nil {
			t.Fatalf("sqlite3: %v: %s", err, out)
		}
	}
	waitEqual(t, src, rep, 10*time.Second)
	if e.Rebased() != 1 {
		t.Errorf("passive checkpoints must not force rebase; rebased = %d", e.Rebased())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/capture -v`
Expected: FAIL — `NewEngine` undefined.

- [ ] **Step 3: Write the implementation**

`internal/capture/state.go`:

```go
package capture

import (
	"encoding/json"
	"os"
)

type State struct {
	Off   int64  `json:"off"`
	Salt1 uint32 `json:"salt1"`
	Salt2 uint32 `json:"salt2"`
}

func LoadState(path string) (State, bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, false, nil // corrupt state ⇒ treat as absent ⇒ rebase
	}
	return s, true, nil
}

func SaveState(path string, s State) error {
	b, _ := json.Marshal(s)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
```

`internal/capture/engine.go`:

```go
// Package capture implements the foreign-connection WAL capture engine:
// the read-lock dance, incremental frame capture, checkpoint takeover, and
// rebase-on-divergence. This is offshoot's risk-spike core.
package capture

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sricola/offshoot/internal/wal"
)

type Sink interface {
	Rebase(snapshotPath string) error
	Apply(pageSize uint32, frames []wal.Frame) error
}

type Options struct {
	DBPath   string
	StateDir string
	Sink     Sink
	Poll     time.Duration
}

type Engine struct {
	o        Options
	db       *sql.DB
	conn     *sql.Conn
	inTx     bool
	reader   *wal.Reader
	pageSize uint32
	rebased  int
	captured int // txns since last takeover
}

func NewEngine(o Options) *Engine {
	if o.Poll == 0 {
		o.Poll = 10 * time.Millisecond
	}
	return &Engine{o: o}
}

func (e *Engine) Rebased() int { return e.rebased }

func (e *Engine) statePath() string    { return filepath.Join(e.o.StateDir, "capture-state.json") }
func (e *Engine) snapshotPath() string { return filepath.Join(e.o.StateDir, "snapshot.db") }

func (e *Engine) Run(ctx context.Context) error {
	var err error
	e.db, err = sql.Open("sqlite3",
		e.o.DBPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return err
	}
	defer e.db.Close()
	e.conn, err = e.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer e.conn.Close()
	if _, err := e.conn.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		return err
	}
	if err := e.conn.QueryRowContext(ctx, "PRAGMA page_size").Scan(&e.pageSize); err != nil {
		return err
	}

	if err := e.rebase(ctx); err != nil {
		return err
	}

	idle := time.Now()
	tick := time.NewTicker(e.o.Poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			e.drain(ctx)
			e.endRead(ctx)
			return nil
		case <-tick.C:
		}
		n, err := e.drain(ctx)
		if err == wal.ErrWALRestarted {
			// Our lock lapsed (or an external RESTART happened): continuity
			// lost — detected. Re-establish from a fresh snapshot.
			if err := e.rebase(ctx); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if n > 0 {
			idle = time.Now()
			e.captured += n
		}
		if e.captured >= 64 || (e.captured > 0 && time.Since(idle) > 5*time.Second) {
			e.takeover(ctx)
		}
	}
}

// drain consumes all currently available committed transactions.
func (e *Engine) drain(ctx context.Context) (int, error) {
	n := 0
	for {
		frames, err := e.reader.Next()
		if err != nil {
			return n, err
		}
		if frames == nil {
			return n, nil
		}
		if err := e.o.Sink.Apply(e.pageSize, frames); err != nil {
			return n, err
		}
		off, s1, s2 := e.reader.Offset()
		if err := SaveState(e.statePath(), State{Off: off, Salt1: s1, Salt2: s2}); err != nil {
			return n, err
		}
		n++
	}
}

func (e *Engine) beginRead(ctx context.Context) error {
	if e.inTx {
		return nil
	}
	if _, err := e.conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	if _, err := e.conn.ExecContext(ctx, "SELECT count(*) FROM sqlite_master"); err != nil {
		e.conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	e.inTx = true
	return nil
}

func (e *Engine) endRead(ctx context.Context) {
	if e.inTx {
		e.conn.ExecContext(ctx, "COMMIT")
		e.inTx = false
	}
}

// rebase: checkpoint, snapshot the main file, reset reader + sink + state.
func (e *Engine) rebase(ctx context.Context) error {
	e.endRead(ctx)
	if err := e.checkpoint(ctx, "TRUNCATE"); err != nil {
		// Fall back to VACUUM INTO — needs no exclusive checkpoint lock.
		os.Remove(e.snapshotPath())
		if _, verr := e.conn.ExecContext(ctx,
			fmt.Sprintf("VACUUM INTO %q", e.snapshotPath())); verr != nil {
			return fmt.Errorf("rebase: checkpoint %v; vacuum %v", err, verr)
		}
	} else {
		if err := copyFile(e.o.DBPath, e.snapshotPath()); err != nil {
			return err
		}
	}
	if err := e.beginRead(ctx); err != nil {
		return err
	}
	if err := e.o.Sink.Rebase(e.snapshotPath()); err != nil {
		return err
	}
	e.reader = wal.NewReader(e.o.DBPath + "-wal")
	e.rebased++
	e.captured = 0
	return SaveState(e.statePath(), State{})
}

// takeover: restart the WAL under our control. Only safe when fully drained.
func (e *Engine) takeover(ctx context.Context) {
	e.endRead(ctx)
	if err := e.checkpoint(ctx, "RESTART"); err == nil {
		// Restart succeeded: new salts. All prior frames were captured
		// (drain precedes takeover), so a fresh reader is correct, not lossy.
		e.reader = wal.NewReader(e.o.DBPath + "-wal")
		e.captured = 0
	}
	e.beginRead(ctx) // best effort; next drain surfaces any problem
}

func (e *Engine) checkpoint(ctx context.Context, mode string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		var busy, logN, ckptN int
		err := e.conn.QueryRowContext(ctx,
			"PRAGMA wal_checkpoint("+mode+")").Scan(&busy, &logN, &ckptN)
		if err == nil && busy == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("checkpoint %s: busy", mode)
		}
		time.Sleep(50 * time.Millisecond)
	}
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/capture -v -timeout 120s`
Expected: PASS ×3. The passive-checkpoint test is the subtle one: if it flakes with `Rebased() > 1`, the read-lock dance is not actually preventing WAL restarts — that is a real spike finding, not a test bug. Investigate with `PRAGMA wal_checkpoint` return values before touching the test.

- [ ] **Step 5: Commit**

```bash
git add internal/capture
git commit -m "feat: WAL capture engine with read-lock dance, takeover, and rebase"
```

---

### Task 6: Torture harness — writer kill loop

**Files:**
- Create: `cmd/torture/main.go`
- Test: `internal/capture/torture_test.go` (build tag `torture`)

**Interfaces:**
- Consumes: `capture.Engine`, `replay.Replica`, `replay.Dump` (Tasks 4-5)
- Produces: `bin/torture` binary; `TestTortureWriterKill` (tagged, 5-minute default)

Harness design: one process runs the engine in-process; writers are **stock `sqlite3` CLI subprocesses** (foreign by construction) executing batches; a killer goroutine `kill -9`s the writer at random 0-200ms intervals. Rounds repeat: spawn → kill or complete → quiesce → verify dumps equal → next round. Any dump mismatch = immediate failure with both dumps saved to the state dir.

- [ ] **Step 1: Write the failing test**

`internal/capture/torture_test.go`:

```go
//go:build torture

package capture

import (
	"context"
	"math/rand"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/wal"
)

const writerSQL = `PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);
BEGIN; INSERT INTO t (v, n) VALUES (randomblob(200), 1); COMMIT;
BEGIN; UPDATE t SET n = n + 1 WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 5); COMMIT;
BEGIN; INSERT INTO t (v, n) SELECT randomblob(100), 0 FROM t LIMIT 3; COMMIT;
BEGIN; DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 1); COMMIT;`

type tortureSink struct{ r *replay.Replica }

func (s tortureSink) Rebase(p string) error                 { return s.r.Rebase(p) }
func (s tortureSink) Apply(ps uint32, f []wal.Frame) error  { return s.r.Apply(ps, f) }

func TestTortureWriterKill(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e := NewEngine(Options{DBPath: src, StateDir: dir, Sink: tortureSink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	engDone := make(chan error, 1)
	go func() { engDone <- e.Run(ctx) }()
	defer func() { cancel(); <-engDone }()

	deadline := time.Now().Add(5 * time.Minute)
	round := 0
	for time.Now().Before(deadline) {
		round++
		cmd := exec.Command("sqlite3", src, writerSQL)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if rand.Intn(2) == 0 {
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			cmd.Process.Signal(syscall.SIGKILL)
		}
		cmd.Wait() // reap either way; exit status irrelevant

		// Quiesce: wait for the replica to converge with the live source.
		if !converged(t, src, rep, 15*time.Second) {
			t.Fatalf("round %d: replica diverged and did not converge (rebases=%d)",
				round, e.Rebased())
		}
	}
	t.Logf("torture complete: %d rounds, %d rebases", round, e.Rebased())
}

func converged(t *testing.T, src string, rep *replay.Replica, d time.Duration) bool {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		sd, e1 := replay.Dump(src)
		rd, e2 := replay.Dump(rep.Path())
		if e1 == nil && e2 == nil && sd == rd {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
```

- [ ] **Step 2: Run test to verify it fails (compiles but no harness binary yet)**

Run: `go vet -tags=torture ./internal/capture`
Expected: clean vet. Then run a 30-second smoke of the real thing:
`go test ./internal/capture -tags=torture -run TestTorture -timeout 10m -v` (let it run ≥1 round, then it either passes rounds or exposes engine bugs — expected initially: possible failures; that is the point of the spike. Debug with the systematic-debugging skill, not by weakening `converged`.)

- [ ] **Step 3: Write the standalone harness binary**

`cmd/torture/main.go` (same loop, flag-configurable, for manual long runs):

```go
// Command torture runs the offshoot capture engine against stock sqlite3 CLI
// writers under random kill -9, verifying dump equivalence every round.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sricola/offshoot/internal/capture"
	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/wal"
)

const writerSQL = `PRAGMA busy_timeout=5000;
BEGIN; INSERT INTO t (v, n) VALUES (randomblob(200), 1); COMMIT;
BEGIN; UPDATE t SET n = n + 1 WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 5); COMMIT;
BEGIN; INSERT INTO t (v, n) SELECT randomblob(100), 0 FROM t LIMIT 3; COMMIT;
BEGIN; DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 1); COMMIT;`

type sink struct{ r *replay.Replica }

func (s sink) Rebase(p string) error                { return s.r.Rebase(p) }
func (s sink) Apply(ps uint32, f []wal.Frame) error { return s.r.Apply(ps, f) }

func main() {
	dur := flag.Duration("d", 10*time.Minute, "total duration")
	dir := flag.String("dir", "", "work dir (default: temp)")
	flag.Parse()

	if *dir == "" {
		d, err := os.MkdirTemp("", "offshoot-torture-*")
		if err != nil {
			log.Fatal(err)
		}
		*dir = d
	}
	src := filepath.Join(*dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);").CombinedOutput(); err != nil {
		log.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(*dir, "replica.db"))
	e := capture.NewEngine(capture.Options{DBPath: src, StateDir: *dir, Sink: sink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := e.Run(ctx); err != nil {
			log.Fatalf("engine: %v", err)
		}
	}()
	defer cancel()

	deadline := time.Now().Add(*dur)
	rounds, kills := 0, 0
	for time.Now().Before(deadline) {
		rounds++
		cmd := exec.Command("sqlite3", src, writerSQL)
		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}
		if rand.Intn(2) == 0 {
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			cmd.Process.Signal(syscall.SIGKILL)
			kills++
		}
		cmd.Wait()
		if !converge(src, rep, 15*time.Second) {
			sd, _ := replay.Dump(src)
			rd, _ := replay.Dump(rep.Path())
			os.WriteFile(filepath.Join(*dir, "source.dump"), []byte(sd), 0o644)
			os.WriteFile(filepath.Join(*dir, "replica.dump"), []byte(rd), 0o644)
			log.Fatalf("DIVERGED at round %d (kills=%d rebases=%d); dumps in %s",
				rounds, kills, e.Rebased(), *dir)
		}
		if rounds%50 == 0 {
			fmt.Printf("round %d ok (kills=%d rebases=%d)\n", rounds, kills, e.Rebased())
		}
	}
	fmt.Printf("PASS: %d rounds, %d kills, %d rebases, dir=%s\n", rounds, kills, e.Rebased(), *dir)
}

func converge(src string, rep *replay.Replica, d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		sd, e1 := replay.Dump(src)
		rd, e2 := replay.Dump(rep.Path())
		if e1 == nil && e2 == nil && sd == rd {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
```

- [ ] **Step 4: Build and run a 2-minute smoke**

Run: `make build && ./bin/torture -d 2m`
Expected: `PASS: N rounds ...` with N ≥ 50 and zero divergence. Then run the tagged test:
`make test-torture` (5 minutes). Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/torture internal/capture/torture_test.go
git commit -m "test: kill -9 torture harness for foreign-writer capture"
```

---

### Task 7: Capturer-crash detection — the dirty-checkout path

**Files:**
- Modify: `internal/capture/engine.go` (startup resume logic in `Run`, before initial `rebase`)
- Test: `internal/capture/engine_test.go` (append)

**Interfaces:**
- Consumes: `capture.State` (Task 5)
- Produces: engine behavior — on startup with a saved state file present, the engine either **resumes** (state matches the live WAL: same salts, offset ≤ WAL length, checksum chain valid at offset) or **detects divergence and rebases**, incrementing `Rebased()`. Silent wrong-resume is the failure mode this task exists to kill.

Resume rule (conservative, spike-grade): resume **only** when the saved salts match the current WAL header **and** the saved offset equals `HeaderSize` (i.e., nothing was captured since the last takeover/rebase, so the header checksum seed is valid). Any other state — salts differ (WAL restarted while we were dead: frames may have been checkpointed unseen), offset > HeaderSize (we cannot reconstruct the running checksum without trusting our own historical bytes) — rebases. Plan 2 refines resume (LTX carries the running checksum); the spike only has to prove *detection is airtight*, not that resume is cheap.

- [ ] **Step 1: Write the failing test**

Append to `internal/capture/engine_test.go`:

```go
func TestEngineDetectsMissedWritesAfterCrash(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(dir, "replica.db"))
	e1 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- e1.Run(ctx1) }()

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)

	// "Crash" the engine (cancel = its lock vanishes, like kill -9 would).
	cancel1()
	<-done1

	// While the engine is dead: write AND checkpoint+restart the WAL, so
	// frames pass through the WAL unseen and the salts change.
	if out, err := exec.Command("sqlite3", src, "PRAGMA busy_timeout=5000;"+
		"INSERT INTO t (v) VALUES (randomblob(64));"+
		"PRAGMA wal_checkpoint(RESTART);"+
		"INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Restart the engine on the same StateDir.
	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()

	// It must converge (via rebase), and it must KNOW it rebased.
	waitEqual(t, src, rep, 10*time.Second)
	if e2.Rebased() < 1 {
		t.Fatal("engine resumed silently across missed writes — undetected divergence")
	}
}
```

- [ ] **Step 2: Run test to verify it fails or passes for the right reason**

Run: `go test ./internal/capture -run TestEngineDetectsMissed -v`
Expected with current code: PASS trivially (the engine always rebases on start — `Rebased() ≥ 1` unconditionally). That makes the test vacuous, so tighten it: this task's implementation adds *resume*, and the test must distinguish resume from rebase. Proceed to Step 3, then confirm both this test AND the new resume test pass.

- [ ] **Step 3: Implement conservative resume**

In `Engine.Run`, replace the unconditional `e.rebase(ctx)` with:

```go
	if resumed, err := e.tryResume(ctx); err != nil {
		return err
	} else if !resumed {
		if err := e.rebase(ctx); err != nil {
			return err
		}
	}
```

Add to `engine.go`:

```go
// tryResume resumes capture without a rebase when — and only when — it can
// prove nothing was missed: saved salts match the live WAL header and the
// saved offset is exactly HeaderSize (checksum seed = header seed). Anything
// else returns false and forces a rebase. NOTE: the sink must also still
// match the saved state; the spike ties them by storing sink state in the
// same StateDir, so state file + replica travel together.
func (e *Engine) tryResume(ctx context.Context) (bool, error) {
	st, ok, err := LoadState(e.statePath())
	if err != nil || !ok || st.Off != int64(wal.HeaderSize) {
		return false, err
	}
	b := make([]byte, wal.HeaderSize)
	f, err := os.Open(e.o.DBPath + "-wal")
	if err != nil {
		return false, nil // no WAL ⇒ can't prove continuity ⇒ rebase
	}
	defer f.Close()
	if _, err := io.ReadFull(f, b); err != nil {
		return false, nil
	}
	hdr, err := wal.ParseHeader(b)
	if err != nil || hdr.Salt1 != st.Salt1 || hdr.Salt2 != st.Salt2 {
		return false, nil // restarted while we were dead ⇒ rebase
	}
	if err := e.beginRead(ctx); err != nil {
		return false, err
	}
	e.reader = wal.NewReader(e.o.DBPath + "-wal")
	e.rebased = 0 // a true resume is not a rebase
	return true, nil
}
```

Also update `rebase` to save salts after reset — after `e.reader = wal.NewReader(...)`, drain once so the reader binds, then persist its `Offset()` (or persist `State{}` and let the first `drain` persist real values, which the existing code already does; verify one path is actually saving salts and fix if not).

Append a resume-positive test:

```go
func TestEngineResumesCleanly(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}
	rep := replay.New(filepath.Join(dir, "replica.db"))

	// First engine: capture, takeover (drained, WAL restarted by us), stop.
	e1 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- e1.Run(ctx1) }()
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	cancel1()
	<-done1

	// No writes while dead. Second engine must resume without rebase.
	e2 := NewEngine(Options{DBPath: src, StateDir: dir, Sink: replicaSink{rep}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()
	defer func() { cancel2(); <-done2 }()

	if out, err := exec.Command("sqlite3", src,
		"PRAGMA busy_timeout=5000; INSERT INTO t (v) VALUES (randomblob(64));").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitEqual(t, src, rep, 10*time.Second)
	if e2.Rebased() != 0 {
		t.Errorf("clean restart should resume, not rebase (rebased=%d)", e2.Rebased())
	}
}
```

Caveat for the implementer: `TestEngineResumesCleanly` requires that engine shutdown leaves `Off == HeaderSize` — which is true only if a takeover ran after the last capture. If the test proves flaky for that reason, make shutdown perform a final takeover (drain → checkpoint RESTART → save state) in the `ctx.Done()` path; that is the correct production behavior anyway.

- [ ] **Step 4: Run the full package**

Run: `go test ./internal/capture -v -timeout 180s`
Expected: PASS ×6 (three Task 5 tests, two crash tests, plus resume). Then re-run torture: `make test-torture`. Expected: PASS.

- [ ] **Step 5: Extend torture to kill the capturer too**

In `internal/capture/torture_test.go`, add engine-restart rounds — every 10th round, cancel the engine context mid-write, wait for `Run` to return, create a fresh `Engine` on the same `StateDir` (same replica), and continue. The `converged` check after each round is unchanged: equivalence or explicit rebase, never silent divergence. Show the code:

```go
		// every 10th round: bounce the engine mid-traffic
		if round%10 == 0 {
			cancel()
			<-engDone
			e = NewEngine(Options{DBPath: src, StateDir: dir, Sink: tortureSink{rep}})
			ctx, cancel = context.WithCancel(context.Background())
			engDone = make(chan error, 1)
			go func() { engDone <- e.Run(ctx) }()
		}
```

(Adjust variable declarations: `ctx`, `cancel`, `e`, `engDone` must be reassignable — declare with `var` / `:=` at loop scope accordingly.)

Run: `make test-torture`
Expected: PASS, with `rebases` in the log > 0 (crash rounds force some) and zero divergence.

- [ ] **Step 6: Commit**

```bash
git add internal/capture
git commit -m "feat: conservative resume with airtight divergence detection; capturer-kill torture"
```

---

### Task 8: Spike report and go/no-go

**Files:**
- Create: `docs/superpowers/specs/2026-07-29-offshoot-spike-report.md`
- Modify: `README.md` (status line)

**Interfaces:**
- Consumes: results of Tasks 6-7 torture runs
- Produces: the written go/no-go verdict Plan 2 is gated on

- [ ] **Step 1: Run the full evidence suite and record real numbers**

```bash
make test 2>&1 | tail -5
./bin/torture -d 10m
make test-torture
```

Record: rounds, kills, rebases, divergences (must be 0), and capture-lag observations.

- [ ] **Step 2: Write the report**

`docs/superpowers/specs/2026-07-29-offshoot-spike-report.md` — structure (fill every bracket with measured values; a bracket left unfilled means the report is not done):

```markdown
# offshoot capture-spike report

**Verdict:** GO / NO-GO for Plan 2.

## What was proven
- Foreign-writer capture (stock sqlite3 CLI): [N] torture rounds, [K] writer kills, 0 undetected divergences.
- Capturer crash: [M] engine bounces; every miss detected (rebases=[R]); silent resume only in provably-clean cases.
- Passive foreign checkpoints survived without rebase: yes/no.

## What was NOT proven (deferred to Plan 2+)
- LTX encoding (spike used raw frame replay), S3 backends, fork/branch ops.
- Resume from mid-WAL offsets (spike resumes only at offset 32; else rebases).
- Sustained-throughput capture lag under continuous write load.
- macOS AND Linux both torture-tested: [state which ran where].

## Surprises / constraints discovered
[List every deviation from the design spec found during implementation —
these feed spec rev 3 if any invalidate § WAL capture.]

## Go/no-go rationale
[2-3 sentences tying the numbers to the spec's risk #4.]
```

- [ ] **Step 3: Update README status**

Change the status line to: `**Status: pre-alpha — capture spike complete ([GO/NO-GO]), storage layer next.**`

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-07-29-offshoot-spike-report.md README.md
git commit -m "docs: capture-spike report and go/no-go verdict"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** This plan intentionally covers only spec § "WAL capture and the connection contract" (the lock dance, restart detection, dirty-checkout path) and § "Testing strategy" (risk spike, torture CI seed). The connection-contract *enforcement* (journal-mode polling, SHM probe), LTX, storage, and lifecycle ops are Plans 2-3 by design — recorded in the Plan sequence section so nothing is silently dropped.
2. **Placeholder scan:** all code blocks are complete; the only bracketed values are in the Task 8 report template, which are measurement slots by definition, with an explicit "unfilled = not done" rule.
3. **Type consistency:** `Sink` (Rebase/Apply) matches `replay.Replica`'s methods via the test adapters; `wal.Reader.Next/Offset/Bind`, `capture.State{Off,Salt1,Salt2}`, and `Engine.Rebased()` are used with identical signatures across Tasks 3-7. `writerSQL` differs deliberately between the tagged test (includes CREATE TABLE IF NOT EXISTS) and the harness (table created at init) — both shown in full.
