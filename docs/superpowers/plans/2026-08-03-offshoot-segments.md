# offshoot Plan 7: Incremental LTX Segments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A daemon flush writes only the pages that changed since the last flush, so repeated flushes on a large database cost bytes instead of gigabytes.

**Architecture:** The capture engine already tells us exactly which pages changed — that is what a WAL frame is. A session accumulates those pages between flushes and writes them as an LTX *segment* covering `(lastFlushed+1 … newTXID)`; every `SnapshotEvery` flushes it writes a full snapshot instead, so materializing never replays an unbounded chain. Reading a branch means finding the newest snapshot at or before the target TXID and applying the segments after it in order. The at-rest CLI `Checkpoint` keeps writing full snapshots — it has no capture engine and no page-change information, and that stays true and documented.

**Tech Stack:** Go 1.24+, existing `internal/ltxio` (superfly/ltx), `internal/store`, `internal/session`, `internal/ops`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md` § Storage layout, § Fork mechanics ("v2 may add content-addressed page-level dedupe"), and the storage-amplification risk it names. **Plan sequence:** Plans 1-6 merged (capture spike GO; local lifecycle; S3 backends; leases and fencing; daemon with live capture; MCP server and demo) → **this plan** → Plan 8 (TTL reaping, Python/TS SDKs, LangGraph adapter).

## Global Constraints

- Module `github.com/sricola/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only; no new module dependencies
- **A branch must always be materializable to exactly the bytes that were flushed** — this plan changes how state is stored, never what state a reader sees; the equivalence tests are the contract
- Segments are immutable and epoch-fenced exactly like snapshots: `data/{lineage}/{epoch}/…`, written create-only, never rewritten
- **Materialization must be bounded:** the number of segments replayed for any read is capped by the snapshot cadence, and the cap is asserted by a test, not assumed
- A partially-written or missing member of a chain is a hard, loud failure — never a silently short read; LTX checksums are verified on every member
- Fork/rollback/promote keep minting a fresh lineage seeded from one materialized snapshot (Plan 2's one-writer-per-lineage invariant is untouched)
- The at-rest `ops.Checkpoint` continues to write full snapshots; that asymmetry is documented where a user meets it
- Every Plan-1..6 test must keep passing unmodified
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/ltxio/segment.go        EncodeSegment; chain materialization
internal/ltxio/segment_test.go
internal/store/store.go          (modify) SegmentKey + chain listing/parsing helpers
internal/store/store_test.go     (modify)
internal/session/pages.go        Page accumulator: changed pages between flushes
internal/session/pages_test.go
internal/session/flush.go        (modify) write a segment, or a snapshot on cadence
internal/session/flush_test.go   (modify)
internal/ops/materialize.go      Chain resolution used by every read path
internal/ops/materialize_test.go
internal/ops/gc.go               (modify) sweep segments with their lineage
README.md                        (modify) what a flush costs, and the CLI asymmetry
```

---

### Task 1: Encode a segment and materialize a chain

**Files:**
- Create: `internal/ltxio/segment.go`, `internal/ltxio/segment_test.go`

**Interfaces:**
- Consumes: `github.com/superfly/ltx`, the existing `EncodeSnapshot`/`Materialize` in `internal/ltxio/ltxio.go` (read them — this task follows their conventions for page size, checksums, and atomic destination writes)
- Produces:

```go
package ltxio

// Page is one database page destined for a segment.
type Page struct {
	Pgno uint32
	Data []byte // exactly PageSize bytes
}

// EncodeSegment writes an LTX segment carrying only pages, covering
// transactions [minTXID, maxTXID]. commit is the database size in pages after
// maxTXID — the value SQLite's own commit frame records — so a reader can
// truncate correctly when the database shrank. Pages must be sorted by Pgno
// and contain no duplicates; EncodeSegment returns an error otherwise rather
// than writing a segment a reader would misinterpret.
func EncodeSegment(pageSize uint32, commit uint32, minTXID, maxTXID uint64, pages []Page, w io.Writer) error

// MaterializeChain writes the database formed by a full snapshot followed by
// zero or more segments applied in order into dbPath. Every member's checksum
// is verified; the destination is written atomically (temp + rename) so a
// failure anywhere leaves no partial file. Segments must be contiguous in
// TXID: each segment's MinTXID must be exactly the previous member's MaxTXID+1,
// and a gap is an error, never a silent skip. Returns the resulting MaxTXID.
func MaterializeChain(snapshot io.Reader, segments []io.Reader, dbPath string) (uint64, error)
```

**API-adaptation authorization:** the encoder/decoder calls below follow what `ltxio.go` already does with `superfly/ltx`. Read that file first and match it; if the real API differs from anything sketched here, follow the library and record it in your report.

- [ ] **Step 1: Write the failing test**

`internal/ltxio/segment_test.go`:

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

// buildDB makes a quiesced SQLite file and returns its path.
func buildDB(t *testing.T, stmts ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range append([]string{"PRAGMA journal_mode=WAL"}, stmts...) {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func dumpOf(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", path, ".dump").Output()
	if err != nil {
		t.Fatalf("dump %s: %v", path, err)
	}
	return string(out)
}

// readPages returns every page of a quiesced database.
func readPages(t *testing.T, path string) (pageSize uint32, commit uint32, pages []Page) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize = uint32(data[16])<<8 | uint32(data[17])
	if pageSize == 1 {
		pageSize = 65536
	}
	commit = uint32(data[28])<<24 | uint32(data[29])<<16 | uint32(data[30])<<8 | uint32(data[31])
	for i := uint32(0); i < commit; i++ {
		off := int(i) * int(pageSize)
		pages = append(pages, Page{Pgno: i + 1, Data: data[off : off+int(pageSize)]})
	}
	return pageSize, commit, pages
}

func TestSegmentAppliedToSnapshotEqualsTheLaterDatabase(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	// v1: the snapshot's state. v2: after more writes.
	v1 := buildDB(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
		"INSERT INTO t (v) VALUES ('a'), ('b')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	_, _, before := readPages(t, v1)

	// Apply more writes to a copy, then diff pages to build the segment.
	v2 := filepath.Join(t.TempDir(), "v2.sqlite")
	if err := copyFileForTest(v1, v2); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", v2,
		"INSERT INTO t (v) VALUES ('c'); UPDATE t SET v='A' WHERE id=1;").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	pageSize, commit, after := readPages(t, v2)

	var changed []Page
	for i, p := range after {
		if i >= len(before) || !bytes.Equal(p.Data, before[i].Data) {
			changed = append(changed, p)
		}
	}
	if len(changed) == 0 {
		t.Fatal("expected some pages to differ")
	}
	if len(changed) == len(after) {
		t.Fatal("every page changed; this test cannot show a segment is smaller")
	}

	var seg bytes.Buffer
	if err := EncodeSegment(pageSize, commit, 2, 2, changed, &seg); err != nil {
		t.Fatal(err)
	}
	if seg.Len() >= snap.Len() {
		t.Errorf("a partial segment (%d bytes) should be smaller than a full snapshot (%d)",
			seg.Len(), snap.Len())
	}

	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	txid, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(seg.Bytes())}, out)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 2 {
		t.Errorf("txid = %d, want 2", txid)
	}
	if dumpOf(t, out) != dumpOf(t, v2) {
		t.Fatal("snapshot+segment does not reproduce the later database")
	}
}

func TestMaterializeChainWithNoSegmentsIsJustTheSnapshot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	v1 := buildDB(t, "CREATE TABLE t (v)", "INSERT INTO t VALUES ('only')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 5, &snap); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	txid, err := MaterializeChain(bytes.NewReader(snap.Bytes()), nil, out)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 5 {
		t.Errorf("txid = %d, want 5", txid)
	}
	if dumpOf(t, out) != dumpOf(t, v1) {
		t.Fatal("chain with no segments must equal the snapshot")
	}
}

func TestChainRejectsAGap(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	v1 := buildDB(t, "CREATE TABLE t (v)", "INSERT INTO t VALUES ('x')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	pageSize, commit, pages := readPages(t, v1)
	var seg bytes.Buffer
	// Covers txids 5..5 — a gap after the snapshot's txid 1.
	if err := EncodeSegment(pageSize, commit, 5, 5, pages[:1], &seg); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	if _, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(seg.Bytes())}, out); err == nil {
		t.Fatal("a TXID gap in the chain must be an error, never a silent skip")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("a failed chain must leave no destination file")
	}
}

func TestEncodeSegmentRejectsUnsortedOrDuplicatePages(t *testing.T) {
	p := func(n uint32) Page { return Page{Pgno: n, Data: make([]byte, 4096)} }
	var buf bytes.Buffer
	if err := EncodeSegment(4096, 3, 2, 2, []Page{p(2), p(1)}, &buf); err == nil {
		t.Error("unsorted pages must be rejected")
	}
	if err := EncodeSegment(4096, 3, 2, 2, []Page{p(1), p(1)}, &buf); err == nil {
		t.Error("duplicate pages must be rejected")
	}
}

func TestCorruptSegmentFailsClosed(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	v1 := buildDB(t, "CREATE TABLE t (v)", "INSERT INTO t VALUES ('x')")
	var snap bytes.Buffer
	if err := EncodeSnapshot(v1, 1, &snap); err != nil {
		t.Fatal(err)
	}
	pageSize, commit, pages := readPages(t, v1)
	var seg bytes.Buffer
	if err := EncodeSegment(pageSize, commit, 2, 2, pages[:1], &seg); err != nil {
		t.Fatal(err)
	}
	b := seg.Bytes()
	b[len(b)/2] ^= 0xFF
	out := filepath.Join(t.TempDir(), "rebuilt.sqlite")
	if _, err := MaterializeChain(bytes.NewReader(snap.Bytes()),
		[]io.Reader{bytes.NewReader(b)}, out); err == nil {
		t.Fatal("a corrupt segment must fail closed")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("a failed chain must leave no destination file")
	}
}

func copyFileForTest(from, to string) error {
	b, err := os.ReadFile(from)
	if err != nil {
		return err
	}
	return os.WriteFile(to, b, 0o644)
}
```

(add `"io"` to the imports)

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/ltxio -run 'Segment|Chain|Corrupt' -v`
Expected: FAIL — `EncodeSegment` undefined.

- [ ] **Step 3: Implement**

`internal/ltxio/segment.go`. Follow `ltxio.go`'s existing conventions exactly — read how it opens the encoder, accumulates the post-apply checksum, skips the lock page, and writes the destination atomically, and mirror that. The shape:

```go
package ltxio

// Page is one database page destined for a segment.
type Page struct {
	Pgno uint32
	Data []byte
}

// EncodeSegment writes an LTX segment carrying only pages, covering
// [minTXID, maxTXID]. Unlike a snapshot (MinTXID 1), a segment is applied on
// top of an earlier state, so a reader must have every member of the chain.
func EncodeSegment(pageSize, commit uint32, minTXID, maxTXID uint64, pages []Page, w io.Writer) error {
	if minTXID == 0 || maxTXID < minTXID {
		return fmt.Errorf("ltxio: bad segment range [%d,%d]", minTXID, maxTXID)
	}
	if minTXID == 1 {
		return fmt.Errorf("ltxio: a segment cannot start at TXID 1 (that is a snapshot)")
	}
	for i, p := range pages {
		if len(p.Data) != int(pageSize) {
			return fmt.Errorf("ltxio: page %d is %d bytes, want %d", p.Pgno, len(p.Data), pageSize)
		}
		if i > 0 && p.Pgno <= pages[i-1].Pgno {
			return fmt.Errorf("ltxio: pages must be sorted and unique (pgno %d after %d)",
				p.Pgno, pages[i-1].Pgno)
		}
	}
	// Encode header with MinTXID=minTXID, MaxTXID=maxTXID, Commit=commit,
	// then each page, then close. Match ltxio.go's checksum handling; a
	// segment's post-apply checksum cannot be computed from its own pages
	// alone, so follow what the ltx library requires for non-snapshot files
	// (check the library: if it requires a PreApplyChecksum, thread it
	// through — extend this function's signature and record the change in
	// your report, updating the tests' call sites).
}

// MaterializeChain applies a snapshot then segments into dbPath atomically.
func MaterializeChain(snapshot io.Reader, segments []io.Reader, dbPath string) (uint64, error) {
	// 1. Decode the snapshot into a temp file (reuse the logic Materialize
	//    already has — factor it into a shared helper rather than copying).
	// 2. For each segment in order: decode its header, require
	//    MinTXID == prevMaxTXID+1 (else a gap error), apply its pages by
	//    WriteAt, truncate to Commit*PageSize, verify its checksum on Close.
	// 3. fsync, rename into place, remove -wal/-shm siblings.
	// Any error returns before the rename so dbPath is never partially written.
}
```

**Note on `PreApplyChecksum`:** the ltx format may require a segment to carry the checksum of the state it applies to. If so, `EncodeSegment` needs that value passed in and `MaterializeChain` must verify it — which is a stronger guarantee than the TXID contiguity check and worth having. Determine this from the library, implement whichever it requires, adjust the test call sites, and say what you found.

- [ ] **Step 4: Run**

Run: `go test ./internal/ltxio -v -race`
Expected: PASS — including the existing snapshot tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add internal/ltxio
git commit -m "feat: encode incremental LTX segments and materialize a snapshot+segment chain"
```

---

### Task 2: Segment keys and chain listing

**Files:**
- Modify: `internal/store/store.go`, `internal/store/store_test.go`

**Interfaces:**
- Consumes: `Backend.List`, `SnapshotKey`, `LineagePrefix` (Plan 2/4)
- Produces:

```go
package store

// SegmentKey locates an incremental segment covering (minTXID, maxTXID].
// Sorting by key sorts by maxTXID, so a lexical List is already in apply order.
func SegmentKey(lineage string, epoch, minTXID, maxTXID uint64) string
// "data/{lineage}/{epoch}/segment-{maxTXID:016x}-{minTXID:016x}.ltx"

// ChainMember identifies one object in a materialization chain.
type ChainMember struct {
	Key            string
	Snapshot       bool
	MinTXID, MaxTXID uint64
	Epoch          uint64
}

// ParseMemberKey parses a snapshot or segment key back into a ChainMember.
func ParseMemberKey(key string) (ChainMember, bool)

// Chain returns the members needed to materialize lineage at target: the
// newest snapshot with MaxTXID <= target, followed by every segment after it
// up to target, in apply order. It returns an error when no snapshot covers
// the target or the segments do not form a contiguous run — a caller must
// never be handed a chain with a hole.
func (s *Store) Chain(lineage string, target uint64) ([]ChainMember, error)
```

`Chain` lists `LineagePrefix(lineage)` once and works from the parsed keys, so it costs one `List` regardless of epoch count — segments from superseded epochs are simply members like any other, since an epoch bump does not move objects.

- [ ] **Step 1: Write the failing test**

Append to `internal/store/store_test.go`:

```go
func TestSegmentKeySortsByMaxTXID(t *testing.T) {
	a := SegmentKey("lin", 1, 2, 5)
	b := SegmentKey("lin", 1, 6, 9)
	if !(a < b) {
		t.Fatalf("segment keys must sort in apply order: %s !< %s", a, b)
	}
	m, ok := ParseMemberKey(a)
	if !ok || m.Snapshot || m.MinTXID != 2 || m.MaxTXID != 5 || m.Epoch != 1 {
		t.Fatalf("round trip failed: %+v ok=%v", m, ok)
	}
	sm, ok := ParseMemberKey(SnapshotKey("lin", 3, 7))
	if !ok || !sm.Snapshot || sm.MaxTXID != 7 || sm.Epoch != 3 {
		t.Fatalf("snapshot round trip failed: %+v ok=%v", sm, ok)
	}
	if _, ok := ParseMemberKey("data/lin/1/not-a-member.txt"); ok {
		t.Fatal("a non-member key must not parse")
	}
}

func TestChainPicksNewestSnapshotThenSegments(t *testing.T) {
	s := newStore(t)
	put := func(k string) {
		if err := s.B.Put(k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	put(SnapshotKey("lin", 1, 1))
	put(SegmentKey("lin", 1, 2, 3))
	put(SegmentKey("lin", 1, 4, 5))
	put(SnapshotKey("lin", 2, 6)) // a later full snapshot, different epoch
	put(SegmentKey("lin", 2, 7, 8))

	// Target after the second snapshot: chain starts there, not at txid 1.
	got, err := s.Chain("lin", 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].Snapshot || got[0].MaxTXID != 6 || got[1].MaxTXID != 8 {
		t.Fatalf("chain = %+v", got)
	}

	// Target in the middle of the first run: snapshot 1 plus one segment.
	got, err = s.Chain("lin", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].Snapshot || got[0].MaxTXID != 1 || got[1].MaxTXID != 3 {
		t.Fatalf("chain = %+v", got)
	}

	// Exactly on a snapshot: just that snapshot.
	got, err = s.Chain("lin", 6)
	if err != nil || len(got) != 1 || !got[0].Snapshot {
		t.Fatalf("chain = %+v err=%v", got, err)
	}
}

func TestChainRefusesAHole(t *testing.T) {
	s := newStore(t)
	if err := s.B.Put(SnapshotKey("lin", 1, 1), []byte("x")); err != nil {
		t.Fatal(err)
	}
	// 2..3 present, 4..5 missing, 6..7 present: a hole before the target.
	if err := s.B.Put(SegmentKey("lin", 1, 2, 3), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.B.Put(SegmentKey("lin", 1, 6, 7), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Chain("lin", 7); err == nil {
		t.Fatal("a hole in the chain must be an error")
	}
}

func TestChainRefusesWhenNoSnapshotCoversTarget(t *testing.T) {
	s := newStore(t)
	if err := s.B.Put(SegmentKey("lin", 1, 2, 3), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Chain("lin", 3); err == nil {
		t.Fatal("a chain with no base snapshot must be an error")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/store -run 'Segment|Chain' -v`
Expected: FAIL — `SegmentKey` undefined.

- [ ] **Step 3: Implement**

Add to `internal/store/store.go`. `SegmentKey` formats both TXIDs as 16-hex so keys sort by `maxTXID` then `minTXID`. `ParseMemberKey` splits the key, recognizes the `snapshot-` and `segment-` prefixes, and parses the hex fields; anything else returns `ok=false`. `Chain` lists the lineage prefix, parses every member, picks the newest snapshot with `MaxTXID <= target`, then walks segments in order requiring `MinTXID == prev.MaxTXID+1` until reaching `target`, erroring on a hole or on overshooting without landing exactly on `target`.

- [ ] **Step 4: Run**

Run: `go test ./internal/store -v -race && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat: segment keys and chain resolution in the store layout"
```

---

### Task 3: Read every branch through the chain

**Files:**
- Create: `internal/ops/materialize.go`, `internal/ops/materialize_test.go`
- Modify: `internal/ops/ops.go` (route `materializeAt` through the new resolver)

**Interfaces:**
- Consumes: `store.Chain`, `store.ChainMember`, `ltxio.MaterializeChain` (Tasks 1-2)
- Produces:

```go
package ops

// materializeChainAt writes the state of ref's lineage at cp into dst by
// resolving the object chain and applying it. It replaces the single-snapshot
// read path: a lineage that has only ever been snapshotted resolves to a
// one-member chain, so behavior for existing stores is unchanged.
func (w *Workspace) materializeChainAt(ref store.Ref, cp store.Checkpoint, dst string) error
```

`materializeAt` becomes a thin wrapper over this so every existing caller (Checkout, Rollback's refresh, Promote's refresh, `copySnapshotToNewLineage`'s read side) picks it up with no signature churn. **`copySnapshotToNewLineage` must keep producing a single full snapshot in the child's fresh lineage** — a fork's base is always one materialized snapshot, never a copied chain, which is what keeps children storage-independent.

- [ ] **Step 1: Write the failing test**

`internal/ops/materialize_test.go`:

```go
package ops

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/sricola/offshoot/internal/ltxio"
	"github.com/sricola/offshoot/internal/store"
)

// TestCheckoutReadsASnapshotPlusSegments builds a chain by hand in the store
// and proves every read path resolves it.
func TestCheckoutReadsASnapshotPlusSegments(t *testing.T) {
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
		"CREATE TABLE t (v); INSERT INTO t VALUES ('base');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "base"); err != nil {
		t.Fatal(err)
	}

	// Hand-build the next state and store it as a SEGMENT, advancing the ref.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	next := path + ".next"
	if err := copyFile(path, next); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", next,
		"INSERT INTO t VALUES ('from-segment');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	segTXID := ref.HeadTXID + 1
	pageSize, commit, changed := changedPagesForTest(t, path, next)
	var seg bytes.Buffer
	if err := ltxio.EncodeSegment(pageSize, commit, segTXID, segTXID, changed, &seg); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.B.PutIf(
		store.SegmentKey(ref.Lineage, ref.Epoch, segTXID, segTXID), seg.Bytes(), ""); err != nil {
		t.Fatal(err)
	}
	ref.HeadTXID, ref.HeadEpoch = segTXID, ref.Epoch
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	// A fresh checkout must contain both rows.
	got, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout across a chain: %v", err)
	}
	out, err := exec.Command("sqlite3", got, "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "2\n" {
		t.Fatalf("rows = %q err=%v (segment not applied)", out, err)
	}
}

// TestForkFromAChainProducesASingleSnapshot proves a child's lineage is not a
// copied chain: it holds exactly one snapshot object, so it stays independent.
func TestForkFromAChainProducesASingleSnapshot(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	path, _ := w.Checkout("app", "main")
	exec.Command("sqlite3", path, "CREATE TABLE t (v); INSERT INTO t VALUES ('base');").Run()
	if _, err := w.Checkpoint("app", "main", "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "child", ""); err != nil {
		t.Fatal(err)
	}
	cref, _, err := w.Store.GetRef("app", "child")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(cref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("a fork's lineage must hold exactly one snapshot, got %v", keys)
	}
	if m, ok := store.ParseMemberKey(keys[0]); !ok || !m.Snapshot {
		t.Fatalf("fork's only object must be a snapshot: %s", keys[0])
	}
}

// changedPagesForTest diffs two quiesced databases into segment pages.
func changedPagesForTest(t *testing.T, before, after string) (uint32, uint32, []ltxio.Page) {
	t.Helper()
	// Implement with os.ReadFile on both files, reading page size from byte 16
	// and page count from byte 28 of `after`, comparing page by page. Return
	// the pages of `after` that differ from `before` (or are new), in Pgno order.
	panic("implement in Step 3")
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/ops -run 'ReadsASnapshotPlus|ForkFromAChain' -v`
Expected: FAIL — `materializeChainAt` missing and the helper panics.

- [ ] **Step 3: Implement**

Write `internal/ops/materialize.go` with `materializeChainAt`: call `w.Store.Chain(ref.Lineage, cp.TXID)`, `Get` each member, hand the first (snapshot) and the rest to `ltxio.MaterializeChain`, wrapping any failure with the lineage and target so a broken chain names itself. Change `materializeAt` to delegate. Implement `changedPagesForTest` for real (replace the panic).

- [ ] **Step 4: Run**

Run: `go test ./internal/ops -v -race -timeout 300s && go test ./... -count=1`
Expected: PASS, with every Plan-2..6 ops test unchanged.

- [ ] **Step 5: Commit**

```bash
git add internal/ops
git commit -m "feat: resolve a snapshot+segment chain on every read path"
```

---

### Task 4: A flush writes only what changed

**Files:**
- Create: `internal/session/pages.go`, `internal/session/pages_test.go`
- Modify: `internal/session/session.go` (record pages as the sink applies them), `internal/session/flush.go`

**Interfaces:**
- Consumes: `wal.Frame` (Plan 1), `ltxio.EncodeSegment`, `store.SegmentKey`
- Produces:

```go
package session

// pageSet accumulates the latest content of every page changed since the last
// flush. It is the segment's payload: the capture engine hands us exactly the
// pages SQLite wrote, so a flush never has to diff the database.
type pageSet struct { /* unexported */ }

func newPageSet() *pageSet
// record keeps the newest version of each page; a page written twice between
// flushes is stored once, at its latest content.
func (p *pageSet) record(frames []wal.Frame)
// drain returns the accumulated pages sorted by Pgno and resets the set.
func (p *pageSet) drain() []ltxio.Page
func (p *pageSet) len() int
```

And on `Session`:

```go
// Options gains:
//   SnapshotEvery int // full snapshot every N flushes (default 16); 1 means
//                     // always snapshot, restoring pre-segment behavior
```

`Flush` decides: if this is the session's first flush, or `flushesSinceSnapshot >= SnapshotEvery`, or the accumulated pages are a large fraction of the database, write a full snapshot as today and reset the counter; otherwise encode the drained pages as a segment covering `(lastFlushedTXID+1 … newTXID)` and write it at `SegmentKey`. Either way the ref advances exactly as before — readers resolve whatever chain results.

The page accumulation happens where the replica sink already receives frames, under the same `replicaMu`, so a page recorded is exactly a page applied — they cannot diverge.

- [ ] **Step 1: Write the failing test**

`internal/session/pages_test.go`:

```go
package session

import (
	"testing"

	"github.com/sricola/offshoot/internal/wal"
)

func frame(pgno uint32, b byte) wal.Frame {
	d := make([]byte, 8)
	for i := range d {
		d[i] = b
	}
	return wal.Frame{Header: wal.FrameHeader{Pgno: pgno}, Data: d}
}

func TestPageSetKeepsLatestAndSorts(t *testing.T) {
	p := newPageSet()
	p.record([]wal.Frame{frame(3, 'a'), frame(1, 'a')})
	p.record([]wal.Frame{frame(3, 'b')}) // page 3 written again
	if p.len() != 2 {
		t.Fatalf("len = %d, want 2 distinct pages", p.len())
	}
	got := p.drain()
	if len(got) != 2 || got[0].Pgno != 1 || got[1].Pgno != 3 {
		t.Fatalf("drain must sort by pgno: %+v", got)
	}
	if got[1].Data[0] != 'b' {
		t.Fatalf("page 3 must hold its latest content, got %q", got[1].Data[0])
	}
	if p.len() != 0 {
		t.Fatal("drain must reset the set")
	}
}

func TestPageSetCopiesFrameData(t *testing.T) {
	p := newPageSet()
	f := frame(1, 'x')
	p.record([]wal.Frame{f})
	f.Data[0] = 'z' // the caller reuses its buffer
	got := p.drain()
	if got[0].Data[0] != 'x' {
		t.Fatal("pageSet must copy frame data, not alias the caller's buffer")
	}
}
```

Append to `internal/session/flush_test.go`:

```go
func TestFlushWritesASegmentThenSnapshotsOnCadence(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if out, err := exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	var members []store.ChainMember
	for i := 0; i < 5; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			"INSERT INTO t (v) VALUES ('row');").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := s.Flush(""); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok {
			members = append(members, m)
		}
	}
	var snaps, segs int
	for _, m := range members {
		if m.Snapshot {
			snaps++
		} else {
			segs++
		}
	}
	if segs == 0 {
		t.Fatal("with SnapshotEvery=3 some flushes must write segments")
	}
	if snaps < 2 {
		t.Fatalf("the cadence must produce periodic snapshots, got %d", snaps)
	}
	// The branch still reads correctly across the mixed chain.
	path, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sqlite3", path, "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "5\n" {
		t.Fatalf("rows after mixed chain = %q err=%v", out, err)
	}
}

func TestSnapshotEveryOneKeepsOldBehavior(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 3; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	ref, _, _ := w.Store.GetRef("app", "main")
	keys, _ := w.Store.B.List(store.LineagePrefix(ref.Lineage))
	for _, k := range keys {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot {
			t.Fatalf("SnapshotEvery=1 must never write a segment, found %s", k)
		}
	}
}
```

(add `"github.com/sricola/offshoot/internal/store"` to that file's imports if missing)

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/session -run 'PageSet|Segment|SnapshotEvery' -v`
Expected: FAIL — `newPageSet` undefined, `SnapshotEvery` not a field.

- [ ] **Step 3: Implement**

Write `pages.go` (a `map[uint32][]byte` plus sorted drain, copying frame data). In `session.go`, add `SnapshotEvery` to `Options` with the default, and have the replica sink's `Apply` record frames into the session's `pageSet` while it holds `replicaMu`. In `flush.go`, branch between snapshot and segment as described, tracking `flushesSinceSnapshot` and `lastFlushedTXID` under the session mutex, and reset the page set on every successful flush — **including the snapshot path**, or the next segment would carry stale pages.

Note the interaction with the existing orphan-overwrite logic: a segment key encodes both TXIDs, so a retried flush after a lost CAS targets the same key; keep the existing create-only-then-overwrite handling and make sure it applies to segment keys too.

- [ ] **Step 4: Run**

Run: `go test ./internal/session -v -race -timeout 600s && go test ./... -count=1 -race`
Expected: PASS, including Plan 5's stress and durability tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add internal/session
git commit -m "feat: flush writes an incremental segment, snapshotting on cadence"
```

---

### Task 5: GC and bounded replay

**Files:**
- Modify: `internal/ops/gc.go`, `internal/ops/gc_test.go`, `README.md`

**Interfaces:**
- Consumes: `store.ParseMemberKey`, `store.Chain`
- Produces: GC keeps sweeping whole dead lineages (unchanged), and gains a check that a *live* lineage's chain is intact — plus the documentation of what a flush now costs.

Two things this task must establish:
1. **Bounded replay.** A test asserts that after many flushes the chain for the head is no longer than the snapshot cadence — the guarantee that makes segments safe.
2. **GC correctness with segments.** Sweeping a dead lineage removes its segments as well as its snapshots (it lists the whole prefix, so this should already hold — the test proves it rather than assuming).

- [ ] **Step 1: Write the tests**

Append to `internal/ops/gc_test.go`:

```go
func TestReplayStaysBoundedAcrossManyFlushes(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 20; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
		if _, err := s.Flush(""); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := w.Store.Chain(ref.Lineage, ref.HeadTXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) > 4 {
		t.Fatalf("replay must stay bounded by the snapshot cadence, chain is %d members", len(chain))
	}
	if !chain[0].Snapshot {
		t.Fatal("a chain must start at a snapshot")
	}
}

func TestGCSweepsSegmentsWithTheirLineage(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Fork("app", "main", "doomed", ""); err != nil {
		t.Fatal(err)
	}
	s, err := session.Open(context.Background(), session.Options{
		WS: w, DB: "app", Branch: "doomed", SnapshotEvery: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 3; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	ref, _, _ := w.Store.GetRef("app", "doomed")
	lineage := ref.Lineage
	keysBefore, _ := w.Store.B.List(store.LineagePrefix(lineage))
	var segs int
	for _, k := range keysBefore {
		if m, ok := store.ParseMemberKey(k); ok && !m.Snapshot {
			segs++
		}
	}
	if segs == 0 {
		t.Fatal("setup produced no segments to sweep")
	}
	s.Close()

	if err := w.Destroy("app", "doomed", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	after, _ := w.Store.B.List(store.LineagePrefix(lineage))
	if len(after) != 0 {
		t.Fatalf("GC must sweep segments with their lineage, %d objects remain: %v", len(after), after)
	}
}
```

(add the `session` and `store` imports and `context`)

- [ ] **Step 2: Run and fix what they expose**

Run: `go test ./internal/ops -run 'ReplayStaysBounded|GCSweepsSegments' -v -race -timeout 600s`
Expected: PASS if Tasks 1-4 are right. A bounded-replay failure means the cadence logic is off (likely `flushesSinceSnapshot` not resetting); a GC failure means the sweep is key-shape-sensitive. Fix source, not tests.

- [ ] **Step 3: Document**

Add to `README.md` in the daemon section:

```markdown
### What a flush costs

A daemon flush writes only the pages that changed since the previous flush —
the capture engine already knows exactly which those are. Every sixteenth flush
(configurable) writes a full snapshot instead, so materializing a branch never
replays an unbounded chain: a read applies one snapshot plus at most that many
segments.

The at-rest `offshoot checkpoint` still writes a full snapshot every time. It
runs without a daemon, so it has no record of which pages changed and would
have to diff the whole database to find out. If you checkpoint large databases
in a loop, run a daemon.
```

- [ ] **Step 4: Full suite and commit**

Run: `go test ./... -count=1 -race && go vet ./...`

```bash
git add internal/ops README.md
git commit -m "test: bounded replay and segment-aware GC; document flush cost"
```

---

### Task 6: Adversarial pass — chains under stress and interruption

**Files:**
- Create: `internal/session/segment_stress_test.go`
- Modify: source only if the tests expose a real gap

**Interfaces:** none new. These are the equivalence and durability guarantees the format change must not break.

- [ ] **Step 1: Write the tests**

`internal/session/segment_stress_test.go`:

```go
package session

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
)

// TestEveryFlushIsExactlyMaterializable is the format change's core contract:
// whatever a flush reported durable must materialize to exactly the database
// the agent had at that moment, whether it was stored as a snapshot or a
// segment.
func TestEveryFlushIsExactlyMaterializable(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec.Command("sqlite3", s.CheckoutPath(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").Run()

	for i := 0; i < 10; i++ {
		if out, err := exec.Command("sqlite3", s.CheckoutPath(),
			fmt.Sprintf("INSERT INTO t (v) VALUES ('row-%d');", i)).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		name := fmt.Sprintf("cp%d", i)
		if _, err := s.Flush(name); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
		// Every checkpoint so far must still materialize to the right row count.
		for j := 0; j <= i; j++ {
			br := fmt.Sprintf("check-%d-%d", i, j)
			if _, err := w.Fork("app", "main", br, fmt.Sprintf("cp%d", j)); err != nil {
				t.Fatalf("fork at cp%d: %v", j, err)
			}
			p, err := w.Checkout("app", br)
			if err != nil {
				t.Fatalf("checkout %s: %v", br, err)
			}
			out, err := exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
			if err != nil {
				t.Fatalf("%s: %v", br, err)
			}
			if want := fmt.Sprintf("%d\n", j+1); string(out) != want {
				t.Fatalf("cp%d materialized %s rows, want %s", j, out, want)
			}
		}
	}
}

// TestChainSurvivesSessionRestart proves a chain written by one session is
// readable and extendable by the next.
func TestChainSurvivesSessionRestart(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", s1.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 3; i++ {
		exec.Command("sqlite3", s1.CheckoutPath(), "INSERT INTO t VALUES ('a');").Run()
		if _, err := s1.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	out, err := exec.Command("sqlite3", s2.CheckoutPath(), "SELECT count(*) FROM t;").Output()
	if err != nil || string(out) != "3\n" {
		t.Fatalf("second session sees %q err=%v", out, err)
	}
	exec.Command("sqlite3", s2.CheckoutPath(), "INSERT INTO t VALUES ('b');").Run()
	if _, err := s2.Flush(""); err != nil {
		t.Fatalf("flush after restart: %v", err)
	}
	p, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	out, _ = exec.Command("sqlite3", p, "SELECT count(*) FROM t;").Output()
	if string(out) != "4\n" {
		t.Fatalf("rows after restart+flush = %q", out)
	}
}

// TestMissingSegmentIsLoud proves a chain with a deleted member fails closed
// rather than silently materializing an older state.
func TestMissingSegmentIsLoud(t *testing.T) {
	requireSQLite(t)
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), Options{
		WS: w, DB: "app", Branch: "main", SnapshotEvery: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	exec.Command("sqlite3", s.CheckoutPath(), "CREATE TABLE t (v);").Run()
	for i := 0; i < 3; i++ {
		exec.Command("sqlite3", s.CheckoutPath(), "INSERT INTO t VALUES ('x');").Run()
		if _, err := s.Flush(""); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	ref, _, _ := w.Store.GetRef("app", "main")
	keys, _ := w.Store.B.List(storeLineagePrefixForTest(ref.Lineage))
	var victim string
	for _, k := range keys {
		if m, ok := parseMemberForTest(k); ok && !m.Snapshot {
			victim = k
		}
	}
	if victim == "" {
		t.Skip("no segment was written; nothing to remove")
	}
	if err := w.Store.B.Delete(victim); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkout("app", "main"); err == nil {
		t.Fatal("a missing chain member must fail loudly, not silently read an older state")
	}
}
```

Add the two small test shims (`storeLineagePrefixForTest`, `parseMemberForTest`) as thin wrappers over `store.LineagePrefix`/`store.ParseMemberKey` in the same file, or import `store` directly and drop them — whichever keeps the file clean.

- [ ] **Step 2: Run, investigate, fix**

Run: `go test ./internal/session -run 'ExactlyMaterializable|SurvivesSessionRestart|MissingSegment' -v -race -count=2 -timeout 900s`
Expected: PASS. Failures worth taking seriously: a checkpoint materializing the wrong row count means the segment's TXID range or the chain resolution is wrong; a restart failure means `lastFlushedTXID` is not recovered from the ref; a silent read past a missing segment means `Chain` is not enforcing contiguity. Fix source and document.

- [ ] **Step 3: Full suite**

Run: `go test ./... -count=1 -race && go vet ./... && make test-torture`
Expected: PASS. The torture run matters because Task 4 touched the sink path that the capture engine feeds — report its numbers.

- [ ] **Step 4: Commit**

```bash
git add internal/session
git commit -m "test: chain equivalence under repeated flushes, restart, and a missing member"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** implements the storage-amplification concern the spec names under § Fork mechanics ("N materialized forks of a G-byte database cost up to N×G… v2 may add content-addressed page-level dedupe") by making *flushes* incremental, which is where the repeated cost actually falls; § Storage layout is extended with a segment key that preserves the epoch fence and the create-only discipline. Deliberately out of scope and stated in the header: TTL reaping and the SDK/LangGraph integrations (Plan 8). Fork-time dedupe across lineages is explicitly NOT attempted — children stay storage-independent, which Plan 2's tests enforce and Task 3 re-asserts.
2. **Placeholder scan:** one deliberate `panic("implement in Step 3")` inside a test helper, paired with an explicit Step 3 instruction to replace it and a description of exactly what it must do — the alternative was duplicating page-diff code in the plan twice. Task 1 carries a bounded API-verification authorization for the ltx segment/checksum surface, as Plans 3 and 6 did successfully.
3. **Type consistency:** `ltxio.Page{Pgno,Data}`, `EncodeSegment(pageSize, commit, minTXID, maxTXID, pages, w)`, and `MaterializeChain(snapshot, segments, dbPath)` are used identically in Tasks 1, 3, 4; `store.SegmentKey(lineage, epoch, minTXID, maxTXID)`, `ChainMember{Key,Snapshot,MinTXID,MaxTXID,Epoch}`, `ParseMemberKey`, and `(*Store).Chain(lineage, target)` match across Tasks 2, 3, 5, 6; `pageSet`'s `record`/`drain`/`len` and `Options.SnapshotEvery` match between Tasks 4, 5, 6; `materializeChainAt(ref, cp, dst)` matches Plan 4's existing `materializeAt` signature so its callers need no change.
