# offshoot Plan 4: Leases and Epoch Fencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a branch leasable so a long-running writer can hold it exclusively, and make epoch fencing real, so a paused-then-resumed writer's uploads land in a dead prefix instead of corrupting a lineage.

**Architecture:** Leases live in the branch ref (holder, expiry, epoch) and are taken, renewed, released, and reclaimed by CAS on that ref — the same primitive every other mutation already uses. Acquiring or reclaiming a branch bumps its epoch, and every object write goes under the current epoch, so a stale writer that wakes up after losing its lease writes into an orphaned prefix that nothing references. Reaching a snapshot therefore needs its epoch, so the ref schema gains per-checkpoint epochs with a read-time upgrade of existing v1 refs.

**Tech Stack:** Go 1.24+, existing `internal/store` and `internal/ops`, no new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md` § Storage layout (fencing), § Core model. **Plan sequence:** Plans 1-3 merged (capture spike GO; local lifecycle; S3 backends) → **this plan** → Plan 5 (daemon process: `offshoot serve`, unix-socket lifecycle API, live WAL capture via `internal/capture`, TTL reaping) → Plan 6 (incremental LTX segments, MCP server, SDKs).

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only
- `Backend` contract is fixed and MUST NOT change: `Get`/`Put`/`PutIf(key,data,ifMatch)`/`List`/`Delete`, sentinels `ErrNotFound`, `ErrCAS`
- **One writer per lineage, for its entire life** — every repoint (fork/rollback/promote) mints a fresh lineage; this plan does not change that
- **Epoch invariant:** acquiring or reclaiming a branch bumps `Ref.Epoch`; every object write lands under the current epoch; a write under a superseded epoch must be unreachable, never corrupting
- Lease expiry is wall-clock and advisory against a *cooperative* holder; correctness against an *uncooperative* one comes from the epoch fence plus ref CAS, never from the clock alone
- All ref mutations are CAS'd (`PutIf` with the read etag); a lost CAS is a loud, retryable error
- Existing v1 refs must keep working — read-time upgrade, no migration command, no data rewrite
- Every Plan-2/3 test must keep passing unmodified; CLI at-rest behavior is unchanged when no lease is held
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/store/store.go        (modify) Ref schema v2: Checkpoint{TXID,Epoch}, lease fields, read-time upgrade
internal/store/store_test.go   (modify) schema-upgrade and codec tests
internal/store/lease.go        Lease acquire/renew/release/reclaim over ref CAS
internal/store/lease_test.go
internal/ops/ops.go            (modify) resolve a checkpoint's epoch when materializing; write under ref.Epoch
internal/ops/lease.go          Workspace-level lease helpers used by ops and (later) the daemon
internal/ops/lease_test.go
internal/ops/fencing_test.go   Adversarial: stale writer under a dead epoch cannot corrupt
cmd/offshoot/main.go           (modify) `lease` subcommand (status/acquire/release) + usage
```

---

### Task 1: Ref schema v2 — per-checkpoint epochs with read-time upgrade

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go` (append)

**Interfaces:**
- Consumes: `Backend`, `ErrNotFound`, `ErrCAS` (Plan 2)
- Produces:

```go
package store

const RefSchema = 2

// Checkpoint locates a snapshot object: its transaction id and the epoch of
// the prefix it was written under.
type Checkpoint struct {
	TXID  uint64 `json:"txid"`
	Epoch uint64 `json:"epoch"`
}

type Ref struct {
	Schema      int                   `json:"schema"`
	Lineage     string                `json:"lineage"`
	Epoch       uint64                `json:"epoch"`
	HeadTXID    uint64                `json:"head_txid"`
	HeadEpoch   uint64                `json:"head_epoch"`
	Checkpoints map[string]Checkpoint `json:"checkpoints"`
	Parent      string                `json:"parent,omitempty"`
	Protected   bool                  `json:"protected"`
	// Lease fields are empty when no writer holds the branch.
	LeaseHolder string `json:"lease_holder,omitempty"`
	LeaseExpiry string `json:"lease_expiry,omitempty"` // RFC3339Nano UTC
}

// SetCheckpoint records name at (txid, epoch), allocating the map if needed.
func (r *Ref) SetCheckpoint(name string, txid, epoch uint64)
```

Read-time upgrade: `GetRef` unmarshals into a tolerant shape. A v1 ref has `"schema":1` and `checkpoints` as `name → number`; upgrade it in memory by setting every checkpoint's `Epoch` to the ref's `Epoch`, setting `HeadEpoch` to `Epoch`, and `Schema` to 2. Nothing is rewritten to the store until the next normal `PutRef`. A ref whose `Schema` is greater than `RefSchema` is an error (a newer binary wrote it).

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/store_test.go`:

```go
func TestRefV2RoundTrip(t *testing.T) {
	s := newStore(t)
	r := Ref{Schema: RefSchema, Lineage: NewLineageID(), Epoch: 3, HeadTXID: 9, HeadEpoch: 3}
	r.SetCheckpoint("init", 1, 1)
	r.SetCheckpoint("v1", 9, 3)
	etag, err := s.PutRef("app", "main", r, "")
	if err != nil {
		t.Fatal(err)
	}
	got, gotEtag, err := s.GetRef("app", "main")
	if err != nil || gotEtag != etag {
		t.Fatalf("get: %v", err)
	}
	if got.Checkpoints["init"] != (Checkpoint{TXID: 1, Epoch: 1}) {
		t.Errorf("init checkpoint = %+v", got.Checkpoints["init"])
	}
	if got.Checkpoints["v1"] != (Checkpoint{TXID: 9, Epoch: 3}) {
		t.Errorf("v1 checkpoint = %+v", got.Checkpoints["v1"])
	}
	if got.HeadEpoch != 3 {
		t.Errorf("head epoch = %d", got.HeadEpoch)
	}
}

func TestGetRefUpgradesV1(t *testing.T) {
	s := newStore(t)
	// Hand-write a v1 ref exactly as Plan 2 wrote them.
	v1 := `{"schema":1,"lineage":"abc","epoch":2,"head_txid":7,` +
		`"checkpoints":{"init":1,"v1":7},"protected":true}`
	if _, err := s.B.PutIf(RefKey("app", "main"), []byte(v1), ""); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema != RefSchema {
		t.Errorf("schema = %d, want %d", got.Schema, RefSchema)
	}
	// Every v1 checkpoint lived under the ref's epoch.
	if got.Checkpoints["init"] != (Checkpoint{TXID: 1, Epoch: 2}) {
		t.Errorf("init = %+v, want txid 1 epoch 2", got.Checkpoints["init"])
	}
	if got.Checkpoints["v1"] != (Checkpoint{TXID: 7, Epoch: 2}) {
		t.Errorf("v1 = %+v, want txid 7 epoch 2", got.Checkpoints["v1"])
	}
	if got.HeadEpoch != 2 {
		t.Errorf("head epoch = %d, want 2", got.HeadEpoch)
	}
	if !got.Protected || got.Lineage != "abc" || got.HeadTXID != 7 {
		t.Errorf("other fields lost: %+v", got)
	}
}

func TestGetRefRejectsNewerSchema(t *testing.T) {
	s := newStore(t)
	future := `{"schema":99,"lineage":"abc","epoch":1,"head_txid":1}`
	if _, err := s.B.PutIf(RefKey("app", "main"), []byte(future), ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetRef("app", "main"); err == nil {
		t.Fatal("a ref from a newer binary must be refused, not guessed at")
	}
}

func TestSetCheckpointAllocates(t *testing.T) {
	var r Ref
	r.SetCheckpoint("a", 4, 2)
	if r.Checkpoints["a"] != (Checkpoint{TXID: 4, Epoch: 2}) {
		t.Fatalf("checkpoints = %+v", r.Checkpoints)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store -run 'TestRefV2|TestGetRefUpgrades|TestGetRefRejects|TestSetCheckpoint' -v`
Expected: FAIL — `RefSchema`/`Checkpoint`/`SetCheckpoint` undefined, and the existing `Ref.Checkpoints` is `map[string]uint64`.

- [ ] **Step 3: Implement the schema and upgrade**

In `internal/store/store.go`, replace the `Ref` type and add the codec. Keep `LayoutVersion`, `manifestKey`, and every existing function name unchanged:

```go
const (
	LayoutVersion = 1
	RefSchema     = 2
	manifestKey   = "offshoot.json"
	maxNameLen    = 128
)

// Checkpoint locates a snapshot object: its transaction id and the epoch of
// the prefix it was written under. Epoch matters because acquiring or
// reclaiming a branch bumps the epoch, and objects stay where they were
// written.
type Checkpoint struct {
	TXID  uint64 `json:"txid"`
	Epoch uint64 `json:"epoch"`
}

type Ref struct {
	Schema      int                   `json:"schema"`
	Lineage     string                `json:"lineage"`
	Epoch       uint64                `json:"epoch"`
	HeadTXID    uint64                `json:"head_txid"`
	HeadEpoch   uint64                `json:"head_epoch"`
	Checkpoints map[string]Checkpoint `json:"checkpoints"`
	Parent      string                `json:"parent,omitempty"`
	Protected   bool                  `json:"protected"`
	LeaseHolder string                `json:"lease_holder,omitempty"`
	LeaseExpiry string                `json:"lease_expiry,omitempty"`
}

// SetCheckpoint records name at (txid, epoch).
func (r *Ref) SetCheckpoint(name string, txid, epoch uint64) {
	if r.Checkpoints == nil {
		r.Checkpoints = map[string]Checkpoint{}
	}
	r.Checkpoints[name] = Checkpoint{TXID: txid, Epoch: epoch}
}

// refWire is the tolerant on-disk shape: checkpoints are either v1 numbers or
// v2 objects, so one decode handles both schemas.
type refWire struct {
	Schema      int                        `json:"schema"`
	Lineage     string                     `json:"lineage"`
	Epoch       uint64                     `json:"epoch"`
	HeadTXID    uint64                     `json:"head_txid"`
	HeadEpoch   uint64                     `json:"head_epoch"`
	Checkpoints map[string]json.RawMessage `json:"checkpoints"`
	Parent      string                     `json:"parent,omitempty"`
	Protected   bool                       `json:"protected"`
	LeaseHolder string                     `json:"lease_holder,omitempty"`
	LeaseExpiry string                     `json:"lease_expiry,omitempty"`
}

func decodeRef(data []byte) (Ref, error) {
	var w refWire
	if err := json.Unmarshal(data, &w); err != nil {
		return Ref{}, err
	}
	if w.Schema > RefSchema {
		return Ref{}, fmt.Errorf(
			"store: ref schema %d is newer than this binary supports (%d)", w.Schema, RefSchema)
	}
	r := Ref{
		Schema: RefSchema, Lineage: w.Lineage, Epoch: w.Epoch,
		HeadTXID: w.HeadTXID, HeadEpoch: w.HeadEpoch,
		Parent: w.Parent, Protected: w.Protected,
		LeaseHolder: w.LeaseHolder, LeaseExpiry: w.LeaseExpiry,
	}
	// A v1 ref predates per-checkpoint epochs: everything it references was
	// written under the ref's own epoch.
	if r.HeadEpoch == 0 {
		r.HeadEpoch = w.Epoch
	}
	for name, raw := range w.Checkpoints {
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err == nil && cp.TXID != 0 {
			r.SetCheckpoint(name, cp.TXID, cp.Epoch)
			continue
		}
		var txid uint64
		if err := json.Unmarshal(raw, &txid); err != nil {
			return Ref{}, fmt.Errorf("store: bad checkpoint %q: %w", name, err)
		}
		r.SetCheckpoint(name, txid, w.Epoch)
	}
	return r, nil
}
```

Then change `GetRef` to use it (leave the rest of the function as-is):

```go
func (s *Store) GetRef(db, branch string) (Ref, string, error) {
	data, etag, err := s.B.Get(RefKey(db, branch))
	if err != nil {
		return Ref{}, "", err
	}
	r, err := decodeRef(data)
	if err != nil {
		return Ref{}, "", fmt.Errorf("store: ref %s@%s: %w", db, branch, err)
	}
	return r, etag, nil
}
```

`PutRef` needs one added line before marshalling so every write is stamped:

```go
	r.Schema = RefSchema
	if r.HeadEpoch == 0 {
		r.HeadEpoch = r.Epoch
	}
```

- [ ] **Step 4: Fix the ripple in `internal/ops`**

`ops` currently does `ref.Checkpoints[name]` expecting a `uint64` and `ref.Checkpoints[name] = txid`. Update every site mechanically:
- reads become `cp := ref.Checkpoints[name]` then use `cp.TXID` (and `cp.Epoch` where an object is fetched — see Task 2 for materialization; for now pass `ref.Epoch` exactly where the old code did, so behavior is unchanged)
- writes become `ref.SetCheckpoint(name, txid, ref.Epoch)`
- `Rollback`'s `kept` filter compares `cp.TXID <= txid` and copies whole `Checkpoint` values
- `Status`'s checkpoint-name listing is unchanged (it only ranges over keys)

Do not change any behavior in this step — it is a type migration only.

- [ ] **Step 5: Run everything**

Run: `go test ./... -count=1 && go vet ./...`
Expected: PASS. Every Plan-2/3 test must pass unmodified; if one needed an assertion change, that is a behavior change and belongs in a later task — revert and re-check.

- [ ] **Step 6: Commit**

```bash
git add internal/store internal/ops
git commit -m "feat: ref schema v2 with per-checkpoint epochs and read-time v1 upgrade"
```

---

### Task 2: Materialize by the checkpoint's own epoch

**Files:**
- Modify: `internal/ops/ops.go`
- Test: `internal/ops/ops_test.go` (append)

**Interfaces:**
- Consumes: `store.Checkpoint`, `store.Ref.HeadEpoch` (Task 1)
- Produces: `func (w *Workspace) materializeAt(ref store.Ref, cp store.Checkpoint, dst string) error` — fetches `SnapshotKey(ref.Lineage, cp.Epoch, cp.TXID)`; `Checkout` uses `Checkpoint{TXID: ref.HeadTXID, Epoch: ref.HeadEpoch}`

Today `materialize` always uses `ref.Epoch`, which is correct only while epochs never change. Task 3 starts bumping them, so every read path must use the epoch recorded with the object.

- [ ] **Step 1: Write the failing test**

Append to `internal/ops/ops_test.go`:

```go
func TestMaterializeUsesCheckpointEpochNotRefEpoch(t *testing.T) {
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
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatal(err)
	}

	// Simulate a later epoch bump: the ref advances, but the objects written
	// under the old epoch stay exactly where they are.
	ref, etag, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	ref.Epoch++
	if _, err := w.Store.PutRef("app", "main", ref, etag); err != nil {
		t.Fatal(err)
	}

	// Checkout and fork --at must both still find the old-epoch objects.
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("checkout after epoch bump: %v", err)
	}
	got, _ := exec.Command("sqlite3", p2, "SELECT v FROM t;").Output()
	if string(got) != "1\n" {
		t.Fatalf("content after epoch bump: %q", got)
	}
	if _, err := w.Fork("app", "main", "child", "v1"); err != nil {
		t.Fatalf("fork --at across an epoch bump: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/ops -run TestMaterializeUsesCheckpointEpoch -v`
Expected: FAIL — the checkout after the bump looks for the snapshot under the new epoch and reports "not found".

- [ ] **Step 3: Implement**

In `internal/ops/ops.go`, replace `materialize` with an epoch-aware version and update its callers:

```go
// materializeAt writes the snapshot identified by cp into dst. The epoch
// comes from the checkpoint, not from the ref: acquiring or reclaiming a
// branch bumps the ref's epoch, but objects stay in the prefix they were
// written under.
func (w *Workspace) materializeAt(ref store.Ref, cp store.Checkpoint, dst string) error {
	data, _, err := w.Store.B.Get(store.SnapshotKey(ref.Lineage, cp.Epoch, cp.TXID))
	if err != nil {
		return fmt.Errorf("ops: snapshot txid %d (epoch %d) in lineage %s: %w",
			cp.TXID, cp.Epoch, ref.Lineage, err)
	}
	if _, err := ltxio.Materialize(bytes.NewReader(data), dst); err != nil {
		return err
	}
	return nil
}

// headCheckpoint is the ref's current head as a Checkpoint.
func headCheckpoint(ref store.Ref) store.Checkpoint {
	return store.Checkpoint{TXID: ref.HeadTXID, Epoch: ref.HeadEpoch}
}
```

Update every call site: `Checkout` uses `w.materializeAt(ref, headCheckpoint(ref), path)`; `Rollback` and `Promote` materialize `headCheckpoint(next)` after repointing; `copySnapshotToNewLineage` reads `SnapshotKey(src.Lineage, cp.Epoch, cp.TXID)` for the checkpoint it is copying and writes the child at epoch 1 (children start fresh, so `SetCheckpoint(name, txid, 1)` and `HeadEpoch: 1`); `Fork --at` passes the named checkpoint rather than head.

Checkpoint-writing paths write under `ref.Epoch` and record that epoch: in `Checkpoint`, the object key is `store.SnapshotKey(ref.Lineage, ref.Epoch, txid)` and the ref update is `ref.SetCheckpoint(name, txid, ref.Epoch)` plus `ref.HeadTXID, ref.HeadEpoch = txid, ref.Epoch`.

- [ ] **Step 4: Run everything**

Run: `go test ./... -count=1 -race && go vet ./...`
Expected: PASS, including the new test and every Plan-2/3 test unmodified.

- [ ] **Step 5: Commit**

```bash
git add internal/ops
git commit -m "feat: materialize snapshots by their recorded epoch"
```

---

### Task 3: Lease acquire, renew, release, reclaim

**Files:**
- Create: `internal/store/lease.go`, `internal/store/lease_test.go`

**Interfaces:**
- Consumes: `Store`, `Ref`, `ErrCAS` (Tasks 1-2)
- Produces:

```go
package store

// ErrLeaseHeld reports that another holder owns an unexpired lease.
var ErrLeaseHeld = errors.New("store: branch lease is held")

// ErrLeaseLost reports that the caller no longer holds the lease it claimed —
// someone reclaimed it, so the caller's epoch is dead and it must stop.
var ErrLeaseLost = errors.New("store: branch lease lost")

// Lease is a claim on a branch, valid until Expiry unless renewed.
type Lease struct {
	DB, Branch string
	Holder     string
	Epoch      uint64
	Expiry     time.Time
}

// AcquireLease claims db@branch for holder until now+ttl. It succeeds when
// the branch is unleased or the existing lease has expired; either way the
// ref's epoch is bumped, fencing any previous holder's writes into a dead
// prefix. Returns ErrLeaseHeld when a live lease belongs to someone else.
func (s *Store) AcquireLease(db, branch, holder string, ttl time.Duration, now time.Time) (Lease, error)

// RenewLease extends the caller's own lease. Returns ErrLeaseLost if the
// holder or epoch no longer matches — the caller has been fenced.
func (s *Store) RenewLease(l Lease, ttl time.Duration, now time.Time) (Lease, error)

// ReleaseLease clears the caller's lease. It does NOT bump the epoch: a clean
// release leaves the writer's own objects reachable. Returns ErrLeaseLost if
// the caller was already fenced.
func (s *Store) ReleaseLease(l Lease) error
```

Semantics: `now` is a parameter, never `time.Now()` inside, so tests are deterministic. Expiry is stored RFC3339Nano UTC. A lease with an empty holder or a past expiry is available. Reclaiming (taking an expired lease from someone else) and first acquisition both bump `Epoch` by 1 and set `HeadEpoch` unchanged — head still points at objects in their original epoch, which Task 2 made safe.

- [ ] **Step 1: Write the failing tests**

`internal/store/lease_test.go`:

```go
package store

import (
	"errors"
	"testing"
	"time"
)

func seedBranch(t *testing.T, s *Store) {
	t.Helper()
	r := Ref{Lineage: NewLineageID(), Epoch: 1, HeadTXID: 1, HeadEpoch: 1}
	r.SetCheckpoint("init", 1, 1)
	if _, err := s.PutRef("app", "main", r, ""); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireLeaseBumpsEpoch(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if l.Epoch != 2 {
		t.Errorf("epoch = %d, want 2 (bumped on acquisition)", l.Epoch)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.Epoch != 2 || ref.LeaseHolder != "daemon-a" {
		t.Fatalf("ref = %+v", ref)
	}
	// Head still points at the object written under epoch 1.
	if ref.HeadEpoch != 1 {
		t.Errorf("head epoch = %d, want 1 (objects don't move)", ref.HeadEpoch)
	}
}

func TestAcquireRefusesLiveLease(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	if _, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now); err != nil {
		t.Fatal(err)
	}
	_, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(30*time.Second))
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != "daemon-a" || ref.Epoch != 2 {
		t.Fatalf("a refused acquisition must not disturb the ref: %+v", ref)
	}
}

func TestReclaimExpiredLeaseBumpsEpochAgain(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("expired lease must be reclaimable: %v", err)
	}
	if b.Epoch != a.Epoch+1 {
		t.Errorf("reclaim epoch = %d, want %d", b.Epoch, a.Epoch+1)
	}
	// The fenced holder can no longer renew.
	if _, err := s.RenewLease(a, time.Minute, now.Add(2*time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fenced holder renew: want ErrLeaseLost, got %v", err)
	}
}

func TestRenewExtendsOwnLease(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.RenewLease(l, time.Minute, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !l2.Expiry.After(l.Expiry) {
		t.Errorf("renew must extend: %v then %v", l.Expiry, l2.Expiry)
	}
	if l2.Epoch != l.Epoch {
		t.Errorf("renew must NOT bump the epoch: %d then %d", l.Epoch, l2.Epoch)
	}
}

func TestReleaseFreesBranchWithoutBumping(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseLease(l); err != nil {
		t.Fatal(err)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != "" || ref.LeaseExpiry != "" {
		t.Fatalf("release must clear the lease: %+v", ref)
	}
	if ref.Epoch != l.Epoch {
		t.Errorf("clean release must not bump the epoch: %d vs %d", ref.Epoch, l.Epoch)
	}
	// A fresh acquisition after release still bumps.
	l2, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(time.Second))
	if err != nil || l2.Epoch != l.Epoch+1 {
		t.Fatalf("post-release acquire: epoch %d err %v", l2.Epoch, err)
	}
}

func TestReleaseByFencedHolderIsRefused(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, _ := s.AcquireLease("app", "main", "daemon-a", time.Minute, now)
	if _, err := s.AcquireLease("app", "main", "daemon-b", time.Minute, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseLease(a); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("a fenced holder must not clear the new holder's lease, got %v", err)
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != "daemon-b" {
		t.Fatalf("holder = %q, want daemon-b", ref.LeaseHolder)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/store -run 'Lease' -v`
Expected: FAIL — `AcquireLease` undefined.

- [ ] **Step 3: Implement**

`internal/store/lease.go`:

```go
package store

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrLeaseHeld reports that another holder owns an unexpired lease.
	ErrLeaseHeld = errors.New("store: branch lease is held")
	// ErrLeaseLost reports that the caller no longer holds the lease it
	// claimed: someone reclaimed the branch, the caller's epoch is dead, and
	// anything it writes now lands in an unreferenced prefix.
	ErrLeaseLost = errors.New("store: branch lease lost")
)

// Lease is a claim on a branch, valid until Expiry unless renewed.
type Lease struct {
	DB, Branch string
	Holder     string
	Epoch      uint64
	Expiry     time.Time
}

func parseExpiry(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// AcquireLease claims db@branch for holder until now+ttl, bumping the epoch
// so any previous holder's subsequent writes are fenced into a dead prefix.
func (s *Store) AcquireLease(db, branch, holder string, ttl time.Duration, now time.Time) (Lease, error) {
	if holder == "" {
		return Lease{}, errors.New("store: lease holder must be named")
	}
	ref, etag, err := s.GetRef(db, branch)
	if err != nil {
		return Lease{}, err
	}
	if exp, ok := parseExpiry(ref.LeaseExpiry); ok && ref.LeaseHolder != "" &&
		ref.LeaseHolder != holder && now.Before(exp) {
		return Lease{}, fmt.Errorf("%w by %q until %s",
			ErrLeaseHeld, ref.LeaseHolder, ref.LeaseExpiry)
	}
	expiry := now.Add(ttl).UTC()
	ref.Epoch++
	ref.LeaseHolder = holder
	ref.LeaseExpiry = expiry.Format(time.RFC3339Nano)
	if _, err := s.PutRef(db, branch, ref, etag); err != nil {
		return Lease{}, fmt.Errorf("store: acquire lease on %s@%s: %w", db, branch, err)
	}
	return Lease{DB: db, Branch: branch, Holder: holder, Epoch: ref.Epoch, Expiry: expiry}, nil
}

// RenewLease extends the caller's own lease without touching the epoch.
func (s *Store) RenewLease(l Lease, ttl time.Duration, now time.Time) (Lease, error) {
	ref, etag, err := s.GetRef(l.DB, l.Branch)
	if err != nil {
		return Lease{}, err
	}
	if ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch {
		return Lease{}, fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
			ErrLeaseLost, l.DB, l.Branch, ref.LeaseHolder, ref.Epoch)
	}
	expiry := now.Add(ttl).UTC()
	ref.LeaseExpiry = expiry.Format(time.RFC3339Nano)
	if _, err := s.PutRef(l.DB, l.Branch, ref, etag); err != nil {
		return Lease{}, fmt.Errorf("store: renew lease on %s@%s: %w", l.DB, l.Branch, err)
	}
	l.Expiry = expiry
	return l, nil
}

// ReleaseLease clears the caller's lease. The epoch is left alone: a clean
// release means the holder's own objects stay reachable.
func (s *Store) ReleaseLease(l Lease) error {
	ref, etag, err := s.GetRef(l.DB, l.Branch)
	if err != nil {
		return err
	}
	if ref.LeaseHolder != l.Holder || ref.Epoch != l.Epoch {
		return fmt.Errorf("%w: %s@%s now held by %q at epoch %d",
			ErrLeaseLost, l.DB, l.Branch, ref.LeaseHolder, ref.Epoch)
	}
	ref.LeaseHolder = ""
	ref.LeaseExpiry = ""
	if _, err := s.PutRef(l.DB, l.Branch, ref, etag); err != nil {
		return fmt.Errorf("store: release lease on %s@%s: %w", l.DB, l.Branch, err)
	}
	return nil
}
```

- [ ] **Step 4: Run**

Run: `go test ./internal/store -v -race && go test ./... -count=1`
Expected: PASS (six lease tests plus everything prior).

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat: branch leases with epoch bump on acquisition and reclaim"
```

---

### Task 4: Fencing — a stale writer cannot corrupt

**Files:**
- Create: `internal/ops/fencing_test.go`
- Modify: `internal/ops/ops.go` only if the tests expose a real gap

**Interfaces:**
- Consumes: `store.AcquireLease/RenewLease/ReleaseLease`, `Workspace.Checkpoint`, `store.SnapshotKey`

This task adds no feature; it proves the invariant the whole scheme rests on. If a test fails, the fix belongs in `ops`/`store` — never in the test's expectations.

- [ ] **Step 1: Write the adversarial tests**

`internal/ops/fencing_test.go`:

```go
package ops

import (
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// TestStaleWriterCannotCorruptLineage simulates the pause-and-resume hazard:
// holder A is fenced by holder B, then wakes up and writes. A's object must
// land under a dead epoch and must not be reachable from any ref.
func TestStaleWriterCannotCorruptLineage(t *testing.T) {
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
		"CREATE TABLE t (v); INSERT INTO t VALUES ('good');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if _, err := w.Checkpoint("app", "main", "good"); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, err := w.Store.AcquireLease("app", "main", "writer-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	// A is fenced by B reclaiming after expiry.
	b, err := w.Store.AcquireLease("app", "main", "writer-b", time.Minute, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if b.Epoch <= a.Epoch {
		t.Fatalf("reclaim must bump the epoch: %d -> %d", a.Epoch, b.Epoch)
	}

	// A wakes up and writes an object under ITS epoch, as a resumed writer
	// mid-flush would.
	staleKey := store.SnapshotKey(mustRef(t, w).Lineage, a.Epoch, 99)
	if err := w.Store.B.Put(staleKey, []byte("garbage-from-a-fenced-writer")); err != nil {
		t.Fatal(err)
	}

	// The branch is untouched: content still reads, and nothing references
	// the stale object.
	p2, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatalf("fenced writer damaged the branch: %v", err)
	}
	got, _ := exec.Command("sqlite3", p2, "SELECT v FROM t;").Output()
	if string(got) != "good\n" {
		t.Fatalf("content = %q, want good", got)
	}
	ref := mustRef(t, w)
	for name, cp := range ref.Checkpoints {
		if cp.Epoch == a.Epoch {
			t.Fatalf("checkpoint %q references the fenced epoch %d", name, a.Epoch)
		}
	}
	if ref.HeadEpoch == a.Epoch && ref.HeadTXID == 99 {
		t.Fatal("head references the fenced writer's object")
	}

	// A's attempt to advance the ref is refused outright.
	if _, err := w.Store.RenewLease(a, time.Minute, now.Add(3*time.Minute)); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("fenced renew: want ErrLeaseLost, got %v", err)
	}
}

// TestFencedEpochObjectsAreCollectable proves the fenced writer's garbage is
// not immortal: it lives in the branch's lineage, so destroying the branch
// and running GC removes it along with everything else in that lineage.
func TestFencedEpochObjectsAreCollectable(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	ref := mustRef(t, w)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, err := w.Store.AcquireLease("app", "main", "writer-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Store.AcquireLease("app", "main", "writer-b", time.Minute, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	staleKey := store.SnapshotKey(ref.Lineage, a.Epoch, 42)
	if err := w.Store.B.Put(staleKey, []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	if err := w.Destroy("app", "main", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.GC(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Store.B.Get(staleKey); err == nil {
		t.Fatal("fenced-epoch object survived GC of its lineage")
	}
}

func mustRef(t *testing.T, w *Workspace) store.Ref {
	t.Helper()
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
```

- [ ] **Step 2: Run and investigate**

Run: `go test ./internal/ops -run 'Fenc|Stale' -v -race -count=2`
Expected: PASS if Tasks 1-3 are right. A failure is a real finding — most likely `GC`'s lineage sweep listing only the current epoch's prefix, or `Checkout` picking the ref epoch instead of the head epoch. Fix the code, not the test, and document what you found.

- [ ] **Step 3: Full suite**

Run: `go test ./... -count=1 -race && go vet ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/ops
git commit -m "test: prove fenced writers cannot corrupt or strand a lineage"
```

---

### Task 5: Workspace lease helpers and the `lease` subcommand

**Files:**
- Create: `internal/ops/lease.go`, `internal/ops/lease_test.go`
- Modify: `cmd/offshoot/main.go`, `README.md`

**Interfaces:**
- Consumes: `store.AcquireLease/RenewLease/ReleaseLease`, `store.Lease`, `store.ErrLeaseHeld/ErrLeaseLost`
- Produces:

```go
package ops

// DefaultLeaseTTL is how long an acquired lease stays valid without renewal.
const DefaultLeaseTTL = 30 * time.Second

// AcquireLease claims db@branch for this process. holder identifies the
// claimant in diagnostics and in the ref; pass ops.LocalHolder() for the
// conventional "<hostname>/<pid>" form.
func (w *Workspace) AcquireLease(db, branch, holder string, ttl time.Duration) (store.Lease, error)
func (w *Workspace) RenewLease(l store.Lease, ttl time.Duration) (store.Lease, error)
func (w *Workspace) ReleaseLease(l store.Lease) error

// LeaseInfo describes a branch's current lease for display.
type LeaseInfo struct {
	DB, Branch, Holder string
	Epoch              uint64
	Expiry             time.Time
	Expired            bool
}

// Leases lists every branch that carries a lease record, sorted.
func (w *Workspace) Leases() ([]LeaseInfo, error)

// LocalHolder returns "<hostname>/<pid>".
func LocalHolder() string
```

These wrap the store calls with `time.Now()` so callers do not pass clocks around; the store keeps its injectable `now` for tests.

CLI: `offshoot lease list`, `offshoot lease acquire <db>[@branch] [--ttl 30s]`, `offshoot lease release <db>[@branch]`. `acquire` prints the holder and epoch; because a CLI process exits immediately, it also prints that the lease will expire unless a long-running holder renews it — this subcommand exists for inspection and for breaking a stuck lease, not as a workflow.

- [ ] **Step 1: Write the failing tests**

`internal/ops/lease_test.go`:

```go
package ops

import (
	"errors"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

func TestWorkspaceLeaseLifecycle(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	l, err := w.AcquireLease("app", "main", "tester", DefaultLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if l.Holder != "tester" || l.Epoch < 2 {
		t.Fatalf("lease = %+v", l)
	}
	if _, err := w.AcquireLease("app", "main", "other", DefaultLeaseTTL); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	l2, err := w.RenewLease(l, DefaultLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := w.Leases()
	if err != nil || len(infos) != 1 {
		t.Fatalf("infos=%v err=%v", infos, err)
	}
	if infos[0].Holder != "tester" || infos[0].Expired {
		t.Fatalf("info = %+v", infos[0])
	}
	if err := w.ReleaseLease(l2); err != nil {
		t.Fatal(err)
	}
	infos, _ = w.Leases()
	if len(infos) != 0 {
		t.Fatalf("released lease should not be listed: %v", infos)
	}
}

func TestLeasesReportsExpired(t *testing.T) {
	w := newWS(t)
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "tester", time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	infos, err := w.Leases()
	if err != nil || len(infos) != 1 {
		t.Fatalf("infos=%v err=%v", infos, err)
	}
	if !infos[0].Expired {
		t.Fatalf("a lease past its expiry must report Expired: %+v", infos[0])
	}
	// An expired lease is reclaimable by anyone.
	if _, err := w.AcquireLease("app", "main", "other", DefaultLeaseTTL); err != nil {
		t.Fatalf("expired lease must be reclaimable: %v", err)
	}
}

func TestLocalHolderIsStable(t *testing.T) {
	if a, b := LocalHolder(), LocalHolder(); a != b || a == "" {
		t.Fatalf("LocalHolder unstable or empty: %q %q", a, b)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/ops -run 'Lease|LocalHolder' -v`
Expected: FAIL — `AcquireLease` undefined on `*Workspace`.

- [ ] **Step 3: Implement**

`internal/ops/lease.go`:

```go
package ops

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/offshoot-db/offshoot/internal/store"
)

// DefaultLeaseTTL is how long an acquired lease stays valid without renewal.
const DefaultLeaseTTL = 30 * time.Second

// LocalHolder returns "<hostname>/<pid>", the conventional holder identity.
func LocalHolder() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

func (w *Workspace) AcquireLease(db, branch, holder string, ttl time.Duration) (store.Lease, error) {
	return w.Store.AcquireLease(db, branch, holder, ttl, time.Now())
}

func (w *Workspace) RenewLease(l store.Lease, ttl time.Duration) (store.Lease, error) {
	return w.Store.RenewLease(l, ttl, time.Now())
}

func (w *Workspace) ReleaseLease(l store.Lease) error { return w.Store.ReleaseLease(l) }

// LeaseInfo describes a branch's current lease for display.
type LeaseInfo struct {
	DB, Branch, Holder string
	Epoch              uint64
	Expiry             time.Time
	Expired            bool
}

// Leases lists every branch carrying a lease record, sorted by db then branch.
func (w *Workspace) Leases() ([]LeaseInfo, error) {
	refs, err := w.Store.ListRefs()
	if err != nil {
		return nil, err
	}
	var dbs []string
	for db := range refs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	now := time.Now()
	var out []LeaseInfo
	for _, db := range dbs {
		for _, br := range refs[db] {
			ref, _, err := w.Store.GetRef(db, br)
			if err != nil {
				return nil, err
			}
			if ref.LeaseHolder == "" {
				continue
			}
			exp, err := time.Parse(time.RFC3339Nano, ref.LeaseExpiry)
			if err != nil {
				return nil, fmt.Errorf("ops: bad lease expiry on %s@%s: %w", db, br, err)
			}
			out = append(out, LeaseInfo{
				DB: db, Branch: br, Holder: ref.LeaseHolder, Epoch: ref.Epoch,
				Expiry: exp, Expired: !now.Before(exp),
			})
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Add the CLI subcommand**

In `cmd/offshoot/main.go`, add a `lease` case to the switch:

```go
	case "lease":
		if len(rest) == 0 {
			return fmt.Errorf("usage: offshoot lease list|acquire|release ...")
		}
		switch rest[0] {
		case "list":
			infos, err := w.Leases()
			if err != nil {
				return err
			}
			for _, in := range infos {
				state := "held"
				if in.Expired {
					state = "expired"
				}
				fmt.Printf("%s@%s %s by %s epoch=%d until %s\n",
					in.DB, in.Branch, state, in.Holder, in.Epoch,
					in.Expiry.Format(time.RFC3339))
			}
			return nil
		case "acquire":
			args := rest[1:]
			ttl := ops.DefaultLeaseTTL
			if len(args) == 3 && args[1] == "--ttl" {
				d, err := time.ParseDuration(args[2])
				if err != nil {
					return err
				}
				ttl = d
				args = args[:1]
			}
			if len(args) != 1 {
				return fmt.Errorf("usage: offshoot lease acquire <db>[@branch] [--ttl 30s]")
			}
			db, branch, err := ops.ParseTarget(args[0])
			if err != nil {
				return err
			}
			l, err := w.AcquireLease(db, branch, ops.LocalHolder(), ttl)
			if err != nil {
				return err
			}
			fmt.Printf("acquired %s@%s as %s (epoch %d) until %s\n",
				db, branch, l.Holder, l.Epoch, l.Expiry.Format(time.RFC3339))
			fmt.Println("note: this command exits immediately; the lease expires unless a" +
				" long-running holder renews it")
			return nil
		case "release":
			if len(rest) != 2 {
				return fmt.Errorf("usage: offshoot lease release <db>[@branch]")
			}
			db, branch, err := ops.ParseTarget(rest[1])
			if err != nil {
				return err
			}
			infos, err := w.Leases()
			if err != nil {
				return err
			}
			for _, in := range infos {
				if in.DB == db && in.Branch == branch {
					return w.ReleaseLease(store.Lease{
						DB: db, Branch: branch, Holder: in.Holder, Epoch: in.Epoch,
					})
				}
			}
			return fmt.Errorf("offshoot: no lease on %s@%s", db, branch)
		default:
			return fmt.Errorf("unknown lease subcommand %q", rest[0])
		}
```

(add `"time"` and the `internal/store` import if missing; add usage lines for the three forms)

- [ ] **Step 5: Document**

Add to `README.md` after the Storage section:

```markdown
## Leases and fencing

A long-running writer (Plan 5's daemon) claims a branch with a lease:

    offshoot lease list
    offshoot lease acquire app@main --ttl 60s
    offshoot lease release app@main

Acquiring or reclaiming a branch **bumps its epoch**, and every object is
written under the epoch current at the time. A writer that pauses, loses its
lease, and later resumes therefore writes into a superseded prefix that no ref
points at — it cannot corrupt the branch, and its garbage is collected with the
lineage. Expiry is wall-clock and advisory; the guarantee against an
uncooperative writer comes from the epoch fence and ref compare-and-swap, not
from the clock.

`offshoot lease acquire` exits immediately, so its lease expires unless a
long-running process renews it. It exists for inspection and for breaking a
stuck lease.
```

- [ ] **Step 6: Run everything**

Run: `go test ./... -count=1 -race && go vet ./... && go build ./cmd/offshoot`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/ops cmd/offshoot README.md
git commit -m "feat: workspace lease helpers and the lease subcommand"
```

---

### Task 6: Adversarial pass — concurrency and clock hazards

**Files:**
- Modify: `internal/store/lease_test.go` (append), `internal/ops/lease_test.go` (append)
- Modify: source only if a test exposes a real bug

**Interfaces:** none new. The tests are the spec; keep the invariants: exactly one acquirer wins a contested branch; a fenced holder can never mutate; epochs are monotonic.

- [ ] **Step 1: Write the adversarial tests**

Append to `internal/store/lease_test.go`:

```go
func TestConcurrentAcquireHasOneWinner(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	const n = 12
	var wg sync.WaitGroup
	wins := make(chan Lease, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			l, err := s.AcquireLease("app", "main", fmt.Sprintf("holder-%d", idx), time.Minute, now)
			if err == nil {
				wins <- l
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	var won []Lease
	for l := range wins {
		won = append(won, l)
	}
	if len(won) != 1 {
		t.Fatalf("exactly one acquirer must win an unleased branch, got %d", len(won))
	}
	ref, _, _ := s.GetRef("app", "main")
	if ref.LeaseHolder != won[0].Holder || ref.Epoch != won[0].Epoch {
		t.Fatalf("ref %+v disagrees with the winning lease %+v", ref, won[0])
	}
}

func TestEpochNeverDecreasesAcrossReclaims(t *testing.T) {
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	last := uint64(0)
	for i := 0; i < 5; i++ {
		l, err := s.AcquireLease("app", "main", fmt.Sprintf("h%d", i), time.Second, now)
		if err != nil {
			t.Fatal(err)
		}
		if l.Epoch <= last {
			t.Fatalf("epoch went backwards: %d after %d", l.Epoch, last)
		}
		last = l.Epoch
		now = now.Add(2 * time.Second) // let it expire so the next holder reclaims
	}
}

func TestRenewAfterExpiryButBeforeReclaimStillWorks(t *testing.T) {
	// A holder whose lease lapsed but whom nobody has displaced may renew:
	// expiry alone does not fence, only another acquisition does.
	s := newStore(t)
	seedBranch(t, s)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	l, err := s.AcquireLease("app", "main", "slow", time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.RenewLease(l, time.Minute, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("uncontested lapsed holder must be able to renew: %v", err)
	}
	if l2.Epoch != l.Epoch {
		t.Errorf("renew bumped the epoch: %d -> %d", l.Epoch, l2.Epoch)
	}
}
```

(add `"fmt"` and `"sync"` to that file's imports)

Append to `internal/ops/lease_test.go`:

```go
func TestFencedHolderCannotCheckpoint(t *testing.T) {
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
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	a, err := w.AcquireLease("app", "main", "writer-a", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease("app", "main", "writer-b", DefaultLeaseTTL); err != nil {
		t.Fatal(err)
	}
	// A is fenced. Its lease operations must fail loudly rather than silently
	// succeeding against the new epoch.
	if _, err := w.RenewLease(a, DefaultLeaseTTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("fenced renew: want ErrLeaseLost, got %v", err)
	}
	if err := w.ReleaseLease(a); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("fenced release: want ErrLeaseLost, got %v", err)
	}
	// The branch itself is still usable by whoever holds it.
	if _, err := w.Checkpoint("app", "main", "v1"); err != nil {
		t.Fatalf("branch unusable after fencing: %v", err)
	}
}
```

(add `"errors"`, `"os/exec"` to that file's imports if missing)

- [ ] **Step 2: Run, investigate, fix**

Run: `go test ./internal/store ./internal/ops -v -race -count=3 -timeout 300s`
Expected: PASS. If `TestConcurrentAcquireHasOneWinner` reports more than one winner, the acquisition path is missing its CAS etag — a real bug. If `TestRenewAfterExpiryButBeforeReclaimStillWorks` fails, decide deliberately: the documented semantics are that expiry alone does not fence, so the fix belongs in `RenewLease`, not the test.

- [ ] **Step 3: Full suite**

Run: `go test ./... -count=1 -race && go vet ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/store internal/ops
git commit -m "test: adversarial lease concurrency and clock-boundary coverage"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** implements spec § Storage layout's fencing requirement (epoch bumped on acquisition/reclaim; every write under the current epoch; superseded writes unreachable) and the lease half of § Architecture's "lease manager (ref CAS, epochs)". Deferred and stated in the header: the daemon process, unix-socket lifecycle API, live WAL capture wiring, and TTL reaping (Plan 5); incremental LTX segments and MCP/SDKs (Plan 6). Ref schema v2 is introduced here because materialization must resolve a checkpoint's epoch before epochs can move.
2. **Placeholder scan:** none. Task 4 and Task 6 prescribe what to do when a test fails (fix source, not expectations) rather than leaving the outcome open.
3. **Type consistency:** `store.Checkpoint{TXID,Epoch}` and `Ref.SetCheckpoint(name,txid,epoch)` are used identically in Tasks 1, 2, 4; `store.Lease{DB,Branch,Holder,Epoch,Expiry}` and the three store methods (with their `now time.Time` parameter) match their `Workspace` wrappers in Task 5, which supply `time.Now()`; `ErrLeaseHeld`/`ErrLeaseLost` are used consistently in Tasks 3, 5, 6; `materializeAt(ref, cp, dst)` and `headCheckpoint(ref)` from Task 2 are the only snapshot-read paths referenced later.
