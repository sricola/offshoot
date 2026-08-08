# Offshoot Copy-on-Write (Object-Sharing Forks) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fork stops being a full N×G copy in the object store; it records a `{base_lineage, base_txid}` pointer into the parent's durable chain and writes new objects only as it diverges, with reachability GC reclaiming a shared object when no live branch references it.

**Architecture:** A new `Ref.Base` pointer; `store.Chain` follows it under a strict never-merge-across-lineages concatenation rule; two automatic snapshot-floor triggers keep reads bounded; `internal/ops/gc.go`'s lineage-granular mark-sweep is rewritten to object-granular transitive-base-closure reachability; a new `compact` op cuts the cord; a LayoutVersion bump locks old binaries out.

**Tech Stack:** Go 1.24+, cgo; no new dependencies. Spec: `docs/superpowers/specs/2026-08-07-offshoot-copy-on-write-design.md` (read it — it is the contract).

## Global Constraints (the reviewer's five Criticals — binding)

- Module `github.com/sricola/offshoot`; cgo; Linux/macOS; **no new dependencies**.
- **The base pin is `{Lineage, TXID}` — NO epoch.** `base.TXID` is always a committed txid whose correct bytes are the highest-epoch object, which `store.Chain` already selects. An epoch pin re-creates the Plan-7 fenced-orphan bug; it is forbidden.
- **Resolution NEVER merges members across lineages.** Resolve parent chain ≤ `base.TXID` within the parent lineage; resolve child chain > `base.TXID` within the child lineage; concatenate. `keepHighestEpoch` (range key has no lineage) must never see a cross-lineage union — a union silently serves parent bytes for the child's timeline.
- **Bounded materialization is preserved by construction, automatically — NOT by manual `compact`.** The Global Constraint `len(chain) ≤ SnapshotEvery` (asserted by `TestReplayStaysBoundedAcrossManyFlushes`) must hold across a deep FORK chain. Two triggers: fork-time floor (fork materializes a snapshot when the resolved base depth would exceed the bound) and divergence floor (a shared child self-snapshots once its own divergence crosses `SnapshotEvery`).
- **GC mark uses the REAL `store.Chain` resolver, never a reimplementation.** A one-object divergence between mark and read-path sweeps a live member.
- **LayoutVersion → 2**, enforced before the first base-pointer fork; old binaries refuse the store via `CheckManifest`. No per-object refcounts (CAS-only backend → distributed-refcount race).
- Any change under `internal/session` flush path or `internal/capture` → `make test-torture` SYNCHRONOUSLY (single foreground Bash call, tee'd) with numbers.
- Every existing test passes unmodified unless a spec change makes an old assertion wrong (controller sign-off then).
- The LTX checksum chain (`MaterializeChain` PreApplyChecksum) is the fail-closed safety net — a mis-resolved splice with divergent content must fail loud, verified by test.
- CHANGELOG under [Unreleased] per task; conventional commits + repo trailers.

## File Structure

```
internal/store/store.go        (modify) Ref.Base + BasePointer type; LayoutVersion=2; CheckManifest gate; Chain base-following
internal/store/manifest.go     (modify or in store.go) EnsureLayoutV2 CAS bump
internal/store/store_test.go   (modify) base roundtrip, old-ref decode, version gate
internal/ops/ops.go            (modify) Fork shared path + fork-time floor; compact
internal/ops/gc.go             (modify — rewrite core) object-granular reachability mark-sweep
internal/ops/compact.go        Compact op
internal/session/session.go + flush.go  (modify) divergence-floor self-snapshot for a base-pointer session
internal/daemon/protocol.go + server.go  (modify) compact op; base in BranchInfo cost reporting
cmd/offshoot/main.go           (modify) compact command; status cost/base reporting
docs/*                         status/reference/CHANGELOG + spec-text rewrite (pending/fork-pin)
```

---

### Task 1: LayoutVersion 2 + the base pointer field + version gate
`Ref.Base *BasePointer` (`BasePointer{Lineage string; TXID uint64}`, json `base,omitempty`), threaded through `refWire`/`decodeRef` (tolerant, like TTL/Meta). `LayoutVersion` const → 2. `InitManifest` writes v2 for NEW stores; a v2 binary READS a v1 store fine (no base pointers present). `Store.EnsureLayoutV2()` CAS-bumps an existing v1 manifest to v2, called by the shared-fork path (Task 3) before writing the first base ref. `CheckManifest` already refuses `manifest > LayoutVersion` — verify a v1 binary against a v2 manifest refuses the whole store. Tests: base roundtrip; an OLD ref (no base) decodes with `Base==nil`; EnsureLayoutV2 idempotent + CAS-safe; a simulated v1 binary (LayoutVersion=1) refuses a v2 manifest. No Chain/GC changes yet. NOTE: `Ref.Base` is DISTINCT from `Ref.Parent` (the human breadcrumb) — do not overload Parent.

### Task 2: Chain base-following resolution
`store.Chain(lineage, target)` gains base-following per the strict concatenation rule (Global Constraints). Read the current Chain body first: it Lists one lineage prefix and runs keepHighestEpoch. New logic: load the ref for `lineage` to get its `Base`; if nil → today's path. If set: for `target > base.TXID`, resolve the child's own members in `(base.TXID, target]` within the child lineage (existing single-lineage resolution restricted to that range) and concatenate the base's chain resolved at `base.TXID` (recurse: `Chain(base.Lineage, base.TXID)`); for `target ≤ base.TXID`, return `Chain(base.Lineage, target)`. Each half is resolved within ONE lineage — keepHighestEpoch never sees a union. Chain must still start at a snapshot and error on a gap (existing invariants). Signature note: Chain currently takes `(lineage, target)` and Lists internally — it now needs the ref's Base, so either it loads the ref itself (a Store method already has backend access) or callers pass the ref; decide and keep callers working. Tests: (a) a child reading `target > base.TXID` gets ITS members concatenated onto the parent's ≤base chain, in order; (b) MUTATION test — a resolver that unions-then-keepHighestEpoch across lineages MUST fail an assertion where child epoch 1 and parent epoch ≥2 share a range; (c) fenced-orphan: parent bumps epoch after fork, child's fork point still resolves correct bytes; (d) fork-of-fork transitive; (e) `target ≤ base.TXID` resolves purely in the base. This is the correctness core — no floors yet, so tests use shallow chains.

### Task 3: Base-pointer fork + fork-time snapshot-floor
`ops.Fork` shared path: instead of `copySnapshotToNewLineage`, write the child ref with `Base{parentLineage, forkTXID}` and call `Store.EnsureLayoutV2()` first. Fork-from-live-session still flushes the parent first (existing behavior). FORK-TIME FLOOR: before sharing, resolve the base's chain length from its nearest snapshot to forkTXID (+ accumulated ancestor depth via the transitive base); if it would exceed `SnapshotEvery`, fall back to the existing materialize path (`copySnapshotToNewLineage`) — a hybrid. The M2 reflink/CopyObject fast path is now only reached on the materialize fallback (document). Tests: a shallow fork writes ZERO data objects (assert the child lineage prefix has no snapshot/segment, only a ref) and reads correctly; a fork whose base chain is already at the bound materializes (assert a snapshot exists in the child lineage); byte-equivalence — a shared fork and a materialized fork of the same point resolve to identical `.dump`.

### Task 4: Divergence floor (shared child self-snapshots at SnapshotEvery)
A session on a base-pointer branch must self-snapshot once its OWN divergence (segments written since the fork) crosses `SnapshotEvery`, writing a full snapshot in its own lineage and dropping the base pointer for reads past that snapshot (the ref's Base can be cleared once the child has a snapshot covering ≥ the fork point — decide: clear Base on first own-snapshot, since reads ≤ fork then resolve via the child's own snapshot? NO — reads ≤ fork still need the parent until the child snapshot covers txid 1..fork; a child snapshot is at txid > fork so it does NOT cover ≤fork. So Base stays until compact. The divergence floor bounds reads > fork by giving the child its own snapshot; reads ≤ fork still hop to the parent but that's ONE bounded hop. Re-verify the bounded claim: reads > child-snapshot = child chain only (bounded); reads in (fork, child-snapshot] = child segments (bounded by SnapshotEvery by construction of the floor); reads ≤ fork = parent chain (bounded). Every range bounded.). This touches the flush/session path → TORTURE required, synchronous tee'd. Tests: THE deep-fork bounded-materialization test (mirror `TestReplayStaysBoundedAcrossManyFlushes` across N nested forks each writing a few segments — assert every read's resolved chain ≤ SnapshotEvery + bounded hop count); a session that writes >SnapshotEvery past its fork has its own snapshot.

### Task 5: GC reachability rewrite (object-granular, transitive base closure)
Rewrite `gc.go`'s core (currently lineage-granular `liveLineages` + whole-prefix sweep + lineage-keyed tombstones). New: (1) reachable object set = union over all live refs of each ref's RESOLVED chain members (via `store.Chain` following base pointers — the REAL resolver, not a reimpl); (2) object-granular mark/sweep on individual keys; (3) tombstone/grace at object granularity (or the compensating rule: never object-delete inside a lineage a live ref still names as head — spec allows either; pick object-granular, it's the general case); (4) keep two-phase tombstone→grace(>max fork duration)→RE-LIST→delete, both phases following the base closure; (5) per-pass cache: memoize resolved member-sets per lineage-prefix so a shared ancestor is Listed once, not once-per-descendant; incremental mark (re-resolve only refs whose head moved, via a per-ref generation). Tests: parent kept alive ONLY by a child's base pointer is never swept; a destroyed parent's ≤base objects survive while its >base range is reclaimed; MARK==READ property test (GC's reachable set for a ref == MaterializeChain's object set at head); GC-race fault-injection (ancestor destroyed mid-fork-of-fork, mirror the existing GC-racing-forks test); mixed store (v1 full-copy fork + v2 base fork) GCs correctly. This is the heavyweight — expect fix rounds.

### Task 6: compact op
`ops.Compact(db, branch)`: materialize the branch head (via `materializeChainAt`), re-encode as a fresh self-contained snapshot in the branch's own lineage (`copySnapshotToNewLineage`-shaped), CAS the ref to drop `Base`. CLI `offshoot compact <db>@<branch>`; daemon `compact` op + SDK parity if trivial (else CLI-only, note it). Tests: after compact the branch has its own snapshot and Base==nil; the previously-shared ancestor's objects become reclaimable by GC (compact then destroy-parent then GC → parent objects gone); compact races a concurrent flush → CAS retry; compact of a non-shared branch is a no-op-or-clear-error (decide).

### Task 7: Docs + cost reporting + spec-text reconciliation
`status`/`branches` report whether a branch is shared (has a Base) and its cost class (shared vs materialized) — the two-cost-model asymmetry (fork shares, promote/rollback/compact materialize) called out honestly. README/reference/operations: the CoW model, `compact`, the destroy-lingers-until-last-child semantics, the LayoutVersion 2 gate. Rewrite the original design spec's §Fork mechanics `pending`/fork-pin text — a base-pointer fork has no async upload/pending window (the machinery is removed for shared forks; materialize paths keep their handshake). CHANGELOG [0.2.0] (this is a storage-format change → minor bump, and the layout gate is the reason). ROADMAP: CoW checked off; page-level dedupe still deferred.

## Self-Review
1. Spec coverage: base pointer (T1), Chain resolution + no-cross-lineage + fenced-orphan (T2), fork + fork-time floor (T3), divergence floor + bounded-fork test (T4), GC reachability rewrite + mark==read + race (T5), compact (T6), layout gate (T1), fail-closed checksum net (tested in T2), docs + cost model + spec reconciliation (T7). All spec sections covered. Out-of-scope (page-level dedupe, promote/rollback sharing) explicitly excluded.
2. Placeholder scan: the two "decide" points (Chain's ref-loading shape in T2; object-granular vs compensating-rule tombstone in T5) are bounded single decisions assigned to their tasks with the recommended choice stated — not TBDs.
3. Type consistency: `BasePointer{Lineage,TXID}` and `Ref.Base` consistent T1/T2/T3/T6; `EnsureLayoutV2` T1/T3; the fork-time and divergence floors both keyed on `SnapshotEvery` consistent T3/T4; `Chain` following base consistent T2/T5.
