# Offshoot Copy-on-Write (Object-Sharing Forks) — Design

**Status:** design, reviewed (senior-storage-engineer pass folded in). Targets the v2 storage-amplification arc the original design deferred.

**One-line:** A fork stops being a full N×G copy in the object store; it records a pointer into the parent's already-durable chain and writes new objects only as it diverges. Reachability GC reclaims a shared object when no live branch's chain still references it.

## Problem

Today every fork is a fully materialized, storage-independent lineage: `copySnapshotToNewLineage` re-encodes a fresh snapshot per child (`internal/ops/ops.go`), so N forks of a G-byte database cost up to **N×G** in the bucket. That independence is what buys the current design's simplicity — "destroy any parent, anytime; children don't care" — and TTL-reaped attempt branches are the only mitigation. For fork-heavy workloads (the eval seed-once-fork-many pattern, agent fork-trees) the bucket bill is the complaint. Fork *latency* is already solved (M2's reflink/CopyObject fast path); this is purely about **storage cost**.

## Non-goals

- **Not** content-addressed / page-level dedupe across unrelated databases (the maximal-dedupe arc). Sharing is per-object, between a fork and its ancestors only.
- **Not** a change to checkout materialization — reflink forks, the ro-cache, and per-branch working files are untouched. CoW is entirely about **store objects**.
- **Not** unifying rollback/promote onto sharing — they keep materializing independent lineages (see §Two cost models). Revisiting rollback-to-a-kept-checkpoint is an explicit follow-up, out of scope here.

## Core model

### The base pointer

A shared fork writes the child ref with a new structured field:

```go
// On store.Ref (new, distinct from the human-facing Parent breadcrumb):
//   Base *BasePointer `json:"base,omitempty"`
type BasePointer struct {
    Lineage string `json:"lineage"` // the ancestor lineage this fork shares from
    TXID    uint64 `json:"txid"`    // the fork-point committed txid within that lineage
}
```

**No epoch in the pin.** `base.TXID` is always a *committed* txid (a `HeadTXID` or a named checkpoint's txid, both of which only advance on a successful ref CAS). For a committed txid the correct bytes are always the highest-epoch object, which the existing `store.Chain` already selects (`Chain` is epoch-agnostic for selection; `keepHighestEpoch` picks the live epoch per txid-range). A fenced-writer orphan can only ever collide at `HeadTXID+1`, which is strictly greater than any fork point — so the fork point is never ambiguous. Pinning a specific epoch would *invert* this and could select a fenced orphan (the Plan-7 bug class); the epoch field is therefore deliberately absent.

Fork writes zero new objects and no snapshot. It is O(1) and near-instant. Because a fork can only share *durable* objects, forking a live daemon session flushes the parent first — already the daemon's behavior.

### Chain resolution (the one hard rule)

`store.Chain(lineage, targetTXID)` gains base-pointer following, under a **strict, non-negotiable invariant: resolution never merges members across lineages.**

- If the ref has no `Base` → resolve exactly as today (self-contained lineage). Unchanged path for every pre-CoW branch.
- If the ref has a `Base`:
  - **targetTXID > base.TXID** (reading the child's own divergent state): resolve the child's own objects `(base.TXID .. targetTXID]` *within the child lineage*, then concatenate the base's resolved chain up to `base.TXID` *underneath* (recursively resolved in the base lineage). The two halves are resolved independently, each within its own lineage prefix, then concatenated — `keepHighestEpoch` is only ever run on a single lineage's members. It must never see a cross-lineage union: a union would let the higher-epoch parent object win the child's same txid-range and silently serve the parent's timeline for the child's writes.
  - **targetTXID ≤ base.TXID** (reading at/below the fork point): resolve entirely against the base lineage at `targetTXID` (transitively following the base's own base pointer if it has one). The child contributes nothing.

Transitivity (fork-of-fork) walks the base pointers down until `targetTXID` lands in some ancestor's own objects.

### Bounded materialization is preserved by construction

The Global Constraint "materialization must be bounded" (asserted by `TestReplayStaysBoundedAcrossManyFlushes`, `len(chain) ≤ SnapshotEvery`, chain starts at a snapshot) is **load-bearing** and must survive CoW. A naïve pure-sharing design breaks it precisely on the fork-spam workload: a deep chain of shallowly-diverged forks, none tripping its own `SnapshotEvery` self-snapshot, resolves to one ancestor snapshot plus segments spanning *every* intervening fork level — unbounded in depth.

Two automatic snapshot-floor triggers keep it bounded (manual `compact` is **not** relied on for correctness):

1. **Fork-time floor:** at fork, compute the resolved base-chain length from the base's nearest snapshot to `base.TXID`, plus accumulated ancestor depth. If sharing would produce a resolved chain exceeding the bound, the fork **materializes a snapshot floor** instead of pure-sharing — a hybrid: cheap share when the ancestor chain is short, one materialized snapshot when it is deep. (Falls back to the existing `copySnapshotToNewLineage` machinery for that case.)
2. **Divergence floor:** a shared child self-snapshots (writes its own full snapshot in its own lineage, dropping the base pointer for reads past that snapshot) once its *own* divergence crosses the `SnapshotEvery` threshold — reusing the daemon's existing flush cadence. After a child has written its own snapshot at txid S, reads at `targetTXID > S` never touch any ancestor.

A new test mirrors `TestReplayStaysBoundedAcrossManyFlushes` across a deep **fork** chain (not just flushes) and asserts the bound holds.

**Fail-closed safety net.** `MaterializeChain`'s per-member `PreApplyChecksum` verification (`internal/ltxio/segment.go`) turns any mis-resolved splice with divergent child content into a *loud* materialization failure, not silent corruption. Combined with the no-cross-lineage-merge rule and the no-epoch-pin rule, the only residual silent window (a pure read-fork that never diverged, resolved against a wrong object) is closed. This fail-closed property is an explicit design guarantee.

## The four operations

| Op | Behavior under CoW |
|---|---|
| **fork** | Write child ref with a `Base` pointer. Zero new objects (unless the fork-time floor trips → one materialized snapshot). Forking a live session flushes the parent first. |
| **promote** | Unchanged: mint a fresh, fully-materialized independent lineage from one snapshot. A cord-cutter. |
| **rollback** | Unchanged: mint a fresh materialized lineage from the target checkpoint. A cord-cutter. |
| **compact `<branch>`** (new) | Materialize the branch head as its own full snapshot in its own lineage, CAS the ref to drop its `Base` → the branch stops pinning every ancestor. The explicit "pay N×G now to release a dead ancestor" lever. Racing GC is safe (the base pointer keeps ancestors reachable until the CAS cutover); a concurrent flush that advanced HeadTXID fails compact's CAS → retry. |

## GC redesign (reachability mark-sweep — a rewrite, not an evolution)

The current GC (`internal/ops/gc.go`) marks liveness at **lineage** granularity (`liveLineages` reads only `r.Lineage`), tombstones by lineage, and sweeps **whole lineage prefixes**. None of that survives partial sharing, where a lineage can be simultaneously live-at-head-for-one-branch and dead-below-a-fork-point-for-another. The redesign:

1. **Reachability = transitive base closure over all live refs.** A ref's live object set is its *resolved chain's* members (following base pointers to every ancestor). The union across all live refs is the live object set. A lineage kept alive *only* by a descendant's base pointer stays reachable — the current single-hop `liveLineages` would tombstone it and sweep the descendant's base out from under it (data loss); the closure prevents that.
2. **Object granularity.** Mark and sweep operate on individual object keys, not lineage prefixes. A partially-shared ancestor keeps its ≤ `base.TXID` objects and sheds the rest once its head branch dies. The tombstone/grace state is object-granular (or gated by the compensating rule: never object-delete inside a lineage any live ref still names as head).
3. **Mark uses the real resolver.** The mark pass computes reachable objects with the *same* `Chain` / `keepHighestEpoch` code the read path uses — never a reimplementation. A one-object divergence between mark and read-path resolution sweeps a live member; sharing the code path makes that impossible by construction.
4. **Two-phase preserved.** Keep tombstone → grace (`> max fork duration`) → **re-list** → delete. Both the phase-1 mark and the phase-2 re-list follow the base closure. Base-pointer forks write zero objects and are near-instant, so the fork-in-flight window *shrinks* vs. today; the residual ancestor-destroyed-mid-fork-of-fork race is caught by the phase-2 re-list plus the mint-this-run skip. A fault-injection test covers exactly that race.
5. **Affordable at scale (no refcounts).** Per-object refcounts over a CAS-only backend with no atomic multi-object write are the classic distributed-refcount race — rejected. Instead: memoize resolved member-sets per lineage-prefix **per GC pass** (collapses thousands-of-descendants×depth Lists to O(distinct lineages) Lists), and make the mark incremental (re-resolve only refs whose head moved since the last pass, tracked by a per-ref generation).

**Destroy semantics change, stated honestly.** `Destroy` still deletes the ref instantly and is always allowed (the claim-guarded machinery from M4 is unchanged). But a parent's *bytes* are reclaimed only when no surviving child's chain references them — so a destroyed parent shared by a live keeper lingers until that keeper is destroyed or `compact`ed. The original invariant "destroying a parent is always safe/allowed" holds; the *storage reclaim* is what defers. TTL-reaped attempt branches remain the churn mitigation, and `compact` is the manual release.

## Two cost models (the accepted wart)

Fork is free/shared; rollback and promote still pay a full N×G materialize. "fork-at-checkpoint-X is free but rollback-to-X costs N×G" is a genuinely asymmetric surface. It is kept because rollback/promote *abandon* the old head lineage (it gets GC'd), and base-pointing into a lineage that is supposed to die would pin it forever. The asymmetry is called out explicitly in user docs and in `status` cost reporting rather than hidden. Unifying rollback-to-a-*kept*-checkpoint onto base pointers is a follow-up, not in this design.

## Backward compatibility

- **LayoutVersion bump to 2**, required before the first base-pointer fork is written. `CheckManifest` then makes every pre-CoW binary refuse the whole store up front. This is the *only* enforcement that works on S3, where you cannot rely on per-ref schema checks firing in a particular order — an old binary's lineage-granular GC that ignored a base pointer would sweep a child's shared objects (silent data loss), so old binaries must be locked out of the store entirely once any base pointer exists.
- Going forward a v2 binary treats an absent `Base` as a self-contained lineage (exactly current behavior) and follows it when present; GC's reachability handles a mixed store (old full-copy forks + new shared forks) uniformly.
- The `pending` marker and fork-pin machinery from the original spec's §Fork mechanics are **removed for shared forks** — a base-pointer fork has no async snapshot upload and no pending window. (The full-materialize paths — promote/rollback/fork-time-floor — retain whatever durability handshake they already use.) The spec text is rewritten so the two models are not both half-described.

## Testing

- Bounded-materialization across a deep **fork** chain (mirrors `TestReplayStaysBoundedAcrossManyFlushes`) — both floor triggers exercised.
- Resolution correctness: a shared child reading its own divergent txids gets *its* bytes, not the parent's (the cross-lineage-merge hazard, mutation-verified: a resolver that unions across lineages must fail this).
- Fenced-orphan safety: a parent lineage that bumps epoch after a fork still resolves the child's fork point to the correct bytes (no epoch pin regression).
- GC reachability: a parent kept alive only by a descendant's base pointer is never swept; a destroyed parent's below-fork objects survive while an above-fork range is reclaimed.
- GC race: ancestor destroyed mid-fork-of-fork (fault-injection, mirrors the existing GC-racing-forks test).
- `compact` cuts the cord: after compact, the ancestor's objects become reclaimable; compact racing a flush retries on CAS.
- Backward compat: a v1 (no-base) ref and a v2 (base) ref coexist; GC handles the mix; an old binary refuses the store post-bump.
- Mark == read-path resolution: a property test that the GC mark's reachable set for a ref equals `MaterializeChain`'s object set for that ref at head.

## Scope boundary

In: base-pointer fork, base-following `Chain`, the two snapshot-floor triggers, object-granular reachability GC with the per-pass cache, `compact`, the LayoutVersion 2 bump, docs + `status` cost reporting. Out: page-level/content-addressed dedupe, rollback/promote sharing, cross-database dedupe, any checkout-materialization change.
