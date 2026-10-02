# Pointers and deltas, not copies — design

**Status:** approved for implementation 2026-09-28 (Tier 2 item 1 of the 2026-09-26 roadmap). Decided by the controller acting as product manager and staff engineer under the standing "decide, do not ask back" instruction; every ruling is reversible and recorded in the plan's ledger.

## Problem, measured

Tier 1 shipped two benchmarks that put numbers on where the per-attempt hot path spends its time (docs/benchmarks.md, one run each, 2026-09-26):

| What | Cost today | Why |
|---|---|---|
| `fork` from a checkpoint | ~9 ms, flat to 1 GB | a base pointer; two metadata objects |
| `fork` at head | 17 → 418 ms for 12 MB → 1 GB | `warnIfUncheckpointed` SHA-256-hashes the parent's whole checkout |
| `checkout` / `Session.Open` of a fresh fork | 68 ms @ 10 MB, 421 ms @ 100 MB | the LTX snapshot is decoded page by page into a new file, even when a byte-identical file already exists in `checkouts-ro` or as a sibling's checkout |
| at-rest `checkpoint` | full snapshot every time; 1,000 checkpoints of a 17 MiB db → 26.5 GiB | without a daemon there is no record of which pages changed |
| `rollback`, `promote` | O(size) copy | they materialize a fresh lineage; the roadmap deferred base pointers because "base-pointing into a lineage meant to die would pin it forever" |
| clean-skip on `checkout` | O(size) | `checkoutState` re-hashes the whole file to prove it is unmodified |

BranchBench's MCTS row spends 96% of wall time in branch management for exactly these reasons. The eval-harness pitch ("fork per test is cheaper than a Postgres template clone") holds at 10 MB and loses at 100 MB because of the checkout decode.

## Design

Four independent changes, each behind an existing primitive, none changing the on-bucket format (`LayoutVersion` stays 2: segments on a lineage and base pointers on a lineage are both shapes v2 already reads).

### 1. Clone a checkout from an identical local file

A materialized checkout's content is fully determined by the resolved chain of store objects that produced it (`store.Chain`, epoch-collapsed, base pointers followed). Define `chainID = sha256(join(member keys, "\n"))`. Two targets with equal chainID are byte-identical by construction: a fresh shared fork's head resolves to exactly its parent's chain at the fork point.

- The `.sum` sidecar gains `chain_id`. Every materialization records it.
- `checkouts-ro` gains a by-chain area (directory name chosen outside `store.ValidateName`'s charset so it can never collide with a database name), holding `<chainID>.db` (mode 0444) plus its `.sum`. Entries are immutable content, participate in `-ro-cache-budget` LRU eviction like today's entries, and remain safe to `rm -rf`.
- `CheckoutProven`, on the stale/missing path, resolves the chain first. On a by-chain hit it `reflink.Clone`s the entry to a temp file, renames it over the checkout path (the existing write-temp-then-rename discipline), and stamps a sidecar with the *destination* branch's identity, the entry's recorded hash and checksum (identical bytes, so no re-hash), and the chainID. On a miss it materializes as today and then populates the by-chain entry by cloning the fresh checkout; population is skipped, silently, when the filesystem cannot clone.
- Prefix hits: if no entry matches the full chain but one matches the chain's newest-snapshot-plus-k-segments prefix, clone it and apply only the remaining segments through `ltxio.MaterializeChain`'s segment loop. This is the deep-tree case (a child of a child).
- `reflink.Clone(dst, src)` is a new strict variant of the existing `reflink.CopyFile`: clone or `ErrUnsupported`, never a plain copy, so a non-CoW filesystem never pays a second full write.

`Session.Open` reuses `CheckoutProven`, so a cloned, correctly-stamped checkout is "clean at open" and the settling snapshot flush is suppressed exactly as for a reused checkout today.

### 2. Clean-skip without hashing

The sidecar additionally records the file's size, modification time (nanoseconds) and SQLite's header change counter (bytes 24–27) at stamp time. `checkoutState` reports "clean" without hashing when the identity matches and all three equal the live file; any difference falls back to the full hash exactly as today. `warnIfUncheckpointed` (fork at head) inherits this, so fork at head becomes near-constant for a checked-in checkout. Correctness argument: a rollback-journal commit bumps the change counter and mtime; a WAL-mode write leaves a non-empty WAL, and the `wal_checkpoint(TRUNCATE)` the caller already performs rewrites the main file (mtime changes) before the comparison. The fallback is the existing full hash, so a fingerprint mismatch can only cost time, never correctness.

### 3. At-rest checkpoints as deltas

After every successful checkpoint or materialization of a writable checkout, keep a shadow: a `reflink.Clone` of the checkout as of that durable state, next to it, with the sidecar recording that the shadow corresponds to the sidecar's identity. The next `Checkpoint`:

1. quiesces and re-reads the ref as today;
2. if a shadow exists, the sidecar identity equals the ref's head, the page size is unchanged, the lineage's current chain is shorter than the snapshot bound (`SnapshotEvery` or `ForkShareMaxDepth`), the changed-page fraction is below the session path's large-segment threshold, and the caller did not ask for a snapshot: compares the checkout with the shadow page by page, builds the changed-page set (including growth and truncation), derives the post-apply checksum incrementally from the sidecar's recorded pre-apply checksum via `ltxio.UpdateChecksum`, and writes one `ltxio.EncodeSegment` object under `store.SegmentKey(lineage, epoch, txid, txid)` with the same create-only put and ref CAS the snapshot path uses;
3. otherwise writes a full snapshot exactly as today;
4. after the CAS, re-stamps the sidecar and refreshes the shadow.

Reads stay O(size) (two local sequential files); bytes written become O(changed pages). Where cloning is unsupported no shadow is kept and behaviour is today's. `offshoot checkpoint` prints whether it wrote a segment or a snapshot and gains `--snapshot` to force the latter.

### 4. Rollback and promote to a kept checkpoint through a base pointer

`RollbackWith` and `PromoteWith` gain Fork's share-versus-materialize decision with the same depth floor. Below the floor: mint a lineage, `EnsureLayoutV2`, `WriteLineageBase(new, {old lineage, checkpoint txid})`, repoint the ref with `Base` mirrored, and stop copying every other kept checkpoint (they resolve through the pointer, since their txids are at or below the base txid). At or above the floor, or with `--materialize`: today's path. The safety forks are unchanged (they already are shared forks).

Why the roadmap's objection does not apply: GC's `BaseSpine` marking pins only objects at or below the base txid, which are exactly the kept checkpoints rollback copies forward today on purpose; everything above the base txid, the abandoned future, is unreachable through the new lineage and reclaimable once the safety fork's TTL lapses. `compact` remains the way to detach. The exploration found no code-level blocker, only the stated product objection, which this design answers by pointing only at kept checkpoints.

## What changes for users

- Fork-per-test with a live session: near-constant open after the first materialization of a seed.
- `offshoot fork` at head: near-constant when the checkout is clean.
- `offshoot checkpoint` in a loop without a daemon: bytes written proportional to the change.
- `offshoot rollback` / `promote`: constant-time, like fork.
- Nothing changes on a filesystem without reflink support except the sidecar's extra fields.

## Non-goals

Page-level dedupe across unrelated databases; merge; a daemon-side `checkpoint` op (the in-process lock hazard on `Checkpoint` stands); Windows.

## Verification

Each change has a behavioural test (bytes written, objects created, byte-equality of materialized content against an independent copy, GC reachability) and is measured by the existing benchmarks (`make bench`, `make bench-isolation`, `make bench-branchbench`), re-run and re-pasted in one commit at the end.
