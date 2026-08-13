# Stability contract

offshoot is pre-1.0, and the [CHANGELOG](../CHANGELOG.md) says so bluntly:
the on-disk/on-bucket storage format (refs, checkpoints, segments, leases)
may change in a backward-incompatible way before 1.0. This page is the
contract that sentence lives inside — what "pre-1.0" concretely means here,
the promise that bounds it, the mechanism that enforces it, and the
criteria under which the format freezes at 1.0.

## What pre-1.0 means here, concretely

- **Versioning:** offshoot adheres to [Semantic
  Versioning](https://semver.org/spec/v2.0.0.html) once it reaches 1.0.
  Before 1.0, a **minor** version (0.2 → 0.3) may include breaking changes;
  a **patch** release (0.2.6 → 0.2.7) does not. Pin an exact version if you
  depend on format stability. (This is the CHANGELOG's own stated policy,
  restated — not a new promise.)
- **What can break:** the storage layout — how refs, checkpoints, segments,
  leases, and the store manifest are keyed and encoded. It has already
  changed once: v0.2.0's copy-on-write forks introduced layout version 2
  (base pointers in refs), and that transition is the template for how any
  future change is handled (see [the enforcement gate](#the-enforcement-gate-layoutversion)
  below).
- **What does not silently break:** your data. Every checkout and every
  `export` is a stock SQLite file — whatever happens to offshoot's storage
  format, the databases it manages are always extractable as plain `.db`
  files any SQLite tool reads. The stock-file invariant is the standing
  floor under every format risk on this page.

## The promise

**Any storage-format break before 1.0 ships, in the same release, with
either (a) an in-place migration, or (b) a documented
`export` → `create --from` path.** "May change without a migration path"
in the CHANGELOG describes the *license* semver gives a pre-1.0 project,
not what a release will actually do to you: no offshoot release will leave
data written by the previous release unreadable without also shipping, in
that same release's notes, the exact steps that carry it forward.

The `export` → `create --from` path is not hypothetical — both halves are
shipped, tested code, and they round-trip today:

- **`offshoot export <db>[@branch[@checkpoint]] out.db`** materializes a
  branch's state to a plain SQLite file with zero ongoing relationship to
  the store. The write is atomic (temp file in the destination's own
  directory, renamed into place only after every chain member's checksum
  verifies — a failed export never leaves a partial file). Implementation:
  `ops.Workspace.Export` (`internal/ops/export.go`); CLI behavior pinned by
  `cmd/offshoot/export_test.go`.
- **`offshoot create <db> --from file`** imports an existing SQLite file
  into a fresh store: the source is copied (plus `-wal`/`-shm` if present),
  the copy is quiesced with a full WAL checkpoint, and that becomes the new
  database's root snapshot — the source file is never modified.
  Implementation: `ops.Workspace.CreateFrom` (`internal/ops/ops.go`);
  source-untouched behavior pinned by
  `TestCreateFromImportsWithoutTouchingSource` (`internal/ops/ops_test.go`).

Round-trip, verified against a build of this revision: `export` from store
A, `create --from` into a fresh store B, `checkout` from B — the two
databases' `sqlite3 .dump` outputs are identical. That is the worst-case
escape hatch for any format change: old binary exports, new binary
imports, at the cost of a full copy per database. An in-place migration,
when one ships instead, is the cheaper path — but the export path is the
one that can never be blocked by the nature of the format change itself,
because its interchange format is a stock SQLite file, not anything
offshoot-specific.

What this promise deliberately does **not** cover: lineage history.
`create --from` seeds a fresh database whose history begins at import
(one `init` checkpoint) — named checkpoints, branch structure, and TTLs
on the old store don't travel through a `.db` file. If a pre-1.0 break
ever requires the export path rather than a migration, the release notes
will say exactly that, and `export` accepts a checkpoint name
(`db@branch@checkpoint`) so any named state you need can be carried over
as its own file.

## The enforcement gate: LayoutVersion

The promise above is enforced by a version gate in the store manifest, not
by convention:

- Every store's manifest records a `layout_version`
  (`store.Manifest`, `internal/store/store.go`). The current binary's
  supported version is `store.LayoutVersion` (currently `2`).
- **Every attach checks it.** `ops.Open` calls `store.Store.CheckManifest`
  before any command touches the store (`internal/ops/ops.go`), and
  `CheckManifest` refuses a manifest newer than the binary
  (`internal/store/store.go`):

  ```
  store: layout version 3 is newer than this binary supports (2)
  ```

  (the exact message format of `CheckManifest`'s refusal, shown here for a
  hypothetical future layout version 3).

  An old binary pointed at a newer store fails loudly and immediately, on
  every command — it can never half-read, and more importantly never
  half-*write* (or garbage-collect), a format it doesn't understand. Pinned
  by `TestCheckManifestRefusesNewerLayout`
  (`internal/store/store_test.go`).
- **Upgrades are one-way and explicit.** The v1 → v2 transition shows the
  shape: the first shared (copy-on-write) fork CAS-bumps the manifest to
  version 2 (`store.Store.EnsureLayoutV2`), from which point every
  pre-copy-on-write binary refuses the entire store. That lock-out is the
  safety property, not a side effect — an old binary's GC reasons about
  whole lineages, cannot see base pointers, and would sweep a shared
  ancestor out from under live children. Silent data loss is prevented by
  refusing up front. See [operations.md](operations.md#storage-sharing-copy-on-write-forks)
  for the operator-facing version of this rule.

So the failure mode of a format change is always "old binary says no,
loudly" — never "old binary corrupts new store quietly." That, plus the
same-release migration-or-export promise above, is the whole contract.

## Proposed v1.0 criteria

These are **proposed, not yet committed** — published here so the bar is
visible and arguable before it's frozen. offshoot tags 1.0 when all of the
following hold:

1. **Format frozen-with-migrations.** The storage layout changes only via
   a `LayoutVersion` bump that ships with an in-place migration; the
   `export` → `create --from` path stops being an acceptable substitute
   for one. Post-1.0, a format change without a migration is a 2.0.
2. **AWS-proper S3 verification.** The S3 backend is verified against AWS
   itself, not only S3-compatible providers. Today the full backend
   conformance suite runs against real MinIO in CI
   (`.github/workflows/ci.yml`, `s3-conformance` job) and the nightly
   workflow has a credentialed real-provider job
   (`.github/workflows/nightly.yml`, `real-provider-conformance`, gated on
   the `NIGHTLY_S3` repo variable); the 1.0 bar is that job running green
   against an AWS bucket on a sustained basis, since multipart
   checksum/precondition behavior is exactly where providers differ.
3. **A soak period of nightly torture runs with zero divergence.** The
   kill-9 torture harness runs on the CI cadence stated in
   [testing.md](testing.md#the-kill--9-torture-harness) — that page is
   canonical (`.github/workflows/nightly.yml`, `torture` job). The
   1.0 bar: a defined consecutive window — proposed: 3 months — of nightly
   runs with no divergence and no corruption, restarting the clock on any
   failure whose fix touches the capture or flush path.
4. **API surface frozen.** The CLI command surface, the daemon wire
   protocol (`internal/daemon/protocol.go`), the SDK client APIs, and the
   metric names (already declared frozen-at-first-public-tag in
   [operations.md](operations.md#metrics)) change only additively.

Until then: pin versions, read release notes before upgrading a binary
that shares a store with others, and never point an older binary at a
store a newer one has written — the manifest gate will stop you, but it's
politer not to need it.

## See also

- [testing.md](testing.md) — how the durability claims above are actually
  tested (torture harness, conformance suite, CI gates).
- [operations.md](operations.md) — the layout v2 gate from the operator's
  side; metrics-name freeze policy.
- [reference.md](reference.md) — `export` and `create --from` flag-level
  reference.
- [CHANGELOG.md](../CHANGELOG.md) — the pre-release status statement this
  page is the contract for.
