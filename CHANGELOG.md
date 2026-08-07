# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches 1.0. Before 1.0, minor versions may include breaking changes.

**Pre-release status:** offshoot is pre-1.0. The on-disk/on-bucket storage
format (refs, checkpoints, segments, leases) may change in a
backward-incompatible way before 1.0 without a migration path. Pin an exact
version if you depend on format stability.

## [Unreleased]

### Added

- **Branch state taxonomy** (Milestone 4 Task 1): `ops.BranchStateAt`
  (`internal/ops/status.go`) computes a branch's state — `active`, `dirty`,
  `detached`, `idle` — from its ref, lease liveness, and checkout `.sum`
  sidecar alone, no daemon dependency; a daemon layers two more states
  (`pending`, `error`) on top from its own in-memory session map
  (`internal/daemon/server.go`'s `Server.branchState`), since only a daemon
  knows which branches have a session reserved or errored. Precedence:
  `error` > `pending` > `active` > `dirty` > `detached` > `idle`. `idle` is
  a deliberate addition beyond the design spec's original taxonomy, which
  assumed a daemon was always present — see
  [docs/reference.md](docs/reference.md#branch-states) for the full table
  and rationale. Surfaced in `offshoot status` (`BranchStatus.State`), the
  daemon `branches` op (`BranchInfo.state`, always present — additive, not
  `omitempty`), and both SDKs' `Branch.state` (Python dataclass field,
  TypeScript interface field; both default to `""` against an older
  daemon that never sends it, so existing SDK code is unaffected).
  **Disclosed cost:** determining `dirty` for a checked-out, unleased
  branch whose sidecar identity already matches the ref requires a real
  WAL checkpoint (quiesce, up to a 3s busy timeout) followed by a full
  SHA-256 hash of the checkout — per branch, per `status`/`branches` call;
  a checkout that's busy during that checkpoint attempt is itself reported
  `dirty` rather than `idle` — see
  [docs/reference.md](docs/reference.md#branch-states)'s Cost/Known
  blind spot notes.

- **Metrics registry + instrumentation** (Milestone 4 Task 2):
  `internal/metrics` is a hand-rolled, zero-dependency, concurrent-safe
  Prometheus text-exposition registry — `Counter`/`Gauge`/`Histogram` (fixed
  buckets, cumulative `_bucket`/`+Inf`/`_sum`/`_count`), `CounterVec`/
  `GaugeVec` for bounded label sets, `Registry.WritePrometheus(io.Writer)`.
  Kept `internal` and zero-dep by design (one-sentence rationale on the
  package doc comment, per PM Amendment 7) so a later `client_golang` swap
  is a call-site-only change. `internal/daemon`'s `newMetrics` registers
  every metric on the plan's LOCKED name list: `offshoot_build_info{version}`,
  `offshoot_sessions_open`, `offshoot_capture_lag_bytes{db,branch}` /
  `offshoot_durable_age_seconds{db,branch}` (open sessions only, computed at
  SCRAPE TIME from the sessions map via a `Registry.Collect` callback, never
  continuously), `offshoot_flush_total{result,kind}` /
  `offshoot_flush_duration_seconds`, `offshoot_fork_total{path}` /
  `offshoot_fork_duration_seconds`, `offshoot_checkpoint_duration_seconds`,
  `offshoot_reap_total`, `offshoot_gc_tombstoned_total` /
  `offshoot_gc_deleted_total` / `offshoot_gc_backlog`,
  `offshoot_ro_cache_bytes` / `offshoot_ro_cache_evictions_total`
  (registered now at zero — Task 5 wires real numbers),
  `offshoot_janitor_runs_total{result}`. Instrumentation: `internal/ops`
  gained `ObserveFork`/`ObserveCheckpoint`, package-level nil-checked
  injected-hook vars (ops must not import `internal/metrics`) the daemon
  assigns once at `NewServer` construction; `internal/session` gained an
  `OnTransition` hook fired from the SAME `logTransition` call site
  Milestone 2 already used for its transition logs (not a new call site —
  Task 2's brief was explicit: hook the existing sites, don't restructure),
  which the daemon uses to drive flush counters/durations from the existing
  "flushed"/"flush-failed" events (and `flush()` now times itself and
  reports `duration_seconds` in that same kv). Janitor-loop metrics
  (reap/GC/backlog/`janitor_runs_total`) are updated from
  `Server.janitorTick`, split out of `StartJanitor`'s ticker body so a test
  can drive one tick deterministically. **PM Amendment 7 hard gate:**
  `WritePrometheus` output is golden-file-tested
  (`internal/metrics/testdata/golden.txt`) and validated against Prometheus's
  own `promtool check metrics` linter (`TestPromtoolCheckMetrics` in
  `internal/metrics`, `TestPromtoolCheckRealMetrics` in `internal/daemon`
  against the real, fully-wired registry) — both skip loudly (not silently)
  when `promtool` isn't on `PATH`; `.github/workflows/ci.yml` gained a new
  `metrics-lint` job that downloads a pinned `promtool` release and runs
  both. `internal/daemon/metrics_test.go`'s `TestMetricsSmokeOpenWriteFlushFork`
  opens a session, writes, flushes, and forks over the real unix-socket RPC
  path, then scrapes the registry directly (no HTTP yet — that's Task 3) and
  asserts the counters moved. Not yet exposed over HTTP (`GET /metrics`,
  Task 3) — see [docs/status.md](docs/status.md) for the exact split.

## [0.1.2] - 2026-08-06

Milestone 3: the eval-harness release. The target persona's first hour is
now paved end to end — install, seed once, fork per test, inspect, export,
clean up — from Python (`offshoot.pytest_plugin`) or TypeScript
(`testkit`), with [docs/eval-harness.md](docs/eval-harness.md) as the
serious tutorial and framework recipes for Claude Code, the OpenAI Agents
SDK, LlamaIndex, and CrewAI in [docs/recipes/](docs/recipes/). New protocol
surface (`dbs`, metadata/timestamps, `export`, read-only historical
checkouts) lands first, the fixture plugin and TS testkit are built on top
of it, and `offshoot diff` closes the daily "what changed between these two
attempts" loop. Two Milestone-2 performance follow-ups (settling-flush
suppression; sidecar refresh on clean close) ride along because the
fixture's session-per-test pattern is exactly the workload they fix. SDK
publishing is prepared (workflows, manifests) but actual PyPI/npm
publication, and the MCP registry/LangGraph listing submissions, stay
user-gated — see [docs/status.md](docs/status.md) and
[ROADMAP.md](ROADMAP.md)'s Milestone 3 section for exactly what shipped vs.
what's a stated, pre-written deferral.

### Added

- **Docs sweep** (Milestone 3 Task 8): [docs/eval-harness.md](docs/eval-harness.md),
  the serious tutorial — install, the pytest plugin end to end (named-seed
  factory, fork-per-test, `pytest-xdist` with the measured per-worker
  number, mid-test flush checkpoints, golden assertions via `offshoot_dump`
  — never byte-compare, why), export for handoff/debug, `offshoot diff` for
  the failed-vs-passed loop, TTL hygiene, a CI recipe (this repo's own
  `ci.yml` `sdks` job as a live example), what it costs, and the TypeScript
  `testkit` section mirroring the same semantics for vitest/jest/`node:test`.
  Every pytest example in it was actually run against a real daemon while
  writing it (a scratch project, real output pasted in, not invented); the
  TypeScript `testkit` section's `node:test` example was likewise run
  against a real build of `sdk/typescript`. `sdk/python/README.md`'s
  pytest-fixture-plugin section is superseded by the tutorial as the
  primary teaching surface and now points to it, staying as the
  PyPI-landing-page-sized condensed reference.
  - `docs/recipes/claude-agent-sdk.md`: `claude mcp add` config for
    `offshoot mcp`, and a hooks pattern (fork on `SessionStart`, checkpoint
    on `Stop`, roll back on failure) — explicitly checked against the MCP
    daemon-mode reality documented in the README's MCP section and
    `docs/reference.md`: `offshoot mcp` never opens a session itself, so
    the recipe's actual content is opening the session via the CLI/SDK
    *before* the agent's first tool call, with the hooks shown as one way
    to automate that open/close, not a substitute for it. The hooks JSON
    shape itself is marked illustrative (Claude Code's own surface, not
    offshoot's, and outside what this task could pin the way the rest of
    this repo's docs are).
  - `docs/recipes/openai-agents.md`: the OpenAI Agents SDK's
    `SQLiteSession(session_id, db_path)` pointed at an offshoot checkout
    path, fork-per-attempt via the Python SDK. Actually run end to end
    against a real `pip install openai-agents` (no API key needed to prove
    the storage integration — `add_items`/`get_items` stand in for
    `Runner.run`); real output pasted in, including two forked attempts'
    histories staying independent after diverging from a shared checkpoint.
  - `docs/recipes/frameworks.md`: short, honest LlamaIndex/CrewAI notes —
    recipes, not adapters, per the plan's stance. Explicitly marked
    unverified against a live install this pass (LlamaIndex's SQL chat
    store needs the `aiosqlite` extra; CrewAI's package failed to build
    locally — `tiktoken`, one of its transitive dependencies, has no
    prebuilt wheel for this Python/platform combination) rather than
    presented with false confidence.
  - README: integration-surface refresh — pytest plugin, TypeScript
    `testkit`, `export`/`checkout_at`/`dbs` all get their own lines under
    the Python/TypeScript SDK sections; a new "Other agent frameworks"
    section points at `docs/recipes/`; the Docs line and Quickstart section
    both gain pointers to the new tutorial.
  - `docs/reference.md`: verified complete for `export`/`diff`/
    `checkout --at` (documented per-task already; no gaps found) — added
    one pointer line at the top to the tutorial for readers who want the
    narrative walkthrough instead of the flag-by-flag reference.
  - `docs/status.md`: two new deferral rows, both pre-written per PM
    Amendment — `create --from` daemon/SDK/MCP reach (needs an upload
    channel or a same-host path-trust story like `export`'s; CLI remains
    the import path) and MCP session open/close (the fixture plugin and TS
    testkit are now real lifecycle owners for their own harness workload,
    but neither is an MCP tool pair — the no-natural-owner reasoning from
    Milestone 2's amendment stands, confirmed rather than just carried
    forward).
  - `ROADMAP.md`: Milestone 3 checked off item-by-item against what
    actually shipped, with the two deferrals above plus listings
    submission and actual SDK publication called out as pre-written,
    user-gated deferral rows rather than silently-dropped scope; Milestone
    2's "MCP rides the daemon" bullet updated to note the fixture/testkit
    now exist without overclaiming they closed the MCP-tool-pair gap.
- **List databases** (Milestone 3 Task 1): new daemon protocol op `dbs`
  returns every database the store has at least one ref for
  (`store.Store.ListRefs`'s keys, sorted). CLI: `offshoot session dbs`.
  SDK: Python `Client.dbs() -> list[str]`, TypeScript
  `Client.dbs(): Promise<string[]>`.
- **Branch/checkpoint metadata and timestamps** (Milestone 3 Task 1):
  - `Ref.Meta map[string]string` (branch-level lineage metadata) and
    per-checkpoint `store.Checkpoint.CreatedAt`/`Meta` — all new, omitempty
    fields, no store schema bump (verified with a round-trip + old-ref/
    old-checkpoint decode test mirroring the existing TTL-fields test).
  - `ops.Workspace.Fork`/`Checkpoint` gain a `meta map[string]string` param
    (`nil` = none), capped at the ops layer via the new `ops.ValidateMeta`
    (at most 32 keys, keys ≤ 64 bytes, values ≤ 512 bytes — Global
    Constraints; clear errors naming the exact limit hit). `Fork`'s meta
    describes the new branch's lineage (`Ref.Meta`); `Checkpoint`'s meta
    describes that one checkpoint (`Checkpoint.Meta`). Every checkpoint-
    creating call site (`Create`'s `init`, `Checkpoint`, `Fork`'s `fork`,
    `Promote`'s `promote`, and a daemon session's named `flush`) now stamps
    `CreatedAt` (RFC3339 UTC).
  - `Rollback`'s kept-checkpoint relocation used to silently drop
    `CreatedAt`/`Meta` when rewriting a checkpoint's epoch to the new
    lineage's `1` — fixed to preserve both.
  - Daemon protocol: `BranchInfo` gains `touched_at` (the ref's activity-
    clock stamp) and `checkpoints_v2` (`[]{name, txid, created_at}`) —
    the existing `checkpoints` (bare names) field is untouched for wire
    compat. `fork` and `flush` ops gain an optional `meta` field; there is
    no separate daemon "checkpoint" op — a live session's named `flush` is
    how its checkpoints are created (parity note: `flush`'s `meta` is
    rejected if `name` is empty — there's no checkpoint for it to attach
    to). All new fields are additive/optional: an old-client-shaped request
    (no `meta`, reading only `checkpoints`) still works against the new
    daemon, pinned by wire-compat tests using hand-written JSON (not just a
    Go zero-value round trip).
  - CLI: `fork`/`checkpoint` gain repeatable `--meta k=v`.
  - SDK parity (Python + TypeScript): `fork`/`Session.flush` accept `meta`;
    `branches()`/`Client.branches` surface `touched_at`/`checkpoints_v2`
    (new `CheckpointInfo`/`Branch` fields).
  - MCP tool metadata exposure is explicitly out of scope for this task —
    `offshoot_fork`/`offshoot_checkpoint` pass `nil` meta; see
    `docs/status.md`.
- **Publish pipeline, prepared and gated** (Milestone 3 Task 7): the SDKs
  are ready to publish; actual publication needs the user to claim the
  `offshoot-db` PyPI name and `@offshoot-db` npm scope first (see
  CONTRIBUTING.md's new Release process section).
  - `.github/workflows/publish.yml`: triggered on `sdk-v*` tags or
    `workflow_dispatch`. Two jobs, PyPI (`pypa/gh-action-pypi-publish`,
    Trusted Publishing/OIDC, `id-token: write`) and npm (`npm publish
    --provenance`, registry auth via `NPM_TOKEN` today — npm's own OIDC
    Trusted Publishing is documented as the future swap). Both gated on
    the `PUBLISH_ENABLED` repository variable: off (the default) runs a
    full dry run — real sdist/wheel + `twine check` + wheel install/import
    test; real `npm pack` tarball + install/import test — everything short
    of the upload step. The same dry-run tier now runs in `ci.yml`'s
    `sdks` job on every PR (`make dry-run-sdks`), so a manifest mistake is
    caught long before a release tag exists.
  - `sdk/python/pyproject.toml` filled out for real publication: readme,
    `project.urls` (Homepage/Repository/Issues/Changelog/Documentation),
    authors, classifiers, SPDX `license = "Apache-2.0"` (no redundant
    `License ::` classifier — current PEP 639 practice), reserved
    `[pytest]` extra ahead of Milestone 3 Task 4's fixture plugin.
    `sdk/python/README.md` added (PyPI landing page).
  - `sdk/typescript/package.json` filled out: `repository`/`bugs`/
    `homepage`, `files` whitelist (`dist`, `README.md` — excludes tests/
    tsconfig from the published tarball, verified with `npm pack
    --dry-run`), `publishConfig.access: public` (required for a scoped
    package to publish non-private), `prepublishOnly` builds `dist/`
    before packing. `sdk/typescript/README.md` added.
  - **Version discipline:** both SDKs publish in lockstep from one
    `sdk-v<version>` tag (not two `sdk-py-v`/`sdk-ts-v` tags) — simplest
    scheme for two SDKs on one wire protocol and one review cadence.
    `sdk/VERSION` is the single source of truth; `pyproject.toml`'s and
    `package.json`'s literal version fields are checked against it by the
    new `scripts/check_sdk_versions.py` (`make check-sdk-versions`, and
    the first step of `make dry-run-python-sdk`/CI); `scripts/
    check_sdk_tag_version.py` checks a pushed tag against the same file.
  - `server.json` (repo root): draft MCP registry manifest built only from
    fields this repo's own MCP docs establish (name/description/version/
    repository/command/args) — not submitted; the exact registry schema
    was deliberately not assumed from outside the repo, see
    `docs/launch/mcp-registry.md` and the TODO row in `docs/status.md`.
  - `docs/launch/langgraph-listing.md`: LangGraph community-integration PR
    text (title, description, listing-table entry) for
    `offshoot.langgraph.ThreadForks` — drafted, clearly marked not
    submitted (blocked on PyPI).
- **Export + read-only historical checkouts** (Milestone 3 Task 2):
  - `ops.Workspace.Export(db, branch, checkpoint, dstPath, force)`
    materializes any checkpoint (or head, when `checkpoint == ""`) to a
    plain SQLite file at `dstPath`, anywhere on the local filesystem, with
    zero ongoing relationship to the store afterward: no `.sum` sidecar,
    no lease. Refuses to overwrite an existing `dstPath` unless `force`.
    Reuses `materializeChainAt`/`materializeAt`'s existing atomic
    temp-file-in-the-destination's-own-directory + rename, so a failed
    export (fetch error, checksum mismatch) never leaves a partial or
    truncated file, and discards the `PostApplyChecksum` that machinery
    now threads through (Task 3) since there is no sidecar to stamp with
    it.
  - CLI: `offshoot export <db>[@branch[@checkpoint]] <out.db> [--force]`
    (`ops.ParseExportTarget` parses the triple-`@` target form).
  - Daemon `export` op: `db`/`branch`/`name` (checkpoint)/`path`
    (destination)/`force`. `path` must be an ABSOLUTE path — refused
    otherwise — per the same-host/same-user unix-socket trust model
    documented in `docs/reference.md`'s new daemon-ops section. Reads the
    branch's last DURABLE state from the store, never a live session's
    checkout: an open session's unflushed writes are NOT included, proven
    directly over the wire (`internal/daemon/export_test.go`'s
    `TestOpExportMissesUnflushedSessionWrites`).
  - `ops.Workspace.CheckoutAt(db, branch, checkpoint, force) (string,
    error)` materializes a NAMED checkpoint (no head alias) into a
    dedicated read-only cache path, `<store-root>/checkouts-ro/<db>/
    <branch>@<checkpoint>.db`, `chmod 0444`, distinct from and never
    touching the writable checkout path, its sidecar, or a live capture
    engine's file descriptors on it — safe alongside an open daemon
    session on the SAME branch. A repeat call with `force=false` is a pure
    cache hit (no store access at all — a checkpoint's content is
    immutable); `force=true` re-materializes and re-reads the store.
  - CLI: `offshoot checkout <db>[@branch] --at <checkpoint> --read-only
    [--force]` (must be given together).
  - Daemon `checkout-at` op: same semantics, server-side cache path.
  - SDK parity (Python + TypeScript): `Client.export(db, branch, out_path,
    checkpoint=None, force=False)`, `Client.checkout_at(db, branch,
    checkpoint, force=False)` (TypeScript: `checkoutAt`, options-object
    style matching the rest of that client).
  - README's Resource behavior section gains the read-only-checkout-cache
    paragraph, including the explicit "safe to `rm -rf` the entire
    `checkouts-ro` directory at any time" guarantee.
- **`offshoot.pytest_plugin` fixture plugin** (Milestone 3 Task 4): the
  seed-once-fork-many paved road for pytest-based eval/test suites, shipped
  as the `offshoot-db[pytest]` package extra and registered via a
  `pytest11` entry point (`sdk/python/offshoot/pytest_plugin.py`) — nothing
  to import by hand.
  - `offshoot_daemon` (session-scoped): locates the `offshoot` binary
    (`OFFSHOOT_BIN` env, else `PATH`), starts it on a fresh temp store +
    socket, terminates it at session end; `pytest.skip`s with install
    instructions when no binary is found, rather than failing.
  - `offshoot_db` (session-scoped): a NAMED-SEED FACTORY,
    `offshoot_db(name="default", seed=None)`. The first call for a name
    creates database `eval-{name}`, runs `seed` (a callable given a
    writable sqlite path, or a SQL string run via `sqlite3`), and flushes
    it to a checkpoint named `seed`; later calls for the same name are a
    pure memoization hit. `seed=None` falls back to the new `offshoot_seed`
    ini option (a path to a `.sql` file) — the zero-code default-seed case.
  - `offshoot_fork` (function-scoped): `offshoot_fork(seed_handle=None)`
    forks a fresh, worker-safe-named branch (`t-{worker}-{testname-hash}-
    {n}`, sanitized via the existing `offshoot.langgraph._sanitize`) from
    the seed checkpoint with a TTL (default `1h`, new `offshoot_ttl` ini
    option overrides it), opens a session, and returns an object with
    `.path`/`.client`/`.db`/`.branch`. Teardown closes the session then
    destroys the branch; a destroy failure is a `UserWarning`, never a test
    failure — the TTL is the crashed-run backstop.
  - `offshoot_dump(path) -> str`: `sqlite3 .dump` text — THE way to compare
    two offshoot-materialized SQLite files in a golden-file test. SQLite's
    on-disk bytes are not deterministic for identical logical content
    (page layout, freelist order, vacuum state can all differ) — **never
    byte-compare** two exported/checked-out `.db` files.
  - **xdist stance (locked):** one `offshoot` daemon + temp store PER
    WORKER (pytest has no cross-worker fixture-sharing mechanism), so a
    named seed's cost is paid once per worker, not once total. Measured
    (a `CREATE TABLE` + 200-row seed, macOS arm64): ~80-90ms per worker;
    a 2-worker `pytest -n2` run pays that twice, concurrently (~170ms of
    total seed work, ~85-90ms of wall-clock time). Documented in the
    module's docstring and `sdk/python/README.md`.
  - Found+fixed in passing: a SQL-string seed with no explicit
    `BEGIN`/`COMMIT` used to run every statement as its own autocommit
    transaction under `sqlite3.Connection.executescript` — measured ~1.6s
    for 200 unwrapped `INSERT`s vs. ~17ms wrapped in one transaction
    (~100x). `_run_seed` now wraps a bare multi-statement seed in one
    transaction automatically (a seed that already opens its own
    transaction, or a seed callable, is left alone).
  - Base SDK stays stdlib-only: the `pytest11` entry point is registered
    unconditionally in `pyproject.toml`, but the plugin module
    import-guards `pytest` (a stray direct import without the extra fails
    with an install-instruction message, not a bare traceback) — `pytest`
    is only ever actually imported when pytest itself is already running.
    Verified: `make test-sdks` (the plain-`unittest` suites) passes with no
    pytest installed at all.
  - Tests: `sdk/python/tests/test_pytest_plugin.py` — fixture-logic tests
    (naming, TTL, teardown ordering, factory memoization, skip-when-no-
    binary, destroy-failure-warns) against a directly started daemon, plus
    3 `pytester`-driven smoke scenarios (plugin loads via its entry point,
    fork-per-test isolation actually isolates, an xdist 2-worker run
    passes — the last one is also where the xdist numbers above were
    measured). New `make test-pytest-plugin` target (needs
    `offshoot-db[pytest]` + `pytest-xdist`, wired into `ci.yml`'s `sdks`
    job after `make test-sdks` already proved the base SDK pytest-free).
- **TypeScript testkit** (Milestone 3 Task 5): `sdk/typescript/src/testkit.ts`
  — the vitest/jest counterpart of `offshoot.pytest_plugin`, exported as a
  new subpath, `@offshoot-db/client/testkit` (package.json `exports` map
  added; previously only a bare `main`/`types` pair). Framework-agnostic
  FUNCTIONS, not fixtures — no vitest/jest runtime dependency (this SDK
  stays zero-runtime-deps) and nothing is registered automatically; wire
  them into your own `beforeAll`/`afterEach`.
  - `startDaemon(opts?) -> Promise<DaemonHandle>`: locates the `offshoot`
    binary (`OFFSHOOT_BIN` env, else `PATH` — a clear error naming both
    when neither has one; there is no skip tier here, unlike the pytest
    plugin, since this module has no test-framework integration to skip
    through), starts it on a fresh temp store + socket. Returns
    `{ sock, store, proc, stderrTail(), stop() }`; the caller stops it
    themselves (typically from a top-level `afterAll`).
  - `seedOnce(daemon, {name?, seed}) -> Promise<SeedHandle>`: a NAMED-SEED
    memoization cache keyed on `(daemon, name)`. `seed` is a SQL string, a
    path to a `.sql` file (detected: no newline, ends in `.sql`, and the
    file exists — there's no pytest.ini-style config file here to hold that
    decision separately), or an async `(dbPath) => void` callback. The
    first call for a name creates database `eval-{name}`, runs the seed,
    and checkpoints it `seed`; a later call for the same name with a
    *different* seed (fingerprinted: SQL/path text by content hash,
    callables by identity) throws a clear error rather than silently
    keeping the first one — same mismatch semantics as the pytest plugin's
    `offshoot_db`. Ports `_skip_leading_noise`/`_seed_opens_own_transaction`
    verbatim: a SQL-string seed is wrapped in one transaction unless it
    already opens its own (so a `sqlite3 .dump`'s text — `PRAGMA` before
    `BEGIN TRANSACTION` — works verbatim as a seed, and so a plain
    multi-statement seed doesn't pay one autocommit-transaction-per-
    statement). Since this SDK ships no SQLite driver, seeding (and
    `dump`, below) shells out to the `sqlite3` CLI, same as
    `test/client.test.ts` already does.
  - `forkPerTest(daemon, seedHandleOrName, opts?) -> Promise<ForkedSession>`:
    forks a fresh, worker-safe-named branch
    (`t-{worker}-{sanitized-hint}-{n}`; worker id from `VITEST_POOL_ID` or
    `JEST_WORKER_ID` when present, else `"local"`) from the seed's
    checkpoint with a TTL (default `"1h"`, `opts.ttl` overrides), opens a
    session, and returns `{ path, db, branch, client, flush(name?),
    close() }`. `seedHandleOrName` may be a `SeedHandle` or a plain string
    naming an already-`seedOnce`d name. `close()` closes the session and
    destroys the branch; either failing is a `console.warn`, never a
    throw — call it from `afterEach` with no surrounding try/catch. Each
    `ForkedSession` owns its own connection and teardown independently, so
    (unlike the pytest plugin's shared-per-test `_ForkFactory.teardown()`
    loop) one fork's cleanup trouble can never block another's by
    construction, not by an explicit try/except around each step.
  - `dump(path) -> Promise<string>`: `sqlite3 <path> .dump`'s text — THE
    golden-comparison method. SQLite's on-disk bytes are not deterministic
    across writes with identical logical content — never byte-compare two
    exported/checked-out `.db` files; compare `await dump(a) === await
    dump(b)` instead.
  - Tests: `sdk/typescript/test/testkit.test.ts`, run via `node:test`
    against a real daemon (mirroring the pytest plugin's direct-daemon
    tier): binary resolution (`OFFSHOOT_BIN`/`PATH`/neither), naming,
    TTL default+override, `seedOnce` memoization + fingerprint mismatch, a
    `.dump`-shaped seed, a path-to-`.sql` seed, an async-callback seed,
    fork-per-test isolation, the string-name shorthand, teardown-warn-not-
    throw against a killed daemon, and a golden-file (`dump`, not bytes)
    scenario — plus one integration test wiring `startDaemon`/`seedOnce`/
    `forkPerTest` into `node:test`'s own `before`/`after`/`beforeEach`/
    `afterEach`, the way a real suite would. Wired into the existing
    `make test-ts-sdk` target (it already globs `test-dist/test/*.test.js`)
    — same zero-extra-dependency constraint (`typescript`/`@types/node`
    devDeps only).
  - `make dry-run-ts-sdk`'s tarball exact-match assertion updated to expect
    `dist/testkit.js`/`dist/testkit.d.ts` alongside the existing
    `dist/client.*` files, and a second install-then-`import()` check
    added for the new `@offshoot-db/client/testkit` subpath (not just the
    package root).
- **`offshoot diff`** (Milestone 3 Task 6): `offshoot diff
  <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary]`
  materializes both sides READ-ONLY (never a live checkout, never a lease)
  and either streams `sqldiff`'s output over them or prints a
  `sqldiff`-free table-level row-count summary. CLI-only for this task — no
  daemon op, no SDK parity.
  - `ops.Workspace.MaterializeForDiff(db, branch, checkpoint)
    (DiffSide, error)`: a named checkpoint reuses the existing `CheckoutAt`
    read-only cache (a checkpoint's content is immutable, so this is a
    legitimate cache hit across repeated diffs); a bare head target is
    always freshly `Export`ed to a private temp file, because head moves
    and a head-keyed entry in the checkout-at cache could never be
    idempotently cached the way a checkpoint-keyed one is — this is the
    task's staleness decision, documented on `MaterializeForDiff`'s doc
    comment and pinned by `internal/ops/diff_test.go`'s
    `TestMaterializeForDiffHeadSideAlwaysReflectsANewWrite` (a write
    between two calls against the same head target is visible in the
    second one, never served stale) and the CLI-level
    `TestDiffCLIHeadSideReflectsNewWriteNotStaleCache`. `DiffSide.Close()`
    is a no-op for the cache-backed checkpoint case and removes the temp
    file/directory for the head case.
  - `ops.TableRowCounts(path) (map[string]int, error)` /
    `ops.DiffSummary(leftPath, rightPath) ([]TableDiff, error)`: the
    `--summary` engine, stdlib-`database/sql` + the `mattn/go-sqlite3`
    driver already vendored for `internal/capture`/`internal/ops` (no new
    Go module dependency), opened as
    `file:<abs>?mode=ro&immutable=1` — verified against a real `chmod
    0444` file (`TestTableRowCountsOpensReadOnlyEvenOnA0444File`) that
    SQLite accepts a `mode=ro` URI parameter layered on top of the
    driver's always-requested `SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE`
    flags (SQLite only rejects a mode LESS restrictive than the flags
    argument passed to `sqlite3_open_v2`; `ro` is strictly more
    restrictive). Lists tables from `sqlite_master` (excluding
    `sqlite_%` internals) and counts rows per table on both sides; a
    table present on only one side is reported `added`/`removed` wholesale
    rather than as a row-count delta. The two materialized paths may be
    two entirely different databases — cross-db diff is a legitimate eval-
    comparison shape and is explicitly tested
    (`TestDiffSummaryCLIWorksAcrossTwoDifferentDatabases`).
  - CLI `--summary` output: an aligned `text/tabwriter` table (TABLE/LEFT/
    RIGHT/STATUS columns, `status` one of `same`/`added`/`removed`/
    `changed (+N)`/`changed (-N)`) plus a trailing totals line.
  - Default mode requires the separate `sqldiff` binary (NOT included by
    installing plain `sqlite3` on every platform) and fails with a clear,
    per-OS-hinted error naming the install path when it's missing, plus
    `--summary` as the `sqldiff`-free alternative — found and fixed a
    wrong hint mid-task: a real local Homebrew install of the general
    `sqlite` formula (keg-only, 13 files) confirmed it does NOT include
    `sqldiff`; Homebrew ships `sqldiff` as its OWN separate formula
    (`brew install sqldiff`), corrected in both the error text and
    `docs/diff.md`. The Debian/Ubuntu hint (`sqlite3-tools`) is verified
    against a real `ubuntu:24.04` container's `apt-cache show
    sqlite3-tools` output (lists `/usr/bin/sqldiff`), and
    `.github/workflows/ci.yml`'s Linux job now installs that package
    alongside plain `sqlite3` so `offshoot diff`'s real-`sqldiff` CLI
    tests (`TestDiffCLISqldiffPresentStreamsOutput`,
    `TestDiffCLIIdenticalSidesProduceNoSqldiffOutput`) exercise the actual
    binary in CI instead of permanently skipping — they gate on
    `exec.LookPath("sqldiff")` the same way `requireSQLite3` gates on
    `sqlite3` itself, so a machine without it still skips cleanly.
  - `docs/diff.md`: the command, the raw by-hand `export`-twice-then-
    `sqldiff` recipe, the staleness rule, and a link to the FAQ's
    [no-merge stance](docs/faq.md#why-no-merge) — `offshoot diff` is a read
    tool; it never resolves anything.

### Fixed

- Settling-flush checksum-compare suppression (Milestone 2 follow-up):
  `rebaseline`'s first call now skips a session's mandatory startup settling
  flush when BOTH the checkout `Open` received was already proven
  byte-identical to the branch's head at the moment `Open` checked AND the
  LTX checksum recorded in that same checkout's `.sum` sidecar exactly
  matches what the checkout actually contains once the session's real
  startup rebase finishes running — the second check catches a write
  landing in the window between `Open` returning and its own startup
  rebase actually finishing, which the first check alone cannot see.
  Reading the checksum out of the local sidecar, rather than fetching it
  fresh from the store, costs `Open` no extra store call at all — critically,
  no download of the head object itself, which a full snapshot can be
  (post-Create, post-Fork, every 16th flush, and permanently for a
  read-only branch that never flushes, exactly defeating this
  optimization's own purpose). `ops.Checkout`/`Checkpoint`/`Rollback`/
  `Promote` all now record this checksum when they stamp a sidecar
  (`sumRecord.PostApplyChecksum`, `ltxio.EncodeSnapshot`/`MaterializeChain`
  now return it as a byproduct of work they already do); a sidecar that
  never recorded one settles exactly as before ("fail toward settling"). A
  read-only daemon session reopened against an unmodified checkout no
  longer uploads a full snapshot (previously measured 541.9MB at a 512MB
  db) for doing nothing. A first-ever open, or one against a dirty/stale
  checkout, still settles exactly as before.
- Sidecar refresh on clean Close (Milestone 2 follow-up): `Session.Close`
  now re-stamps the checkout's `.sum` sidecar — including its LTX checksum
  (the row above is what reads it back on the next `Open`) — when the
  close is provably clean (no session error, nothing left unflushed, at
  least one flush succeeded, the branch head hasn't moved past what was
  flushed, the replica was never rebuilt by anything beyond its own
  startup rebase, and — the stamped hash itself — the capture engine's own
  post-shutdown fingerprint, persisted only once its shutdown fully
  verified the checkout's WAL was cleanly folded in, is reused directly
  rather than independently re-derived), so the next `Open`/`Checkout`
  against the same db@branch clean-skips instead of re-materializing —
  restoring, for the daemon-reopen pattern, the disk/descriptor win
  Milestone 2 Task 1 already established for `Checkout`/`Checkpoint`/
  `Rollback`/`Promote`. Ledgered as a documented tradeoff, not a
  regression: a clean-and-current checkout is now served without chain
  validation across a session's clean close too, not only across those
  four `ops` entry points (see docs/status.md).

## [0.1.1] - 2026-08-06

Milestone 2: safe defaults for an unattended agent. The daemon now ships
work on its own cadence instead of relying on an explicit `flush`, forks and
`session.Open` are fast on the common single-snapshot-chain path (measured,
before and after, in `docs/benchmarks.md`), MCP rides an already-open daemon
session for live capture and now TTLs its own forks by default, `status`
answers "which branch is behind and why," and `docs/status.md` /
`docs/reference.md` / this file are the honest accounting of what shipped,
what's merely "shipped" (unverified in some dimension), and what was
deliberately deferred with a stated reason rather than just left undone.
Release tag lands after this branch merges.

### Added

- Fast-path fork (Task 6a, local half): `ops.Workspace.Fork`/`Rollback`/
  `Promote` now copy the source checkpoint's snapshot object directly,
  instead of materializing it to a temp file and re-encoding a fresh
  snapshot, whenever the checkpoint's chain (`store.Chain`) resolves to
  exactly one member and that member is a snapshot — the common case for an
  at-rest fork. New `internal/ops/reflink` package backs this on the local
  backend: a filesystem clone (`clonefile(2)` on darwin, the `FICLONE`
  ioctl on Linux via the new `golang.org/x/sys` dependency) when the
  filesystem supports it, silently falling back to a plain byte copy
  otherwise. `store.Backend` gains `CopyObject(dst, src string) error`; the
  local backend implements it (temp+rename for atomicity, matching its
  existing write pattern), and S3 returns the new `store.ErrCopyUnsupported`
  sentinel unconditionally for now — S3 server-side `CopyObject` (gated to
  objects ≤5GB) is a separate follow-up (Task 6b), so `Fork` against S3
  takes exactly its pre-existing slow path, unchanged. Falls back to the
  slow path automatically whenever the precondition doesn't hold (a branch
  that has been flushed through a daemon session's segment cadence past its
  last snapshot) or a backend can't perform the copy — never a hard
  failure. Measured 512MB fork 2.87s → 198ms locally on APFS (~14.5x;
  ~9.3ms for the copy itself, isolated from a separate pre-existing O(size)
  check — see docs/benchmarks.md), and a real, if smaller, win even without
  clone support (a Linux container without `FICLONE`: 3.02s → 513ms).
  `internal/store/storetest`'s shared conformance suite gained a
  `CopyObject` round-trip + `ErrNotFound` subtest that skips (rather than
  fails) against a backend returning the unsupported sentinel, so it
  already covers local, the in-process S3 fake, and the MinIO-gated
  real-provider suite.

- Fast-path fork (Task 6b, S3 half): `store.S3.CopyObject` now issues a
  real server-side `CopyObject` API call (a `PUT` carrying
  `X-Amz-Copy-Source`, no request body) instead of returning the
  `ErrCopyUnsupported` sentinel unconditionally — `ops.Fork`'s fast path
  (Task 6a) now fires against an S3 backend too, for any single-snapshot
  checkpoint at or under S3's 5GB single-request `CopyObject` limit.
  `CopyObject` HEADs the source first (one request, not a download) and
  returns `ErrCopyUnsupported` for anything over that limit — the same
  fallback signal Task 6a already wired `Fork` to treat as "use the slow,
  materialize-and-re-encode path" — rather than implementing the
  multipart `UploadPartCopy` API this backend doesn't support. The shared
  `storetest.RunConformance` `CopyObject` subtest (added in Task 6a) now
  runs for real against S3 instead of skipping on the sentinel, covering
  both the in-process fake and the MinIO-gated real-provider suite
  (`make test-s3`); `storetest.FakeS3` gained `CopyObject`-request
  handling (it previously had no concept of the header-only, bodyless
  copy request and would have silently overwritten the destination with
  an empty body) and a `SetSizeOverride` test hook so the 5GB gate can be
  exercised without allocating a real multi-gigabyte object. Measured
  (MinIO-local, not an AWS claim; `make bench-s3`) 512MB fork over S3
  4.57s → 1.03s (~4.4x) and 64MB 536.9ms → 153.0ms (~3.5x) — see
  docs/benchmarks.md.

- Benchmarks: `internal/ops/fork_bench_test.go` measures `Fork`'s current
  slow path (`copySnapshotToNewLineage` materializes the source checkpoint
  and re-encodes a fresh snapshot for the child lineage — O(size)),
  `Checkout`'s clean-skip fast path (still O(size): it still quiesces and
  SHA-256-hashes the whole file, just skips the rebuild), and
  `session.Open`'s latency, all against a shared 64MB/512MB size table (plus
  a `size=4GB` case, skipped by default under `-short`) so before/after
  numbers read off identical subtest names once Task 6's fast path lands.
  `BenchmarkSessionOpen` also measures, once per size, the stored size of a
  session's settling flush — the full snapshot every daemon session uploads
  on its first auto-flush tick, even read-only — confirming that cost is
  O(size) rather than fixed. `make bench` runs the default sweep locally;
  `make bench-s3` runs the same benchmarks against a real MinIO container.
  Measured baselines, method, and machine description (host + a Linux
  container) are recorded in `docs/benchmarks.md`.

- MCP rides a running daemon: `offshoot_checkpoint`, `offshoot_fork`, and
  `offshoot_checkout` each probe the daemon fresh on every call (never
  cached — the daemon may start after the MCP server does) and take a
  live-capture path when one is up, falling back silently to exactly
  today's at-rest behavior otherwise. `offshoot_checkpoint` on a branch
  with an open daemon session flushes it live (the daemon's `flush` op —
  no quiesce, no full-snapshot re-encode); `offshoot_fork` routes through
  the daemon's `fork` op whenever a daemon is up, which flushes an open
  source session first so an unflushed write always lands in the new
  branch; `offshoot_checkout` on a branch with an open session returns
  that session's own live checkout path instead of materializing a
  separate at-rest copy — but not a FENCED session's path: a session that
  lost its lease (e.g. to another writer) stays listed until closed, so
  `offshoot_checkout` checks its health and falls back to at-rest with a
  warning naming the session's error rather than handing over a path
  nothing is capturing anymore. No MCP tool opens a session itself — the
  good path requires a harness (the SDKs, `offshoot session open`, or your
  own loop) to have opened one already; without that, MCP checkpoints stay
  at-rest even with a daemon running. `offshoot mcp` gains `-socket PATH`
  (default: the same derivation `offshoot serve` uses for the store), so
  an MCP server and a daemon started against the same store agree on
  where to look without either hardcoding the path. `offshoot_rollback`,
  `offshoot_promote` (on its target), and `offshoot_destroy` now refuse
  (rather than silently fencing a live daemon session) when the daemon has
  any session — healthy or fenced — open on the affected branch: those
  ops repoint or delete a branch's ref outright at rest, bypassing the
  daemon entirely, so without the refusal they could clear a lease or
  repoint storage out from under a session the daemon still believes it
  owns.

- MCP forks expire by default: `offshoot_fork` gains a `ttl` argument (a Go
  duration string, or `"none"` for a branch that never expires) and falls
  back to `offshoot mcp -default-ttl` (default `24h`) when `ttl` is
  omitted — an explicit `ttl` on the call always wins, and `ttl:"none"`
  always yields no TTL even under a configured default. `-default-ttl 0` or
  `-default-ttl none` disables the default entirely. The fork tool's
  response now echoes the TTL it applied and, when there is one, the
  computed expiry timestamp, so both land in the agent's own transcript.
  Reaping still requires a running janitor (`offshoot serve`) — a
  daemonless `offshoot mcp` setup only sweeps expired branches when
  `offshoot gc` is run by hand, and both the tool's Description and the
  README now say so.

- Background flush: `session.Options.FlushEvery`, when > 0, flushes a
  session automatically on that cadence so an agent that never calls `Flush`
  still gets bounded data loss on crash (library default stays `0`, manual
  only). `Session.LastFlush()`/`LastFlushErr()` expose the most recent
  successful flush and the most recent automatic-flush failure — a failure
  is recorded and retried next tick (never one already superseded by a
  fresher success), never kills the session, and a session-closed race never
  surfaces as a spurious flush error. `offshoot serve` gains `-flush-every`
  (default `30s`, `0` disables, negative rejected as a usage error) and
  applies it to every session it opens — the daemon ships work on a cadence
  by default, the safe default lives at that boundary rather than in the
  library primitive. An idle tick (nothing committed via a captured
  transaction, AND no rebase, since the last successful flush) does nothing
  at all — no object write, no ref write — rather than uploading a pointless
  full snapshot every `SnapshotEvery` ticks forever; the rebase half of that
  check matters because a rebase's checkpoint can fold a real commit into
  the baseline without it ever passing through ordinary WAL capture, so
  every session performs one settling flush shortly after its startup
  rebase lands, even if the agent never writes anything.

- Observability (Milestone 2, Task 7): `status` now answers "which branch is
  behind and why." `capture.Engine` gains `Lag()`, WAL bytes a writer has
  committed but this engine's replica has not yet applied — tracked via a new
  atomic offset field (the same lock-free pattern `Rebased`/`Resumed` already
  use, since the engine has no general mutex covering its reader state) so it
  is safe to call from any goroutine concurrently with `Run`. `SessionInfo`
  (the daemon's "status" op) gains `durable_age` (time since the last
  successful flush, empty if never), `last_flush_at` (RFC3339), `flush_error`
  (the most recent automatic-flush failure, mirroring `LastFlushErr`'s
  clear-on-success semantics exactly — `ErrClosed` is never recorded, same as
  the library-level field it surfaces), and `capture_lag_bytes` (always
  present, 0 is meaningful). `offshoot session status`'s output line gains
  `lag=`, and — when set — `last_flush=`, `age=`, and `flush_error=`.
  Every session state transition (opened; flushed, tagged `kind=manual` or
  `kind=auto`, with its txid; flush-failed, with the error — `ErrClosed`
  races excluded, since a `Close`-in-progress isn't an operational failure;
  fenced, with the terminal cause; closed) now writes one structured
  `offshoot: session: db@branch: event key=value ...` line to stderr,
  matching the daemon janitor's existing `offshoot: janitor: ...` prefix
  family rather than a second format.

### Changed

- `Checkout` no longer re-materializes a checkout that is already clean at
  the branch's current head (sidecar fingerprint matches the file, and the
  fingerprint's recorded lineage/epoch/txid matches the ref right now); it
  returns the existing file as-is. Every `session.Open` calls `Checkout`, so
  this avoids an O(size) temp-file-and-rename plus a stranded `dbfile`
  descriptor on every open when the checkout hasn't drifted. Modified and stale
  checkouts are unaffected: they still warn and re-materialize exactly as
  before.

## [0.1.0] - 2026-08-05

Initial pre-release. offshoot brings git-like branching to SQLite databases:
create, fork, checkpoint, rollback, and promote — stock SQLite files, your
choice of storage.

### Added

- Core branch operations: `init`, `create` (optionally `--from` an existing
  file), `checkout`, `checkpoint`, `fork` (optionally from a checkpoint, with
  an optional TTL), `rollback --to`, `promote --onto`, `destroy`, `path`,
  `status`, and `gc` for unreachable-lineage collection.
- Two storage backends behind a common `Backend` interface: a local directory
  store and an S3-compatible store (AWS S3, Cloudflare R2, Tigris, MinIO),
  selected via a store spec (`./path`, `file:///abs/path`, `s3://bucket/prefix`).
- Compare-and-swap safety: every branch ref update is a conditional write.
  `OpenBackend` runs a CAS probe against the store at attach time and refuses
  to operate if conditional writes are not enforced, rather than silently
  degrading. Google Cloud Storage's S3-interop API is unsupported for this
  reason; MinIO is verified via the real-provider conformance suite, AWS S3
  and R2/Tigris are expected to pass but not yet run against for real.
  See `internal/store/s3_integration_test.go` / `make test-s3`.
- Leases with epoch fencing: a long-running writer claims a branch with a
  TTL'd lease; acquiring or reclaiming bumps the branch's epoch so a stale
  writer that resumes after losing its lease cannot corrupt the branch — its
  writes land in a superseded, unreferenced prefix instead.
- Daemon mode (`offshoot serve`): holds branch leases and captures every
  committed transaction while a session stays open, removing the
  quiesce-to-checkpoint constraint of at-rest mode. `offshoot session
  open/flush/status/close/shutdown` drive it from the CLI; flushes are
  incremental (only changed pages) with a full snapshot every 16th flush, so
  materializing a branch replays at most one snapshot plus fifteen segments
  (bounded replay).
- TTL reaping: branches can carry a TTL (set at fork time or via `offshoot
  touch`), measured from the last durable write or lease renewal. The
  daemon's janitor (`-reap-every`, `-gc-grace`) reaps expired, lease-free
  branches and periodically GCs tombstoned lineages. Protected branches
  (`main` by default) are never reaped.
- MCP server (`offshoot mcp`): exposes list, checkout, checkpoint, fork,
  rollback, promote, and destroy as MCP tools over stdio, so an agent can
  branch on its own initiative — with the same protected-branch guardrails
  as the CLI.
- Python and TypeScript SDKs (`sdk/python`, `sdk/typescript`): thin,
  dependency-light clients over the daemon's lifecycle API. Not yet
  published to PyPI/npm; import from a checkout of this repo. Exercised
  against a real daemon by `make test-sdks`.
- `offshoot.langgraph.ThreadForks`: a LangGraph checkpointer companion that
  maps each thread to its own offshoot branch, so rewinding and retrying a
  thread forks the database too. See `examples/langgraph-rewind/`.
- `offshoot version` prints the release version plus the Go runtime version
  and GOOS/GOARCH.
