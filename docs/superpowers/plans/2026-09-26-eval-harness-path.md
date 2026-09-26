# Eval-Harness Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make "seed once, fork k times, grade, reap" a one-decorator default for eval authors: seed a fixture from an existing `.db` file (not only SQL), reach `create --from` through the daemon and SDKs, ship a runnable pass^k example, publish honest per-test-isolation numbers against the primitives teams use today, and give the three harness ecosystems a recipe.

**Architecture:** The daemon's `create` op accepts an absolute `path` (same-host trust model and HTTP refusal as `export`), calling `ops.CreateFrom`. Both SDKs expose it; both fixtures (`offshoot.pytest_plugin`, `testkit.ts`) accept a SQLite-file seed by detecting the file header, import it with `create --from`, and fork from the imported `init` checkpoint (no session, no flush). A stdlib-only benchmark script measures isolation primitives on the same host and its output lands in `docs/benchmarks.md`; a runnable `examples/eval-pass-k/` computes pass^k end to end; `docs/recipes/eval-harnesses.md` maps the pattern onto tau2-style environments, Inspect AI, and promptfoo.

**Tech Stack:** Go 1.26, Python 3.10+ (SDK, plugin; run with `/opt/homebrew/bin/python3.14` in a venv: `python3.14 -m venv /tmp/offshoot-venv && /tmp/offshoot-venv/bin/pip install -e 'sdk/python[pytest]' pytest-xdist`), TypeScript SDK (node:test), Docker (`postgres:16`, cached) for the Postgres primitive.

**Spec:** `reports/offshoot next features roadmap.md`, Tier 1 item "Weeks 6-8: the eval-harness path end to end, with numbers". Product decisions (PM + staff engineer): (1) the fixtures already seed from SQL/callables; the market gap is "I have a golden `.db`, fork it per trial", so `.db` seeds are the feature, not more SQL plumbing; (2) `create --from` reaches the daemon and SDKs under export's existing path-trust model (absolute path, unix socket only); MCP deliberately does not get it (an agent must not import arbitrary host files); (3) numbers are measured primitive-vs-primitive on one host (offshoot fork, SQLite backup API, file copy, Postgres `CREATE DATABASE ... TEMPLATE`, a fresh Postgres container) and labelled as such — no vendor product claims; (4) the recipes for Inspect AI and promptfoo are sketches labelled "not exercised in CI"; the tau2-style loop is a real runnable example that CI runs.

## Global Constraints

- Go directive `1.26.0`; gofmt clean; `go vet ./...` clean; `go test ./... -count=1` green; both SDK suites green; `make test-pytest-plugin` green (venv above).
- Commit trailers: `Co-Authored-By: Claude <committing model> <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B`.
- Package publication deferred: no PyPI/npm install text; examples install the SDK from the repo path.
- `create` with `path` is refused over HTTP exactly like `export`; the path must be absolute; the source file is never modified (`ops.CreateFrom` already copies).
- A `.db` seed is detected by content (the 16-byte header `SQLite format 3\0`), never by extension alone; its fingerprint is `db:<sha256 of file bytes>`; the seed handle's checkpoint is `init`.
- Every number in docs comes from `scripts/bench-isolation.py` output pasted verbatim with the machine and date; every recipe snippet that is not exercised in CI says so in its first line.

---

### Task 1: Daemon `create` with `path`; SDK `create(..., from_path)`

**Files:**
- Modify: `internal/daemon/protocol.go` (`Request.Path` doc: also `create`), `internal/daemon/server.go` (`opCreate`), `internal/daemon/http.go` (refuse `create` with a non-empty `Path`)
- Modify: `sdk/python/offshoot/client.py` (`create(self, db, from_path=None)`), `sdk/typescript/src/client.ts` (`CreateOptions{fromPath?}`, `create(db, opts={})`)
- Test: `internal/daemon/lifecycle_test.go`, `internal/daemon/http_test.go`, `sdk/python/tests/test_client.py`, `sdk/typescript/test/client.test.ts`

**Interfaces:**
- Daemon: `Request{Op:"create", DB, Path}`; `Path==""` → `ws.Create`; else require `filepath.IsAbs(Path)` and call `ws.CreateFrom(db, Path)`. HTTP: in `handleRPC`, after the `httpForbiddenOps` check, `if req.Op == "create" && req.Path != "" { http.Error(..., "daemon: create with path is not available over HTTP; use the local socket", 400) }`.
- Python: `def create(self, db: str, from_path: str | os.PathLike[str] | None = None) -> None` — resolves `from_path` to an absolute path with `os.path.abspath` and sends `path=`; docstring states the same-host/unix-socket trust and "never modifies the source".
- TS: `interface CreateOptions { fromPath?: string }`; `create(db, opts: CreateOptions = {})` sends `path: opts.fromPath ? path.resolve(opts.fromPath) : ""`.

- [ ] **Step 1: Tests first.** Daemon: `TestCreateFromPathImportsAnExistingFile` — build a SQLite file with `sqliteExec` (one table, 3 rows), `call(Request{Op:"create", DB:"imp", Path: abs})` → OK; `checkout imp` and count rows = 3; the source file's bytes are unchanged (compare sha256 before/after); relative `Path` → error containing "absolute"; `Path` to a non-SQLite file → error. HTTP (`http_test.go`): POST `/rpc` `create` with `path` → 400 with the message; `create` without path over HTTP still works. Python test: write a `.db` with `sqlite3`, `c.create("imp", from_path=p)`, open and count rows; TS: same with `{ fromPath }`.
- [ ] **Step 2: Run to fail; Step 3: implement; Step 4:** `go test ./internal/daemon -count=1 && make test-python-sdk PYTHON=/opt/homebrew/bin/python3.14 && make test-ts-sdk`.
- [ ] **Step 5: Commit** `"daemon, sdk: create --from reaches the daemon and both SDKs (unix socket only)"`.

---

### Task 2: `.db` file seeds in the pytest plugin and the TypeScript testkit

**Files:**
- Modify: `sdk/python/offshoot/pytest_plugin.py` (`_Seed` docs, `_is_sqlite_file`, `_fingerprint_seed`, `_SeedFactory.__call__`, `offshoot_seed` ini help)
- Modify: `sdk/typescript/src/testkit.ts` (`Seed` docs, `isSqliteFile`, `resolveSeed` → `{kind, fingerprint, ...}`, `runSeedOnce`, `SeedOnceOptions` docs)
- Test: `sdk/python/tests/test_pytest_plugin.py`, `sdk/typescript/test/testkit.test.ts`

**Interfaces:**
- Python: `_is_sqlite_file(p: str | Path) -> bool` (exists, is a regular file, first 16 bytes == `b"SQLite format 3\x00"`). In `_SeedFactory.__call__`: if `seed` is a `str` and `_is_sqlite_file(seed)` (or the ini default path is a SQLite file), then `self._client.create(db, from_path=os.path.abspath(seed))` and `handle = SeedHandle(db=db, checkpoint="init", name=name)`; fingerprint `f"db:{sha256(file bytes)}"` (read once); no session is opened. `_run_seed` is untouched. Docstrings: the module header's seed forms gain "a path to an existing SQLite database file".
- TS: `isSqliteFile(p): boolean` (sync `readFileSync` of 16 bytes via `openSync`/`readSync`); `resolveSeed` returns `{ kind: "sql" | "callable" | "dbfile", fingerprint, run?: ..., filePath?: string }`; `runSeedOnce` branches: `dbfile` → `client.create(db, { fromPath })` and `checkpoint: "init"`, no session.
- Both: a SQL string that merely ends in `.db` but is not a file is still treated as SQL (content detection only).

- [ ] **Step 1: Tests.** Python (`test_pytest_plugin.py`, Tier 1 style against a directly started daemon; read the file's helpers first): `test_db_file_seed_imports_and_forks_from_init` — write `golden.db` (2 tables, rows) with `sqlite3`, `factory("golden", seed=str(path))` → handle.checkpoint == "init", forking it yields the rows; second call with the same path is a memo hit; editing the file then calling again raises the mismatch error; a SQL string ending in `.db` that is not a file still runs as SQL (create a table named in it). Also an ini-default test: `_SeedFactory(client, default_seed_path=str(golden))` with no `seed=` uses the file. TS (`testkit.test.ts`): the same four behaviors via `seedOnce(daemon, { name, seed: goldenPath })`.
- [ ] **Step 2-4:** run (fail), implement, `make test-pytest-plugin PYTHON=/tmp/offshoot-venv/bin/python` (create the venv first as in Tech Stack) and `make test-ts-sdk`.
- [ ] **Step 5: Commit** `"fixtures: seed from an existing SQLite file via create --from; fork from init"`.

---

### Task 3: `scripts/bench-isolation.py` and the numbers

**Files:**
- Create: `scripts/bench-isolation.py` (stdlib + `sdk/python` on `sys.path`; uses the `offshoot` binary at `$OFFSHOOT_BIN` or `bin/offshoot-bench`; Docker CLI for Postgres)
- Modify: `Makefile` (`bench-isolation` target: builds the binary, creates the venv if missing, runs the script with `--sizes 10,100 --iters 20`)
- Modify: `docs/benchmarks.md` (new section `## Per-test isolation primitives (v0.2.11)` near the top, after the copy-on-write section), `docs/eval-harness.md` ("What it costs": one paragraph + a pointer to the new section)

**What it measures** (each as median and p95 over `--iters`, per seed size in MB):
1. `offshoot fork + open + close` via the Python SDK against a seeded `eval-bench` database (seed: one table with `size_mb` of random blobs, imported via `create(from_path=)` from Task 1).
2. `offshoot fork` alone (no session): the pure branch cost.
3. `sqlite3.Connection.backup()` of the seed file into a fresh file.
4. `shutil.copyfile` of the seed file.
5. Postgres `CREATE DATABASE trial TEMPLATE seed` inside `docker run -d postgres:16` (seed built with `generate_series` to approximately the same MB via `pg_total_relation_size`), timed with `docker exec ... psql -c`; `DROP DATABASE` between iterations.
6. Postgres cold container per test: `docker run -d postgres:16` until `pg_isready`, then `docker rm -f` (3 iterations only; labelled).
Output: a markdown table (rows = primitive, columns = size, values `median / p95 ms`) plus a caveats block (same host, Docker Desktop VM, no network, offshoot local-directory store). Skip Postgres rows cleanly with a note if Docker is unavailable.

- [ ] **Step 1:** write the script with a `--dry-run` that validates tooling and prints the plan; `make bench-isolation` runs it. **Step 2:** run it for real on this machine and paste the table verbatim into `docs/benchmarks.md` with `## Per-test isolation primitives` prose: what each row is, why the SQLite rows are near-constant in size and the Postgres rows are not, and the honest statement that offshoot's fork adds a daemon round trip and a settling flush check on `open`. **Step 3:** update `docs/eval-harness.md` "What it costs" to cite the section. **Step 4: Commit** `"bench: per-test isolation primitives, measured; docs cite the table"`.

---

### Task 4: Runnable pass^k example and the eval-harness recipe page

**Files:**
- Create: `examples/eval-pass-k/README.md`, `examples/eval-pass-k/run.py` (Python, SDK from `sdk/python`), `examples/eval-pass-k/golden.sql` (builds the seed and the expected end state)
- Create: `docs/recipes/eval-harnesses.md`
- Modify: `site/gen/nav.go` (Guides: `{"docs/recipes/eval-harnesses.md", "eval-harnesses", "Eval harnesses"}` after the frameworks recipe), `Makefile` (`example-pass-k` target), `.github/workflows/ci.yml` (`sdks` job: run `make example-pass-k` after the Python SDK tests — read the job to place it), `README.md` (one line under the eval-harness paragraph pointing at the example and recipe), `docs/eval-harness.md` (pointer)

**`run.py` behavior:** start `offshoot serve` on a temp store (`subprocess`, socket path from `OFFSHOOT_SOCKET`-style env the SDK's tests use; read `sdk/python/tests/test_client.py`'s daemon fixture for the exact spawn pattern), seed `evals` from `golden.sql` (a `tasks` table with 5 tasks and an `orders` table), then for each task run `k` trials: fork `attempt-<task>-<trial>` from `seed`, open, run a stub "agent" that applies the task's migration with a deterministic per-trial failure (e.g. every 3rd trial forgets the ROUND), close, `client.diff("evals@attempt-...","evals@golden@expected")` (the golden branch is built once by applying the correct migration to a fork and checkpointing `expected`), mark pass when every table is `same`, destroy the fork. Print per-task pass@1 and pass^k (all k trials pass) and the total wall time; exit 0. `--k 4 --tasks 5` defaults; runtime target under 30s.

**`docs/recipes/eval-harnesses.md`:** three sections. (1) "tau2-style environments and pass^k": the loop, why identical initial state per trial matters, the runnable example and its real output pasted. (2) "Inspect AI": a `@solver` sketch that forks per epoch through the Python SDK and hands the checkout path to the task; first line: "This sketch is not exercised in CI; it targets Inspect's public `solver` API as of 2026-09." (3) "promptfoo": a JS `extensions` hook sketch (`beforeEach` forks via the TS SDK, `afterEach` diffs and destroys); same disclaimer. Close with the seeding options (SQL, `.sql`, callable, `.db` file) and the `--from` note.

- [ ] **Step 1:** write `run.py` + `golden.sql`; run it; paste real output into the README and the recipe. **Step 2:** `make example-pass-k` and CI wiring. **Step 3:** nav + README pointers. **Step 4: Commit** `"examples: runnable pass^k eval loop; recipe page for tau2-style, Inspect AI, promptfoo"`.

---

### Task 5: Status, reference, changelog

**Files:**
- Modify: `docs/status.md` (`create --from reach` row: shipped-and-tested for daemon + both SDKs + fixtures; MCP deliberately excluded; test names `TestCreateFromPathImportsAnExistingFile`, the HTTP refusal test, `test_db_file_seed_imports_and_forks_from_init`, and the TS twin; "v0.2.11 (unreleased)"), `docs/reference.md` (daemon `create` op gains `path`; parity table row `create --from` → CLI yes / daemon yes (socket only) / SDK yes / MCP no by design; fixture docs mention `.db` seeds), `docs/eval-harness.md` (the named-seed section: `.db` file seeds; remove "CLI-only import path" wording), `sdk/python/README.md` and `sdk/typescript/README.md` if they document `create` or seeds, `CHANGELOG.md` (Unreleased Added: daemon/SDK `create --from`, `.db` seeds, `bench-isolation`, pass^k example, recipe page).
- Verify: `grep -rn 'CLI-only import\|CLI-only import path' docs README.md` prints nothing; `go test ./internal/daemon -count=1 && make check-plugin`.
- [ ] Commit `"docs: create --from everywhere but MCP; .db seeds; isolation numbers; eval recipes"`.

---

## Self-review notes

- Coverage vs spec: seeding wired into fixtures (T2) on top of daemon/SDK reach (T1); recipes with measured numbers (T3, T4); head-to-head table (T3); the "delete your reseed step" story lives in the recipe and README (T4, T5).
- Consistency: `from_path` (Python) / `fromPath` (TS) / wire `path`; handle checkpoint `init` for `.db` seeds in both fixtures; fingerprint prefix `db:` in both.
- Honesty: Postgres rows are the primitive (template clone / cold container), not integresql/pgtestdb/Testcontainers products; the recipe sketches for Inspect/promptfoo are labelled unexercised.
