# The eval-harness tutorial

This is the paved road for the workload offshoot was actually built around:
an agent (or a candidate model, or a migration script) runs against a
private copy of a real database, you check what it did, and you throw the
copy away — hundreds or thousands of times an hour, in CI, without ever
touching the database everyone else is using. offshoot's answer is
seed-once-fork-many: checkpoint a database once, then `fork` it in
milliseconds per attempt instead of copying gigabytes of files or standing
up a fresh Postgres per test.

This doc is the full story for Python (the `offshoot.pytest_plugin` fixture
plugin) and TypeScript (the `testkit` module), end to end: install, seed,
fork-per-test, inspect, export, clean up. Every command below was actually
run against a real `offshoot` binary and a real pytest/node:test process
while writing this doc — nothing here is aspirational. If you find a command
that doesn't work as shown, that's a doc bug; file an issue.

If you already know offshoot and just want the fixture reference,
`sdk/python/README.md`'s "pytest fixture plugin" section has the condensed
version (install line, fixture signatures, the xdist number) — this doc is
the fuller tutorial that superseded it as the primary teaching surface; the
README section stays as the PyPI-landing-page-sized summary and now points
here for the walkthrough.

## Install

offshoot ships as one static-ish Go binary (cgo for the SQLite driver) plus
thin SDKs. Prebuilt binaries are published on the project's
[GitHub releases](https://github.com/sricola/offshoot/releases) page for
each tagged release — check there first if you'd rather not build from
source.

```
git clone https://github.com/sricola/offshoot
cd offshoot
go build -o offshoot ./cmd/offshoot
```

Requires Go 1.25+, cgo, and a C toolchain (already on most macOS/Linux dev
machines). Put the resulting `offshoot` binary on `PATH`, or point
`OFFSHOOT_BIN` at it — both the pytest plugin and the TS testkit look there
first, then fall back to `PATH`.

For the Python pieces in this tutorial:

```
pip install "offshoot-db[pytest] @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python"
```

The base `offshoot-db` package is stdlib-only; the `[pytest]` extra pulls in
`pytest>=7` (only that — the plugin itself adds no other dependency). For
`pytest-xdist` parallelism (below), also `pip install pytest-xdist`.
`offshoot-db` and `@offshoot-db/client` aren't on PyPI/npm yet in this
prerelease — see [docs/status.md](status.md)'s publish-pipeline row; in the
meantime `pip install -e "sdk/python[pytest]"` / `npm install
/path/to/sdk/typescript` from a checkout works identically, and is what this
tutorial itself uses. (The TS package's `prepare` script builds its `dist/`
during that directory install — `dist/` isn't checked into git, so if you
install with `--ignore-scripts`, run `npm install && npm run build` inside
`sdk/typescript` yourself first.)

## The shape of it

Three fixtures carry the whole workflow:

- `offshoot_daemon` (session-scoped) — a private `offshoot serve` on a
  throwaway store, started once per pytest session (once per **worker**
  under xdist — see below) and torn down at the end.
- `offshoot_db` (session-scoped) — a **named-seed factory**. First call for
  a name seeds a database and checkpoints it; every later call for that
  same name is a cache hit. The common case (one seed for the whole suite)
  needs zero fixture code at all — see the ini option below.
- `offshoot_fork` (function-scoped) — forks a fresh, isolated branch from a
  seed's checkpoint for **this one test**, opens a session on it, and
  destroys it on teardown (TTL as the backstop if teardown itself never
  runs — a crash, a `kill -9`, a CI runner that gets yanked).

None of this is exotic: `offshoot_fork` calls `offshoot_db()` for you when
you don't pass a seed handle, and `offshoot_db`'s default seed is whatever
`pytest.ini`'s `offshoot_seed` option names. That's the whole zero-code
path.

## Quickstart: a real pytest project

Everything in this section is copy-pasteable and was actually run to write
this doc — a scratch project, `pytest.ini`, a seed file, four tests, one
plain run and one `-n2` xdist run.

```
mkdir eval-harness-tutorial && cd eval-harness-tutorial
mkdir tests
```

`pytest.ini` — the zero-code default seed:

```ini
[pytest]
offshoot_seed = tests/seed.sql
```

`tests/seed.sql`:

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, credits INTEGER);
INSERT INTO users (name, credits) VALUES ('ada', 100);
INSERT INTO users (name, credits) VALUES ('grace', 100);
```

`tests/test_agent.py`:

```python
import sqlite3

from offshoot.pytest_plugin import offshoot_dump


def test_agent_spends_credits(offshoot_fork):
    forked = offshoot_fork()  # forks from the ini-configured default seed
    conn = sqlite3.connect(forked.path)
    conn.execute("UPDATE users SET credits = credits - 10 WHERE name = 'ada'")
    conn.commit()

    row = conn.execute("SELECT credits FROM users WHERE name = 'ada'").fetchone()
    assert row[0] == 90


def test_agent_run_is_isolated_from_other_tests(offshoot_fork):
    # Every test gets its OWN fork of the same seed checkpoint — this test
    # never sees test_agent_spends_credits' write above, even though both
    # forked from the same 'seed' checkpoint.
    forked = offshoot_fork()
    conn = sqlite3.connect(forked.path)
    row = conn.execute("SELECT credits FROM users WHERE name = 'ada'").fetchone()
    assert row[0] == 100
```

Run it (`OFFSHOOT_BIN` points at the binary built above; the plugin finds it
on `PATH` too if it's installed there instead):

```
OFFSHOOT_BIN=/path/to/offshoot python3 -m pytest -v
```

Real output:

```
============================= test session starts ==============================
plugins: xdist-3.8.0, offshoot-db-0.1.0
collecting ... collected 2 items

tests/test_agent.py::test_agent_spends_credits PASSED                    [ 50%]
tests/test_agent.py::test_agent_run_is_isolated_from_other_tests PASSED  [100%]

============================== 2 passed in 0.18s ===============================
```

That's the whole loop: two tests, two independent forks of the same seeded
`users` table, no shared state, no cleanup code written by hand.

## The named-seed factory

`offshoot_db` isn't limited to the one ini-configured default. Call it
directly with a `name` to get an additional, independently-cached seed —
useful when one test file needs an empty-table baseline and another needs
the full fixture data, without reseeding either on every test:

```python
def test_named_seed_factory_is_separate_from_default(offshoot_db, offshoot_fork):
    empty = offshoot_db(
        name="empty",
        seed="CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, credits INTEGER);",
    )
    forked = offshoot_fork(empty)
    conn = sqlite3.connect(forked.path)
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    assert count == 0  # the 'empty' seed has no rows, unlike the default seed
```

`seed` can be a callable `(path) -> None` (open the path yourself, do
anything you want, including something that isn't plain SQL — populate it
from a fixture-data generator, run migrations, whatever), a SQL string, or a
`.dump`-shaped SQL string (the exact text `sqlite3 <file> .dump` produces —
`offshoot_db`'s seed runner detects and handles the `PRAGMA`-before-
`BEGIN TRANSACTION` shape correctly, so a real database's dump works
verbatim as a seed). A second call for the same `name` with a *different*
seed raises, rather than silently keeping whichever one happened to run
first — that mismatch is almost always a bug (two test files disagreeing
about what "the empty seed" means), not something to paper over.

Added to `tests/test_agent.py` and re-run (both tests above plus this one,
`-v`):

```
tests/test_agent.py::test_agent_spends_credits PASSED                    [ 33%]
tests/test_agent.py::test_agent_run_is_isolated_from_other_tests PASSED  [ 66%]
tests/test_agent.py::test_named_seed_factory_is_separate_from_default PASSED [100%]

============================== 3 passed in 0.27s ===============================
```

## xdist: per-worker daemon, the stance and the number

`pytest-xdist` runs your suite across multiple worker **processes**, and
pytest has no built-in way to share a fixture's value across those
processes. So `offshoot_daemon` being "session-scoped" means session-scoped
**per worker**: each xdist worker gets its own `offshoot serve`, its own
temp store, and therefore its own copy of every named seed. This is a
deliberate design choice (documented in the plugin's module docstring, not
a compromise found after the fact):

- No cross-process coordination needed — no lockfile, no "which worker seeds
  first" race, no shared daemon address to agree on.
- One worker's daemon dying never takes another worker's tests down with it.
- The cost: seed work is **not shared** across workers. With N workers, a
  seed's setup cost is paid N times, not once — though workers seed
  concurrently, not serially, so wall-clock time scales much better than
  total CPU time does.

The measured number, from this repo's own smoke test (a `CREATE TABLE` +
200-row `INSERT` seed run as one transaction, macOS arm64): **~80-90ms per
worker** to seed and checkpoint; a 2-worker run pays that twice
(~170ms of total, redundant seed work) but only **~85-90ms of wall-clock
time**, because the two workers seed concurrently on independent
daemons/stores. If your seed is expensive and you're running many workers,
either keep worker count modest for that suite, or seed a shared file
out-of-band once and `create --from` it into each worker's own store (see
[docs/status.md](status.md)'s `create --from` reach row — today that's a CLI
step, not something the fixture wires up for you).

Running the quickstart project's 3 tests with `-n2`:

```
OFFSHOOT_BIN=/path/to/offshoot python3 -m pytest -n2 -v
```

Real output:

```
============================= test session starts ==============================
plugins: xdist-3.8.0, offshoot-db-0.1.0
created: 2/2 workers
2 workers [3 items]

scheduling tests via LoadScheduling

[gw0] [ 33%] PASSED tests/test_agent.py::test_agent_spends_credits
[gw1] [ 66%] PASSED tests/test_agent.py::test_agent_run_is_isolated_from_other_tests
[gw0] [100%] PASSED tests/test_agent.py::test_named_seed_factory_is_separate_from_default

============================== 3 passed in 0.49s ===============================
```

Nothing in your test code changes to get here — the plugin's worker-safe
branch naming (`t-{worker}-{testname-hash}-{n}`) is what keeps two workers
forking from the same seed checkpoint from ever colliding on a branch name.

## Mid-test flush checkpoints

A `ForkedSession` (what `offshoot_fork()` returns) has a `.flush(name="",
meta=None)` passthrough to the underlying session — call it whenever you
want the test's writes so far to be durable and independently inspectable
(via `offshoot checkout --at`, `offshoot export`, `offshoot diff`) without
closing the session or ending the test:

```python
def test_mid_test_checkpoint_then_golden_compare(offshoot_fork, tmp_path):
    forked = offshoot_fork()
    conn = sqlite3.connect(forked.path)
    conn.execute("UPDATE users SET credits = credits + 5 WHERE name = 'grace'")
    conn.commit()

    forked.flush("after-bonus")   # durable now, inspectable by name

    golden = tmp_path / "golden.db"
    src = sqlite3.connect(str(golden))
    src.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, credits INTEGER)")
    src.execute("INSERT INTO users (id, name, credits) VALUES (1, 'ada', 100)")
    src.execute("INSERT INTO users (id, name, credits) VALUES (2, 'grace', 105)")
    src.commit()
    src.close()

    assert offshoot_dump(forked.path) == offshoot_dump(str(golden))
```

Real output, added to the same project (now 4 tests total):

```
tests/test_agent.py::test_agent_spends_credits PASSED                    [ 25%]
tests/test_agent.py::test_agent_run_is_isolated_from_other_tests PASSED  [ 50%]
tests/test_agent.py::test_named_seed_factory_is_separate_from_default PASSED [ 75%]
tests/test_agent.py::test_mid_test_checkpoint_then_golden_compare PASSED [100%]

============================== 4 passed in 0.34s ===============================
```

The checkpoint name (`after-bonus`) is scoped to
the fork's own branch, same as any `offshoot checkpoint`/`session flush`
name — it doesn't collide with another test's checkpoint of the same name
on a different branch.

## Golden assertions: compare `.dump` text, never bytes

**Never `assert open(a, "rb").read() == open(b, "rb").read()` two SQLite
files, and never byte-compare an `offshoot export`ed or `checkout --at`ed
file against a golden `.db` on disk.** SQLite's on-disk representation is
not deterministic for identical logical content — page packing, freelist
ordering, and vacuum state can all differ between two databases that
contain exactly the same rows, so a byte-for-byte comparison is a test that
fails on machine/version/history differences that have nothing to do with
whether your agent did the right thing.

The right comparison is `sqlite3 <path> .dump`'s **text** output, which
normalizes all of that away and describes only the logical content
(schema + rows, in a canonical statement form). Both SDKs ship this as a
one-line helper:

- Python: `offshoot.pytest_plugin.offshoot_dump(path) -> str` — a plain
  function, and also available as a fixture of the same name
  (`def test_x(offshoot_dump): ...`).
- TypeScript: `dump(path) -> Promise<string>` from
  `@offshoot-db/client/testkit`.

Both call the `sqlite3` CLI's `.dump` under the hood — same tool, same
output, same guarantee, in both languages. A quick proof this actually
matters (not run as a pytest test above, but exactly what
`test_offshoot_dump_is_the_right_comparison_not_bytes` in the SDK's own test
suite pins): running `VACUUM` on a SQLite file changes its bytes on disk but
not its `.dump` text — a byte-compare would flag that as a difference; a
dump-compare correctly says nothing changed.

## Export for handoff/debug

`offshoot_fork()` gives you a live, ephemeral branch — perfect for the test
itself, useless once the test process exits (the branch either TTLs out or
gets destroyed at teardown). When you need to hand a failing case to a
teammate, attach it to a bug report, or just poke at it after the test run
is over, `offshoot export` copies a checkpoint out to a plain `.db` file
with zero ongoing relationship to the store — no sidecar, no lease, nothing
else in offshoot ever looks at it again:

```
offshoot export evals@attempt-failed@done /tmp/failed.db
```

```
$ sqlite3 /tmp/failed.db "SELECT * FROM results;"
1|1
2|0
3|1
```

This is exactly `ops.Workspace.Export` under the CLI — see
[docs/reference.md](reference.md)'s `offshoot export` entry for the flag
reference (refuses to overwrite without `--force`; atomic temp+rename so a
failed export never leaves a truncated file). From a
fixture test, call `forked.flush("some-name")` first (above) so the state
you want is durable, then `offshoot export` it from outside the test
process, or use the SDK's `Client.export(...)` directly if you'd rather stay
in-process.

## `offshoot diff`: the failed-vs-passed loop

The daily question this exists for: "attempt-2 passed, attempt-3 failed,
what's actually different?" Two branches, checkpointed, diffed:

```
offshoot create evals
sqlite3 "$(offshoot checkout evals)" "CREATE TABLE results (case_id INTEGER, passed INTEGER);"
offshoot checkpoint evals seed

offshoot fork evals attempt-passed --at seed
offshoot fork evals attempt-failed --at seed

sqlite3 "$(offshoot checkout evals@attempt-passed)" "INSERT INTO results VALUES (1,1),(2,1),(3,1);"
offshoot checkpoint evals@attempt-passed done

sqlite3 "$(offshoot checkout evals@attempt-failed)" "INSERT INTO results VALUES (1,1),(2,0),(3,1);"
offshoot checkpoint evals@attempt-failed done
```

Default mode (needs the separate `sqldiff` binary — see [docs/diff.md](diff.md)
for install hints):

```
$ offshoot diff evals@attempt-passed@done evals@attempt-failed@done
left:  evals@attempt-passed@done right: evals@attempt-failed@done
UPDATE results SET passed=0 WHERE rowid=2;
```

That's the entire answer: case 2 flipped from passed to failed, one row.

`--summary` needs no `sqldiff` at all, but **read it carefully** — it's a
row-count diff, not a content diff. Same data, same command, `--summary`
this time:

```
$ offshoot diff evals@attempt-passed@done evals@attempt-failed@done --summary
left:  evals@attempt-passed@done right: evals@attempt-failed@done
TABLE    evals@attempt-passed@done  evals@attempt-failed@done  STATUS
results  3                          3                          same
1 tables: 1 same, 0 changed, 0 added, 0 removed
```

Both sides have 3 rows in `results`, so `--summary` reports `same` — which
is *true at the row-count level* and *misses the actual regression* (case 2
flipped). This is the exact failure mode `--summary`'s own docs warn about
(row count only, not values); it's a fast triage tool for "did anything
change at all," not a substitute for the default `sqldiff` mode when you
need to know what. Reach for `--summary` first when comparing many
attempts' shapes cheaply; reach for the default mode the moment you need to
know *which* rows moved.

That row-count-only comparison also means `--summary` still catches a
*structural* difference — a whole table present on one side and missing on
the other — even though it can't see value-level changes. One more table,
checked into `attempt-passed` only, to show what that looks like for real:

```
sqlite3 "$(offshoot checkout evals@attempt-passed)" "CREATE TABLE scratch (note TEXT); INSERT INTO scratch VALUES ('local notes, never checked in');"
offshoot checkpoint evals@attempt-passed done2
offshoot diff evals@attempt-passed@done2 evals@attempt-failed@done --summary
```

```
left:  evals@attempt-passed@done2 right: evals@attempt-failed@done
TABLE    evals@attempt-passed@done2  evals@attempt-failed@done  STATUS
results  3                           3                          same
scratch  1                           -                          removed
2 tables: 1 same, 0 changed, 0 added, 1 removed
```

`scratch` exists on the left (`attempt-passed`) and not on the right
(`attempt-failed`) — hence `removed` and the `-` in the right-hand count
column. Swap which side you name first and the same table reports `added`
instead; "removed"/"added" is about which side of *this specific command*
a table is missing from, not a judgment about which attempt is more
"complete."

Both modes materialize read-only (never a live checkout, never a lease), so
running `offshoot diff` alongside a live daemon session on either branch is
always safe. Full walkthrough, the raw by-hand recipe (`export` twice +
`sqldiff` yourself), and the staleness rule for a bare-head (no checkpoint)
target: [docs/diff.md](diff.md).

## TTL hygiene

Every `offshoot_fork()` call sets a TTL on the branch it creates — **1
hour by default**, overridable per-project via the `offshoot_ttl` ini
option. That ini option is the only TTL knob: neither the `offshoot_fork`
fixture nor its underlying factory takes a per-call TTL argument — the
factory's only parameter is the seed handle (see the plugin's docstring),
and the ini value is baked in at factory construction. The TTL is the crashed-run
backstop, not the primary cleanup mechanism: `offshoot_fork`'s teardown
closes the session and destroys the branch on every normal test run,
whether the test passed or failed — the TTL only matters when teardown
itself never gets to run at all (the process is `kill -9`ed, the CI runner
is yanked mid-suite, a segfault skips Python's own cleanup).

A destroy failure at teardown is a `UserWarning`, never a test failure —
losing one fork's cleanup shouldn't fail the test that already passed or
failed on its own merits, and shouldn't stop the rest of that test's forks
(or the next test's) from tearing down cleanly. If you see that warning
repeatedly, something's actually wrong (a daemon that died mid-suite, a
lease that got stuck) — don't silence it, chase it.

If a CI run gets killed hard enough that even the daemon process itself
never shuts down cleanly, the store is a throwaway temp directory per
`offshoot_daemon` session (removed with the runner's workspace) — the TTL
matters for a **persistent** store shared across runs (a long-lived daemon,
a real bucket you reuse), not the ephemeral temp store this fixture spins
up for a single pytest invocation. `offshoot gc` (or a running daemon's
janitor, `offshoot serve -reap-every`) is what actually reaps a TTL-expired
branch — see the README's [TTLs and the reaping
janitor](../README.md#ttls-and-the-reaping-janitor) section for exactly
when a branch becomes reap-eligible and what protects it until then.

## CI recipe: the daemon in CI

There's no special "CI mode" — the fixture plugin starts and stops its own
daemon per worker regardless of where it runs, so a CI job needs exactly
three things: the `offshoot` binary on `PATH` (or `OFFSHOOT_BIN` pointing at
it), `sqlite3` on `PATH` (the plugin shells out to it for seeding/dumping),
and the `offshoot-db[pytest]` extra installed. This project's own CI is a
live, running example — `.github/workflows/ci.yml`'s `sdks` job:

```yaml
  sdks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
        with:
          persist-credentials: false

      - uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff # v5.6.0
        with:
          go-version-file: go.mod

      - uses: actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4.4.0
        with:
          node-version: "22"

      - name: Verify system python3 is present
        run: python3 --version

      # No sqldiff: the SDK suites drive the daemon, never offshoot diff.
      - uses: ./.github/actions/setup-sqlite

      - name: make test-sdks
        run: make test-sdks

      # test-pytest-plugin exercises the offshoot.pytest_plugin fixture plugin
      # (the `offshoot-db[pytest]` extra) — its own suite, separate from
      # test-sdks above, because it genuinely needs pytest + pytest-xdist
      # installed (unlike the plain-unittest suites test-sdks just proved
      # pass WITHOUT pytest on PATH — see Milestone 3 Task 4's status.md
      # row for why that separation is load-bearing: the base SDK must stay
      # stdlib-only). Installed editable so its `pytest11` entry point
      # registration is exercised for real, exactly as an installed
      # `offshoot-db[pytest]` would be.
      #
      # A venv, NOT `pip install --break-system-packages` straight onto
      # system python: this runner is non-root/no-sudo, so PEP 668's
      # externally-managed guard plus --break-system-packages makes pip
      # silently fall back to a *user*-site install
      # (~/.local/lib/pythonX.Y/site-packages) — which is only importable
      # via $HOME. That's fine for the outer `make test-pytest-plugin`
      # process itself, but three of its tests use pytest's own `pytester`
      # fixture in *subprocess* mode (`runpytest_subprocess`, needed for a
      # real xdist -n2 run, a real cwd-vs-rootdir check, and a real -W
      # error flag) — and `pytester` deliberately repoints HOME at a
      # throwaway tmp dir for every nested run (see _pytest/pytester.py's
      # Pytester.__init__, "Ensure no user config is used"). With pytest
      # installed only under the REAL $HOME, that nested subprocess can no
      # longer find it at all: it exits with "No module named pytest"
      # before printing a terminal summary, which pytester.py:585 surfaces
      # as "ValueError: Pytest terminal summary report not found". A venv's
      # site-packages path is baked into the interpreter itself, independent
      # of $HOME, so the nested subprocess (same interpreter, same venv)
      # keeps seeing pytest regardless of what HOME it's given — the same
      # reason this never reproduces in a local dev venv.
      - name: Install offshoot-db[pytest] + pytest-xdist
        run: |
          python3 -m venv .venv-pytest-plugin
          .venv-pytest-plugin/bin/pip install --quiet -e "sdk/python[pytest]" pytest-xdist

      - name: make test-pytest-plugin
        run: PATH="$PWD/.venv-pytest-plugin/bin:$PATH" make test-pytest-plugin

      # dry-run-sdks builds the real sdist/wheel and npm tarball, runs twine
      # check, and install-tests both — the same build-verification tier
      # .github/workflows/publish.yml runs in dry-run mode (PUBLISH_ENABLED
      # off) or right before a real upload (PUBLISH_ENABLED on). Running it
      # here, on every PR, is the simplest way to prove the SDKs still
      # build and install correctly without duplicating any of publish.yml's
      # upload logic or adding a second trigger to reason about — see
      # publish.yml's top comment for the fuller rationale. --break-system-
      # packages is needed because Ubuntu's system python3 (PEP 668) refuses
      # a bare `pip install`; this is an ephemeral CI container, not a dev
      # machine, so that's the right call here (contrast: publish.yml's own
      # jobs use actions/setup-python, which isn't externally managed and
      # doesn't need this flag).
      - name: Install SDK build/publish check tooling
        run: python3 -m pip install --break-system-packages --quiet build twine

      - name: make dry-run-sdks
        run: make dry-run-sdks
```

That's `.github/workflows/ci.yml`'s `sdks` job pasted verbatim, comments
included — not an adapted excerpt. A few of its choices are this repo's
policies rather than requirements of the plugin, and your own job can
simplify them: the actions are SHA-pinned with the tag in a trailing
comment (`@v4`/`@v5`-style tags work the same), `persist-credentials:
false` is a hardening default, and `./.github/actions/setup-sqlite` is this
repo's composite action whose ubuntu path boils down to `sudo apt-get
update && sudo apt-get install -y sqlite3` (its long comment in the job
explains why the pytest install uses a venv rather than `pip install
--break-system-packages`: on a non-root runner the latter silently becomes
a user-site install under `$HOME`, which pytest's `pytester`-subprocess
tests then can't see). The `setup-node` step is there because `make
test-sdks` and `make dry-run-sdks` both also exercise the TypeScript SDK
(`test-ts-sdk`, `dry-run-ts-sdk`) in the same job; a pytest-only CI setup
that skips the TS pieces can drop that step and the two `make
dry-run-sdks`-related steps at the end, keeping just checkout, setup-go,
the `sqlite3` install, `make test-sdks`, and the two `pytest-plugin` steps.

That `Makefile` target (`make test-pytest-plugin`) is the exact template for
your own CI job:

```makefile
test-pytest-plugin:
	go build -o bin/offshoot-pytest-plugin-test ./cmd/offshoot
	OFFSHOOT_BIN=$(CURDIR)/bin/offshoot-pytest-plugin-test \
	  python3 -m pytest sdk/python/tests/test_pytest_plugin.py -v
```

Build the binary once (or download a tagged release binary — see Install
above), pin `OFFSHOOT_BIN` to it so the plugin never has to guess where it
is, install `offshoot-db[pytest]` (+ `pytest-xdist` if you run `-n`), and
run pytest normally. No bucket, no external service, no privileged
container — every daemon this fixture starts is local-only, on a temp
store, for the duration of one worker's test session. If your suite runs
against a shared/remote store in other jobs, that's an orthogonal choice
this fixture doesn't make for you — `offshoot_daemon` always starts its own
private, throwaway `offshoot serve` regardless.

The base SDK's own separation is worth copying into your own CI setup:
`make test-sdks` (no pytest installed) runs **before** `make
test-pytest-plugin` installs pytest, proving the base `offshoot-db` package
genuinely has no hard pytest dependency — not just "happens to work because
pytest is already on the image."

## What it costs

The fixture's per-test cost is dominated by two things offshoot already
measures and documents elsewhere, not by anything specific to the plugin
itself:

- **Fork cost** (once per test, via `offshoot_fork()`): since v0.2.0,
  forks are copy-on-write — the common-case fork writes a base pointer
  into the seed's already-durable chain and **zero data objects**, so its
  storage cost is near-zero regardless of database size. (The
  reflink/`CopyObject` numbers in [docs/benchmarks.md](benchmarks.md)
  describe the materialize path — the fork-floor fallback and
  promote/rollback/compact — measured on pre-copy-on-write builds; see
  that page's version note.)
- **Session-open / settling-flush cost** (once per test, when
  `offshoot_fork()` opens its session): see the README's [What a flush
  costs](../README.md#what-a-flush-costs) section, and specifically the
  [settling-flush suppression](benchmarks.md#settling-flush-cost-task-2-controller-decision)
  measurement — a session opened against an already-clean, already-current
  checkout (the common shape right after a fresh fork) skips its mandatory
  first full-snapshot upload entirely.
- **Seed cost**: paid once per named seed **per xdist worker** (not once per
  test) — see the xdist section above for the measured ~80-90ms/worker
  number and why it's per-worker, not shared.

None of this is fixture-specific benchmarking — it's the same fork/flush
machinery every other offshoot workflow pays, measured once in
`docs/benchmarks.md` and reused here rather than re-measured under a
pytest-specific harness that would just be adding noise.

## TypeScript: the `testkit` module

`@offshoot-db/client/testkit` is the vitest/jest/`node:test` counterpart —
same semantics, same names where a JS equivalent exists
(`startDaemon`/`seedOnce`/`forkPerTest`/`dump` instead of
`offshoot_daemon`/`offshoot_db`/`offshoot_fork`/`offshoot_dump`), but
**functions, not fixtures**: nothing registers itself with your test
framework automatically, and the package stays zero-runtime-dependency (no
vitest or jest package pulled in). You wire these into your own
`before`/`after`/`beforeEach`/`afterEach` (or vitest/jest's identically-named
hooks — this module doesn't care which framework calls it).

The one semantic difference worth calling out: there's no `pytest.ini`
equivalent, so there's no zero-code "default seed" the way
`offshoot_seed` gives Python — every `seedOnce()` call names its seed
explicitly. Worker-id detection for branch naming checks
`VITEST_POOL_ID`/`JEST_WORKER_ID` (falling back to `"local"` under plain
`node:test`, which has no worker-pool concept of its own to read).

This section's example is a real `node:test` file, built against the SDK's
own `dist/` build and run with `node --test` while writing this doc:

```ts
import { test, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { startDaemon, seedOnce, forkPerTest, dump } from "@offshoot-db/client/testkit";

let daemon;
let seed;
let fork;

before(async () => {
  daemon = await startDaemon();
  seed = await seedOnce(daemon, {
    name: "widgets",
    seed: "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER); INSERT INTO widgets (name, qty) VALUES ('bolt', 10);",
  });
});

after(() => daemon.stop());

beforeEach(async () => {
  fork = await forkPerTest(daemon, seed);
});

afterEach(() => fork.close());

test("agent run against a private, isolated copy", () => {
  execFileSync("sqlite3", [fork.path, "UPDATE widgets SET qty = qty - 1 WHERE name = 'bolt';"]);
  const out = execFileSync("sqlite3", [fork.path, "SELECT qty FROM widgets WHERE name = 'bolt';"]).toString().trim();
  assert.equal(out, "9");
});

test("a second test never sees the first test's write", () => {
  const out = execFileSync("sqlite3", [fork.path, "SELECT qty FROM widgets WHERE name = 'bolt';"]).toString().trim();
  assert.equal(out, "10");
});

test("golden assertion compares dump text, not bytes", async () => {
  execFileSync("sqlite3", [fork.path, "VACUUM;"]);
  const golden = await dump(fork.path);
  assert.match(golden, /INSERT INTO widgets VALUES\(1,'bolt',10\);/);
});
```

Run with `node --test` (a plain `.mjs` file needs no vitest/jest install at
all — this is `testkit`'s own zero-framework-dependency point, demonstrated,
not just claimed):

```
$ OFFSHOOT_BIN=/path/to/offshoot node --test testkit-demo.test.mjs
✔ agent run against a private, isolated copy (115ms)
✔ a second test never sees the first test's write (30ms)
✔ golden assertion compares dump text, not bytes (46ms)
ℹ tests 3
ℹ pass 3
ℹ fail 0
```

Under vitest or jest, the only change is which hooks you import
(`beforeAll`/`afterAll`/`beforeEach`/`afterEach` from `vitest` or the
globals jest injects) — `startDaemon`/`seedOnce`/`forkPerTest`/`dump`
themselves don't know or care which framework is calling them. See
`sdk/typescript/README.md`'s testkit section for the vitest-flavored version
of the same wiring, and `sdk/typescript/test/testkit.test.ts` for the SDK's
own `node:test`-based suite (22 tests, including an integration test wiring
all four functions into `node:test`'s hooks exactly as above).
