# CI recipes: seed once, fork per attempt

The eval-harness workload lives in CI: seed a database once, run N attempts
in parallel — each against its own disposable fork — and promote the one
that wins. This page is the copy-pasteable version of that loop for GitHub
Actions. Every command is real (see [reference.md](reference.md) for
flag-by-flag detail); the narrative walkthrough of the same pattern with
the pytest/testkit fixtures is [eval-harness.md](eval-harness.md).

Three recipes:

1. [Shared S3 store, matrix of attempts](#recipe-1-seed-once-fork-per-attempt-over-a-shared-s3-store) —
   the full seed → fork-per-job → promote pipeline.
2. [Single job, local store](#recipe-2-single-job-local-store) — the same
   loop with no bucket at all.
3. [pytest fixtures](#recipe-3-pytest-fixtures) — fork-per-*test* instead
   of fork-per-job, using the shipped `offshoot-db[pytest]` plugin.

## Getting the binary in CI

Two options, both used below:

```yaml
      # Option A: release binary (linux_amd64 for ubuntu-latest runners).
      - name: Install offshoot
        run: |
          curl -sSfL -o /tmp/offshoot.tar.gz \
            "https://github.com/sricola/offshoot/releases/download/${OFFSHOOT_VERSION}/offshoot_${OFFSHOOT_VERSION}_linux_amd64.tar.gz"
          tar -xzf /tmp/offshoot.tar.gz -C /tmp
          sudo mv /tmp/offshoot /usr/local/bin/offshoot
          offshoot version

      # Option B: build from source (cgo — needs a C compiler, which
      # ubuntu-latest has).
      - uses: actions/setup-go@v5
      - run: go install github.com/sricola/offshoot/cmd/offshoot@latest
```

Release tarballs are named `offshoot_<tag>_<goos>_<goarch>.tar.gz` and
contain a single `offshoot` binary (that's the release workflow's package
step, verbatim). Pin `OFFSHOOT_VERSION` — see
[stability.md](stability.md) for why pinning matters pre-1.0.

## Recipe 1: seed once, fork per attempt, over a shared S3 store

The store is an S3-compatible bucket every job can reach; the database name
is scoped to the run so concurrent workflow runs never collide. Forks are
copy-on-write — N attempt branches of a G-byte seed add near-zero bytes to
the bucket until an attempt actually writes.

```yaml
name: evals

on: [push, workflow_dispatch]

env:
  OFFSHOOT_STORE: s3://my-eval-bucket/offshoot
  # Credentials come from the AWS SDK's default chain — plain env vars work:
  AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
  AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
  AWS_REGION: us-east-1
  # For MinIO or another S3-compatible endpoint, also set:
  #   OFFSHOOT_S3_ENDPOINT: https://minio.internal:9000
  #   OFFSHOOT_S3_PATH_STYLE: "1"     # MinIO needs path-style
  DB: evals-${{ github.run_id }}
  OFFSHOOT_VERSION: v0.2.9

jobs:
  seed:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install offshoot
        run: |
          curl -sSfL -o /tmp/offshoot.tar.gz \
            "https://github.com/sricola/offshoot/releases/download/${OFFSHOOT_VERSION}/offshoot_${OFFSHOOT_VERSION}_linux_amd64.tar.gz"
          tar -xzf /tmp/offshoot.tar.gz -C /tmp
          sudo mv /tmp/offshoot /usr/local/bin/offshoot

      # First run against a fresh bucket/prefix only. `offshoot init`
      # deliberately fails on an already-initialized store (don't script it
      # unconditionally — see reference.md); this step tolerates exactly
      # that one case and still fails on real errors. The already-there
      # message differs by backend: local says "key exists", S3 reports
      # the manifest write's lost CAS ("compare-and-swap conflict") — the
      # grep accepts both.
      - name: Init store (first run only)
        run: |
          out=$(offshoot init 2>&1) || {
            echo "$out"
            echo "$out" | grep -Eqi "exist|compare-and-swap conflict" || exit 1
          }

      # Build the seed as a plain SQLite file, then import it. The source
      # file is never modified by the import.
      - name: Seed
        run: |
          sudo apt-get update && sudo apt-get install -y sqlite3
          sqlite3 seed.db < tests/seed.sql
          offshoot create "$DB" --from seed.db

  attempts:
    needs: seed
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        n: [1, 2, 3, 4, 5]
    steps:
      - uses: actions/checkout@v4

      - name: Install offshoot
        run: |
          curl -sSfL -o /tmp/offshoot.tar.gz \
            "https://github.com/sricola/offshoot/releases/download/${OFFSHOOT_VERSION}/offshoot_${OFFSHOOT_VERSION}_linux_amd64.tar.gz"
          tar -xzf /tmp/offshoot.tar.gz -C /tmp
          sudo mv /tmp/offshoot /usr/local/bin/offshoot

      # Fork the seed. Copy-on-write: this writes a base pointer, not a
      # copy. The TTL is the cleanup backstop — if this job dies before
      # its teardown runs, the branch reaps itself at the next gc.
      - name: Fork attempt branch
        run: offshoot fork "$DB" "attempt-${{ matrix.n }}" --ttl 2h

      # Materialize a local, writable, stock SQLite file and run the trial
      # against it. Your trial writes with any ordinary SQLite client.
      - name: Run trial
        id: trial
        run: |
          db_path=$(offshoot checkout "$DB@attempt-${{ matrix.n }}")
          ./run-my-trial --db "$db_path" --out score.txt
          echo "score=$(cat score.txt)" >> "$GITHUB_OUTPUT"

      # Persist the attempt's final state back to the store as a named
      # checkpoint. At rest (no daemon) a checkpoint writes a full
      # snapshot — fine for CI; a long-running trial that wants continuous
      # incremental capture runs `offshoot serve` + `session open` instead
      # (see eval-harness.md).
      - name: Checkpoint result
        run: offshoot checkpoint "$DB@attempt-${{ matrix.n }}" result

      - name: Record score
        run: echo "${{ matrix.n }} $(cat score.txt)" > "score-${{ matrix.n }}.txt"
      - uses: actions/upload-artifact@v4
        with:
          name: score-${{ matrix.n }}
          path: score-${{ matrix.n }}.txt

  promote:
    needs: attempts
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          pattern: score-*
          merge-multiple: true

      - name: Install offshoot
        run: |
          curl -sSfL -o /tmp/offshoot.tar.gz \
            "https://github.com/sricola/offshoot/releases/download/${OFFSHOOT_VERSION}/offshoot_${OFFSHOOT_VERSION}_linux_amd64.tar.gz"
          tar -xzf /tmp/offshoot.tar.gz -C /tmp
          sudo mv /tmp/offshoot /usr/local/bin/offshoot

      # Pick the winner and promote it: the target branch is repointed at
      # a new lineage seeded from the winner's head, atomically (a CAS on
      # the ref). --force because main is protected by default. Promote
      # materializes a full copy — fork is free, picking a winner isn't.
      - name: Promote winner
        run: |
          winner=$(sort -k2 -nr score-*.txt | head -1 | cut -d' ' -f1)
          echo "winner: attempt-$winner"
          offshoot promote "$DB@attempt-$winner" --onto main --force

      # Ship the winning state out of the store as a plain SQLite file.
      - name: Export final database
        run: offshoot export "$DB" final.db
      - uses: actions/upload-artifact@v4
        with:
          name: final-db
          path: final.db

      # Losing attempts need nothing: their 2h TTL + the gc job below
      # reap them. Destroy the run's main branch explicitly (destroy is
      # per-branch; --force because main is protected) if you don't want
      # run-scoped DBs accumulating until their attempts have all reaped:
      - name: Teardown run database
        if: always()
        run: offshoot destroy "$DB" --force || true
```

### Cleanup: gc on a cron

TTLs mark branches reap-*eligible*; something still has to run the reaper.
In CI (no long-running daemon with a janitor), that's a scheduled workflow
running `offshoot gc` against the same store:

```yaml
name: offshoot-gc

on:
  schedule:
    - cron: "17 * * * *"   # hourly
  workflow_dispatch:

jobs:
  gc:
    runs-on: ubuntu-latest
    env:
      OFFSHOOT_STORE: s3://my-eval-bucket/offshoot
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
      AWS_REGION: us-east-1
      OFFSHOOT_VERSION: v0.2.9
    steps:
      - name: Install offshoot
        run: |
          curl -sSfL -o /tmp/offshoot.tar.gz \
            "https://github.com/sricola/offshoot/releases/download/${OFFSHOOT_VERSION}/offshoot_${OFFSHOOT_VERSION}_linux_amd64.tar.gz"
          tar -xzf /tmp/offshoot.tar.gz -C /tmp
          sudo mv /tmp/offshoot /usr/local/bin/offshoot

      # Reaps every TTL-expired branch, then two-phase-collects
      # unreachable storage objects. Objects are tombstoned first and
      # deleted only after --grace (default 1h) — an hourly cron with the
      # default grace means an expired attempt's bytes are gone within
      # ~2 hours of expiry.
      - name: gc
        run: offshoot gc
```

Two notes on the storage bill:

- **Grace is a safety window, not a delay knob to zero out.** An object
  re-referenced during its grace window (a fork racing gc) is left alone;
  `--grace 0` makes tombstoned objects eligible on the very next run.
- **Set a bucket lifecycle rule for incomplete multipart uploads**
  (`AbortIncompleteMultipartUpload`). A CI runner killed mid-upload never
  gets to abort its multipart upload, S3 bills abandoned parts
  indefinitely, and offshoot's gc only reasons about completed objects —
  see [operations.md](operations.md#storage-sharing-copy-on-write-forks).

## Recipe 2: single job, local store

No bucket: the store is a directory on the runner, the whole
fork-many-keep-one loop runs in one job, and the winner leaves as an
artifact. Attempts still run sequentially here — use Recipe 1 when you
want them on separate runners.

```yaml
jobs:
  evals:
    runs-on: ubuntu-latest
    env:
      OFFSHOOT_STORE: ./.offshoot
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
      - run: go install github.com/sricola/offshoot/cmd/offshoot@latest
      - run: sudo apt-get update && sudo apt-get install -y sqlite3

      - name: Seed once
        run: |
          offshoot init
          sqlite3 seed.db < tests/seed.sql
          offshoot create evals --from seed.db

      - name: Fork per attempt, run trials
        run: |
          for n in 1 2 3 4 5; do
            offshoot fork evals "attempt-$n" --ttl 2h
            db_path=$(offshoot checkout "evals@attempt-$n")
            ./run-my-trial --db "$db_path" --out "score-$n.txt"
            offshoot checkpoint "evals@attempt-$n" result
          done

      - name: Promote winner, export
        run: |
          winner=$(for n in 1 2 3 4 5; do echo "$n $(cat score-$n.txt)"; done | sort -k2 -nr | head -1 | cut -d' ' -f1)
          offshoot promote "evals@attempt-$winner" --onto main --force
          offshoot export evals final.db

      - uses: actions/upload-artifact@v4
        with:
          name: final-db
          path: final.db
```

The runner's workspace is discarded with the job, so this recipe needs no
gc job at all — the TTLs are just belt-and-suspenders.

## Recipe 3: pytest fixtures

When the "attempts" are your test cases, don't hand-roll the loop — the
shipped pytest plugin (`offshoot-db[pytest]`) does seed-once/fork-per-test
with teardown and TTL backstops built in. The fixtures (real names, from
`sdk/python/offshoot/pytest_plugin.py`):

- `offshoot_daemon` — session-scoped; finds the binary (`OFFSHOOT_BIN` env,
  else `PATH`), starts a private daemon on a temp store, stops it at
  session end.
- `offshoot_db` — session-scoped named-seed factory:
  `offshoot_db(name="default", seed=None)` seeds once per name and
  memoizes; with no `seed` argument it uses the `offshoot_seed` ini option
  (a path to a `.sql` file).
- `offshoot_fork` — function-scoped fork-per-test factory: forks a fresh
  branch from the seed's checkpoint (TTL default 1h, ini-overridable via
  `offshoot_ttl`), opens a session, returns a handle with
  `.path`/`.client`/`.db`/`.branch`/`.flush()`; teardown closes the
  session and destroys the branch.
- `offshoot_dump` — dump-text helper for golden-file comparisons (never
  byte-compare two SQLite files).

```python
# conftest.py needs nothing; installing offshoot-db[pytest] registers the
# plugin. pytest.ini / pyproject.toml:
#   [tool.pytest.ini_options]
#   offshoot_seed = "tests/seed.sql"
#   offshoot_require_binary = true    # CI: fail loudly, never skip silently

def test_attempt(offshoot_fork):
    fork = offshoot_fork()          # fresh branch of the seed, per test
    run_my_trial(fork.path)         # plain SQLite path, any client
    fork.flush("result")            # optional named checkpoint
```

The workflow around it:

```yaml
jobs:
  evals:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
      - run: sudo apt-get update && sudo apt-get install -y sqlite3
      - run: go install github.com/sricola/offshoot/cmd/offshoot@latest
      # offshoot-db is not yet on PyPI; install from the repo
      - run: pip install "offshoot-db[pytest] @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python" pytest-xdist
      - name: Run evals (fork per test, 4 workers)
        run: pytest tests/ -n4
        env:
          OFFSHOOT_BIN: /home/runner/go/bin/offshoot
```

Under `pytest-xdist` each worker runs its own daemon and store and pays
the seed once per worker — deliberate (no cross-process coordination), and
cheap for SQL seeds (measured ~85 ms for a 200-row seed; see the plugin's
own doc comment and [eval-harness.md](eval-harness.md#xdist-per-worker-daemon-the-stance-and-the-number)
for when seed cost times worker count starts to matter). Set
`offshoot_require_binary = true` in CI so a missing binary fails the suite
instead of skipping it green. The TypeScript equivalent
(`@offshoot-db/client/testkit`: `startDaemon`/`seedOnce`/`forkPerTest`/
`dump`) is covered in
[eval-harness.md](eval-harness.md#typescript-the-testkit-module).

## See also

- [eval-harness.md](eval-harness.md) — the tutorial these recipes are the
  CI-shaped extract of, including running this repo's own CI job verbatim.
- [reference.md](reference.md) — every command above, flag by flag.
- [operations.md](operations.md) — gc/grace semantics, the S3 lifecycle
  rule, and what a flush/checkpoint costs.
- [testing.md](testing.md) — why trusting a fork with your CI data is
  reasonable (the durability evidence).
