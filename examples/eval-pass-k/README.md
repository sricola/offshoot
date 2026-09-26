# Example: a runnable pass^k eval loop

This is the runnable companion to
[docs/recipes/eval-harnesses.md](../../docs/recipes/eval-harnesses.md)'s
"tau2-style environments and pass^k" section: a self-contained script that
seeds a database once, forks a private copy per attempt, runs a
deterministic stub "agent" against it, grades the result with `offshoot
diff` against a golden reference, and throws the fork away — the same loop
a real eval harness runs against a real model, minus the model.

## Why pass^k, not just pass@1

`pass@1` asks "did this one attempt pass?" Averaged over `k` independent
attempts at the same task, it answers "what fraction of the time does this
succeed?" — a fine number for a dashboard, a bad number for a ship
decision, because it hides exactly the failure mode that matters: an agent
that's right 3 times out of 4 is not "75% reliable" in production, it's
wrong on the next request you happen to send it. `pass^k` (all `k`
independent attempts pass) is the stricter, more honest bar tau2-bench and
similar harnesses use for exactly this reason. Both require identical
initial state across every trial — if trial 3's fork saw trial 2's leftover
writes, a pass or fail would tell you nothing about the task itself, only
about fork order. That's the one property offshoot's `fork` primitive
exists to guarantee cheaply: every trial forks from the same `seed`
checkpoint, so every trial starts from bit-identical state no matter how
many trials ran before it.

## What it does

1. Seeds `evals@main` from [`golden.sql`](golden.sql) — a `tasks` table (5
   tasks) and an `orders` table (5 orders, each with `total` left `NULL`)
   — and checkpoints it `seed`.
2. Builds the golden reference **once**: forks `golden` from `seed`,
   applies the one correct migration (`UPDATE orders SET total =
   ROUND(quantity * unit_price * (1 - discount_pct), 2)`), checkpoints it
   `expected`. Every task's every trial is graded against this same
   `evals@golden@expected` target.
3. For each task, runs `k` trials. Each trial forks `attempt-<task>-<trial>`
   from `seed` — tagged with `meta={"task": ..., "trial": ...}`, the
   concrete thing this example exists to demonstrate: offshoot branch
   metadata lets an eval harness label *why* a branch exists, visible via
   `offshoot branches evals`, without inventing its own bookkeeping — runs
   the migration (correct, or deterministically buggy — see below), flushes,
   and diffs the result against `evals@golden@expected`. A trial passes
   when every table in the diff reports `same`. The fork is destroyed
   either way.
4. Prints each task's `pass@1` (fraction of trials that passed) and
   `pass^k` (did every trial pass), then the total wall time.

One process, no threads — trials run strictly sequentially, and the
printed wall time is the whole run's, start to finish.

## The deterministic failure

Every `unit_price` in `golden.sql` carries a fractional cent (`4.995`,
`249.995`, `2.995`, `89.999`) on purpose: multiplying it out and forgetting
to round produces a value that differs from the correctly-rounded total
every time, never "correct by luck." Task 4's stub agent is deterministically
flaky — every 3rd trial (0-indexed: trial 2) runs the migration *without*
`ROUND(...)`, which the script derives from the correct migration text via
regex rather than hand-writing a second, driftable copy. With the default
`--k 4`, that's 1 miss in 4: `pass@1 = 0.75` (neither 0 nor 1), `pass^k =
0`. Every other task's migration is always correct, so their `pass@1` and
`pass^k` both land on a clean `1.0` — the contrast between task 4's row and
the other four is the whole demonstration.

## Seeding choice

`golden.sql` is applied directly to `evals@main` (via `Client.open` +
`sqlite3.connect(...).executescript(...)` + `flush(name="seed")`), rather
than forking a separate `evals` branch off of `main` first. `Client.create`
already hands you a `main` branch with nothing on it — forking `main` from
itself just to rename it `seed` would be one extra fork for no benefit.
`golden` and every `attempt-*` branch then fork from that `seed` checkpoint
on `main`. See [docs/eval-harness.md](../../docs/eval-harness.md) for the
other seeding shapes offshoot supports (a SQL string, a `.sql` file, a
callable, importing an existing `.db` file via `create --from`) — this
example uses the plain-SQL-file form, the closest match to a real
migration's `.sql` fixture.

## Running it

From the repo root, with a Python 3.10+ interpreter (this repo's system
`python3` may be older — see the root `Makefile`'s `PYTHON` override):

```
make PYTHON=/path/to/python3.10+ example-pass-k
```

That builds `bin/offshoot-bench` and runs `run.py --k 4 --tasks 5` with
`OFFSHOOT_BIN` pointed at it. Equivalently, by hand:

```
go build -o bin/offshoot-bench ./cmd/offshoot
OFFSHOOT_BIN=$PWD/bin/offshoot-bench python3 examples/eval-pass-k/run.py --k 4 --tasks 5
```

No `pip install` needed — `run.py` reaches the SDK straight from
`sdk/python` on `sys.path`, the same way `scripts/bench-isolation.py` does
(the SDK isn't published to PyPI yet; install from `sdk/python` if you want
it in your own project — see [docs/status.md](../../docs/status.md)'s
publish-pipeline row).

Flags: `--k` (trials per task, default 4), `--tasks` (how many of
`golden.sql`'s 5 tasks to run, default 5).

## Real output

This is `make example-pass-k`'s actual stdout, pasted verbatim:

```
$ make PYTHON=/opt/homebrew/bin/python3.14 example-pass-k
go build -o bin/offshoot-bench ./cmd/offshoot
OFFSHOOT_BIN=/Users/sray/gits/offshoot/.claude/worktrees/eval-harness-path/bin/offshoot-bench \
	  /opt/homebrew/bin/python3.14 examples/eval-pass-k/run.py --k 4 --tasks 5
task  description                                                     k   pass@1   pass^k
--------------------------------------------------------------------------------------------
0     Fill in order 1's total, rounded to the cent.                   4     1.00     PASS
1     Fill in order 2's total, rounded to the cent.                   4     1.00     PASS
2     Fill in order 3's total, rounded to the cent.                   4     1.00     PASS
3     Fill in order 4's total, rounded to the cent.                   4     1.00     PASS
4     Fill in order 5's total, rounded to the cent.                   4     0.75     FAIL
--------------------------------------------------------------------------------------------
5 tasks, k=4, wall time: 1.95s

Task 4's pass@1 of 0.75 reads as "mostly fine" -- pass@1 only asks
"what fraction of trials passed?" pass^k ("would EVERY one of k independent
attempts have passed?") calls the same task a flat failure. That gap is the
whole reason to measure pass^k, not just pass@1: an agent that's right most
of the time is still wrong every time you'd actually ship it.
```

Total wall time: under 2 seconds for 21 forks (1 golden + 5 tasks × 4
trials), 20 diffs, and 20 destroys, on a local unix-socket daemon — well
under this example's 30s budget. That per-trial cost is fork **+ open**
(which materializes a checkout — not a bare-fork number; see
[docs/benchmarks.md](../../docs/benchmarks.md#per-test-isolation-primitives-v0211)'s
per-test isolation table for how `open`'s checkout materialization and
settling-flush check dominate over the fork itself) **+ close + diff +
destroy**, all four counted in every trial's share of the number above.
