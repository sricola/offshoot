# Recipe: eval harnesses — tau2-style pass^k, Inspect AI, promptfoo

Every eval harness that grades an agent against a stateful environment
needs the same primitive: an identical, private copy of that environment
per attempt, so one trial's writes never leak into the next trial's grade.
offshoot's `fork` is that primitive — near-instant, copy-on-write, and
disposable — for anything backed by SQLite. This page has three sections:
a runnable example of the pattern in its purest form (no framework at all),
then a sketch each for wiring the same fork-per-attempt loop into Inspect
AI and promptfoo specifically.

## tau2-style environments and pass^k

[tau2-bench](https://github.com/sierra-research/tau2-bench) and similar
harnesses grade an agent by running it against a simulated environment `k`
independent times per task and reporting **pass^k**: the fraction of tasks
where *every one* of the `k` attempts passed, not just at least one
(`pass@1`, averaged). The distinction matters because it's the difference
between "this usually works" and "this always works" — and an agent you'd
actually put in front of a customer needs the second one, not the first.

The loop, independent of any specific harness:

```
seed once  -> checkpoint "seed"
for each task:
    for trial in 1..k:
        fork "attempt-<task>-<trial>" from "seed"   # identical starting state, every time
        run the agent against the fork's checkout path
        grade: diff the fork against a golden reference
        destroy the fork
    pass@1 = passed trials / k
    pass^k = all trials passed
```

**Why identical initial state per trial is load-bearing, not a nicety:** if
trial 3 forked from trial 2's leftover writes instead of the shared seed,
a pass or fail would tell you something about fork *order*, not about the
task — the exact confound pass^k exists to rule out by construction. This
is precisely what `fork` guarantees: every trial branches from the same
`seed` checkpoint, copy-on-write, so trial 40 costs the same as trial 1 and
starts from bit-identical state regardless of what the other 39 did to
their own (separate) forks.

### The runnable example

[`examples/eval-pass-k/`](../../examples/eval-pass-k/) is exactly this loop,
runnable end to end with no LLM in it — a deterministic stub "agent"
standing in for a real model call, so the harness plumbing (fork, diff,
pass@1/pass^k accounting) can be verified without needing an API key or
non-deterministic output. Swap the stub for a real agent call and the loop
around it is unchanged.

It seeds a `tasks` table (5 tasks) and an `orders` table from
[`examples/eval-pass-k/golden.sql`](../../examples/eval-pass-k/golden.sql),
builds a golden reference once (the correct migration, checkpointed
`expected`), then runs `k` trials per task: fork from `seed` — tagged with
`meta={"task": ..., "trial": ...}`, offshoot branch metadata visible via
`offshoot branches evals` — apply the task's migration (correct, or, for
one deliberately flaky task, missing its `ROUND()` on every 3rd trial),
diff against the golden reference, destroy the fork.

Run it (see the example's own README for the full explanation, including
why one task is designed to fail under pass^k while passing 75% of its
individual trials):

```
make example-pass-k
```

Real output, pasted verbatim:

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
5 tasks, k=4, wall time: 2.09s

Task 4's pass@1 of 0.75 reads as "mostly fine" -- pass@1 only asks
"what fraction of trials passed?" pass^k ("would EVERY one of k independent
attempts have passed?") calls the same task a flat failure. That gap is the
whole reason to measure pass^k, not just pass@1: an agent that's right most
of the time is still wrong every time you'd actually ship it.
```

Task 4's row is the point of the whole example: `pass@1 = 0.75` reads as
"mostly fine," and `pass^k = FAIL` says what that number actually means for
an agent you'd ship — one miss in four independent attempts is a hard
failure, not a rounding error on a dashboard.

## Inspect AI

**This sketch is not exercised in CI; it targets Inspect's public `solver`
API as of 2026-09.** Inspect represents a running evaluation as a
`TaskState` threaded through a chain of `@solver`-decorated steps; the
pattern below forks a private database copy at the start of each epoch (one
epoch ≈ one independent attempt at a task, Inspect's own unit for repeated
sampling — the `pass^k` unit above) and hands the checkout path to the task
being solved, so each epoch runs against isolated state without the task
definition itself knowing offshoot is involved:

```python
from inspect_ai.solver import solver, Generate, TaskState
import offshoot

@solver
def offshoot_fork_per_epoch(socket_path: str, db: str, seed_checkpoint: str = "seed"):
    client = offshoot.connect(socket_path)

    async def solve(state: TaskState, generate: Generate) -> TaskState:
        branch = f"epoch-{state.sample_id}-{state.epoch}"
        client.fork(db, "main", branch, from_checkpoint=seed_checkpoint,
                    meta={"task": str(state.sample_id), "epoch": str(state.epoch)})
        session = client.open(db, branch)
        try:
            # Hand the live checkout path to whatever the task under
            # evaluation reads/writes as its database.
            state.metadata["db_path"] = session.path
            state = await generate(state)
        finally:
            session.close()
            client.destroy(db, branch)
        return state

    return solve
```

Grading (a separate `scorer`, not shown) diffs the now-closed fork's last
durable state against a golden checkpoint the same way the runnable example
above does, before the branch above destroys it — a scorer needs to run
between `session.close()` and `client.destroy(...)` if it wants
`offshoot diff` to see the fork's final state.

## promptfoo

**This sketch is not exercised in CI; it targets promptfoo's public
`extensions`/hook API as of 2026-09.** promptfoo's `extensionHooks` let a
config wire arbitrary JS into its test lifecycle; `beforeEach` and
`afterEach` are the natural fork/grade-and-destroy pair, run once per test
case (promptfoo's own repeat unit — configure `repeat` in the test suite to
get multiple independent attempts at the same case, the `pass^k` unit
again):

```js
// eval-hooks.js
import { connect } from "@offshoot-db/client";

let client;
const DB = "evals";

export default async function hooks(hookName, context) {
  if (hookName === "beforeAll") {
    client = await connect(process.env.OFFSHOOT_SOCKET);
  }

  if (hookName === "beforeEach") {
    const branch = `case-${context.test.vars.caseId}-${context.test.repeatIndex ?? 0}`;
    await client.fork(DB, "main", branch, {
      from: "seed",
      meta: { case: String(context.test.vars.caseId) },
    });
    const session = await client.open(DB, branch);
    context.vars.dbPath = session.path;   // fed to the prompt/provider under test
    context.test.vars._offshootBranch = branch;
    context.test.vars._offshootSession = session;
  }

  if (hookName === "afterEach") {
    const branch = context.test.vars._offshootBranch;
    const session = context.test.vars._offshootSession;
    await session.close();
    const diff = await client.diff(`${DB}@${branch}`, `${DB}@golden@expected`);
    context.result.namedScores = {
      ...context.result.namedScores,
      offshoot_diff_clean: diff.tables.every((t) => t.status === "same") ? 1 : 0,
    };
    await client.destroy(DB, branch);
  }
}
```

```yaml
# promptfooconfig.yaml
extensionHooks:
  - file://eval-hooks.js
```

Same shape as the Inspect sketch and the runnable example above: fork from
a shared seed before the attempt, diff against a golden reference after,
destroy either way.

## Seeding options, and importing an existing database

All three sketches above (and the runnable example) fork from a `seed`
checkpoint; how that checkpoint gets built is independent of which harness
you're driving it from. offshoot supports the same set of seeding shapes
across both SDKs (see [docs/eval-harness.md](../eval-harness.md)'s "named-
seed factory" section for the pytest-fixture-flavored version of this same
list):

- **A SQL string** — inline `CREATE TABLE`/`INSERT` statements, wrapped in
  one transaction automatically even when the string itself doesn't open
  one.
- **A `.sql` file** — the shape [`golden.sql`](../../examples/eval-pass-k/golden.sql)
  above uses; also accepted as a `.dump`-shaped string (the exact text
  `sqlite3 <file> .dump` produces), so a real database's dump works as a
  seed unmodified.
- **A callable** — `(path) -> None` (Python) or `(path) -> Promise<void>`
  (TypeScript): open the path yourself and populate it however you want,
  including something that isn't plain SQL at all.
- **An existing `.db` file** — via `Client.create(db, from_path=...)` /
  `client.create(db, { fromPath })`, which imports a real SQLite file
  directly rather than replaying SQL against an empty one. The source file
  is never modified — it's copied, the copy is quiesced, and only the copy
  is imported. This is the `--from` path: same underlying operation as the
  CLI's `offshoot create <db> --from <file>`, useful when your seed is
  already a real (or realistically-sized) database rather than something
  you'd want to express as a `.sql` fixture.

Pick whichever shape matches how your own fixture data already lives — a
migration test suite's `golden.sql`-style fixture, a captured production
snapshot as a `.db` file, or a generator function that's easier to write in
code than in SQL.
