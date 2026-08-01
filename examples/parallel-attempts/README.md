# Parallel attempts

A runnable version of offshoot's pitch: fork the same database three times,
let three different migrations race against the forks, promote whichever one
actually works, and throw the other two away.

## Run it

    ./run.sh

It builds `offshoot` from this checkout, sets up a `shop` database with an
`orders` table, and takes it from there — no server, no bucket, nothing to
clean up (it runs entirely out of a temp directory).

## What it does

1. Creates `shop` with three orders (`19.99`, `8.70`, `4.35`) and checkpoints
   it as `before-migration`.
2. Forks `shop` to a `pre-migration` branch at that checkpoint, so the
   original state has a permanent home that survives the promote in step 4
   (see "What to look at" below for why this is necessary).
3. Forks `shop` three more times — `attempt-1`, `attempt-2`, `attempt-3` —
   each an instant, storage-independent branch of the same checkpoint. No
   data is copied.
4. Runs a different `total_cents` migration against each fork:
   - **attempt-1** does `total_cents = total * 100` — naive floating-point
     math. `19.99 * 100` isn't exactly `1999` in IEEE754; the column never
     actually becomes an integer, so the attempt fails its own check.
   - **attempt-2** does `DROP TABLE orders` — a catastrophic migration. The
     subsequent check against the (now missing) table fails outright.
   - **attempt-3** does `total_cents = CAST(ROUND(total * 100) AS INTEGER)` —
     rounds before casting, so every row lands on the correct integer cent
     value. It's the only attempt that passes.
5. Picks the first attempt that passed (`attempt-3`) and promotes it onto
   `main` with `offshoot promote shop@attempt-3 --onto main --force`.
6. Destroys the two losing forks and runs `offshoot gc --grace 0s` twice —
   once to tombstone the now-unreachable lineages, once more (a separate,
   later run, per offshoot's grace-period design) to actually delete them.
7. Prints `main`'s data: `total_cents` now holds `1999`, `870`, `435` — the
   correctly rounded values from `attempt-3`, even though `attempt-1` and
   `attempt-2` ran against forks of the exact same starting point.

## What to look at

- **`attempt-2` deletes the whole `orders` table**, and `main` is completely
  unaffected. Forks are storage-independent: destructive experiments on one
  branch cannot touch another, because each branch's writes land under its
  own lineage, not on top of shared files.
- **`attempt-1` "PASS"/"FAIL" is a real correctness check**, not a coin
  flip — the demo validates that `total_cents` actually ended up as an
  integer, so the migration that merely *looks* plausible (fills the column,
  doesn't error) loses to the one that's actually correct.
- **The pre-migration state is one command away** — but only because the demo
  puts it on its own branch first:

      offshoot checkout shop@pre-migration

  `Promote` is "promote-as-fork": it repoints the target branch (`main`) at a
  new lineage seeded from the source's head, and **resets the target's
  checkpoint map** to just the new `promote` checkpoint. That's what makes
  promote safe to force onto a protected branch — it can't leave the branch
  half-updated — but it also means any checkpoint that lived on `main` before
  the promote (`before-migration` included) is gone from `main` the instant
  the promote lands. `offshoot rollback shop --to before-migration` will fail
  with "no checkpoint" once that's happened.

  If you want to keep a state around after a promote, it needs to live on a
  branch of its own — which is exactly what step 2 above does, forking
  `pre-migration` off the `before-migration` checkpoint *before* anything
  gets promoted onto `main`. That fork has its own lineage, so it's
  untouched by what promote does to `main`.
