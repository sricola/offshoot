# langgraph-checkpoint-offshoot

Offshoot-backed checkpointing for [LangGraph](https://github.com/langchain-ai/langgraph),
with **branching semantics**: each LangGraph thread's state lives in an
offshoot-managed SQLite database, so a thread can be **forked** (per attempt,
per eval trial), **rolled back** to a checkpoint, **promoted**, and
**TTL-reaped** when abandoned.

## How it works

`OffshootSaver` subclasses `langgraph.checkpoint.base.BaseCheckpointSaver`,
but does **no checkpoint I/O of its own**. It wraps the stock
[`langgraph-checkpoint-sqlite`](https://pypi.org/project/langgraph-checkpoint-sqlite/)
`SqliteSaver`, pointed at an offshoot **checkout** — the writable SQLite
file the offshoot daemon keeps under continuous capture. LangGraph's own
battle-tested SQLite serialization does the checkpoint I/O; offshoot manages
the file underneath it. No serialization is reimplemented here.

The offshoot value-add is five extra methods, each mapping **1:1** to one
offshoot daemon op:

| saver method               | offshoot op          | what it does                                                        |
|----------------------------|----------------------|---------------------------------------------------------------------|
| `checkpoint(name, meta=..)`| `Session.flush`      | flush the live checkout to a durable (optionally named, optionally metadata-tagged) checkpoint |
| `fork_thread(new, ttl=..)` | `Client.fork`        | fork the branch; returns a **new saver** on the fork's own checkout |
| `rollback(to)`             | `Client.rollback`    | repoint the branch at a named checkpoint; saver reopens in place    |
| `promote(onto)`            | `Client.promote`     | repoint `onto` (e.g. `main`) at this branch's head                  |
| `destroy()`                | `Client.destroy`     | close the saver and delete its branch (protected branches — `main` by default — require `force=True`) |

Looking for the inverse — keep your existing LangGraph checkpointer and
fork only the *application* database your agent's tools write to? That's
`offshoot.langgraph.ThreadForks` in the core SDK
([`sdk/python`](../python/README.md)); this package is for putting
LangGraph's **own thread state** under offshoot.

## Modes

- **`OffshootSaver.session(socket_path, db, branch="main", *, create=True)`
  — shipped.** A live daemon session: the daemon leases the branch and keeps
  the checkout under continuous capture; `checkpoint()` (and
  `fork_thread`/`promote`, which flush internally) make state durable.
  `create=True` (the default) creates `db` in the store first if it doesn't
  exist yet; pass `create=False` to skip that auto-create. Requires
  `offshoot serve` running on `socket_path`.

- **`OffshootSaver.at_rest(store, db, branch)` — stubbed
  (`NotImplementedError`).** CLI-mode (at-rest checkout + explicit
  `offshoot checkpoint`) would require shelling out to the `offshoot` binary
  per checkpoint: the daemon wire protocol has no "commit an at-rest
  checkout" op (`flush` is session-only). A thin correct adapter beats a
  broad fragile one, so only session mode ships in 0.1.0.

## Install

Neither this package nor its `offshoot-db` dependency is on PyPI yet, so
pip can't resolve `offshoot-db` on its own — install it from the repo
first, then this package (no checkout needed for either):

```sh
pip install "offshoot-db @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python"
pip install "langgraph-checkpoint-offshoot @ git+https://github.com/sricola/offshoot#subdirectory=sdk/python-langgraph"
```

You also need the `offshoot` binary with a running daemon:

```sh
offshoot -store ./store init
offshoot -store ./store serve -socket ./offshoot.sock
```

## Worked example: an eval loop (seed → fork per attempt → keep winner)

```python
from typing import TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph_checkpoint_offshoot import OffshootSaver


class State(TypedDict):
    n: int


def build_app(saver: OffshootSaver):
    def step(state: State) -> dict:
        return {"n": state["n"] + 1}

    g = StateGraph(State)
    g.add_node("step", step)
    g.add_edge(START, "step")
    g.add_edge("step", END)
    return g.compile(checkpointer=saver)


config = {"configurable": {"thread_id": "eval-thread"}}

# 1. Seed the thread on main and make the seed durable.
seed = OffshootSaver.session("./offshoot.sock", "evals")   # db "evals", branch "main"
build_app(seed).invoke({"n": 0}, config)
seed.checkpoint("seeded")

# 2. Fork one branch per attempt. Each fork is copy-on-write, gets its own
#    SQLite checkout, and self-reaps after 1h if nobody promotes it.
attempts = [seed.fork_thread(f"attempt-{i}", ttl="1h") for i in range(3)]
seed.close()   # promote repoints main's checkout; main's session must be closed

# 3. Run the graph independently on every attempt — full thread history
#    included, zero interference between attempts.
results = []
for saver in attempts:
    out = build_app(saver).invoke({"n": 1}, config)
    results.append((out["n"], saver))

# 4. Keep the winner: promote its state onto main. The losers just expire
#    (TTL) — or destroy them now.
best, winner = max(results, key=lambda r: r[0])
winner.promote("main", force=True)   # main is protected by default
for _, saver in results:
    saver.destroy() if saver is not winner else saver.close()

# main now holds the winning attempt's full LangGraph thread state.
with OffshootSaver.session("./offshoot.sock", "evals") as main:
    tup = main.get_tuple(config)
    assert tup.checkpoint["channel_values"]["n"] == best
```

Rollback works the same way inside a single thread's life:

```python
saver.checkpoint("before-risky-step")
app.invoke(risky_input, config)
if it_went_badly:
    saver.rollback("before-risky-step")   # thread state exactly restored
```

## Semantics worth knowing

- **Durability is explicit.** LangGraph writes land in the live checkout
  immediately, but nothing is durable in the offshoot store until
  `checkpoint()` runs (`fork_thread` with no `from_checkpoint`, and
  `promote`, flush for you first). A crash loses only writes since the last
  flush.
- **`fork_thread` returns a new, independent saver** with its own daemon
  connection and session. Close/destroy it independently of the parent.
- **`rollback` discards everything after the target** — including unflushed
  live writes — and replaces the saver's inner `SqliteSaver` in place. Keep
  using the same saver object; a graph compiled against it stays valid (no
  recompile needed).
- **`promote` onto a branch with an open session**: close that session
  first (the session owns the checkout promote repoints), same rule as
  offshoot's own CLI.
- **`destroy()` respects branch protection** (same default as the SDK's
  `Client.destroy`): a bare `destroy()` on protected `main` raises; pass
  `force=True` deliberately. Fork branches are unprotected. On an
  already-closed saver, `destroy()` is a silent no-op.
- **Async**: not yet supported. The async `BaseCheckpointSaver` methods
  delegate to `SqliteSaver`'s, which raise `NotImplementedError` pointing
  at `AsyncSqliteSaver`. An async saver (wrapping `AsyncSqliteSaver`) is
  future work.

## Compatibility

Pinned against `langgraph-checkpoint` 4.2.0 / `langgraph-checkpoint-sqlite`
3.1.1 (signatures verified by introspection: `put`, `put_writes`,
`get_tuple`, `list`, `delete_thread`, `get_next_version`, the async
variants, plus the 4.x additions `copy_thread`, `delete_for_runs`, `prune`,
`get_delta_channel_history`, `with_allowlist` — all delegated). Tested with
`langgraph` 1.2.x. Both checkpoint packages are pinned directly in `pyproject.toml`
(`langgraph-checkpoint>=4.2,<5`, `langgraph-checkpoint-sqlite>=3.1,<4`), and
`tests/test_delegation_tripwire.py` re-introspects the installed surface so
an upstream release that adds a public method fails the suite loudly instead
of silently bypassing the inner saver. The full framework is intentionally a
`test` extra rather than a runtime dependency: applications using this adapter
already supply LangGraph, while the adapter itself only imports its checkpoint
interfaces.

## Tests

```sh
python3 -m venv .venv-langgraph
.venv-langgraph/bin/pip install -e sdk/python -e "sdk/python-langgraph[test]"
PATH="$PWD/.venv-langgraph/bin:$PATH" make test-python-langgraph
```

The tests build the real `offshoot` binary (`go build`, or set
`OFFSHOOT_BIN`), start a real daemon, and drive a real compiled
`StateGraph`. If the full `langgraph` framework isn't installed, the
StateGraph module skips locally; `make test-python-langgraph`,
`OFFSHOOT_REQUIRE_LANGGRAPH`, and CI instead fail loudly.

## License

Apache-2.0, same as offshoot.
