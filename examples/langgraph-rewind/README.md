# LangGraph rewind

LangGraph can rewind the conversation; it can't rewind the database the
agent wrote to — this example rewinds both.

`offshoot.langgraph.ThreadForks` maps each LangGraph thread id to its own
offshoot branch. When an agent's tool calls write to a sqlite database
across a conversation, and the user (or the agent itself) rewinds the
thread to an earlier point and tries something else, `ThreadForks` forks
the *database* at that same point too — so the retried path starts from the
exact data the original conversation had at that checkpoint, not from
whatever the first attempt left behind.

## Run it

From the repo root:

    python3 examples/langgraph-rewind/agent.py

This builds `offshoot` from the checkout, starts a private daemon on a temp
store, runs the simulated flow, and cleans up after itself — no server, no
bucket, nothing to configure.

To run the same flow through a real, compiled LangGraph `StateGraph` instead
of the plain-Python stand-in:

    pip install langgraph
    python3 examples/langgraph-rewind/agent.py --real

(`--real` was verified against `langgraph==1.2.10`; the import is guarded,
so running without `--real` never requires `langgraph` at all.)

## What it does

1. Opens thread `conv-<uuid>` — `forks.path(thread)` forks a fresh offshoot
   branch off `main` for it (24h TTL by default) and returns a writable
   sqlite path.
2. Turn 1 writes an order (`widget`) and calls `forks.checkpoint(thread,
   "turn-1")` — a flush named after the turn.
3. Turn 2 writes another order (`gadget`), checkpointed `"turn-2"`.
4. Turn 3 writes a third order (`gizmo-oops`) — a mistake — checkpointed
   `"turn-3"`. The original thread's world now has 3 orders.
5. `forks.fork_thread(thread, "turn-1", retry_thread)` forks a new thread's
   database from the *turn-1* checkpoint, not from the thread's current
   (turn-3) state.
6. The retried thread writes a different order (`sprocket`) instead of
   `gadget`/`gizmo-oops`. Its world now has 2 orders: `widget`, `sprocket`.
7. Both worlds are printed. The original thread's 3 orders are untouched by
   the retry — they're on a different offshoot branch entirely.

## The integration, in six lines

This is a **companion**, not a `BaseCheckpointSaver` — it doesn't persist
LangGraph's own state (messages, node outputs). It only tracks which
offshoot branch belongs to which thread. Wire it into your own agent loop:

```python
forks = ThreadForks(client, db="agent")           # once, at startup

path = forks.path(thread_id)                       # before graph.stream(...)
# ... graph.stream(...) / graph.invoke(...) runs, tool nodes write to `path` ...
forks.checkpoint(thread_id, checkpoint_id)          # after the step, name it

# later, resuming from a historical checkpoint:
new_path = forks.fork_thread(thread_id, checkpoint_id, new_thread_id)
```

There is no registered LangGraph checkpointer and no node wrapper here —
call `checkpoint()` and `fork_thread()` yourself at the points above. See
`sdk/python/offshoot/langgraph.py`'s module docstring for the full design
rules (branch-per-thread, checkpoint naming, id sanitization, error
messages).

## What to look at

- **The retry never sees `gadget` or `gizmo-oops`.** `fork_thread` branches
  off the checkpoint named `"turn-1"`, not off the thread's live head, so
  the retried path starts from exactly the turn-1 world.
- **Thread ids are sanitized deterministically.** LangGraph thread ids are
  commonly UUIDs, which are already safe; `ThreadForks` also handles ids
  that aren't (see `sdk/python/tests/test_langgraph.py`'s
  `TestNameSanitization` for a UUID and a `../../evil`-shaped id, both
  producing distinct, valid, deterministic branch names).
- **Every per-thread branch carries a TTL** (24h by default). A thread a
  user opens and abandons, or a rewound retry nobody promotes, reaps itself
  — that's the workload TTLs exist for.
