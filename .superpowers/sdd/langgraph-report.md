# LangGraph adapter (`langgraph-checkpoint-offshoot`) — build report

Branch: `langgraph-adapter`. Package: `sdk/python-langgraph/` →
`langgraph_checkpoint_offshoot` 0.1.0.

## The contract pinned (exact signatures + source)

Pinned by **introspection of the installed packages** (not web docs):
`langgraph-checkpoint 4.2.0`, `langgraph-checkpoint-sqlite 3.1.1`,
`langgraph 1.2.11`, installed from PyPI into a Python 3.14 venv on
2026-08-12. Source of truth:
`langgraph/checkpoint/base/__init__.py` (class `BaseCheckpointSaver`) and
`langgraph/checkpoint/sqlite/__init__.py` (class `SqliteSaver`).

`BaseCheckpointSaver` surface (all verified via `inspect.signature`):

```
put(config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata,
    new_versions: ChannelVersions) -> RunnableConfig
put_writes(config: RunnableConfig, writes: Sequence[tuple[str, Any]],
    task_id: str, task_path: str = '') -> None
get_tuple(config: RunnableConfig) -> CheckpointTuple | None
get(config: RunnableConfig) -> Checkpoint | None
list(config: RunnableConfig | None, *, filter: dict[str, Any] | None = None,
    before: RunnableConfig | None = None, limit: int | None = None) -> Iterator[CheckpointTuple]
delete_thread(thread_id: str) -> None
delete_for_runs(run_ids: Sequence[str]) -> None
copy_thread(source_thread_id: str, target_thread_id: str) -> None
prune(thread_ids: Sequence[str], *, strategy: str = 'keep_latest') -> None
get_delta_channel_history(*, config, channels) -> Mapping[str, DeltaChannelHistory]
get_next_version(current: V | None, channel: None) -> V
with_allowlist(extra_allowlist: Collection[tuple[str, ...]]) -> BaseCheckpointSaver[V]
config_specs (property);  serde: SerializerProtocol = JsonPlusSerializer()
+ async twins: aput, aput_writes, aget_tuple, aget, alist, adelete_thread,
  adelete_for_runs, acopy_thread, aprune, aget_delta_channel_history
```

Notes that shaped the design:

- LangGraph `isinstance`-checks the checkpointer against
  `BaseCheckpointSaver` (`langgraph/pregel/main.py`, `langgraph/types.py`),
  so composition alone is not enough — `OffshootSaver` subclasses it and
  **explicitly delegates every method** (base methods are concrete, so
  `__getattr__` fallback would never fire).
- Checkpoint 4.x is a wider surface than the well-known 2.x
  (`copy_thread`, `prune`, `delete_for_runs`, `get_delta_channel_history`,
  `with_allowlist` are new). All delegated; dep pinned
  `langgraph-checkpoint-sqlite>=3.1,<4`.
- `SqliteSaver.from_conn_string` opens sqlite with
  `check_same_thread=False`; we do the same (SqliteSaver serializes access
  with its own lock).
- `SqliteSaver`'s async methods raise `NotImplementedError` pointing at
  `AsyncSqliteSaver`; delegation preserves that honest error.
- `with_allowlist` on the base does `copy.copy(self)` + swaps `self.serde`
  — that would leave the INNER saver's serde (the one that actually
  serializes) untouched, so `OffshootSaver` overrides it to derive a new
  inner and wrap that.

## Design choices

- **Wrapper, not reimplementation.** `OffshootSaver` holds a
  `SqliteSaver(sqlite3.connect(session.path, check_same_thread=False))` on
  the offshoot checkout; every LangGraph-facing method is a one-line
  delegate. Zero serialization code in this package.
- **Value-add methods map 1:1 to offshoot ops** (documented in the module
  docstring + README table): `checkpoint(name)` → `Session.flush`;
  `fork_thread(new, ttl=, from_checkpoint=, meta=)` → `Client.fork`
  (flushes head first when no checkpoint named; returns a NEW saver with
  its own daemon connection + session on the fork's checkout);
  `rollback(to)` → `Client.rollback` (session+conn closed around the op —
  the daemon refuses rollback with an open session — then reopened in
  place, inner SqliteSaver replaced); `promote(onto, force=)` → flush +
  `Client.promote`; `destroy()` → `Client.destroy` + full close.
- **Context manager + explicit `close()`**; closed-saver ops raise
  `OffshootError` naming db@branch.

## Modes shipped

- **`OffshootSaver.session(socket_path, db, branch="main", create=True)` —
  SHIPPED.** Live daemon capture; flush on checkpoint. Every op the saver
  needs exists on the daemon wire protocol; one arbiter for durability and
  branch ops.
- **`OffshootSaver.at_rest(store, db, branch)` — STUBBED** with
  `NotImplementedError` + doc note (README "Modes"). Reason: the daemon
  protocol has no "commit an at-rest checkout" op (`flush` is
  session-only), so a correct at-rest mode would need to shell out to the
  `offshoot` CLI per checkpoint — a broad fragile surface. Thin correct
  adapter over broad fragile one, per the design brief.

## Test results (REAL runs, nothing mocked, nothing skipped)

`sdk/python-langgraph/tests/` — pytest, real `go build`-built binary
(OFFSHOOT_BIN respected), real `offshoot serve` on a temp store (conftest
mirrors sdk/python/tests/test_client.py's DaemonFixture), real langgraph
from PyPI, real compiled `StateGraph`s:

```
7 passed in 12.36s     (Python 3.14.6, langgraph 1.2.11, macOS arm64)
```

- `test_raw_put_get_tuple_round_trip` — BaseCheckpointSaver contract with
  LangGraph's own `empty_checkpoint()`: put → get_tuple → list.
- `test_graph_round_trip_and_survives_checkpoint_reopen` — real StateGraph
  runs; named offshoot checkpoint; a fresh session reads the state back.
- `test_fork_thread_isolates_parent_from_child` — child branch diverges
  (3 vs 1; checkpoint counts compared), parent unchanged; fork carries its
  TTL (`1h0m0s` read back from `branches`).
- `test_rollback_restores_named_checkpoint` — n: 1 → checkpoint("good") →
  3 → rollback → 1; same saver object keeps working after reopen.
- `test_promote_copies_winner_onto_target` — eval-loop ending: winner's
  state promoted onto protected `main` (session closed first, per daemon
  rule), fresh session on main sees it.
- `test_destroy_deletes_branch` + `test_at_rest_is_stubbed`.

Skip discipline (verified in a bare venv): no langgraph → `1 skipped`
locally; with `CI=1` or `OFFSHOOT_REQUIRE_LANGGRAPH=1` → hard
`pytest.UsageError` before collection (same RequireExec stance as the
binary). Package build verified: `pip install .` builds a wheel and
`import langgraph_checkpoint_offshoot` works installed.

## Worked example

README's "Worked example" section: seed thread on main →
`checkpoint("seeded")` → `fork_thread(f"attempt-{i}", ttl="1h")` per
attempt → run graph independently per fork → `winner.promote("main",
force=True)` → losers destroyed or TTL-reaped. Plus a rollback snippet.
The promote/fork/rollback paths in the example are exactly the tested ones.

## Concerns / future work

- Async graphs: not supported (inner SqliteSaver's honest
  `NotImplementedError`); an `AsyncOffshootSaver` over `AsyncSqliteSaver`
  is the natural 0.2 item.
- `offshoot-db` is a plain `>=0.1.0` requirement with a pyproject comment —
  becomes a real PyPI pin at launch; until then `pip install -e sdk/python`
  first (tests import both packages from the repo tree, no install needed).
- `at_rest` stays stubbed until the daemon grows an at-rest commit op (or
  we accept shelling out to the CLI).
- checkpoint 5.x / sqlite 4.x will need a re-pin pass over the delegated
  surface (upper bound `<4` guards against silent drift).
