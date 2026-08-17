# Recipe: other frameworks — notes, not adapters

offshoot doesn't ship a LlamaIndex or CrewAI package, and this page isn't
one in disguise. Per the roadmap's stance, LangGraph gets two real
integrations because its thread/checkpoint model maps onto offshoot's
branch/checkpoint model almost exactly: `langgraph-checkpoint-offshoot`'s
[`OffshootSaver`](../../sdk/python-langgraph/README.md) puts LangGraph's own
checkpoint state under offshoot, while the core SDK's
`offshoot.langgraph.ThreadForks` maps a thread to the separate application
database ([example](../../examples/langgraph-rewind/)). Claude Code hooks
and the OpenAI Agents SDK get full
recipes (see [claude-agent-sdk.md](claude-agent-sdk.md) and
[openai-agents.md](openai-agents.md)) because both have a single,
official, SQLite-backed persistence knob you can point at an offshoot
checkout path with zero glue code. LlamaIndex and CrewAI don't have quite
as clean a single knob — this page is the honest, short version of what's
there and what to actually check before you build on it.

**Neither section below was run against a live install as part of this
task** (LlamaIndex's SQL chat store needs the `aiosqlite` extra and network
access this pass didn't spend on it; CrewAI's `crewai` package failed to
build locally in this environment — `tiktoken`, one of its transitive
dependencies, has no prebuilt wheel for this Python/platform combination
and needs a Rust toolchain to build from source, which isn't installed
here). Treat both as **starting points to verify against the framework's
own current docs**, not pinned recipes the way
[openai-agents.md](openai-agents.md) is (that one *was* run, end to end,
against a real `pip install openai-agents`).

## LlamaIndex

LlamaIndex's chat-memory layer is pluggable storage, same shape as the
OpenAI Agents SDK's `SQLiteSession` in spirit: a `BaseChatStore`
implementation backs a `ChatMemoryBuffer` (or the newer `Memory` API), and
`llama_index.core.storage.chat_store.sql.SQLAlchemyChatStore` is the
SQL-backed one. Its constructor (checked against a real
`pip install llama-index-core` while writing this doc, though the store
itself wasn't exercised end-to-end) is:

```python
SQLAlchemyChatStore(
    table_name: str,
    async_database_uri: str | None = "sqlite+aiosqlite:///:memory:",
    async_engine: AsyncEngine | None = None,
    db_data: list[dict] | None = None,
    db_schema: str | None = None,
)
```

The pattern, if you go this route: point `async_database_uri` at an
offshoot checkout path instead of `:memory:` or a fixed project file —

```python
store = SQLAlchemyChatStore(
    table_name="chat_history",
    async_database_uri=f"sqlite+aiosqlite:///{checkout_path}",
)
```

— using the same `sqlite+aiosqlite:///` URI form SQLAlchemy's async SQLite
driver expects (needs `pip install aiosqlite` alongside `llama-index-core`;
not verified in this pass). From there, the same fork/checkpoint/rollback
loop [openai-agents.md](openai-agents.md) walks through applies unchanged
— offshoot only ever sees the resulting SQLite file, same as with
`SQLiteSession`. **Verify the constructor and the URI form against your
installed LlamaIndex version before relying on this** — LlamaIndex's
storage APIs have moved before (older versions used a different chat-store
surface entirely) and this skill pass didn't confirm read/write behavior
against a live checkout, only that the class and its constructor exist as
shown.

## CrewAI

CrewAI's built-in memory (`Crew(memory=True)`) is backed by SQLite for its
long-term/entity memory by default, written to a fixed path under the
user's local app-data directory rather than a path you hand it directly —
check CrewAI's current memory documentation for the exact storage class
and constructor (`LTMSQLiteStorage` and similar names have existed in past
versions, but this pass could not verify the current shape against a real
install, for the dependency-build reason above). If CrewAI's storage class
accepts an explicit `db_path`-style argument in your installed version, the
same pattern applies: point it at an offshoot checkout path instead of the
framework's default location, then fork/checkpoint/rollback that file
exactly as the other recipes on this page do. If it doesn't expose a path
argument in the version you're on, the fallback is coarser but still
works: point the whole CrewAI storage *directory* at a path under an
offshoot checkout (most of these frameworks' local storage is "a directory
with one or more SQLite files in it," and offshoot can branch a directory
of SQLite files the same way it branches one — checkpoint/fork/rollback
operate on whatever's in the checkout at the time).

## The general shape, if your framework isn't listed

1. Find where the framework writes persistent SQLite state, and whether it
   takes a path (or URI) argument rather than hard-coding one.
2. Point that argument at a path under an offshoot checkout —
   `offshoot session open <db>` (daemon) or `offshoot checkout <db>` (at
   rest) both print one.
3. Checkpoint/flush through offshoot at whatever granularity the framework
   naturally represents (a turn, a task, a run) — not through the
   framework's own API, which has no notion that offshoot exists
   underneath it.
4. Fork before an attempt you want to be able to throw away in isolation;
   `promote` or `destroy` afterward, same as every other offshoot
   workflow.

No adapter package needed — the requirement is just "the framework writes
its state to a SQLite file whose path you control," which most local-first
agent memory/session layers do.
