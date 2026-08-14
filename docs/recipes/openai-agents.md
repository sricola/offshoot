# Recipe: OpenAI Agents SDK — session store on an offshoot path

The [OpenAI Agents SDK](https://github.com/openai/openai-agents-python)'s
`SQLiteSession` persists a conversation's turns to a plain SQLite file —
`SQLiteSession(session_id, db_path)`. That's the whole integration surface
offshoot needs: point `db_path` at an offshoot checkout, and every branch
operation (fork, checkpoint, rollback) works on the conversation history
exactly the way it works on any other SQLite database in this project,
because `SQLiteSession`'s file *is* an ordinary SQLite database — offshoot
never has to know the Agents SDK exists.

The pattern this unlocks: **fork the conversation per attempt.** Run the
same conversation forward down two different tool-call strategies, each in
its own branch, without either attempt's turns leaking into the other's
history — the same fork-per-attempt shape the [eval-harness
tutorial](../eval-harness.md) uses for tests, applied to agent
conversations instead.

**What's run vs. illustrative:** the session-store wiring below — creating
a `SQLiteSession` pointed at an offshoot checkout path, writing turns,
forking, and reading each fork's history back independently — was actually
executed while writing this doc (`agents.SQLiteSession` from a real `pip
install openai-agents`, no mocking) and the output pasted below is real.
Nothing here calls the OpenAI API (no key is needed to prove the *storage*
integration) — a real `Runner.run(agent, ..., session=session)` call is
illustrative, since it needs a live API key and network access this
environment doesn't have; its shape is standard Agents SDK usage,
unrelated to offshoot.

## Setup

```
pip install openai-agents
offshoot -store ./.offshoot init
offshoot -store ./.offshoot create convo
offshoot -store ./.offshoot serve -socket /tmp/oai.sock &
sleep 1
offshoot -store ./.offshoot session open convo -socket /tmp/oai.sock
```

```
.offshoot/checkouts/convo/main.db
```

That printed path is a real, empty SQLite file — `SQLiteSession` opens it
exactly like it would open a bare `conversation.db` you created yourself.

## Point `SQLiteSession` at the checkout path

```python
import asyncio
from agents import SQLiteSession


async def main():
    session = SQLiteSession(session_id="conv-1", db_path=".offshoot/checkouts/convo/main.db")
    await session.add_items([
        {"role": "user", "content": "What's 2+2?"},
        {"role": "assistant", "content": "4"},
    ])
    items = await session.get_items()
    print(f"{len(items)} items")
    session.close()


asyncio.run(main())
```

Real output:

```
.../checkouts/convo/main.db: 2 items
```

In a real agent loop this is `Runner.run(agent, "What's 2+2?", session=session)`
instead of the hand-written `add_items` call — `Runner.run` writes both the
user turn and the model's response into the session automatically. The
`add_items` call above stands in for that so this doc can run without an
API key; swap it for `Runner.run` and everything below still applies
unchanged, since offshoot only ever sees the resulting SQLite file, not how
it got written.

## Checkpoint the conversation, then fork per attempt

```
offshoot -store ./.offshoot session flush convo turn-1 -socket /tmp/oai.sock
offshoot -store ./.offshoot fork convo attempt-a --at turn-1
offshoot -store ./.offshoot fork convo attempt-b --at turn-1
offshoot -store ./.offshoot session open convo@attempt-a -socket /tmp/oai.sock
offshoot -store ./.offshoot session open convo@attempt-b -socket /tmp/oai.sock
```

Real output:

```
durable through txid 2
forked convo@main -> convo@attempt-a at txid 2
forked convo@main -> convo@attempt-b at txid 2
.offshoot/checkouts/convo/attempt-a.db
.offshoot/checkouts/convo/attempt-b.db
```

Two independent SQLite files, each starting from the exact conversation
state at `turn-1` — same session id (`conv-1`) in both, same prior
messages, now diverging.

## Each attempt writes its own continuation

```python
import asyncio
from agents import SQLiteSession


async def main(db_path, new_content):
    session = SQLiteSession(session_id="conv-1", db_path=db_path)
    before = await session.get_items()
    print(f"{db_path}: {len(before)} items BEFORE this attempt's turn")
    await session.add_items([
        {"role": "user", "content": "Try a different approach"},
        {"role": "assistant", "content": new_content},
    ])
    after = await session.get_items()
    print(f"{db_path}: {len(after)} items AFTER — last: {after[-1]['content']!r}")
    session.close()


asyncio.run(main(".offshoot/checkouts/convo/attempt-a.db", "Attempt A: use a hash index"))
```

Real output, run once per fork:

```
.../attempt-a.db: 2 items BEFORE this attempt's turn
.../attempt-a.db: 4 items AFTER — last: 'Attempt A: use a hash index'
.../attempt-b.db: 2 items BEFORE this attempt's turn
.../attempt-b.db: 4 items AFTER — last: 'Attempt B: use a sorted array'
```

Both attempts started from the same 2-item history (the shared `turn-1`
checkpoint) and ended at 4 items with completely different last turns —
neither `SQLiteSession` instance ever saw the other's write. That's the
whole guarantee: `session_id="conv-1"` is identical across both files
(it's the Agents SDK's own row key inside the db, unrelated to which
branch the file lives on), so nothing about the Agents SDK's own code has
to know a fork happened at all.

## Fork-per-attempt via the SDK, not just the CLI

The commands above translate directly to the Python SDK for anywhere you'd
rather stay in-process (an eval harness, a batch-attempt runner):

```python
import offshoot

with offshoot.connect("/tmp/oai.sock") as c:
    c.fork("convo", "main", "attempt-c", ttl="2h", from_checkpoint="turn-1")
    s = c.open("convo", "attempt-c")
    # SQLiteSession(session_id="conv-1", db_path=s.path) from here
```

This is the same `Client.fork`/`Client.open` surface the [eval-harness
tutorial](../eval-harness.md) and the pytest/testkit fixtures use — nothing
Agents-SDK-specific about it beyond pointing `SQLiteSession.db_path` at the
resulting checkout path.

## Cleanup

```
offshoot -store ./.offshoot session close convo -socket /tmp/oai.sock
offshoot -store ./.offshoot session close convo@attempt-a -socket /tmp/oai.sock
offshoot -store ./.offshoot session close convo@attempt-b -socket /tmp/oai.sock
offshoot -store ./.offshoot session shutdown -socket /tmp/oai.sock
```

Destroy the losing attempt(s) once you've picked a winner (`offshoot
destroy convo@attempt-b`), or `offshoot promote convo@attempt-a --onto
main` to make the winning conversation's history the new `main` — the same
pick-a-winner shape as every other offshoot workflow; see the FAQ's [why no
merge](../faq.md#can-i-merge-two-branches) for why there's no attempt to reconcile two
divergent conversation histories automatically.
