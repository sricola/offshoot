# The MCP story: an agent that forks before it breaks anything

This can't be screen-recorded the way [`parallel-attempts`](../../examples/parallel-attempts/)
can — there's no terminal UI to point a camera at. What follows instead is a
setup guide plus a full transcript of a real `offshoot mcp` session: the
exact JSON-RPC exchanged over stdio with a real subprocess, and the exact
SQL run against its real checkouts, captured by driving the server myself.

**Labeling, up front, per this doc's own honesty rules:**

- Every JSON-RPC request/response block and every `sqlite3` command/output
  block below is **real** — copy-pasted verbatim from an actual run of
  [`mcp-session-driver.sh`](mcp-session-driver.sh) (this directory) against a
  real, freshly initialized store. Nothing in those blocks was hand-written,
  edited, or reordered after the fact. The script was run three times against
  three independent fresh stores while producing this document; the JSON-RPC
  and SQL output was byte-identical every time (modulo the random temp-store
  path baked into a couple of response strings — see "Reproducing this"
  below).
- The prose *between* those blocks — the "the agent notices X and decides to
  Y" narration — is **illustrative**. It's how an actual coding agent (e.g.,
  Claude Code with `offshoot` wired in as below) would plausibly narrate and
  sequence these exact tool calls in the course of a real migration task. No
  agent model was actually driving this session; I issued the JSON-RPC calls
  directly against the server to produce the real half of this transcript.

## Setup: wiring `offshoot mcp` into Claude Code

```
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

This is the exact command documented in [`README.md`](../../README.md) and
[`docs/reference.md`](../reference.md#offshoot-mcp) for this repo. It
registers `offshoot mcp` as a stdio MCP server scoped to the current
project's `./.offshoot` store; the agent picks up the seven `offshoot_*`
tools (`list`, `checkout`, `checkpoint`, `fork`, `rollback`, `promote`,
`destroy`) the next time it starts a session with `offshoot` available.

Useful variations, also from `docs/reference.md`:

```
# name a store elsewhere, or ride an already-running daemon's socket for
# live-capture checkpoints:
claude mcp add offshoot -- offshoot -store ./.offshoot -socket /tmp/o.sock mcp
```

`-default-ttl` controls the TTL an agent-created fork gets when its own
`offshoot_fork` call doesn't specify one (default `24h`; `none` disables it).
The session below passes `ttl:"none"` explicitly on its one fork call, so
that default never actually engages — but the tool schema captured in the
transcript still advertises it (`"ttl":{"default":"24h0m0s", ...}`), which is
worth noticing: the schema is what an agent reads to decide whether it needs
to pass `ttl` at all.

## The scenario

Same shape as the [`parallel-attempts`](../../examples/parallel-attempts/)
demo, told as a single agent's story instead of three racing forks: a `shop`
database has an `orders` table with a `total` column stored as text
(`'19.99'`, `'8.70'`, `'4.35'`). The task is a schema migration — add an
integer `total_cents` column — and the first honest attempt at it is subtly
wrong.

## Transcript

### 1. CLI setup (not MCP — this part sets the stage)

```
$ offshoot -store $STORE init
initialized store at /.../store

$ offshoot -store $STORE create shop

$ sqlite3 $(offshoot checkout shop) "CREATE TABLE orders...; INSERT ..."

$ offshoot -store $STORE checkpoint shop baseline
checkpoint "baseline" at txid 2
```

### 2. The agent connects and orients itself

*Narration (illustrative): the agent's harness spawns `offshoot mcp` as a
subprocess on session start; the agent sends `initialize`, gets acknowledged,
then calls `offshoot_list` before touching anything — the tool's own
description tells it to ("Call this first to orient yourself").*

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"claude-code","version":"1"}}}
← {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"offshoot","version":"0.1.0"}}}

→ {"jsonrpc":"2.0","method":"notifications/initialized"}
   (a notification — no response, per JSON-RPC 2.0 and confirmed by
   internal/mcp/server_test.go's TestNotificationGetsNoResponse)
```

`tools/list` next — this is the real, complete tool set an agent sees, not a
paraphrase:

```json
→ {"jsonrpc":"2.0","id":2,"method":"tools/list"}
← {
    "jsonrpc": "2.0",
    "id": 2,
    "result": {
      "tools": [
        {
          "name": "offshoot_list",
          "description": "List every database and branch offshoot is tracking, with each branch's head transaction id, named checkpoints, and whether it is protected. Call this first to orient yourself: to see what databases exist, what branches an attempt could fork from, or which checkpoints are available to roll back to or fork from.",
          "inputSchema": {"properties": {}, "type": "object"}
        },
        {
          "name": "offshoot_checkout",
          "description": "Materialize a database branch to a local SQLite file and return the path to open. Call this before reading or writing a branch's data directly with a SQL client. ... `branch` defaults to \"main\" if omitted.",
          "inputSchema": {
            "properties": {"branch": {"default": "main", "type": "string"}, "database": {"type": "string"}},
            "required": ["database"], "type": "object"
          }
        },
        {
          "name": "offshoot_checkpoint",
          "description": "Name the current state of a branch's checkout so it can be returned to later. Call this after a batch of changes you might want to keep or roll back to individually — e.g. after a migration step succeeds, or before starting a riskier change on the same branch. ...",
          "inputSchema": {
            "properties": {"branch": {"default": "main", "type": "string"}, "database": {"type": "string"}, "name": {"type": "string"}},
            "required": ["database", "name"], "type": "object"
          }
        },
        {
          "name": "offshoot_fork",
          "description": "Create an isolated copy of a database branch before attempting risky or destructive work (schema migrations, bulk deletes, experiments). Fork storage starts with two small metadata objects, regardless of database size. Forking a named checkpoint does not read database contents; the default at-head fork hashes the checkout to warn about uncheckpointed changes. Prefer forking over backing up by hand. ... Forked branches expire 24h0m0s after their last activity by default, unless promoted or touched; pass `ttl:\"none\"` to keep one indefinitely, or `ttl` as a Go duration string (e.g. \"2h\") to override. ...",
          "inputSchema": {
            "properties": {"at": {"type": "string"}, "branch": {"default": "main", "type": "string"}, "database": {"type": "string"}, "new_branch": {"type": "string"}, "ttl": {"default": "24h0m0s", "type": "string"}},
            "required": ["database", "new_branch"], "type": "object"
          }
        },
        {
          "name": "offshoot_rollback",
          "description": "Return a branch to a previously named checkpoint, discarding everything written since. Call this when an attempt on a branch has gone wrong and you want to restore known-good state rather than manually undoing changes. ...",
          "inputSchema": {
            "properties": {"branch": {"default": "main", "type": "string"}, "database": {"type": "string"}, "to": {"type": "string"}},
            "required": ["database", "to"], "type": "object"
          }
        },
        {
          "name": "offshoot_promote",
          "description": "Ship a winning attempt: repoint the target branch (often `main`) at the source branch's current head, which resets the target's checkpoint history to just the new promote checkpoint. Call this once you've validated a forked attempt and are ready to make it the branch of record. Protected branches (main is protected by default) refuse promotion unless `force` is set — treat that refusal as confirmation you need, not a bug. ...",
          "inputSchema": {
            "properties": {"database": {"type": "string"}, "force": {"type": "boolean"}, "source": {"type": "string"}, "target": {"type": "string"}},
            "required": ["database", "source", "target"], "type": "object"
          }
        },
        {
          "name": "offshoot_destroy",
          "description": "Permanently discard a branch and its checkout. Call this to clean up a failed or abandoned attempt once you're done with it. Protected branches refuse destruction unless `force` is set — treat that refusal as confirmation you need, not a bug. ...",
          "inputSchema": {
            "properties": {"branch": {"type": "string"}, "database": {"type": "string"}, "force": {"type": "boolean"}},
            "required": ["database", "branch"], "type": "object"
          }
        }
      ]
    }
  }
```

*(Descriptions above are truncated with `...` only where they repeat text
already quoted in full elsewhere in this doc — see
[`internal/mcp/tools.go`](../../internal/mcp/tools.go) for the untruncated
originals; every field name, type, default, and required-list is exactly as
returned.)*

```json
→ {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}
← {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"shop@main head=2 checkpoints=[baseline init] protected=true checked_out=true\n"}]}}
```

### 3. Fork before the risky migration

*Narration: the agent is about to run a schema migration against `shop`.
`offshoot_fork`'s own description tells it to prefer forking over backing up
by hand, so it forks `main` into `migration-attempt` before writing anything.
It passes `ttl:"none"` — this is a migration it plans to see through to
completion in the same session, not a throwaway experiment to let expire.*

```json
→ {"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"offshoot_fork","arguments":{"database":"shop","new_branch":"migration-attempt","ttl":"none"}}}
← {"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"forked shop@main to shop@migration-attempt at txid 2; ttl=none (never expires)"}]}}
```

```json
→ {"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"offshoot_checkout","arguments":{"database":"shop","branch":"migration-attempt"}}}
← {"jsonrpc":"2.0","id":5,"result":{"content":[{"type":"text","text":"checked out shop@migration-attempt at /.../store/checkouts/shop/migration-attempt.db\nthis checkout is not yet checkpointed: nothing written here can be rolled back to or forked from until you call offshoot_checkpoint"}]}}
```

The response text itself is the nudge: nothing here can be rolled back to
yet. The agent checkpoints immediately, before writing anything, so there's a
real rollback target if the migration goes wrong:

```json
→ {"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"offshoot_checkpoint","arguments":{"database":"shop","branch":"migration-attempt","name":"pre-migration"}}}
← {"jsonrpc":"2.0","id":6,"result":{"content":[{"type":"text","text":"checkpointed shop@migration-attempt as \"pre-migration\" at txid 3"}]}}
```

### 4. Rollback on red

The agent writes the migration directly against the checkout path MCP handed
it — this step is real SQL, run by hand (MCP has no SQL-execution tool; that
part of an agent's toolset is whatever SQL client it already has):

```
$ sqlite3 $(offshoot path shop@migration-attempt) "ALTER TABLE orders ADD COLUMN total_cents INTEGER; UPDATE orders SET total_cents = total * 100;"
```

*Narration: naive floating-point math — `19.99 * 100` isn't exactly `1999`
in IEEE754, so SQLite's type affinity never actually commits the column to
an integer for that row. The agent's test suite (real, run here too) checks
for exactly that:*

```
$ sqlite3 $(offshoot path shop@migration-attempt) "SELECT count(*) FROM orders WHERE typeof(total_cents)='integer';"
0  (want 3)
TESTS: RED
```

*Narration: tests are red. Per the `offshoot_rollback` tool description
("Call this when an attempt on a branch has gone wrong ... rather than
manually undoing changes"), the agent rolls back instead of trying to
hand-patch the bad migration:*

```json
→ {"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"offshoot_rollback","arguments":{"database":"shop","branch":"migration-attempt","to":"pre-migration"}}}
← {"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"rolled back shop@migration-attempt to checkpoint \"pre-migration\"; checkout at /.../store/checkouts/shop/migration-attempt.db"}]}}
```

### 5. Checkpoint on green

The agent retries with the fix — round before casting, so every row lands on
a real integer:

```
$ sqlite3 $(offshoot path shop@migration-attempt) "ALTER TABLE orders ADD COLUMN total_cents INTEGER; UPDATE orders SET total_cents = CAST(ROUND(total * 100) AS INTEGER);"

$ sqlite3 $(offshoot path shop@migration-attempt) "SELECT count(*) FROM orders WHERE typeof(total_cents)='integer';"
3  (want 3)
TESTS: GREEN
```

Tests are green. The agent checkpoints the now-validated state:

```json
→ {"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"offshoot_checkpoint","arguments":{"database":"shop","branch":"migration-attempt","name":"migrated"}}}
← {"jsonrpc":"2.0","id":8,"result":{"content":[{"type":"text","text":"checkpointed shop@migration-attempt as \"migrated\" at txid 4"}]}}
```

### 6. Promote — and the protected-branch guardrail firing for real

*Narration: the agent tries to ship the migration by promoting onto `main`.*

```json
→ {"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"offshoot_promote","arguments":{"database":"shop","source":"migration-attempt","target":"main"}}}
← {"jsonrpc":"2.0","id":9,"result":{"content":[{"type":"text","text":"ops: shop@main is protected; use --force"}],"isError":true}}
```

This refusal is real, not staged narration — `main` is protected by default,
and this is `offshoot_promote`'s guardrail firing exactly as documented in
its tool description ("treat that refusal as confirmation you need, not a
bug") and as an `isError: true` **tool result**, not a transport-level RPC
error — the agent sees it as a normal reply it can reason about and react to,
same as any other tool response.

*Narration: the agent reads the refusal, recognizes it as the expected
protected-branch guardrail (not a bug in its migration), and retries with
`force: true` — a deliberate, informed choice, not blind retry-until-it-works.*

```json
→ {"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"offshoot_promote","arguments":{"database":"shop","source":"migration-attempt","target":"main","force":true}}}
← {"jsonrpc":"2.0","id":10,"result":{"content":[{"type":"text","text":"promoted shop@migration-attempt onto shop@main at txid 4"}]}}
```

### 7. Cleanup and confirmation

```json
→ {"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"offshoot_destroy","arguments":{"database":"shop","branch":"migration-attempt"}}}
← {"jsonrpc":"2.0","id":11,"result":{"content":[{"type":"text","text":"destroyed shop@migration-attempt"}]}}
```

```json
→ {"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}
← {"jsonrpc":"2.0","id":12,"result":{"content":[{"type":"text","text":"shop@main head=4 checkpoints=[promote] protected=true checked_out=true\n"}]}}
```

Note `checkpoints=[promote]`: promote resets the target's checkpoint history
to just the new `promote` checkpoint (see the `parallel-attempts` README's
"What to look at" section for why — it's a side effect of where the new
lineage comes from, not a bug).

And the real data, confirmed by SQL against `main` after the promote:

```
$ sqlite3 -header $(offshoot checkout shop) "SELECT id, total, total_cents FROM orders;"
id|total|total_cents
1|19.99|1999
2|8.70|870
3|4.35|435
```

`main` now has the correctly-rounded integer cents — the migration that
actually got promoted is the corrected one; the naive-float attempt never
reached `main` at all, because it was rolled back before a checkpoint of it
ever existed to promote.

## Reproducing this

```
docs/demo/mcp-session-driver.sh
```

Builds `offshoot` fresh, creates a throwaway store under `mktemp -d`, runs
every step above for real (CLI setup, the full JSON-RPC exchange over a real
`offshoot mcp` subprocess via named pipes, and the real `sqlite3` migration/
test commands), and writes a timestamped log. It needs nothing but `go` and
`sqlite3` on `PATH` — no server, no daemon, no cleanup (everything lives
under the temp dir). This is the actual script used to produce the
transcript above; run it and diff the output against this document if you
want to verify it yourself.
