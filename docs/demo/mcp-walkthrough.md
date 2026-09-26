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
- This transcript was re-driven in full for the guardrails branch (the force
  gate, the rollback safety fork, and `offshoot_diff`): `main` is now
  protected by default and an agent's `force:true` is refused unless the MCP
  server was started with `-allow-force` (this session was not), so the
  script no longer sends a `force:true` tool call at all. It sends
  `offshoot_diff` instead, to show what the agent does with the refusal, and
  the actual promote happens as a `$` CLI line run by a human who has
  reviewed that diff — that shape (agent compares, human forces from the
  CLI) is the intended shape of the guardrail, not a workaround for it.
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
project's `./.offshoot` store; the agent picks up the nine `offshoot_*`
tools (`list`, `checkout`, `checkpoint`, `fork`, `rollback`, `promote`,
`destroy`, `touch`, `diff`) the next time it starts a session with `offshoot`
available.

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
transcript still advertises it (`"ttl":{"default":"24h", ...}`), which is
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
initialized store at /var/folders/r1/h4z43zsj7vlb62zwtkxhgc400000gn/T/tmp.yHvM7s6dZY/store

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
          "inputSchema": {
            "properties": {},
            "type": "object"
          },
          "annotations": {
            "title": "List databases and branches",
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_checkout",
          "description": "Materialize a database branch to a local SQLite file and return the path to open. Call this before reading or writing a branch's data directly with a SQL client. If a daemon session is already open on this branch (opened by a harness, not by this tool — this tool never opens one itself), the result is that session's live checkout path instead: writes there are captured continuously, and offshoot_checkpoint against it flushes live rather than writing a fresh snapshot. Otherwise this is a plain at-rest materialization, even if a daemon happens to be running. Call offshoot_checkpoint to name the current state so it can be rolled back to or forked from later. `branch` defaults to \"main\" if omitted.",
          "inputSchema": {
            "properties": {
              "branch": {
                "default": "main",
                "type": "string"
              },
              "database": {
                "type": "string"
              }
            },
            "required": [
              "database"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Materialize a branch to a SQLite file",
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_checkpoint",
          "description": "Name the current state of a branch's checkout so it can be returned to later. Call this after a batch of changes you might want to keep or roll back to individually — e.g. after a migration step succeeds, or before starting a riskier change on the same branch. If a daemon session is open on this branch, this is a live flush (cheap, only the diff since the last checkpoint, no pause in writes); otherwise it's a full-snapshot checkpoint of the checkout file. `branch` defaults to \"main\" if omitted. Optional `meta` (string->string, at most 32 keys) tags the result with your run id, git SHA, or agent name for later lookup.",
          "inputSchema": {
            "properties": {
              "branch": {
                "default": "main",
                "type": "string"
              },
              "database": {
                "type": "string"
              },
              "meta": {
                "additionalProperties": {
                  "type": "string"
                },
                "type": "object"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "database",
              "name"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Checkpoint a branch",
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": false,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_fork",
          "description": "Create an isolated copy of a database branch before attempting risky or destructive work (schema migrations, bulk deletes, experiments). Fork storage starts with two small metadata objects, regardless of database size. Forking a named checkpoint does not read database contents; the default at-head fork hashes the checkout to warn about uncheckpointed changes. Prefer forking over backing up by hand. Forks from the branch's current head by default, or from a named checkpoint via `at`. If a daemon session is open on the source branch, its unflushed writes are flushed first, so the fork always includes everything written so far. `branch` (the source) defaults to \"main\" if omitted. Forked branches expire 24h0m0s after their last activity by default, unless promoted or touched; pass `ttl:\"none\"` to keep one indefinitely, or `ttl` as a Go duration string (e.g. \"2h\") to override. TTL reaping only happens while a janitor is running (`offshoot serve`); a daemonless setup sweeps expired branches only when `offshoot gc` is run. Optional `meta` (string->string, at most 32 keys) tags the result with your run id, git SHA, or agent name for later lookup.",
          "inputSchema": {
            "properties": {
              "at": {
                "type": "string"
              },
              "branch": {
                "default": "main",
                "type": "string"
              },
              "database": {
                "type": "string"
              },
              "meta": {
                "additionalProperties": {
                  "type": "string"
                },
                "type": "object"
              },
              "new_branch": {
                "type": "string"
              },
              "ttl": {
                "default": "24h",
                "type": "string"
              }
            },
            "required": [
              "database",
              "new_branch"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Fork a branch",
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": false,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_rollback",
          "description": "Return a branch to a previously named checkpoint, discarding everything written since. Call this when an attempt on a branch has gone wrong and you want to restore known-good state rather than manually undoing changes. Reports the checkout path to reopen after the rollback. `branch` defaults to \"main\" if omitted. The branch's previous head is kept first as a TTL'd safety fork `<branch>-pre-rollback` (one per branch, replaced by the next rollback), so a rollback is undone by promoting that fork back onto `branch`. If a daemon session is open on this branch, the call is refused instead of proceeding, since rollback repoints the branch's storage out from under a session the daemon still believes it owns — close the session first (e.g. `offshoot session close`) and retry.",
          "inputSchema": {
            "properties": {
              "branch": {
                "default": "main",
                "type": "string"
              },
              "database": {
                "type": "string"
              },
              "to": {
                "type": "string"
              }
            },
            "required": [
              "database",
              "to"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Roll a branch back to a checkpoint",
            "readOnlyHint": false,
            "destructiveHint": true,
            "idempotentHint": false,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_promote",
          "description": "Ship a winning attempt: repoint the target branch (often `main`) at the source branch's current head, which resets the target's checkpoint history to just the new promote checkpoint. The target's previous head is kept first as a shared safety fork named `<target>-pre-promote` (TTL'd; one per target, replaced by the next promote), so a promote is undone by promoting that fork back onto the target. Call this once you've validated a forked attempt and are ready to make it the branch of record. Protected branches (main is protected by default) refuse promotion. `force` is honored only when the server was started with -allow-force; otherwise a protected target refuses and the answer is to ask the human to promote from the CLI, or work on a fork. If a daemon session is open on the TARGET branch, the call is refused instead of proceeding — `force` does not override this — since promoting repoints the target's storage out from under a session the daemon still believes it owns; close the session first (e.g. `offshoot session close`) and retry. An open session on the SOURCE does not block the call, but the promoted state is the source's last-flushed/checkpointed head, not any write still unflushed in that live session — flush or checkpoint the source first if you need its very latest state promoted.",
          "inputSchema": {
            "properties": {
              "database": {
                "type": "string"
              },
              "force": {
                "type": "boolean"
              },
              "source": {
                "type": "string"
              },
              "target": {
                "type": "string"
              }
            },
            "required": [
              "database",
              "source",
              "target"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Promote a branch onto a target",
            "readOnlyHint": false,
            "destructiveHint": true,
            "idempotentHint": false,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_destroy",
          "description": "Permanently discard a branch and its checkout. Call this to clean up a failed or abandoned attempt once you're done with it. `force` is honored only when the server was started with -allow-force; otherwise a protected branch or a live lease refuses and the answer is to ask the human to destroy from the CLI, or work on a fork. If a daemon session is open on this branch, the call is refused instead of proceeding — `force` does not override this — since destroy deletes the branch's storage out from under a session the daemon still believes it owns; close the session first (e.g. `offshoot session close`) and retry.",
          "inputSchema": {
            "properties": {
              "branch": {
                "type": "string"
              },
              "database": {
                "type": "string"
              },
              "force": {
                "type": "boolean"
              }
            },
            "required": [
              "database",
              "branch"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Destroy a branch",
            "readOnlyHint": false,
            "destructiveHint": true,
            "idempotentHint": false,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_touch",
          "description": "Reset a branch's activity clock so its TTL does not expire mid-task, and optionally change the TTL. Call this when an attempt on a TTL'd fork is taking longer than expected, or before handing a fork to a long-running step. `ttl` omitted keeps the current TTL; a Go duration like \"2h\" sets it; \"none\" clears it so the branch never expires (prefer a longer duration over \"none\" — branches without a TTL are only removed by an explicit destroy). A TTL alone reaps nothing: the janitor (`offshoot serve`) or `offshoot gc` does.",
          "inputSchema": {
            "properties": {
              "branch": {
                "default": "main",
                "type": "string"
              },
              "database": {
                "type": "string"
              },
              "ttl": {
                "type": "string"
              }
            },
            "required": [
              "database"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Extend a branch's life",
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false
          }
        },
        {
          "name": "offshoot_diff",
          "description": "Compare two branches (or checkpoints, `branch@checkpoint`) of one database and report, per table, rows added, removed, and changed plus schema changes — without sqldiff. Call this to decide which attempt to promote, to check what a migration changed against a checkpoint, or to compare an attempt with a golden checkpoint. `table` narrows to one table. `full` also returns the SQL statements that turn left into right (needs sqldiff on the host), capped at `max_bytes` (default 32768, at most 262144) with `truncated` set when cut; prefer the summary first and `full` with `table` for a drill-down. Read-only: never touches a live checkout, never takes a lease; a head-side branch reads its last durable (flushed/checkpointed) state.",
          "inputSchema": {
            "properties": {
              "database": {
                "type": "string"
              },
              "full": {
                "type": "boolean"
              },
              "left": {
                "type": "string"
              },
              "max_bytes": {
                "type": "integer"
              },
              "right": {
                "type": "string"
              },
              "table": {
                "type": "string"
              }
            },
            "required": [
              "database",
              "left",
              "right"
            ],
            "type": "object"
          },
          "annotations": {
            "title": "Compare two branches or checkpoints",
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false
          }
        }
      ]
    }
  }
```

*(Nothing above is truncated: every description, `inputSchema`, and
`annotations` object is reproduced verbatim from a real `tools/list`
response — see [`internal/mcp/tools.go`](../../internal/mcp/tools.go) for
the source. This capture is from the guardrails re-run: `offshoot_rollback`,
`offshoot_promote`, and `offshoot_destroy`'s descriptions changed to describe
the rollback safety fork and the `-allow-force` gate — every `tools/call`
block that follows is from this same run.)*

```json
→ {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}
← {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"shop@main head=2 checkpoints=[baseline init] protected=true checked_out=true\n"}],"structuredContent":{"branches":[{"branch":"main","checked_out":true,"checkpoints":["baseline","init"],"database":"shop","head_txid":2,"protected":true}]}}}
```

### 3. Fork before the risky migration

*Narration: the agent is about to run a schema migration against `shop`.
`offshoot_fork`'s own description tells it to prefer forking over backing up
by hand, so it forks `main` into `migration-attempt` before writing anything.
It passes `ttl:"none"` — this is a migration it plans to see through to
completion in the same session, not a throwaway experiment to let expire.*

```json
→ {"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"offshoot_fork","arguments":{"database":"shop","new_branch":"migration-attempt","ttl":"none"}}}
← {"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"forked shop@main to shop@migration-attempt at txid 2; ttl=none (never expires)"}],"structuredContent":{"branch":"main","database":"shop","expires_at":"","new_branch":"migration-attempt","ttl":"","txid":2}}}
```

```json
→ {"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"offshoot_checkout","arguments":{"database":"shop","branch":"migration-attempt"}}}
← {"jsonrpc":"2.0","id":5,"result":{"content":[{"type":"text","text":"checked out shop@migration-attempt at /var/folders/r1/h4z43zsj7vlb62zwtkxhgc400000gn/T/tmp.yHvM7s6dZY/store/checkouts/shop/migration-attempt.db\nthis checkout is not yet checkpointed: nothing written here can be rolled back to or forked from until you call offshoot_checkpoint"}],"structuredContent":{"branch":"migration-attempt","database":"shop","live":false,"path":"/var/folders/r1/h4z43zsj7vlb62zwtkxhgc400000gn/T/tmp.yHvM7s6dZY/store/checkouts/shop/migration-attempt.db"}}}
```

The response text itself is the nudge: nothing here can be rolled back to
yet. The agent checkpoints immediately, before writing anything, so there's a
real rollback target if the migration goes wrong:

```json
→ {"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"offshoot_checkpoint","arguments":{"database":"shop","branch":"migration-attempt","name":"pre-migration"}}}
← {"jsonrpc":"2.0","id":6,"result":{"content":[{"type":"text","text":"checkpointed shop@migration-attempt as \"pre-migration\" at txid 3"}],"structuredContent":{"branch":"migration-attempt","database":"shop","live":false,"name":"pre-migration","txid":3}}}
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
← {"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"rolled back shop@migration-attempt to checkpoint \"pre-migration\"; the previous head is kept as shop@migration-attempt-pre-rollback (undo: offshoot_promote it back onto migration-attempt); checkout at /var/folders/r1/h4z43zsj7vlb62zwtkxhgc400000gn/T/tmp.yHvM7s6dZY/store/checkouts/shop/migration-attempt.db"}],"structuredContent":{"backup":"migration-attempt-pre-rollback","branch":"migration-attempt","database":"shop","path":"/var/folders/r1/h4z43zsj7vlb62zwtkxhgc400000gn/T/tmp.yHvM7s6dZY/store/checkouts/shop/migration-attempt.db","to":"pre-migration"}}}
```

The bad attempt isn't just discarded, either: rollback keeps the head it
just abandoned (the naive-float `total_cents` write) as its own TTL'd safety
fork, `shop@migration-attempt-pre-rollback` — reachable later by promoting
it back onto `migration-attempt` if the "wrong" attempt turns out to be
wanted after all. It shows up again in the final `offshoot_list` below,
since this session never destroys it.

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
← {"jsonrpc":"2.0","id":8,"result":{"content":[{"type":"text","text":"checkpointed shop@migration-attempt as \"migrated\" at txid 4"}],"structuredContent":{"branch":"migration-attempt","database":"shop","live":false,"name":"migrated","txid":4}}}
```

### 6. Promote — the guardrail holds, the agent compares, the human ships it

*Narration: the agent tries to ship the migration by promoting onto `main`.*

```json
→ {"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"offshoot_promote","arguments":{"database":"shop","source":"migration-attempt","target":"main"}}}
← {"jsonrpc":"2.0","id":9,"result":{"content":[{"type":"text","text":"ops: shop@main is protected; use --force"}],"isError":true}}
```

This refusal is real, not staged narration — `main` is protected by default,
and this is `offshoot_promote`'s guardrail firing exactly as documented in
its tool description, as an `isError: true` **tool result**, not a
transport-level RPC error — the agent sees it as a normal reply it can
reason about and react to, same as any other tool response.

That's where an agent-side retry stops, on this server. This session was
started as plain `offshoot -store $STORE mcp` — no `-allow-force` — so even
if the agent resent the exact same call with `force: true`, `offshoot_promote`'s
own description says it plainly: *"`force` is honored only when the server
was started with `-allow-force`; otherwise a protected target refuses and
the answer is to ask the human to promote from the CLI, or work on a fork."*
There is no argument the agent can pass here that lands a force on a
protected branch — that decision is a server-startup flag the human running
`offshoot mcp` controls, not a tool argument. This is a deliberate
behavior change from earlier versions of this server: nothing in this
transcript ever sends `force: true`.

*Narration: instead of trying to force its way past the refusal, the agent
does the thing the tool description actually points it at — it gives the
human something to decide from. It calls `offshoot_diff` to compare `main`
against the validated attempt:*

```json
→ {"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"offshoot_diff","arguments":{"database":"shop","left":"main","right":"migration-attempt@migrated"}}}
← {"jsonrpc":"2.0","id":10,"result":{"content":[{"type":"text","text":"left:  shop@main right: shop@migration-attempt@migrated\nTABLE   shop@main  shop@migration-attempt@migrated  ADDED  REMOVED  CHANGED  STATUS\norders  3          3                                -      -        -        changed (schema)\n1 tables: 0 same, 1 changed, 0 added, 0 removed\n"}],"structuredContent":{"database":"shop","left":"main","right":"migration-attempt@migrated","tables":[{"added":0,"changed":0,"comparable":false,"key":"","left_exists":true,"left_rows":3,"removed":0,"right_exists":true,"right_rows":3,"schema_changed":true,"status":"changed","table":"orders"}],"totals":{"added":0,"changed":1,"removed":0,"same":0}}}}
```

Same row count on both sides, one table with a schema change — exactly the
new `total_cents` column, and nothing else moved. `offshoot_diff` never
opened a checkout or took a lease to produce this; it read each side's last
durable state. This is what the agent hands the human: not "trust me, force
it," but a concrete answer to "what does this promote actually change."

*Narration: the human looks at the diff, agrees it's the intended migration
and nothing else, and runs the promote themselves, from the CLI, where
`--force` is the operator's own call to make — not the agent's:*

```
$ offshoot -store $STORE promote shop@migration-attempt --onto main --force
promoted shop@migration-attempt -> shop@main at txid 4
kept the previous shop@main head as shop@main-pre-promote (expires in 24h0m0s; undo with: offshoot promote shop@main-pre-promote --onto main --force)
```

Same safety fork the MCP tool would have made (`shop@main-pre-promote`,
TTL'd, one per target), same undo path — `offshoot promote` prints it
directly. The guardrail didn't block the migration from shipping; it moved
the decision to force a protected branch to the human running the CLI,
where it was always meant to sit.

### 7. Cleanup and confirmation

```json
→ {"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"offshoot_destroy","arguments":{"database":"shop","branch":"migration-attempt"}}}
← {"jsonrpc":"2.0","id":11,"result":{"content":[{"type":"text","text":"destroyed shop@migration-attempt"}],"structuredContent":{"branch":"migration-attempt","database":"shop"}}}
```

`migration-attempt` itself was never protected — only `main` is, by
default — so the agent's own `offshoot_destroy` call (no `force`) cleans it
up without needing the human at all.

```json
→ {"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}
← {"jsonrpc":"2.0","id":12,"result":{"content":[{"type":"text","text":"shop@main head=4 checkpoints=[promote] protected=true checked_out=true\nshop@main-pre-promote head=2 checkpoints=[fork] protected=false checked_out=false\nshop@migration-attempt-pre-rollback head=3 checkpoints=[fork] protected=false checked_out=false\n"}],"structuredContent":{"branches":[{"branch":"main","checked_out":true,"checkpoints":["promote"],"database":"shop","head_txid":4,"protected":true},{"branch":"main-pre-promote","checked_out":false,"checkpoints":["fork"],"database":"shop","head_txid":2,"protected":false},{"branch":"migration-attempt-pre-rollback","checked_out":false,"checkpoints":["fork"],"database":"shop","head_txid":3,"protected":false}]}}}
```

Three branches survive, and each is a safety net from somewhere earlier in
this session, not clutter: `main` (`checkpoints=[promote]` — promote resets
the target's checkpoint history to just the new `promote` checkpoint, see
the `parallel-attempts` README's "What to look at" section for why — it's a
side effect of where the new lineage comes from, not a bug); `main-pre-promote`,
the shared TTL'd fork §6's CLI promote kept of `main`'s pre-promote head
(txid 2, the `baseline` state), undone by promoting it back onto `main`; and
`migration-attempt-pre-rollback`, the fork §4's `offshoot_rollback` kept of
the abandoned naive-float attempt (txid 3) — still sitting there because
this session destroyed `migration-attempt` itself but never touched its
safety fork, which only a TTL or an explicit destroy will eventually clear.

And the real data, confirmed by SQL against `main` after the promote:

```
$ sqlite3 -header $(offshoot checkout shop) 'SELECT id, total, total_cents FROM orders;'
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
