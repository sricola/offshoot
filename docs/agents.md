# Agents & MCP

Fork-per-attempt is the workload offshoot is built around: give each agent
attempt its own real database — a copy-on-write fork, not a mock, not a
re-seed — let it write with a stock SQLite client, then keep
the attempt that passed and let the rest expire. This page covers the MCP
server that puts fork/checkpoint/rollback in the agent's own hands, the
daemon workflow that captures the agent's writes live, and where the
safety rails actually are.

## Wire it into your agent

**Claude Code plugin** (MCP server + a skill that teaches the loop + advisory hooks):

```
claude plugin marketplace add sricola/offshoot
claude plugin install offshoot@offshoot
```

**Claude Code, MCP only:**

```
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

**Cursor:** [![Install in Cursor](https://cursor.com/deeplink/mcp-install-dark.svg)](https://cursor.com/en/install-mcp?name=offshoot&config=eyJjb21tYW5kIjoib2Zmc2hvb3QiLCJhcmdzIjpbIm1jcCJdfQ==)
(the link opens Cursor's install page, which hands off to the app to install `offshoot mcp` as a stdio server; the store resolves from `OFFSHOOT_STORE` or `./.offshoot`).

`offshoot mcp` speaks the Model Context Protocol on stdio — no daemon
required for the baseline. The plugin's skill includes the
["rules-file snippet"](#rules-file-snippet) below plus the six-step loop and
rules it's drawn from; its hooks only add context (they never block a
command), and they stay silent when `offshoot` is not installed.

The agent gets nine tools, each described so the model knows *when* to
reach for it: fork before a risky migration, checkpoint when tests pass,
roll back when they don't, diff two attempts to see what actually changed,
promote the attempt that worked. See it work end to end in a real captured
session: [the MCP walkthrough](demo/mcp-walkthrough.md).

## Rules-file snippet

Drop this into the project's `AGENTS.md` or `CLAUDE.md` so any agent reads
the loop before it touches a database, MCP tools or not:

```
Database changes: this project uses offshoot. Before any schema migration,
bulk delete, or data experiment, fork the database (offshoot_fork) and work
in the fork's checkout; checkpoint when tests pass; roll back when they
fail; promote the fork that worked. Never run destructive SQL on main's
checkout directly.
```

This is the same text the Claude Code plugin's skill teaches automatically;
an agent that indexes docs instead of reading a rules file can pull the same
loop from <https://sricola.github.io/offshoot/llms.txt>.

## The nine tools

Verified against `internal/mcp/tools.go`; `branch` defaults to `"main"`
wherever it's optional.

| Tool | Arguments | What it does |
|---|---|---|
| `offshoot_list` | *(none)* | List every database and branch, with head txid, checkpoints, and protected flags — the orient-yourself call |
| `offshoot_checkout` | `database`, `branch?` | Materialize a branch to a local SQLite file and return the path to open |
| `offshoot_checkpoint` | `database`, `name`, `branch?`, `meta?` | Name the current state so it can be rolled back to or forked from |
| `offshoot_fork` | `database`, `new_branch`, `branch?`, `at?`, `ttl?`, `meta?` | Create an isolated branch from head, or from a checkpoint via `at` |
| `offshoot_rollback` | `database`, `to`, `branch?` | Return a branch to a named checkpoint, discarding everything since |
| `offshoot_promote` | `database`, `source`, `target`, `force?` | Repoint `target` at `source`'s head — ship the winning attempt. `target`'s previous head is kept as a safety fork named `<target>-pre-promote` — the undo handle the result names — but that fork always carries a TTL (24h by default) and is one rolling slot per target, replaced by the next promote onto that target, so the undo window closes when either happens |
| `offshoot_destroy` | `database`, `branch`, `force?` | Permanently discard a branch and its checkout |
| `offshoot_touch` | `database`, `branch?`, `ttl?` | Reset a fork's activity clock so its TTL does not expire mid-task; `ttl` sets or (`"none"`) clears it |
| `offshoot_diff` | `database`, `left`, `right`, `table?`, `full?`, `max_bytes?` | Per-table rows added/removed/changed (and schema changes) between two branches or checkpoints — decide which attempt to promote; `full` adds capped sqldiff SQL |

## What the host sees: annotations and structured results

Every tool carries the MCP spec's behavior hints, set explicitly:
`offshoot_list` and `offshoot_diff` are read-only; `checkout`, `fork`,
`checkpoint`, and `touch` are non-destructive; `rollback`, `promote`, and
`destroy` are destructive, so a host that honors `destructiveHint` prompts
before them. Every successful result also returns `structuredContent`
(snake_case JSON:
`txid`, `path`, `ttl`, `expires_at`, `backup`, …) next to the prose, so a
harness reads the fields instead of parsing sentences. `fork` and
`checkpoint` accept `meta` (string→string, at most 32 keys) to tag a
branch or checkpoint with a run id, git SHA, or agent name.

## Forks expire by default

Agent-initiated forks carry a TTL: `offshoot_fork` applies `offshoot mcp
-default-ttl` (default **`24h`**) to any call that omits its own `ttl`, so
a branch an agent forks and forgets becomes reap-eligible a day later
instead of leaking forever. An explicit `ttl:"<duration>"` always wins,
`ttl:"none"` always yields no TTL, and `-default-ttl 0`/`-default-ttl
none` disables the default entirely. The fork tool's response echoes the
applied TTL and computed expiry, so both land in the agent's transcript.

**A TTL alone reaps nothing.** Reaping is the janitor's job (`offshoot
serve`), and `offshoot mcp` runs no daemon of its own — a daemonless setup
only sweeps expired branches when `offshoot gc` is run by hand.

## The safety posture

Destructive tools honor the same protected-branch rules as the CLI: an
unforced `offshoot_promote` onto `main` or `offshoot_destroy` of `main`
is **refused**, and the refusal comes back to the agent as the tool
result — described to the model as "confirmation you need, not a bug" —
rather than a transport error. The agent forks and experiments freely;
touching the branch of record requires the explicit `force` step. Note
what this is and isn't: `force` is an argument the agent *can* pass, so
the protected flag is a deliberate speed bump and an auditable decision
point, not a permission boundary — if promotion must be a human/harness
decision, keep it in the harness (the pattern in the
[Claude Agent SDK recipe](recipes/claude-agent-sdk.md), where the harness
checkpoints or rolls back based on your own success signal).

Also true regardless of tools: the daemon's unix socket is mode `0600`,
and one leased, epoch-fenced writer per branch means concurrent attempts
get isolated forks, never interleaved writes
([architecture](architecture.md#security-posture)).

## Live capture: the daemon workflow

Bare `offshoot mcp` runs every tool **at rest** — checkpoints quiesce the
checkout and write a full snapshot. For an agent writing continuously, put
a daemon session under it and the same tools ride live capture instead
(incremental flushes, no quiesce, writer never paused):

```
offshoot -store ./.offshoot init                 # once
offshoot serve -socket /tmp/o.sock &             # holds leases, captures continuously
sleep 1
offshoot session open app -socket /tmp/o.sock    # the harness-opened session
claude mcp add offshoot -- offshoot -store ./.offshoot mcp -socket /tmp/o.sock
# ... agent works ...
offshoot session close app -socket /tmp/o.sock
```

The rules, exactly as shipped:

- **No MCP tool ever opens a session itself.** That's a harness's job —
  the SDKs, `offshoot session open`, or your own loop. A bare tool call
  has no guaranteed teardown, and an MCP-opened session would leak its
  lease exactly the way TTLs exist to prevent
  ([the design reasoning](status.md#integration-surface)).
- **With a session open on the branch:** `offshoot_checkpoint` flushes
  live through the daemon and `offshoot_checkout` returns the session's
  live checkout path.
- **`offshoot_fork` rides the daemon whenever one is reachable** —
  session or not (an open source session is flushed first, so unflushed
  writes land in the child; the fork uses the daemon's `-snapshot-every`
  share floor and counts in its metrics).
- **With no reachable daemon, every tool works at rest** — exactly as if
  the daemon didn't exist.
- **`offshoot_rollback`, `offshoot_promote` (its `target`), and
  `offshoot_destroy` refuse** — even with `force`, which has no effect on
  this particular refusal — whenever the daemon has any session open on
  the affected branch, because all three repoint or delete a ref out from
  under a session the daemon still owns. Close the session first and
  retry. `offshoot_promote`'s `source` is the one exception: an open
  session there doesn't block, but what gets promoted is the source's
  last-flushed head, not its unflushed writes.

Full flag-level detail: [`offshoot mcp`](reference.md#offshoot-mcp) in the
reference.

## Beyond MCP

- **Test/eval harnesses** — the paved road for fork-per-test: pytest
  fixtures and a vitest/jest testkit, seed-once-fork-many, TTL hygiene,
  CI. [The eval-harness tutorial](eval-harness.md).
- **Python / TypeScript SDKs** — thin clients over the daemon's lifecycle
  API for harnesses that open and close sessions themselves. See the
  [Python SDK](../sdk/python/README.md) and
  [TypeScript SDK](../sdk/typescript/README.md).
- **LangGraph** — `OffshootSaver` is a real `BaseCheckpointSaver` for putting
  LangGraph's own thread state under offshoot; the core SDK's `ThreadForks`
  helper instead keeps an existing checkpointer and maps each thread to a
  branch of the agent's separate application database
  ([LangGraph integrations](../sdk/python-langgraph/README.md)).
- **Other frameworks** — the OpenAI Agents SDK, LlamaIndex, and CrewAI
  each get a short honest recipe: [framework recipes](recipes/frameworks.md),
  [OpenAI Agents SDK](recipes/openai-agents.md).
