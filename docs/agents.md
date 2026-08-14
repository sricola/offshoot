# Agents & MCP

Fork-per-attempt is the workload offshoot is built around: give each agent
attempt its own real database — a copy-on-write fork, not a mock, not a
re-seed — let it write with a stock SQLite client, then keep
the attempt that passed and let the rest expire. This page covers the MCP
server that puts fork/checkpoint/rollback in the agent's own hands, the
daemon workflow that captures the agent's writes live, and where the
safety rails actually are.

## One command to wire up Claude Code

```
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

`offshoot mcp` speaks the Model Context Protocol on stdio — no daemon
required for the baseline. The agent gets seven tools, each described so
the model knows *when* to reach for it: fork before a risky migration,
checkpoint when tests pass, roll back when they don't, promote the attempt
that worked. See it work end to end in a real captured session:
[the MCP walkthrough](demo/mcp-walkthrough.md).

## The seven tools

Verified against `internal/mcp/tools.go`; `branch` defaults to `"main"`
wherever it's optional.

| Tool | Arguments | What it does |
|---|---|---|
| `offshoot_list` | *(none)* | List every database and branch, with head txid, checkpoints, and protected flags — the orient-yourself call |
| `offshoot_checkout` | `database`, `branch?` | Materialize a branch to a local SQLite file and return the path to open |
| `offshoot_checkpoint` | `database`, `name`, `branch?` | Name the current state so it can be rolled back to or forked from |
| `offshoot_fork` | `database`, `new_branch`, `branch?`, `at?`, `ttl?` | Create an isolated branch from head, or from a checkpoint via `at` |
| `offshoot_rollback` | `database`, `to`, `branch?` | Return a branch to a named checkpoint, discarding everything since |
| `offshoot_promote` | `database`, `source`, `target`, `force?` | Repoint `target` at `source`'s head — ship the winning attempt |
| `offshoot_destroy` | `database`, `branch`, `force?` | Permanently discard a branch and its checkout |

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
offshoot session open app -socket /tmp/o.sock    # THE harness-opened session
claude mcp add offshoot -- offshoot -store ./.offshoot -socket /tmp/o.sock mcp
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
  live through the daemon, `offshoot_fork` forks through it (flushing the
  source first, so unflushed writes land in the child), and
  `offshoot_checkout` returns the session's live checkout path.
- **Without one, every tool works exactly as if no daemon existed** — at
  rest.
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
- **LangGraph** — `ThreadForks` maps each thread to its own branch, so
  rewinding a conversation also rewinds the database
  ([LangGraph adapter](../sdk/python-langgraph/README.md)).
- **Other frameworks** — the OpenAI Agents SDK, LlamaIndex, and CrewAI
  each get a short honest recipe: [framework recipes](recipes/frameworks.md),
  [OpenAI Agents SDK](recipes/openai-agents.md).
