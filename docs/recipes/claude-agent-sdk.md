# Recipe: Claude Code / the Claude Agent SDK

Two pieces: getting offshoot into an agent's tool surface (MCP), and wiring
its branch lifecycle to session events (hooks) so a fork happens
automatically at the start of a task and a checkpoint happens automatically
at the end.

**Every claim below is checked against what actually shipped** — the MCP
tool descriptions, the daemon-mode reach documented in the README's
[MCP section](../../README.md#mcp) and [docs/reference.md](../reference.md),
and this repo's own `offshoot mcp` command. The hooks pattern in the second
half is **illustrative**: it shows the shape of the wiring, but the exact
hook-event JSON schema is Claude Code's surface, not offshoot's, and can
change between Claude Code releases — verify the current shape against
Claude Code's own hooks documentation before shipping it, and treat the
JSON field names here as "this is the pattern," not a pinned contract the
way the rest of this repo's docs are.

## MCP config

```
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

This registers `offshoot mcp` as an MCP server over stdio (see
[docs/reference.md](../reference.md)'s `offshoot mcp` entry) — no daemon
required to get seven tools into Claude's tool list: `offshoot_list`,
`offshoot_checkout`, `offshoot_checkpoint`, `offshoot_fork`,
`offshoot_rollback`, `offshoot_promote`, `offshoot_destroy`. Each tool's
description tells Claude *when* to reach for it (fork before a risky
change, checkpoint when something works, roll back when it doesn't), so a
bare `claude mcp add` is enough to get an agent branching on its own
initiative in an at-rest workflow — no hooks needed for that baseline case.

One default worth knowing before you rely on this baseline: every branch
`offshoot_fork` creates gets a **24h TTL by default** (`offshoot mcp
-default-ttl`, itself defaulting to `24h`), not "no TTL" — an agent that
forks and forgets is reap-eligible a day later rather than leaking the
branch forever. An explicit `ttl` argument on the `offshoot_fork` call
always overrides it (including `ttl:"none"` for no TTL); `-default-ttl
none` on `offshoot mcp` itself changes the default for every fork that
doesn't specify one. TTL alone reaps nothing without a running janitor
(`offshoot serve`, or a manual `offshoot gc`) — see
[docs/reference.md](../reference.md)'s `offshoot mcp` entry for the full
detail. This applies with or without the daemon-riding upgrade and the
hooks below; it's true of every `offshoot_fork` call in this at-rest
baseline case too.

The rest of this recipe is about a specific upgrade: making
`offshoot_checkpoint` and `offshoot_fork` ride *live daemon capture*
(incremental, no full-snapshot re-encode) instead of running at rest, by
making sure a session is already open before the agent's first tool call.

## The MCP daemon-mode reality (read this before wiring hooks)

**offshoot mcp never opens a session itself.** This is not a limitation to
work around — it's how the tool avoids the exact leak class the whole
milestone-2/3 TTL and lifecycle work exists to prevent (see
[docs/status.md](../status.md)'s MCP rows and
[ROADMAP.md](../../ROADMAP.md)'s Milestone 3 note on session lifecycle). An
MCP `open` tool with no natural owner responsible for closing it would
recreate that leak; the design deliberately leaves session lifecycle to
something that reliably closes what it opens — a harness, not a bare tool
call.

What that means in practice, quoted from the README's MCP section:

> **MCP rides a running daemon when one is up, but only for a branch a
> session is already open on.** `offshoot mcp` never opens a session
> itself... on every call, `offshoot_checkpoint`, `offshoot_fork`, and
> `offshoot_checkout` each check fresh whether the daemon has one open for
> the branch in question. If so: `offshoot_checkpoint` flushes it live
> through the daemon (no quiesce, no full-snapshot re-encode, no lease
> collision)... **Without an already-open session, every one of those
> tools runs exactly as it does with no daemon at all.**

So the recipe's job is narrow and concrete: **open the session yourself,
before the agent's first tool call, using the SDK or the CLI** — then let
Claude ride it through MCP for the rest of the task. This is exactly the
same shape the pytest fixture and TS testkit use for their own workloads
(see [docs/eval-harness.md](../eval-harness.md)) — a harness that owns
open/close, with MCP (or the agent) only ever touching an already-open
session.

### Opening the session, then letting the agent ride it

```
offshoot -store ./.offshoot init          # once
offshoot serve -socket /tmp/o.sock &      # holds leases, captures continuously
sleep 1
offshoot session open app -socket /tmp/o.sock    # THIS is the harness-opened session
```

```
claude mcp add offshoot -- offshoot -store ./.offshoot -socket /tmp/o.sock mcp
claude "Fix the flaky test in tests/test_orders.py, then checkpoint when it's green"
```

Now every `offshoot_checkpoint` call the agent makes against `app` flushes
through the already-open daemon session — incremental, no quiesce, no
full-snapshot re-encode — instead of running the at-rest path. When the
agent is done, close the session the same way you opened it:

```
offshoot session close app -socket /tmp/o.sock
```

Equivalently, open the session from the Python or TypeScript SDK instead of
the CLI (see the README's [Python SDK](../../README.md#python-sdk) /
[TypeScript SDK](../../README.md#typescript-sdk) sections) — anywhere a
harness can hold a `Client.open(...)` call open for the duration of the
agent's task works. The MCP tool calls don't know or care which one opened
the session; they just check whether one is open.

**This is the recipe's actual content.** Everything below (the hooks
pattern) is one way to automate "open before the agent starts, close after
it's done" so you don't have to run those two extra commands by hand every
time — it is not required to get the live-capture behavior above; the
manual open/close shown here already gets you there.

## Hooks pattern (illustrative)

Claude Code's hooks let you run a command at defined points in a session's
lifecycle — `SessionStart` when a session begins, `Stop` when Claude
finishes responding. The pattern this recipe wants:

- **`SessionStart`**: fork a fresh branch from `main` and open a daemon
  session on it — the harness-opened session the section above requires.
- **`Stop`** (task completion): checkpoint the branch through the now-open
  session.
- **On failure** (however your setup detects it — a failing test, a
  nonzero exit from your own verification step): roll back instead of
  checkpointing, and close the session either way.

A minimal `.claude/settings.json` sketch — **verify the exact hook-event
names and the JSON your Claude Code version passes on stdin/expects on
stdout against Claude Code's own hooks documentation before using this**;
this is the shape of the wiring, and the illustrative caveat at the top of
this doc applies specifically here:

```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "scripts/offshoot-session-start.sh"
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "scripts/offshoot-session-stop.sh"
          }
        ]
      }
    ]
  }
}
```

`scripts/offshoot-session-start.sh` — fork a branch named for this session
and open it:

```bash
#!/usr/bin/env bash
set -euo pipefail
BRANCH="agent-$(date +%s)"
offshoot -store ./.offshoot -socket /tmp/o.sock fork app "$BRANCH" --ttl 2h
offshoot -store ./.offshoot -socket /tmp/o.sock session open "app@$BRANCH"
echo "$BRANCH" > .offshoot-current-branch
```

`scripts/offshoot-session-stop.sh` — checkpoint if your own success check
passes, roll back otherwise, then close:

```bash
#!/usr/bin/env bash
set -euo pipefail
BRANCH="$(cat .offshoot-current-branch)"
if make test >/tmp/offshoot-hook-test.log 2>&1; then
  offshoot -store ./.offshoot -socket /tmp/o.sock session flush "app@$BRANCH" done
else
  offshoot -store ./.offshoot -socket /tmp/o.sock rollback "app@$BRANCH" --to fork
fi
offshoot -store ./.offshoot -socket /tmp/o.sock session close "app@$BRANCH"
```

Note what this pattern is *not* claiming: there is no MCP-level "on tool
failure" event in this repo's own seven tools, and offshoot has no opinion
about how your harness decides a task succeeded — `make test` above is a
stand-in for whatever your project's own success signal is. The fork's
`--ttl 2h` is the backstop if the `Stop` hook never runs at all (a crashed
session, a killed process) — see the eval-harness tutorial's
[TTL hygiene](../eval-harness.md#ttl-hygiene) section for the same pattern
applied to test fixtures.

## Without hooks: the good-path minimum

If hooks are more machinery than you want, the two commands under
"Opening the session, then letting the agent ride it" above are the entire
recipe — open before, close after, by hand or from a wrapper script around
`claude` itself. Hooks just automate that; they don't add anything MCP
itself doesn't already do once a session is open.
