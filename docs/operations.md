# Operations

**offshoot is operable, not orchestrated.** Everything on this page —
metrics, HTTP, events, budgets — describes ONE daemon process on ONE host,
watching over the branches in its own store. There is no cluster, no
placement, no failover, and no cross-node routing anywhere in this codebase;
running "hundreds of agent sessions" means hundreds of sessions against one
`offshoot serve` process (or one per host, each independent), not a fleet
coordinating with each other. See the FAQ's [why not
LiteFS](faq.md#why-not-litefs) for the deliberate reasoning behind that
scope, and [ROADMAP.md](../ROADMAP.md#non-goals-v1) for the standing
non-goal — "we don't use the word cluster" is a real constraint on this
document, not a slogan.

This page is the operator's reference: what to scrape, what a branch state
means when you're paged for it, how to watch the daemon live instead of
polling, how disk use is bounded, and the exact threat model behind
`-http`. For the flag-by-flag CLI reference (arities, defaults, error
messages), see [docs/reference.md](reference.md) — this page is organized
around operator tasks, that one around commands.

## Metrics

`GET /metrics` (behind the same Bearer auth as everything but `/healthz` —
see [HTTP/auth threat model](#httpauth-threat-model) below) serves a
Prometheus text exposition of every metric this daemon knows about, produced
by `internal/metrics`: a small, hand-rolled, zero-dependency registry
(`Counter`/`Gauge`/`Histogram`, `CounterVec`/`GaugeVec` for bounded label
sets) rather than a `prometheus/client_golang` dependency — the exposition
format itself is simple enough to not be worth a new dependency for, and
keeping the package `internal` means a future swap to the real client
library, if ever needed, is a call-site-only change (see
`internal/metrics/metrics.go`'s package doc comment for the one-sentence
rationale this page is intentionally not duplicating).

**Every name below is API.** Per PM Amendment 2/12: metric names freeze at
offshoot's first public tag — renaming any of them after that point is a
breaking change, exactly one free rename window between now and then. Do not
build a dashboard against a name not in this table.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `offshoot_build_info` | gauge | `version` | Always `1`; the `version` label identifies the running build (`"dev"` outside a released binary). |
| `offshoot_sessions_open` | gauge | — | Number of sessions currently open in this daemon. |
| `offshoot_capture_lag_bytes` | gauge | `db`, `branch` | WAL bytes committed by writers but not yet applied to the replica. **Open sessions only** — a branch with no live session reports nothing (not zero — absent). |
| `offshoot_durable_age_seconds` | gauge | `db`, `branch` | Seconds since that session's last successful flush. **Open sessions only**, same absence-not-zero rule. |
| `offshoot_flush_total` | counter | `result` (`ok`/`error`), `kind` (`auto`/`manual`) | Session flushes, by outcome and whether it was `serve -flush-every`'s timer or an explicit `flush` call. All four combinations are pre-registered at `0` from daemon start, so a `rate()` over a combination that's never happened reads `0`, not "no data." |
| `offshoot_flush_duration_seconds` | histogram | — | Flush latency, fixed buckets (see [Histogram buckets](#histogram-buckets) below). |
| `offshoot_fork_total` | counter | `path` (`fast`/`slow`) | Successful forks, by path — `fast` = single-snapshot object copy (reflink/clonefile locally, server-side `CopyObject` on S3-compatible backends), `slow` = materialize + re-encode. Both label values are pre-registered at `0`. |
| `offshoot_fork_duration_seconds` | histogram | — | Successful fork latency. |
| `offshoot_checkpoint_duration_seconds` | histogram | — | **At-rest** checkpoint latency only — a process that calls `ops.Workspace.Checkpoint` directly (the CLI or `offshoot mcp`, no daemon session involved). A live session's named `flush` is *not* counted here; it's a flush, tallied under `offshoot_flush_duration_seconds` instead. This histogram reads all-zero on a daemon that only ever serves live sessions and never itself runs an at-rest checkpoint. |
| `offshoot_reap_total` | counter | — | Branches reaped (TTL-expired, destroyed) by the janitor. |
| `offshoot_gc_tombstoned_total` | counter | — | Lineages newly tombstoned by a GC pass. |
| `offshoot_gc_deleted_total` | counter | — | Lineages actually deleted by GC, after their grace period. |
| `offshoot_gc_backlog` | gauge | — | Tombstoned lineages currently sitting inside their grace period, awaiting deletion. A sustained climb here means GC isn't keeping up with tombstoning, not (by itself) a leak — check `-gc-grace` against your churn rate. |
| `offshoot_ro_cache_bytes` | gauge | — | Bytes currently used by `checkouts-ro` (the read-only checkpoint cache). Updated **once per janitor pass**, not continuously — see [Budgets](#budgets) below for what that staleness window means in practice. |
| `offshoot_ro_cache_evictions_total` | counter | — | `checkouts-ro` entries evicted by the janitor's LRU pass under `-ro-cache-budget`. Zero forever on a daemon started with the default (unlimited) budget. |
| `offshoot_janitor_runs_total` | counter | `result` (`ok`/`error`) | Janitor loop ticks, by whether the tick completed cleanly. Both values pre-registered at `0`; stays entirely at `0` if `-reap-every 0` disabled the janitor. |

**Histogram buckets** (`offshoot_flush_duration_seconds`,
`offshoot_fork_duration_seconds`, `offshoot_checkpoint_duration_seconds`),
in seconds: `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
30, 60, 120, 300, +Inf` — fixed, not configurable.

### Try it

Verified against a real build of this branch (`go build -o offshoot
./cmd/offshoot`):

```
$ offshoot -store ./store init
$ offshoot -store ./store serve -socket ./o.sock -http 127.0.0.1:18080 -token verify-token-123 &
offshoot: http listening on 127.0.0.1:18080 (bearer auth, token fingerprint verify-t)
offshoot serving on ./o.sock

$ curl -s http://127.0.0.1:18080/healthz
{"ok":true,"sessions":0}

$ curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18080/metrics
401

$ curl -s -H "Authorization: Bearer verify-token-123" http://127.0.0.1:18080/metrics | grep '^# TYPE'
# TYPE offshoot_build_info gauge
# TYPE offshoot_sessions_open gauge
# TYPE offshoot_capture_lag_bytes gauge
# TYPE offshoot_durable_age_seconds gauge
# TYPE offshoot_flush_total counter
# TYPE offshoot_flush_duration_seconds histogram
# TYPE offshoot_fork_total counter
# TYPE offshoot_fork_duration_seconds histogram
# TYPE offshoot_checkpoint_duration_seconds histogram
# TYPE offshoot_reap_total counter
# TYPE offshoot_gc_tombstoned_total counter
# TYPE offshoot_gc_deleted_total counter
# TYPE offshoot_gc_backlog gauge
# TYPE offshoot_ro_cache_bytes gauge
# TYPE offshoot_ro_cache_evictions_total counter
# TYPE offshoot_janitor_runs_total counter
```

Sixteen `# TYPE` lines, matching the sixteen rows in the table above exactly
— that grep is the whole verification: run it yourself against a running
daemon any time this table is in doubt.

## Branch states

Every branch is in exactly one of six computed states — nothing about state
is persisted anywhere; `offshoot status` and the daemon `branches` op
recompute it fresh on every call. Full mechanics, cost, and edge cases are
in [docs/reference.md](reference.md#branch-states); this table is the
paged-at-3am version — what each state means for *you*, right now.

| State | What it means operationally | Who can report it |
|---|---|---|
| `active` | Someone holds a live lease and is (or was recently) writing. Normal for any branch in active use. | Both CLI/at-rest and daemon |
| `pending` | This daemon has a session slot reserved and is mid-`open` — not yet live, but spoken for. If a branch sits here for more than a few seconds, `open` is stuck (slow store attach, a wedged lock) — investigate that daemon, not the branch. | Daemon only |
| `error` | A session is open and its `Err()` is non-nil — lease loss, a capture failure, a contract violation. **This is the state to alert on.** `session status` (or the daemon `status` op) names the actual error. | Daemon only |
| `dirty` | No live lease; a checkout exists with un-checkpointed local edits (content hash differs from the ref, sidecar identity otherwise matches). Expected mid-workflow (someone's `sqlite3`'d the checkout by hand); unexpected on a branch you thought was fully flushed. | Both |
| `detached` | No live lease; a checkout's sidecar-recorded lineage no longer matches the ref's current lineage — an orphan left behind by a `rollback`/`promote` whose best-effort checkout refresh didn't run (the checkout was busy at repoint time). Re-run `offshoot checkout` to fix it; the old checkout content isn't wrong, just stale relative to a branch that moved on. | Both |
| `idle` | None of the above — nothing going on. **Not in the original design spec** (see reference.md's note); added because at-rest `offshoot status` has no daemon and needs a name for "quiet." | Both |

**Precedence, most to least specific:** `error` > `pending` > `active` >
`dirty` > `detached` > `idle`. `error` and `pending` can never both apply to
one branch (a daemon's session map holds at most one entry per `db@branch`).
The precedence that actually bites in practice is `active` over
`dirty`/`detached`: a branch can be both leased and locally modified at the
file level, and the lease wins the report — don't read `active` as "nothing
else to worry about here," it just means the lease question was answered
first.

**Cost note for operators:** determining `dirty` requires a real WAL
checkpoint plus a full SHA-256 hash of the checkout, per branch, on every
`status`/`branches` call, for every checked-out-and-unleased branch. A store
with many large, checked-out, unleased branches will feel `offshoot status`
get slower as that set grows — this is not a bug to file, it's the
documented cost of the only correct way to detect it (see
reference.md#branch-states's Cost section for why there's no cheap
short-circuit).

## Eventing

The daemon publishes one versioned JSON event per state transition it
observes — the answer to "stop polling `status` in a loop." Full protocol
detail (exact wire shapes, the `subscribe` op, SDK helpers) lives in
[docs/reference.md](reference.md#eventing-subscribe-op--get-events); this
section is the operational summary.

**Schema**, versioned from day one (`v` is always `1` today):

```json
{"v":1,"ts":"2026-08-07T12:00:00Z","type":"flushed","db":"app","branch":"main","detail":{"kind":"manual","txid":7,"duration_seconds":0.017}}
```

**Eight event types:**

| `type` | Fired when |
|---|---|
| `session_opened` | A session opens |
| `flushed` | A flush succeeds (manual or the `-flush-every` timer) |
| `flush_failed` | A flush fails |
| `fenced` | A session is fenced out by a lease it no longer holds |
| `session_closed` | A session closes |
| `reaped` | The janitor destroys a TTL-expired branch |
| `evicted` | The janitor evicts a `checkouts-ro` entry over `-ro-cache-budget` |
| `dropped_slow_consumer` | Sent to a subscriber right before it's dropped — never to anyone else |

**Two ways to drain the same stream:** the unix socket's `subscribe` op
(dedicated connection — see below) for anything already talking to the
socket, and HTTP `GET /events` (Server-Sent Events) for anything that isn't.
Both are fed by the same publisher and encoded by the same function, so a
socket subscriber and an SSE subscriber watching the same daemon see
identical events in identical order.

**`subscribe` takes over the whole connection.** Sending `{"op":"subscribe"}`
on the unix socket gets one `{"ok":true}` ack and then the connection
*permanently* leaves request/response mode — it streams one JSON event per
line until disconnect, and the daemon stops reading anything else on it.
**Use a fresh, dedicated connection for this** — never your session's own
connection — or you lose the ability to `flush`/`close`/anything else on
that connection for the rest of its life. Both SDKs ship a thin `events()`
helper that already does this correctly (opens its own socket, subscribes,
yields events); reach for that instead of hand-rolling the connection
management. `subscribe` sent over HTTP `POST /rpc` is refused outright,
pointing you at `GET /events`.

**Drop-slow-consumer semantics — the thing to understand before you alert on
it:** publishing an event NEVER blocks the daemon. Each subscriber gets a
bounded 64-event buffer; if a publish finds that buffer full, the daemon
drops the subscriber immediately — removes it, sends exactly one terminal
`dropped_slow_consumer` event, closes the stream — rather than waiting for
it to catch up or slowing down the session/janitor that's trying to
publish. A write-heavy session keeps flushing at full speed even while a
subscriber that never reads its channel gets dropped out from under it.
Separately, a subscriber that stays *connected* but stops reading its
socket/HTTP response is bounded by a re-armed 45-second per-write deadline
— the daemon gives up and closes that connection too, rather than leaking
the goroutine and file descriptor forever. Practical upshot for an
operator: if a monitoring process's event stream drops, that is *never* a
sign of daemon trouble — check the monitor's own read loop first. `GET
/events` also emits a `: ping` comment line every 15 seconds so a
proxy/load balancer/kubelet watching for silent connections doesn't kill
the stream on its own.

## Budgets

Today there is exactly one disk budget: `serve -ro-cache-budget
<bytes|0>` (default `0` = unlimited), bounding `checkouts-ro` — the
read-only cache `offshoot checkout --at --read-only` / the daemon
`checkout-at` op materializes into. **`checkouts/` (the writable, leased
tree) is never evicted, by construction** — there is no code path in the
eviction pass that can even name a `checkouts/` path, so a leased, currently
open session's checkout survives even the most aggressive budget (`1`, which
forces every `checkouts-ro` entry out) untouched. FD budgets beyond this are
deliberately out of scope for this milestone — see
[Deliberately out of scope](#deliberately-out-of-scope-in-m4) below.

**The LRU clock is a `.last-used` touch-on-hit file, not the cache file's
own mtime.** A cache file's mtime is set exactly once, by the materialize
that created it; a repeat cache hit is a pure read that never touches the
file again. Without a separate marker, "least recently used" would silently
become "least recently created" — backwards for a cache whose entire point
is that a checkpoint hit over and over should stay hot. So every cache HIT
touches a `<cachefile>.last-used` sidecar to now; ranking falls back to the
`.db` file's own mtime only for an entry that's never been hit since it was
created.

**`checkouts-ro` remains safe to `rm -rf` at any time, budget or not** — a
budget just automates what manual cleanup would otherwise require by hand;
the next call for anything under it rebuilds what it needs, since a
checkpoint's content never changes.

**Eviction is loud:** one stderr line per entry
(`offshoot: janitor: ro-cache: evicted <db>@<branch>@<checkpoint> (<bytes>
bytes)`), `offshoot_ro_cache_evictions_total` incremented, and an `evicted`
event published on the [event bus](#eventing).

**The eviction-vs-CheckoutAt race, and why it's safe:** a path
`checkout --at --read-only` returns isn't a guarantee the file still exists
by the time you open it, once a nonzero budget is running — a concurrent
janitor pass can evict that exact entry in the window between the call
returning and your own `open()`. Two rules make this a non-issue rather than
a data-loss risk:

- **Already opened the file?** Keep reading. POSIX unlink-of-an-open-file
  semantics mean an eviction racing your open connection never corrupts or
  truncates what you already have a handle on — it just stops being visible
  to a future `open`/`stat` on that path.
- **Got `ENOENT` on a path you were just handed?** That means "evicted since
  that call returned," not corruption or data loss. Re-call `checkout --at
  --read-only`; a checkpoint's content is immutable, so re-materializing
  gives you byte-identical content.

`offshoot status`'s trailing `ro-cache: N entries, B bytes used (budget:
...)` line reports current usage at rest, no daemon required; its own
`-ro-cache-budget` flag there is display-only (echoes back what you intend
to run a daemon with — it's never persisted, so `status` has no other way to
know what a *running* daemon was actually started with).

## HTTP/auth threat model

**This is single-tenant, same-host-or-trusted-network auth — not a
multi-tenant isolation boundary.** Anyone holding the token can do
everything any daemon client can do: open sessions, flush, fork, destroy,
read metrics, pull a pprof profile. That's identical to what anyone able to
open the unix socket today can already do; `-http` doesn't add a new
privilege level, it adds a new transport to the same one. Do not point
`-http` at an address reachable by anyone you wouldn't also hand shell
access on this host. There is no TLS — a non-loopback bind belongs behind a
trusted network boundary (VPN, private subnet) of its own, not exposed
directly.

**Off by default.** `serve -http 127.0.0.1:PORT` binds loopback with no
further acknowledgment needed. Any other address requires BOTH
`-http-allow-non-loopback` (an explicit ack) AND an explicit
`-token`/`OFFSHOOT_TOKEN` — the auto-generated, printed-once token is a
loopback-only convenience, refused outright for a non-loopback bind. Missing
either is a distinct startup error; the daemon never starts under-
acknowledged.

**The token is a shared secret, checked in constant time**
(`crypto/subtle.ConstantTimeCompare`), and **never logged in full again**
after the one line it's printed on at startup — every later reference to it
(status output, log lines) is an 8-character fingerprint only, never enough
to reconstruct the token. **Treat stderr as sensitive at daemon startup**:
when no `-token`/`OFFSHOOT_TOKEN` is given, the auto-generated token is
printed to stderr exactly once, and that includes your terminal scrollback
and shell history if you didn't redirect it — treat that moment, not just
the process's ongoing logs, as the sensitive one.

**Prefer `OFFSHOOT_TOKEN` over `-token` on a shared host.** `-token TOKEN`
on the command line is visible to any other user on the host via `ps`
(process argument lists are not private) for as long as the daemon runs;
`OFFSHOOT_TOKEN=... offshoot serve ...` is not — environment variables set
this way aren't listed in `ps` output the same way. This is a minor but real
gap between the two equivalent-looking ways to set the same token; the
environment variable is the better default on any host you don't fully
control.

**Every route but `GET /healthz` requires `Authorization: Bearer
<token>`**, including `GET /debug/pprof/*` — the highest-value 3am
debugging tool (goroutine dumps, CPU profiles, live traces) sits behind the
same auth as everything else, not left open because "it's just profiling."
`/healthz` is deliberately unauthenticated so a liveness/readiness probe
doesn't need the token at all.

`http.Server` runs explicit timeouts rather than the stdlib's unbounded
defaults: `ReadHeaderTimeout` 5s, `ReadTimeout` 30s, `WriteTimeout` 90s
(sized to leave headroom for `/debug/pprof/profile`'s default 30s capture
window — a longer `?seconds=` request than that budget allows gets cut off;
request a shorter window or profile out-of-band), `IdleTimeout` 2 minutes.
`GET /events` is the one route NOT bound by that 90s `WriteTimeout` — it
manages its own per-write deadline instead (see [Eventing](#eventing)
above), since a live subscription is expected to outlive 90 seconds by
design. `POST /rpc` request bodies are capped at 1MiB
(`http.MaxBytesReader`); an oversized body gets `413`, and the connection
stays usable afterward.

## Tuning flags

All five are `offshoot serve` flags; none are persisted, so a restarted
daemon needs them passed again (or scripted the same way every time —
there's no config file).

| Flag | Default | What it trades off |
|---|---|---|
| `-flush-every DURATION` | `30s` (`0` disables) | How much committed-but-unflushed work is ever at risk: a daemon that dies loses at most one interval's worth of writes. Lower = tighter bound on data loss, more frequent background upload traffic. `0` returns to "durability only advances on explicit `flush`," this project's original behavior. |
| `-snapshot-every N` | `16` (must be `>= 1`) | How many segments a read replays past the last snapshot before it's capped by a fresh full upload. Lower N = cheaper, more tightly bounded reads, at the cost of a full-database upload more often; higher N amortizes upload cost across more flushes at the cost of longer per-read replay. See [docs/benchmarks.md](benchmarks.md) for the measured trade-off at the default of 16, and the flush-cost interaction below. |
| `-reap-every DURATION` | `1m` (`0` disables the janitor entirely) | How often the janitor sweeps for TTL-expired branches, runs GC, and (if a budget is set) evicts over-budget `checkouts-ro` entries. `0` doesn't just slow this down — it turns the whole janitor loop off; `offshoot gc` remains available on demand. Every metric this page's [Budgets](#budgets)/GC rows describe as "once per pass" is gated on this same interval. |
| `-gc-grace DURATION` | `15m` | How long a tombstoned (unreferenced) lineage sits before it's actually deleted. `0` makes it eligible on the very next `-reap-every` tick after tombstoning, rather than disabling GC. A lineage re-referenced during the grace window (e.g. a fork racing GC) is left alone. |
| `-ro-cache-budget BYTES` | `0` (unlimited) | See [Budgets](#budgets) above in full; accepts a bare byte count or a `K`/`M`/`G`/`T` power-of-1024 suffix. |

**The flush-cost/replay interaction, in one place:** `-flush-every` and
`-snapshot-every` compose. Under continuous writing, the default
`-flush-every 30s` × the default `-snapshot-every 16` means a full snapshot
ships roughly every 8 minutes (30s × 16 ticks) for as long as the agent
keeps writing between every tick; an idle session (nothing committed, no
rebase pending) skips the tick entirely — no object write at all — so a
quiet session pays none of this regardless of how the two flags are tuned.
Tightening `-flush-every` (more frequent flushes) without also raising
`-snapshot-every` means paying that full-snapshot cost more often in
wall-clock terms, not just more often per flush count; see [What a flush
costs](../README.md#what-a-flush-costs) in the README and
[docs/benchmarks.md](benchmarks.md) for the measured numbers behind this
trade-off.

## Deliberately out of scope (in M4)

Considered for this milestone and explicitly declined, not simply not yet
started:

- **Multi-node anything** — placement, failover, cross-node routing. See
  the top of this page and [ROADMAP.md](../ROADMAP.md#non-goals-v1).
- **TLS** — loopback/token-only for now; revisit with real non-loopback
  demand.
- **Per-branch at-rest metrics by default** — a gauge per branch that has
  never been opened this process would make label cardinality scale with
  total branch count instead of open-session count; not the default
  (`capture_lag_bytes`/`durable_age_seconds` stay open-sessions-only). A
  `dbs`-scoped scrape option for at-rest branches is a future addition, not
  built here.
- **FD budgets beyond the documented dbfile retention story** — the
  descriptors `internal/dbfile` holds are deliberately unclosable by
  design (see that package's doc comment); an idle-checkout eviction budget
  on top of that is future work, not built this milestone.
- **Metrics push/remote-write** — `/metrics` is pull-only; no push gateway
  integration.

See [docs/status.md](status.md) for the authoritative shipped/deferred
matrix these bullets summarize.
