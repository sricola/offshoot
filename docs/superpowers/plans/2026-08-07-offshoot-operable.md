# Offshoot Milestone 4: Operable at Scale

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A platform running hundreds of agent sessions can see (metrics, states, events), bound (budgets), and automate (HTTP + token) offshoot.

**Architecture:** A hand-rolled Prometheus-text metrics registry (zero new deps — the exposition format is trivial and the spec's stdlib-preferred stance holds) instrumented at the ops/session/daemon layers, exposed with health and RPC over an opt-in loopback HTTP listener with single-token auth. Eventing is an in-daemon bus fed by the existing transition-log sites, streamed over the unix socket (a `subscribe` op switches the connection to line-per-event mode) and as SSE over HTTP. Budgets start where the disk actually grows without bound today: the ro-cache, LRU-evicted at janitor cadence. Branch states are computed, not stored.

**Tech Stack:** Go stdlib only (net/http, no prometheus client dep). SDK typing polish rides along (typed TS responses; Python py.typed).

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; cgo; Linux/macOS; **no new dependencies** (the Prometheus text format is hand-rolled; SSE is stdlib)
- HTTP is OFF by default; `serve -http 127.0.0.1:PORT` binds loopback; any non-loopback bind additionally requires `-http-allow-non-loopback` (named ack flag) AND a token; token from `-token`/`OFFSHOOT_TOKEN`, else auto-generated and printed ONCE to stderr at startup; every HTTP request requires `Authorization: Bearer <token>` except `/healthz`
- The unix socket keeps working exactly as today with zero configuration — HTTP is additive, never required
- Eventing must never block the daemon: slow subscribers get bounded buffers and are DROPPED with a terminal event, never back-pressure a session or the janitor
- Budgets: writable leased checkouts are NEVER evicted; ro-cache eviction only; a budget of 0 = unlimited (default); eviction is loud (log line + metric)
- Branch states are COMPUTED from existing truth (ref, lease, sidecar, session map) — no new persisted state
- Any change under `internal/session`/`internal/capture` flush/capture paths → synchronous tee'd torture with numbers (foreground single Bash call)
- Wire compat: new ops/fields optional; existing SDKs unchanged against the new daemon
- Every doc command runs verbatim; status.md flips per task; CHANGELOG [Unreleased]; conventional commits + repo trailers

## PM Amendments (binding — from the standing advisory posture)

1. **Execution order:** T1 (states) → T2 (metrics registry) → T3 (HTTP+auth+expose) → T4 (eventing) → T5 (budgets) → T6 (daemon tuning + CAS delete) → T7 (SDK typing) → T8 (docs/recipes). T4 and T5 are the designated cut candidates if review cycles blow out.
2. **Metrics names are API**: `offshoot_` prefix, snake_case, labels `{db,branch}` only where cardinality is naturally bounded (branches can be many: per-branch gauges limited to OPEN SESSIONS; per-branch durable-age for at-rest branches is a `dbs`-scoped scrape option, not default). Lock the metric name list in T2's dispatch; renames after publish are breaking.
3. **The token is a shared secret, not auth theater**: constant-time compare, never logged, redacted from status output; the threat-model paragraph states plainly this is single-tenant same-host-or-trusted-network auth, not multi-tenant isolation.
4. **SSE and socket streams carry the SAME event JSON** (one encoder); event schema versioned with a `v` field from day one.
5. **CAS-conditional ref delete**: local backend gets true conditional delete via its existing lock; S3 documents the claim-marker pattern (Reaping-flag generalization) as THE mechanism — do not pretend S3 DeleteObject has preconditions.
6. **T3 additionally ships**: token-gated `/debug/pprof` (net/http/pprof, behind the same Bearer auth — the highest-value 3am tool, near-free), and explicit `http.Server` timeouts + a request-body size limit on `/rpc`.
7. **T2 hard gate**: CI validates `WritePrometheus` output with `promtool check metrics` (downloaded in CI; local runs skip if absent) plus golden-file tests; `internal/metrics` stays internal so a later client_golang swap is a non-event; docs own the zero-dep choice in one sentence.
8. **T4 split**: T4a = bus + socket `subscribe` + drop semantics + SSE (one encoder); T4b = SDK stream helpers (Python generator, TS async iterator) — independently cuttable to post-launch. Cut order under pressure: T4b → T5 → T4a.
9. **T6b timebox**: one review cycle; fallback is the narrower Destroy-only claim-guard, NO rewrite of the existing Reaping-flag mechanics.
10. **Token tightening**: auto-generate-and-print satisfies LOOPBACK only; `-http-allow-non-loopback` additionally REQUIRES an explicit `-token`/`OFFSHOOT_TOKEN`. `status` may show a token fingerprint (first 8 chars), never the token. operations.md: "treat stderr as sensitive at startup."
11. **T5 pre-decision**: the LRU clock is a `.last-used` touch-on-hit file (mtime lies after materialization).
12. **SSE emits periodic `: ping` keepalive comments** (proxies/kubelets kill silent streams). operations.md's FIRST paragraph restates single-node scope. CHANGELOG notes metric names freeze at the first public tag (one free rename window until then).
13. **Parallel, not part of this plan's tasks**: the launch demo assets (asciinema of parallel-attempts; the MCP-in-Claude-Code story) are NOT user-gated and get recorded alongside T1; a standing user-gated launch-items nag lives in status.md.

## File Structure

```
internal/metrics/metrics.go        registry: counters/gauges/histograms, prom text exposition
internal/metrics/metrics_test.go
internal/ops/status.go             (extract/modify) branch-state computation
internal/daemon/http.go            HTTP listener: /rpc /metrics /healthz /events, token auth
internal/daemon/http_test.go
internal/daemon/events.go          event bus + subscribe op streaming
internal/daemon/events_test.go
internal/daemon/server.go          (modify) instrumentation + wiring
internal/session/session.go        (modify) transition events emitted via a callback the daemon injects
internal/ops/ops.go                (modify) fork/checkpoint latency instrumentation hooks
internal/ops/gc.go + reap.go       (modify) backlog/reap metrics; ro-cache eviction
internal/store/local.go + s3.go    (modify) DeleteRefIf / claim-marker doc
cmd/offshoot/main.go               (modify) serve flags: -http -token -http-allow-non-loopback -ro-cache-budget -snapshot-every
sdk/typescript/src/client.ts       (modify) typed per-op response shapes (replace internal any)
sdk/python/offshoot/py.typed       + minor annotations
docs/operations.md                 the operator page: metrics reference, states, events, budgets, HTTP/auth threat model
docs/recipes/kubernetes.md         sidecar recipe (shared volume, socket + checkouts co-local)
```

## Task List (outline granularity; dispatches carry exact interfaces; reviewers hold the bar)

### Task 1: Branch-state taxonomy
`ops.BranchState(db, branch)` and surfaced in `Status()`/`branches` op/CLI: `active` (live lease), `pending` (session slot reserved, mid-open), `dirty` (checkout modified vs head — sidecar/hash truth), `detached` (checkout exists whose recorded lineage no longer matches the ref — post-promote/rollback orphan), `error` (session present with Err() set), `idle` (none of the above; NOTE: the spec's taxonomy had no idle — it assumed daemon-full-time; add it, document the addition). Computed only; states documented in docs/operations.md with exactly-one-state-per-branch invariant tested.

### Task 2: Metrics registry + instrumentation
`internal/metrics`: tiny registry (Counter/Gauge/Histogram with fixed buckets; `WritePrometheus(w io.Writer)`), zero deps, concurrent-safe. Locked metric list (names are API): `offshoot_build_info{version}`, `offshoot_sessions_open`, `offshoot_capture_lag_bytes{db,branch}` (open sessions only), `offshoot_durable_age_seconds{db,branch}` (open sessions only), `offshoot_flush_total{result=ok|error,kind=auto|manual}`, `offshoot_flush_duration_seconds` (histogram), `offshoot_fork_total{path=fast|slow}`, `offshoot_fork_duration_seconds`, `offshoot_checkpoint_duration_seconds`, `offshoot_reap_total`, `offshoot_gc_tombstoned_total`, `offshoot_gc_deleted_total`, `offshoot_gc_backlog` (tombstones awaiting grace), `offshoot_ro_cache_bytes`, `offshoot_ro_cache_evictions_total`, `offshoot_janitor_runs_total{result}`. Instrumentation points wired (ops fork/checkpoint timers via injected hooks to avoid an ops→metrics dependency — decide the injection shape; session flush counters via the daemon's existing transition callback).

### Task 3: HTTP listener + token auth + exposure
`internal/daemon/http.go`: `POST /rpc` (the existing Request/Response JSON, one op per POST — same dispatch, byte-compatible semantics), `GET /metrics` (prom text), `GET /healthz` (no auth, `{"ok":true,"sessions":N}`), 404 else. Token per Global Constraints + PM 3 (constant-time compare; auto-gen printed once). Flags per Global Constraints; non-loopback without the ack flag = startup error. Shutdown integrates with the existing ordering discipline. Tests: auth rejects/accepts, rpc parity with socket (same op same result), healthz unauthenticated, non-loopback refusal, token redaction (never in status/logs — grep-test the log output).

### Task 4: Eventing
`internal/daemon/events.go`: bus with bounded per-subscriber buffers (drop-with-terminal-event on overflow per Global Constraints). Event JSON (one schema, `v:1`): `{v,ts,type,db,branch,detail{}}` for types `session_opened|flushed|flush_failed|fenced|session_closed|reaped|evicted`. Sources: the existing session transition-callback sites + janitor. Socket: `subscribe` op acks then streams line-per-event until client disconnect (document: connection leaves request/response mode — SDKs must use a dedicated connection; add `events()` generator to Python SDK + async iterator to TS as thin helpers on a fresh connection). HTTP: `GET /events` = SSE, same JSON in `data:`. Tests: subscriber sees open/flush/close for a session; slow-subscriber dropped with terminal event and daemon unaffected (write-heavy session continues); SSE parity.

### Task 5: Ro-cache disk budget
`serve -ro-cache-budget <bytes|0>` (default 0=unlimited): janitor pass computes checkouts-ro usage (`offshoot_ro_cache_bytes`), evicts LRU (atime unreliable — use mtime, or a `.last-used` touch on cache HIT in CheckoutAt; decide and document) until under budget; never touches `checkouts/` (writable); eviction logs + metric. `status` shows cache usage. docs/operations.md budget section reiterates safe-to-rm and the writable-never-evicted guarantee.

### Task 6: Daemon tuning + CAS-conditional ref delete
(a) `serve -snapshot-every N` (session Options plumb, default 16 unchanged); document the flush-cost interaction. (b) `Backend.DeleteRefIf`-shaped conditional delete: local = real (existing lock CAS); S3 = the claim-marker pattern formalized (Destroy CAS-writes a `Deleting` claim via PutIf before DeleteRef — generalize/replace the Reaping-flag mechanics so ALL destroys are claim-guarded, closing the M2-noted Destroy TOCTOU); wire Destroy; tests incl. the concurrent-lease-acquire race the M2 review documented.

### Task 7: SDK typing polish (pre-publish list)
TS: per-op internal response interfaces replacing the internal `Promise<any>` plumbing (public signatures unchanged — verify d.ts diff is additive); `_call` demoted from the published d.ts if achievable without breaking the testkit (module-internal symbol). Python: `py.typed` marker + annotations already largely present — mypy --strict pass on the package, fix what it flags (no behavior changes). Both SDK suites green; tarball assertions updated if file lists change.

### Task 8: Operator docs + recipes
docs/operations.md: metrics reference table (every metric from T2's locked list with meaning + labels), states table, events schema, budgets, HTTP/auth threat model (PM 3 wording), tuning flags. docs/recipes/kubernetes.md: sidecar pattern (shared emptyDir volume for socket + checkouts; SQLite path must be co-local — restate; liveness via /healthz; metrics scrape annotation) — honest about single-node scope (no multi-node; link FAQ). README operator paragraph + integration-surface refresh; reference.md new flags; status.md flips (metrics/HTTP/auth/eventing/budgets rows → shipped; spec's deferred rows retired); CHANGELOG [0.1.3] drafted; ROADMAP M4 checked off.

## Deliberately out of scope (status.md rows)
- Multi-node anything; TLS (loopback/token only — v2 with real demand); per-branch at-rest metrics by default (cardinality); FD budgets beyond the documented dbfile retention story (the descriptors are deliberately unclosable — see internal/dbfile); metrics push/remote-write.

## Self-Review
1. Roadmap M4 coverage: metrics T2/T3, HTTP+auth T3, states T1, eventing T4, budgets T5, SnapshotEvery T6a, CAS delete T6b, typed TS T7, recipes T8. All bullets have tasks; FD-budget bullet consciously narrowed with a stated reason (dbfile's design makes closing unsafe; documented not built).
2. Placeholder scan: outline granularity per M3 precedent; no TBDs; the two "decide and document" points (metrics injection shape, LRU clock source) are bounded single decisions assigned to their tasks.
3. Type consistency: metric names locked once in T2 and referenced in T3/T5/T8; event schema defined once in T4; state names defined once in T1 and used in T8's table.
