# offshoot — product gap analysis (PM view, 2026-08-12)

State at analysis time: v0.2.7. Engine complete and hardened (CoW forks, torture-verified
durability, MinIO-verified S3, bounded RPC waits), site live, docs accurate. Repo private.
Inside view: this session's build/audit history. Outside view: market scan of 2026-08-12
(competitors, adoption norms, positioning evidence) — scratchpad/market-scan.md, key claims
restated inline below.

## Verdict in one paragraph

The engineering is ahead of the product. offshoot has a validated category (Databricks/Neon
report agents now create the overwhelming majority of database branches; Turso markets
"SQLite for the agentic era"), a pre-cited pain (Anthropic and Braintrust both preach
clean-environment-per-trial; practitioners complain re-seeding turns 5-minute suites into
40-minute ones), and an EMPTY niche — no one ships local, OSS, stock-file SQLite branching
with live-writer capture (the closest OSS attempts, LiteTree and gRPSQLite, died). But the
product is unreachable (private repo), its headline claim has zero published evidence (all
benchmarks pre-date CoW), and its distribution surfaces (PyPI, npm, brew, MCP registry,
LangGraph) are unclaimed. Every week of delay is a week Dolt — publishing "database for AI
agents" content weekly and now running BranchBench — defines the category unopposed.

## Gaps by adoption funnel

### 1. Discover — the product effectively does not exist
- **P0 — Launch is the product gap.** Repo private: `go install` fails, site links are
  dead-ends, no HN moment possible, zero stars/social proof. Everything below compounds
  only after this. (User-gated: flip public, claim `offshoot-db` on PyPI + `@offshoot-db`
  on npm, publish SDKs, MCP registry + LangGraph listings — the standing launch checklist.)
- **P0 — The headline claim is unevidenced.** The pitch is O(1) forks / near-zero added
  bytes; every published number measures the pre-CoW materialize path. Ship a CoW benchmark
  page (fork latency + added-bytes vs N, vs `cp`, local + S3) and run BranchBench once
  public — Dolt built the yardstick; being absent from it cedes the comparison.
- **P2 — No demo asset.** A 60-second asciinema of seed → 100 forks → keep winner would
  carry the site and the launch post.

### 2. Try — install friction above the audience's floor
- **P1 — Packaging.** cgo makes `go install` fragile even once public; binaries per release
  exist but engineers expect `brew install`, a `docker run` image, and (increasingly) a nix
  flake. Litestream's launch landed on "single binary" simplicity — match it.
- **P2 — Windows.** No-Windows is acceptable for the CI/agent audience (Litestream
  precedent) but WSL2 must be documented as the supported path; today it isn't mentioned.

### 3. Integrate — the strongest surfaces are built but unshipped
- Built and genuinely differentiating: pytest fixtures, vitest/jest testkit, MCP tools
  (agents branch autonomously), daemon + SSE. These are the real distribution per the
  outside view — "the HN flywheel is worth ~1.4 stars/upvote then flatlines; fixtures,
  MCP registry, and a LangGraph adapter are the real distribution."
- **P1 — LangGraph checkpointer adapter.** The agent-framework audience's on-ramp; today
  they use LangGraph checkpointers over Postgres/SQLite without branching semantics.
  A `langgraph-checkpoint-offshoot` package is a weekend of work and a durable wedge.
- **P1 — CI cookbook.** A copy-pasteable GitHub Actions recipe: seed once, fork per test
  shard, matrix over attempts. The eval-harness segment lives in CI; meet them there.
- **P2 — SDK parity audit.** SDKs are daemon-first; document (or close) the gaps vs CLI
  (compact, lease, gc are CLI-only today).

### 4. Trust — strong evidence, weak packaging; one explicit anti-trust statement
- The torture harness (kill -9 half of every round, byte-identical convergence) is the
  single best trust asset and is underleveraged: one site bullet. Litestream's lesson —
  and HN's day-one question ("as well-tested as SQLite?") — argue for a dedicated
  "how offshoot is tested" page: torture numbers, the fencing/CAS model, the audit trail.
- **P1 — Stability contract.** CHANGELOG says the storage format "may change in a
  backward-incompatible way before 1.0 without a migration path." That sentence is an
  adoption blocker for exactly the cautious users the durability story attracts. Publish
  v1.0 criteria and commit now to either format migration or a documented export/reimport
  path for any pre-1.0 break.
- **P1 — AWS-proper verification.** S3 support is MinIO-verified (real provider, honest
  claim) but AWS itself is untested; the multipart checksum/precondition path is exactly
  where providers differ. One `OFFSHOOT_S3_TEST_BUCKET` run closes it; do it before the
  launch post claims S3 support.
- **P2 — Merge-question preemption.** Dolt's public takedown of fork-style branching is
  "forks aren't branches without merge." offshoot's answer is honest and good — attempts
  are disposable; promote ships the winner whole; row-level merge is a non-goal for the
  eval/agent workload — but it must be an FAQ entry before someone else writes it for us.

### 5. Scale / retain — adequate for the segment, two cheap wins
- Observability: 18 metric families is above par; a ready-made Grafana dashboard JSON
  (P2) converts it from "metrics exist" to "ops story".
- Single-writer-per-branch, no multi-node: correct scope (the same "stays single-node"
  discipline that made Litestream credible). Say it as a feature, not a limitation.
- Flank risk, watch-only: Morph Infinibranch / Modal snapshot whole VMs (<250ms) for
  funded agent labs. offshoot's durable wedges are free/local, SQL-granular,
  CI-native — positioning, not roadmap, handles this.

## Segments (sharpened)

1. **Primary: eval-harness engineers.** Pain pre-cited, cost-sensitive, CI-native. Served
   by: benchmark numbers, fixtures, CI cookbook, stock-file exit hatch. Buy on speed+trust.
2. **Secondary: agent-framework builders.** Served by: MCP registry listing, LangGraph
   adapter, TTL'd autonomous branching. Buy on autonomy+safety rails.
3. **Tertiary (later): local-first app developers.** Undo/branch UX over user data. Do not
   build for them yet; do not break them either (the stock-file invariant serves all three).

## Positioning correction

Soften "git for SQLite" as the lead. It is contested ground (overdone per the scan; Dolt
has pre-written the no-merge rebuttal, and offshoot deliberately lacks merge). Lead with
the job and the two proof points instead: **"Fork your SQLite database per attempt —
near-zero bytes, kill-9 durable, always a stock file."** Keep the git vocabulary for the
command surface (it aids learnability) without staking the identity on the analogy.

## Priority stack

| P | Gap | Cost | Why now |
|---|-----|------|---------|
| P0 | Launch: repo public, PyPI/npm claimed, SDKs published, MCP registry | user-gated | Nothing else compounds while private |
| P0 | CoW benchmark page (+ BranchBench entry once public) | small | Headline claim currently unevidenced; Dolt owns the yardstick |
| P1 | Packaging: brew tap, docker image, nix flake; WSL2 doc | small-med | Table stakes for try-in-5-minutes |
| P1 | LangGraph checkpointer adapter | small | The agent segment's on-ramp; real distribution |
| P1 | CI cookbook (GH Actions eval recipe) | small | Primary segment lives in CI |
| P1 | Stability contract: 1.0 criteria + migration/export promise | small | Converts durability story into adoption trust |
| P1 | AWS-proper S3 run | tiny (needs bucket) | Close the last honesty asterisk before launch claims |
| P2 | "How we test durability" page; merge FAQ; Grafana JSON; asciinema; SDK parity | small each | Trust packaging + objection pre-emption |

## What is NOT a gap (deliberate scope, keep saying no)

Row-level merge; multi-writer branches; Windows-native; a managed cloud service; page-level
cross-database dedupe. Each has a competitor whose complexity is the cautionary tale; the
single-binary, stock-file, single-writer discipline is the moat's other half.
