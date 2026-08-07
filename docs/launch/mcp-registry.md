# MCP registry manifest — notes

`server.json` (repo root) is a draft manifest for submitting `offshoot mcp`
to the [MCP registry](https://github.com/modelcontextprotocol). It is
**not submitted**.

## Provenance and scope of what's verified

This manifest's fields (`name`, `description`, `version`, `repository`,
`command`, `args`) were chosen from what this repo's own docs establish
about `offshoot mcp`:

- `command`/`args`: `offshoot -store ./.offshoot mcp` — the exact invocation
  [README.md](../../README.md)'s MCP section documents
  (`claude mcp add offshoot -- offshoot -store ./.offshoot mcp`), and what
  `cmd/offshoot/main.go`'s `mcp` subcommand actually parses (global `-store`
  before the subcommand; `-socket`/`-default-ttl` are optional and omitted
  here since they have working defaults — see README's MCP section for what
  they do).
- `description`: the seven tools (list, checkout, checkpoint, fork,
  rollback, promote, destroy) and the protected-branch guardrail, both
  documented in README.md's MCP section and implemented in
  `internal/mcp/tools.go`.
- `version`: `0.1.0`, tracking the `offshoot` **binary's** release
  (`v0.1.x` git tags, `.github/workflows/release.yml`) — deliberately
  **not** `sdk/VERSION`. The MCP server ships inside the Go binary, not
  either SDK; the two versioning tracks are independent (see
  CONTRIBUTING.md's Release process) and happen to both read `0.1.0` right
  now only because both are still pre-1.0 initial releases.

## What is not verified

The *shape* this manifest should take — field names beyond the obvious,
required-vs-optional fields, whether `command`/`args` is even the right
top-level structure for a registry submission (vs. a `packages` array with
a `transport` object, or something else the registry has since adopted) —
was not looked up against any live/current MCP registry schema. This repo's
own MCP docs (README.md's MCP section, `internal/mcp/*.go`) describe what
`offshoot mcp` does and how to invoke it; they don't describe the registry's
submission schema, and this task was scoped to not fetch or assume that
schema from outside the repo.

**TODO before actual submission** (tracked in
[docs/status.md](../status.md)): fetch the current registry schema at
submission time, validate `server.json` against it, and adjust field names/
structure as needed. Submission also needs a published `offshoot` binary
release to point at, which already exists (`v0.1.0`), so nothing here is
blocked on SDK publication the way the LangGraph listing
([docs/launch/langgraph-listing.md](langgraph-listing.md)) is.
