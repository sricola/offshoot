# Security policy

## Supported versions

offshoot is at 0.x. There is no long-term support branch and no
backport policy yet — the only supported version is the latest commit on
`main`. If you're running an older tag, upgrade before reporting; the fix is
very likely already in `main`.

Once there's a 1.0, this file will get a real support-window table. Until
then, treat everything as "best effort, latest only."

## Verifying a release

Releases are built and signed in CI, not by hand: every tagged tarball and
the GHCR image carry a keyless Sigstore signature and SLSA build
provenance, with no maintainer-held key involved. The identity to pin when
verifying is the release workflow itself
(`.github/workflows/release.yml` on `sricola/offshoot`), not a person. See
the [installation page](docs/installation.md#verify-what-you-downloaded)
for the exact verification commands
(<https://github.com/sricola/offshoot/blob/main/docs/installation.md#verify-what-you-downloaded>).

## Reporting a vulnerability

**Do not open a public issue for a security report.** Use GitHub's private
vulnerability reporting instead, at
<https://github.com/sricola/offshoot/security/advisories/new> — or by hand:

1. Go to the repo's **Security** tab.
2. Click **Report a vulnerability**.
3. Describe the issue, how to reproduce it, and what you think the impact is.

This opens a private advisory visible only to the maintainer (and to you)
until it's resolved and a disclosure timeline is agreed on.

If GitHub's reporting flow isn't available to you for some reason, email the
maintainer directly (see the CODE_OF_CONDUCT.md contact section) with
`[security]` in the subject.

## Threat model: what is actually enforced

The boundaries offshoot enforces today, and the ones it does not. Each
claim names the code that implements it.

**Local socket.** `offshoot serve` listens on a unix socket created `0600`
under a `0700` cache directory (`internal/daemon/server.go`). Anyone who
can open that socket is trusted as the daemon's own user: there is no
per-request authentication, and a socket client's `path` field (`export`'s
destination, `create`'s import source) is used as an ordinary server-side
path that same-user caller could already read or write.

**HTTP.** Off by default. `-http` binds loopback with no further
acknowledgment; a non-loopback bind additionally requires
`-http-allow-non-loopback` and an explicit operator token of at least 16
characters (`-token`/`OFFSHOOT_TOKEN`). Every request but `GET /healthz`
must carry `Authorization: Bearer <token>`, compared with
`crypto/subtle.ConstantTimeCompare`; `export` and `create` with a path are
refused over HTTP (`internal/daemon/http.go`). There is no TLS: a
non-loopback bind belongs behind a trusted network boundary of your own.

**Names and paths.** Database, branch and checkpoint names are 1–128
characters of `[a-z0-9-_.]`, never starting with `-`, never `.` or `..`
and never containing `..`, validated at the ops layer, at `PutRef` in the
store, and at the MCP tool boundary (`store.ValidateName`). Every local
path derived from a name goes through one containment check against the
workspace root (`internal/ops/ops.go`), and the local store refuses keys
that escape its directory.

**File modes.** Store objects and writable checkouts are `0600`;
read-only historical checkouts (`checkouts-ro`) are `0444`; the `.sum`
sidecars beside a checkout hold metadata only (lineage id, txid, checksum)
and are `0644`; directories are `0700`.

**MCP.** `offshoot mcp` speaks stdio only — no network listener. Its
`promote` and `destroy` tools honor protected branches and refuse an
agent-supplied `force` against one unless the server was started with
`-allow-force` (`internal/mcp/tools.go`).

**Not protected against:**

- Other local users on a shared host, beyond ordinary file permissions.
- Anyone with write access to the store bucket or directory: a writer
  there is fully trusted, and can replace refs and objects.
- A hostile `PATH`: `offshoot diff` resolves `sqldiff` via `PATH`
  (`internal/ops/diff.go`). offshoot never invokes `sqlite3` itself; the
  `sqlite3` you run against a checkout is your own.

## What counts as a security issue

offshoot is a durability tool — the failure modes that matter most are the
ones that silently lose or corrupt data, not just the ones that leak a
secret. In rough priority order:

- **Data-loss or silent-corruption bugs** — anything where a write that was
  acknowledged as durable (a checkpoint or flush) can actually be lost, or
  where the capture engine could absorb a divergence instead of detecting it
  (see `internal/capture`'s "every divergence must be detected, never
  silent" invariant — a bug that breaks that invariant is a security issue
  here even if no attacker is involved).
- **Fencing bypasses** — anything that lets a writer with a superseded epoch
  or a stale/expired lease successfully write to a branch, or that breaks the
  ref compare-and-swap guarantee two concurrent writers rely on not to
  corrupt each other.
- **Path traversal / prefix escape** — anything in checkout materialization,
  store key construction, or import (`--from`) that lets a branch name, db
  name, or checkpoint name reach outside its intended directory or store
  prefix. By design, a file path the caller names outright is not a name:
  the daemon trusts a unix-socket client's `path` field (`export`'s
  destination, `create`'s import source) as an ordinary server-side path
  that same-user caller could already write or read, and refuses both over
  HTTP (see [docs/reference.md](docs/reference.md)'s daemon-ops threat
  model).
- **Credential handling** — anything that logs, persists, or leaks S3
  credentials, the single-token auth secret, or other configured secrets
  beyond their intended scope.

Ordinary crashes, panics on malformed input that don't lead to any of the
above, and performance issues are regular bugs — file those as normal GitHub
issues, not security reports.

## Response expectations

This is a single-maintainer project right now. Reports get a best-effort
response, not an SLA. In practice that has meant: an initial acknowledgment
within a few days, and a fix or a documented mitigation before public
disclosure — but there's no team behind this to guarantee turnaround, and I'd
rather say that plainly than promise a number I can't back. If a report is
urgent (actively exploited, or data loss in the wild), say so explicitly in
the report — that gets it moved to the front of the queue.
