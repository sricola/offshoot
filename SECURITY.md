# Security policy

## Supported versions

offshoot is pre-release (0.1.x). There is no long-term support branch and no
backport policy yet — the only supported version is the latest commit on
`main`. If you're running an older tag, upgrade before reporting; the fix is
very likely already in `main`.

Once there's a 1.0, this file will get a real support-window table. Until
then, treat everything as "best effort, latest only."

## Reporting a vulnerability

**Do not open a public issue for a security report.** Use GitHub's private
vulnerability reporting instead:

1. Go to the repo's **Security** tab.
2. Click **Report a vulnerability**.
3. Describe the issue, how to reproduce it, and what you think the impact is.

This opens a private advisory visible only to the maintainer (and to you)
until it's resolved and a disclosure timeline is agreed on.

If GitHub's reporting flow isn't available to you for some reason, email the
maintainer directly (see the CODE_OF_CONDUCT.md contact section) with
`[security]` in the subject.

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
  prefix.
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
