# Governance

## Where this stands today

offshoot has one maintainer (Sri Ray) and no formal governance body yet.
Decisions get made by the maintainer, informed by issue and PR discussion —
a BDFL model, stated plainly rather than dressed up as something more
committee-driven than it is. That's an accurate description of a
single-maintainer pre-1.0 project, not a permanent design choice; see "path
to additional maintainers" below.

## How decisions get made

- **Day-to-day** — bug fixes, docs, CLI ergonomics, test coverage: reviewed
  and merged directly, same bar as any PR (see CONTRIBUTING.md).
- **Design-sensitive changes** — anything touching the storage format, the
  wire protocol (daemon lifecycle API, MCP surface), or a public API
  (CLI flags/subcommands, SDK method signatures, the `Sink`/capture
  interfaces) needs an RFC-style issue *before* a PR: state the problem,
  the proposed change, and the compatibility impact, and let it sit open for
  discussion before code gets written against it. This isn't bureaucracy for
  its own sake — these are the surfaces where a bad call is expensive to
  reverse once something has written data or a client has integrated against
  it.
- **Disagreements** get resolved by the maintainer, with reasoning stated in
  the issue — not by vote count. If that reasoning is unsatisfying, the
  right response is to argue the technical merits in the thread, not to
  escalate elsewhere; there isn't an elsewhere yet.

## Invariants and versioning

offshoot's safety guarantees (CAS-everywhere fencing, layout-versioned
storage, "every capture divergence detected, never silently absorbed") are
the actual contract with anyone building on this. The commitment: those
invariants get written down where users can hold the project to them (the
README and the design docs today; a dedicated versioning-policy doc as the
format stabilizes toward 1.0), and a change that would break one goes through
the RFC-style process above, not a quiet PR. "We changed the format" without
a version bump and a migration note is a bug, not a release.

## Path to additional maintainers

There is no formal maintainer-nomination process yet because there's been no
occasion for one — see ROADMAP.md's 90-day success metric of "3+
non-author contributors" as the actual trigger. In practice: sustained,
trusted contribution (a track record of PRs in a given area, sound judgment
in review discussions, showing up over time rather than a single large drop)
is what gets someone offered commit access and a voice in design-sensitive
decisions, evaluated by the current maintainer. As the project grows past
one maintainer, this document is where the process for that gets written
down properly instead of decided ad hoc — expect it to change.
