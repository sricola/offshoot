# Workflows hardening report (pass-3 security audit: MED-1, LOW-1..LOW-4, LOW-6; hygiene H3, M1, M2)

Branch: `workflows-hardening`. Scope: `.github/**` + one CHANGELOG line. Date: 2026-08-13.

## 1. SHA pins (MED-1)

Every `uses:` in `.github/` is now SHA-pinned with a `# vX.Y.Z` comment — including
GitHub-owned `actions/*` (cheap with Dependabot in place; one policy, no exceptions).
Resolution: `gh api repos/OWNER/REPO/commits/<major-tag> --jq .sha`, then the exact
version tag found by enumerating `git/matching-refs/tags/<major>.` and matching commit
SHAs. **Round-trip verified**: a script re-resolved each claimed version tag via
`gh api repos/OWNER/REPO/commits/<vX.Y.Z>` and compared it to the pinned SHA — all 13 OK.

| Action | Pinned SHA | Version | Verified |
|---|---|---|---|
| actions/checkout | 11d5960a326750d5838078e36cf38b85af677262 | v4.4.0 | gh api round-trip OK |
| actions/setup-go | 40f1582b2485089dde7abd97c1529aa768e1baff | v5.6.0 | gh api round-trip OK |
| actions/setup-node | 49933ea5288caeca8642d1e84afbd3f7d6820020 | v4.4.0 | gh api round-trip OK |
| actions/setup-python | a26af69be951a213d495a4c3e4e4022e16d87065 | v5.6.0 | gh api round-trip OK |
| actions/upload-artifact | ea165f8d65b6e75b540449e92b4886f43607fa02 | v4.6.2 | gh api round-trip OK |
| actions/download-artifact | d3f86a106a0bac45b974a628896c90dbdf5c8093 | v4.3.0 | gh api round-trip OK |
| actions/configure-pages | 983d7736d9b0ae728b81ab479565c72886d7745b | v5.0.0 | gh api round-trip OK |
| actions/upload-pages-artifact | 56afc609e74202658d3ffba0e8f6dda462b719fa | v3.0.1 | gh api round-trip OK |
| actions/deploy-pages | d6db90164ac5ed86f2b6aed7e0febac5b3c0c03e | v4.0.5 | gh api round-trip OK |
| docker/setup-buildx-action | 8d2750c68a42422c14e847fe6c8ac0403b4cbd6f | v3.12.0 | gh api round-trip OK |
| docker/login-action | c94ce9fb468520275223c153574b00df6fe4bcc9 | v3.7.0 | gh api round-trip OK |
| docker/build-push-action | 10e90e3645eae34f1e60eeb005ba3a3d33f178e8 | v6.19.2 | gh api round-trip OK |
| pypa/gh-action-pypi-publish | dc37677b2e1c63e2034f94d8a5b11f265b73ba33 | v1.14.2 (was mutable `release/v1` **branch**) | latest-release tag → commit via gh api, round-trip OK |

Non-action pins added the same way, resolved live and cross-checked:

| Artifact | Pin | Verified |
|---|---|---|
| prometheus tarball (ci.yml promtool) | sha256 `0e8c4d46101bd025ea8265e377d2caabc57f488fc1be1c367f37db69ea41be6f` (v3.13.2 linux-amd64) | computed by downloading the exact URL, AND matched upstream release `sha256sums.txt` |
| minio/minio (ci.yml s3-conformance) | `@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` | registry `docker-content-digest` for `:latest`; Hub tags API maps it to RELEASE.2025-09-07T16-13-09Z |
| minio/mc (ci.yml s3-conformance) | `@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` | registry `docker-content-digest` for `:latest`; = RELEASE.2025-08-13T08-35-41Z |

`.github/dependabot.yml` added: `github-actions` weekly (keeps SHA pins fresh) +
`gomod` weekly. Grep confirms zero remaining `@vN` / `@release/*` action refs.

## 2. Permissions matrix — release.yml (MED-1, part 2)

| Scope | Before | After |
|---|---|---|
| workflow-level | contents: write, packages: write (inherited by ALL jobs) | contents: read |
| build (×4 matrix) | write+write (unneeded) | contents: read (inherited) |
| collect | write+write | contents: write (job-level; the `gh release create` job) |
| docker | write+write | contents: read + packages: write (job-level) |

No other job needs write: build only compiles/uploads artifacts (upload-artifact uses
its own artifact API, needs no contents scope beyond read); collect's `gh` uses the
GH_TOKEN env var, not git credentials. All other workflows were already least-privilege
and are unchanged (pages keeps pages/id-token; publish keeps per-job id-token: write).

## 3. Expression injection (LOW-1)

release.yml `build`/`smoke`/`package` steps now receive `REFNAME` (and matrix values)
via `env:`; collect's release step gets `EVENT_NAME`/`REPO` via env. Grepped ALL
workflows for further `${{ }}` inside `run:` — found and fixed publish.yml's two
dry-run notices (interpolated `github.ref` — ref-derived!) and the gate step's
`vars.PUBLISH_ENABLED`; nightly's activity script now takes event name/schedule via
env too. A scanner script confirms **zero** `${{ }}` occurrences remain inside any
`run:` block in any workflow or the composite action. The docker `tags:`/`build-args:`
expression uses are action inputs, not shell (audit-noted as fine).

## 4. persist-credentials per checkout (LOW-3, part 2)

All 15 checkouts across 5 workflows: `persist-credentials: false` (grep-verified
count == checkout count per file). Per-file conclusion:

- **ci.yml** (4): no job pushes; token was read-only anyway → false, no caveats.
- **nightly.yml** (4): same → false.
- **release.yml** (3): build never pushes; collect creates the release via `gh` with
  `GH_TOKEN` env (not git creds — verified the step) → false safe; docker only reads
  the context → false.
- **publish.yml** (3): check runs version scripts; pypi/npm build+upload via
  OIDC/NPM_TOKEN env, never git → false.
- **pages.yml** (1): deploy uses the pages OIDC flow, no git ops → false.

No step anywhere performs a git push/fetch after checkout, so nothing keeps credentials.

## 5. :latest only on tag pushes (LOW-4)

release.yml docker job: `tags:` is now a ternary — `push` events (tag builds) get
`:<tag>` + `:latest`; workflow_dispatch gets only `:dev-<sha>`. collect's dispatch
behavior (dev-* GitHub prerelease) intentionally unchanged.

## 6. Nightly cron double-fire + string factoring (LOW-6 + H3)

- Daily cron is now `0 9 * * 1-6`; Sunday 09:00 fires only the weekly cron — the
  duplicated Sunday ubuntu-torture run is gone. Comments explain the exclusion.
- The `'0 9 * * 0'` literal now appears in exactly TWO places: `on.schedule` and ONE
  comparison in the `activity` job, which emits a `weekly` output (true for the Sunday
  leg and every manual dispatch — the same predicate the three old comparisons encoded).
  `torture`'s matrix ternary and `macos-test`'s `if` consume
  `needs.activity.outputs.weekly`. Both sites carry comments pointing at each other.
  `fresh` semantics unchanged (weekly ⇒ fresh, else 25h git-log check).

## 7. Dead macOS step (M1)

ci.yml's never-runnable "Install sqlite3 CLI (macos)" step removed (matrix is
ubuntu-only). The macOS-absence comment now notes the brew/keg-only logic lives in
`.github/actions/setup-sqlite` (used by nightly's macos-test), so re-adding macOS to
the matrix is a one-line change.

## 8. Composite action (M2)

`.github/actions/setup-sqlite/action.yml` — input `sqldiff: 'true'|'false'`
(default false). Ubuntu: `apt-get update` + `sqlite3` (+`sqlite3-tools` when sqldiff);
macOS: `brew install sqlite [sqldiff] || true` + keg-only PATH append. The detailed
sqlite3-tools and macos-26 rationale comments moved into the action. Per-site behavior
(exact match to the pre-change blocks):

| Call site | sqldiff | Pre-change behavior preserved | Site comment |
|---|---|---|---|
| ci.yml `test` | true | apt sqlite3+sqlite3-tools (macOS branch was dead — removed per M1) | diff_test.go needs sqldiff |
| ci.yml `metrics-lint` | false (default) | apt sqlite3 only | promtool gate never touches diff |
| ci.yml `sdks` | false (default) | apt sqlite3 only | SDK suites never touch diff |
| nightly.yml `torture` (ubuntu+macos) | false (default) | apt sqlite3 / brew sqlite (no sqldiff) | torture never exercises diff |
| nightly.yml `macos-test` | true | brew sqlite+sqldiff | full go test includes diff tests |

Verify steps stay at the call sites (they differ per site and are one-liners, not the
duplicated block M2 targeted).

## Validation

- `yaml.safe_load` OK on all 5 workflows + action.yml + dependabot.yml.
- **actionlint v1.7.12 (via `go run`): exit 0, zero findings** — validates the
  needs-in-matrix expression, the tags ternary, and shellchecks every run block.
- All 13 action pins round-trip verified via `gh api` (table above); promtool sha256
  matched upstream sha256sums.txt; minio digests matched Hub RELEASE tags.
- Greps: zero `uses: ...@v<N>` / `@release/*` refs; zero `${{ }}` in run blocks;
  persist-credentials count == checkout count in every file.
- **Not executed**: workflows cannot be run from this environment. Changes are
  structured to be behavior-preserving except where the audit demanded otherwise
  (Sunday single-fire, dispatch no longer moving `:latest`, checksum hard-fail paths).
