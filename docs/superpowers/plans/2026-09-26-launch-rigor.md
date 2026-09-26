# Launch Rigor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the trust gaps an OSS adopter checks before depending on a pre-1.0 storage tool: signed, attested releases with an OpenSSF Scorecard; a BranchBench-topology benchmark with published numbers; a first screen that leads with install, the recording, and the stability contract; and the "how offshoot is tested" page finished as the launch artifact. Announcing and the bounty payout stay user-gated.

**Architecture:** Supply-chain work lives entirely in `.github/workflows/` (a new `scorecard.yml`; `release.yml` gains keyless cosign signatures, SLSA provenance via GitHub attestations, and an SPDX SBOM for the tarballs and the GHCR image) plus the verification recipe in docs. The benchmark is a new Go program `cmd/branchbench` driving `internal/ops.Workspace` directly against a local store, emitting one markdown table that docs/benchmarks.md pastes verbatim. README/testing/status changes are docs only.

**Tech Stack:** GitHub Actions (SHA-pinned), sigstore cosign (keyless, GitHub OIDC), `actions/attest-build-provenance`, `actions/attest-sbom`, `anchore/sbom-action` (syft), `ossf/scorecard-action`, Go 1.26+ with cgo (`database/sql` + the repo's existing SQLite driver), Make.

**Spec:** Tier 1 item 5 of `reports/offshoot next features roadmap.md` ("Weeks 8-10: launch rigor, then announce"), git-ignored; its research is under `research_notes/offshoot next features roadmap/adoption_playbook.md` (Key question 6, items 7-10) and `research_notes/launch-rigor/branchbench-methodology.md`.

## Global Constraints

- **Every third-party action is pinned to a full commit SHA with a `# vX.Y.Z` comment**, like every existing workflow. SHAs resolved 2026-09-26: `ossf/scorecard-action@2d1146689b8cda280b9bc96326124645441f03bc # v2.4.4`; `actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8 # v4.2.2`; `actions/attest-sbom@c604332985a26aa8cf1bdc465b92731239ec6b9e # v4.1.0`; `sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6 # v4.1.2`; `anchore/sbom-action@3ad7283483fc7af8ff2b4ea19663c2d5ca935e26 # v0.24.2`; `github/codeql-action/upload-sarif@2892aa5e19bbd11bc0cff5427e3b750a04d9e3c2 # v4`. Reuse the repo's existing pins for `actions/checkout` (`3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`) and `actions/upload-artifact` (`043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1`).
- **Least privilege stays per-job.** Workflow-level `permissions: contents: read` (or `read-all` for Scorecard); `id-token: write` and `attestations: write` only on the two jobs that publish. No `${{ }}` interpolation of ref-derived values into `run:` scripts — pass them through `env:` exactly as `release.yml` already does.
- **No install text for unpublished packages**: never `pip install offshoot`, `npm install @offshoot-db/...` (publication is deferred indefinitely). Existing `go install`, Homebrew, Docker and release-tarball instructions are fine.
- **Benchmarks honesty.** Every number in docs prose derives from a pasted table produced by one run of the committed program, with the machine and date line. BranchBench's Neon/Dolt figures are quoted from the paper with citation and labelled as *their* runs on hosted Postgres systems; never present them as a head-to-head we ran. Name the topology each quote comes from.
- **Nothing outward-facing runs from a task.** No tag push, no release, no announcement, no bounty commitment, no Scorecard publish from a feature branch. Validation of `release.yml` happens by a `workflow_dispatch` dev build (which the workflow already supports) run by the controller, not by an implementer.
- **Docs are the launch artifact**: docs/testing.md remains the single canonical "How offshoot is tested" page (the site's nav already lists it); no separate blog.
- Commit trailers on every commit: `Co-Authored-By: Claude <model> <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B`.

---

### Task 1: OpenSSF Scorecard workflow and badge

**Files:**
- Create: `.github/workflows/scorecard.yml`
- Modify: `README.md` (badge line, line 10)

**Interfaces:**
- Produces: a weekly + on-push-to-main Scorecard run publishing to scorecard.dev and uploading SARIF to code scanning; badge URL `https://img.shields.io/ossf-scorecard/github.com/sricola/offshoot?label=openssf%20scorecard&style=flat-square&labelColor=1b1a17&color=3c7a1a` linking to `https://scorecard.dev/viewer/?uri=github.com/sricola/offshoot`.

- [ ] **Step 1: Write the workflow**

```yaml
name: scorecard

on:
  branch_protection_rule:
  schedule:
    - cron: "17 6 * * 1"   # Mondays 06:17 UTC
  push:
    branches: [main]
  workflow_dispatch:

# Scorecard reads repo metadata only; the job below elevates exactly what
# publishing results and uploading SARIF need.
permissions: read-all

jobs:
  analysis:
    name: Scorecard analysis
    runs-on: ubuntu-latest
    permissions:
      security-events: write   # upload-sarif
      id-token: write          # publish_results signs the result with the workflow's OIDC identity
      contents: read
      actions: read
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false

      - name: Run analysis
        uses: ossf/scorecard-action@2d1146689b8cda280b9bc96326124645441f03bc # v2.4.4
        with:
          results_file: results.sarif
          results_format: sarif
          # Public repo: publishing makes the badge and the scorecard.dev
          # viewer page real. Runs on the default branch only, by the
          # action's own rule.
          publish_results: true

      - uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: SARIF file
          path: results.sarif
          retention-days: 5

      - name: Upload to code-scanning
        uses: github/codeql-action/upload-sarif@2892aa5e19bbd11bc0cff5427e3b750a04d9e3c2 # v4
        with:
          sarif_file: results.sarif
```

- [ ] **Step 2: Lint it**

Run: `go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/scorecard.yml`
Expected: no output (clean). If `actionlint` cannot be fetched offline, run `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/scorecard.yml'))"` and say so in the report.

- [ ] **Step 3: Add the badge**

In `README.md` line 10, append after the `docs` badge (same style tokens):

```markdown
[![openssf scorecard](https://img.shields.io/ossf-scorecard/github.com/sricola/offshoot?label=openssf%20scorecard&style=flat-square&labelColor=1b1a17&color=3c7a1a)](https://scorecard.dev/viewer/?uri=github.com/sricola/offshoot)
```

The badge renders "no score" until the first run on `main` publishes; that is expected and the report must say so.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/scorecard.yml README.md
git commit -m "ci: OpenSSF Scorecard workflow (weekly + on push to main), badge in README"
```

---

### Task 2: Signed, attested releases — cosign, SLSA provenance, SBOM; the verification recipe

**Files:**
- Modify: `.github/workflows/release.yml` (`collect` and `docker` jobs)
- Modify: `SECURITY.md` (new section "Verifying a release")
- Modify: `docs/installation.md` (new section "## Verify what you downloaded" after "## Prebuilt binaries"; one line under "## Docker")
- Modify: `README.md` (one line in the Install table's "Prebuilt binaries" row: "signed and attested; verify →")

**Interfaces:**
- Produces, per tagged release: for each `offshoot_<tag>_<os>_<arch>.tar.gz` a cosign keyless bundle `<asset>.sigstore.json`, one SLSA provenance bundle `offshoot_<tag>_provenance.intoto.jsonl` covering all tarballs, one SBOM `offshoot_<tag>.spdx.json`; a build-provenance attestation and an SBOM attestation in GitHub's attestation store for the tarballs; a build-provenance attestation pushed to GHCR and a cosign signature for the image digest. Verification commands (exact, used in docs and by the controller's validation):
  - `gh attestation verify offshoot_<tag>_linux_amd64.tar.gz --repo sricola/offshoot`
  - `cosign verify-blob --bundle offshoot_<tag>_linux_amd64.tar.gz.sigstore.json --certificate-identity-regexp '^https://github.com/sricola/offshoot/\.github/workflows/release\.yml@refs/' --certificate-oidc-issuer https://token.actions.githubusercontent.com offshoot_<tag>_linux_amd64.tar.gz`
  - `gh attestation verify oci://ghcr.io/sricola/offshoot:<tag> --repo sricola/offshoot`
  - `cosign verify ghcr.io/sricola/offshoot:<tag> --certificate-identity-regexp '^https://github.com/sricola/offshoot/\.github/workflows/release\.yml@refs/' --certificate-oidc-issuer https://token.actions.githubusercontent.com`

- [ ] **Step 1: `collect` job — permissions and signing steps**

Change the job's permissions block to:

```yaml
    permissions:
      contents: write        # gh release create
      id-token: write        # keyless cosign + attestations sign with the job's OIDC identity
      attestations: write    # actions/attest-*
```

After the existing "List collected assets" step and before "Create GitHub release", insert:

```yaml
      - name: Resolve tag
        id: tag
        env:
          EVENT_NAME: ${{ github.event_name }}
        run: |
          if [ "$EVENT_NAME" = "workflow_dispatch" ]; then
            echo "name=dev-$(git rev-parse --short HEAD)" >> "$GITHUB_OUTPUT"
          else
            echo "name=${GITHUB_REF_NAME}" >> "$GITHUB_OUTPUT"
          fi

      - uses: sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6 # v4.1.2

      # Keyless: the signing identity is this workflow's OIDC token
      # (…/release.yml@refs/tags/vX), recorded in the Rekor transparency
      # log. Verifiers pin that identity, not a key we'd have to guard.
      - name: Sign release tarballs (cosign, keyless)
        run: |
          for f in dist/*.tar.gz; do
            cosign sign-blob --yes --bundle "${f}.sigstore.json" "$f"
          done
          ls -la dist

      - name: SBOM (SPDX JSON, from the Go module graph)
        uses: anchore/sbom-action@3ad7283483fc7af8ff2b4ea19663c2d5ca935e26 # v0.24.2
        with:
          path: .
          format: spdx-json
          output-file: offshoot_${{ steps.tag.outputs.name }}.spdx.json
          upload-artifact: false
          upload-release-assets: false

      - name: SLSA build provenance for the tarballs
        id: provenance
        uses: actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8 # v4.2.2
        with:
          subject-path: dist/*.tar.gz

      - name: Attach the provenance bundle as a release asset
        env:
          BUNDLE: ${{ steps.provenance.outputs.bundle-path }}
          TAG: ${{ steps.tag.outputs.name }}
        run: cp "$BUNDLE" "dist/offshoot_${TAG}_provenance.intoto.jsonl"

      - name: SBOM attestation for the tarballs
        uses: actions/attest-sbom@c604332985a26aa8cf1bdc465b92731239ec6b9e # v4.1.0
        with:
          subject-path: dist/*.tar.gz
          sbom-path: offshoot_${{ steps.tag.outputs.name }}.spdx.json
```

`output-file`/`sbom-path` use `${{ }}` in action inputs, not shell — safe, same as the existing `tags:` input rationale. Then extend the `gh release create` asset list to:

```yaml
          gh release create "$tag" \
            dist/*.tar.gz dist/*.tar.gz.sha256 dist/*.sigstore.json dist/*.intoto.jsonl \
            offshoot_"$tag".spdx.json THIRD_PARTY_LICENSES.csv \
```

Before finalizing asset names, WebFetch `https://raw.githubusercontent.com/ossf/scorecard/main/checks/raw/signed_releases.go` and confirm which suffixes the Signed-Releases check recognises; the `.intoto.jsonl` name is what makes the check pass, so keep it exactly if the source agrees, and record the confirmed suffix list in the report.

- [ ] **Step 2: `docker` job — permissions and image attestation**

Change permissions to:

```yaml
    permissions:
      contents: read
      packages: write
      id-token: write
      attestations: write
```

After "Verify published image platforms", append:

```yaml
      - uses: sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6 # v4.1.2

      - name: Sign the image digest (cosign, keyless)
        env:
          IMAGE: ghcr.io/${{ github.repository }}@${{ steps.image.outputs.digest }}
        run: cosign sign --yes "$IMAGE"

      - name: SLSA build provenance for the image (pushed to GHCR)
        uses: actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8 # v4.2.2
        with:
          subject-name: ghcr.io/${{ github.repository }}
          subject-digest: ${{ steps.image.outputs.digest }}
          push-to-registry: true
```

- [ ] **Step 3: Lint**

Run: `go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/release.yml`
Expected: clean.

- [ ] **Step 4: Verification recipe in docs**

`docs/installation.md`, new section after "## Prebuilt binaries":

```markdown
## Verify what you downloaded

Every tagged release is signed and attested by the release workflow itself
— no maintainer-held key. Three independent checks, pick any:

```sh
# 1. SLSA build provenance (GitHub attestation store; needs gh >= 2.49)
gh attestation verify offshoot_v0.2.11_linux_amd64.tar.gz --repo sricola/offshoot

# 2. cosign keyless signature, pinned to this repo's release workflow identity
cosign verify-blob \
  --bundle offshoot_v0.2.11_linux_amd64.tar.gz.sigstore.json \
  --certificate-identity-regexp '^https://github.com/sricola/offshoot/\.github/workflows/release\.yml@refs/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  offshoot_v0.2.11_linux_amd64.tar.gz

# 3. the plain checksum, as before
shasum -a 256 -c offshoot_v0.2.11_linux_amd64.tar.gz.sha256
```

Each release also ships `offshoot_<tag>.spdx.json`, an SPDX SBOM of the Go
module graph, with a matching SBOM attestation (`gh attestation verify
--predicate-type https://spdx.dev/Document/v2.3 …`). Releases before
v0.2.11 carry checksums only.
```

Under "## Docker", add one line: "Images are signed and attested too: `cosign verify ghcr.io/sricola/offshoot:<tag> --certificate-identity-regexp '^https://github.com/sricola/offshoot/\.github/workflows/release\.yml@refs/' --certificate-oidc-issuer https://token.actions.githubusercontent.com`, or `gh attestation verify oci://ghcr.io/sricola/offshoot:<tag> --repo sricola/offshoot`."

`SECURITY.md`, new section "## Verifying a release" after "## Supported versions": three sentences — releases are built and signed in CI with keyless Sigstore signatures and SLSA provenance, the identity to pin is the release workflow, and a pointer to the installation page's commands. `README.md` Install table, "Prebuilt binaries" row: append " — signed and attested since v0.2.11 ([verify](https://sricola.github.io/offshoot/docs/installation/#verify-what-you-downloaded))".

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml SECURITY.md docs/installation.md README.md
git commit -m "release: keyless cosign signatures, SLSA provenance and SPDX SBOM for tarballs and the GHCR image; verification recipe"
```

The controller validates this end to end with `gh workflow run release.yml --ref feat/launch-rigor` (a `dev-<sha>` build), runs all four verification commands against the produced assets, then deletes the dev release and tag. That validation is not an implementer step.

---

### Task 3: BranchBench topologies against offshoot

**Files:**
- Create: `cmd/branchbench/main.go` (flags, workflow table, runner, report), `cmd/branchbench/seed.go` (CH-benCHmark-shaped SQLite seed generator), `cmd/branchbench/workload.go` (the five workflow definitions + per-step SQL), `cmd/branchbench/main_test.go` (quick-scale run of every workflow)
- Modify: `Makefile` (`bench-branchbench` target, `.PHONY`), `docs/benchmarks.md` (new section `## BranchBench topologies (v0.2.11)` between the isolation section and `## Method`)

**Interfaces:**
- Consumes: `internal/ops`: `ops.Init(spec) (*Workspace, error)`, `(*Workspace).Create(db)`, `Checkout(db, branch) (path, error)`, `Checkpoint(db, branch, name, meta) (txid, error)`, `Fork(db, src, new, at, ttl, meta) (txid, error)`, `Destroy(db, branch, force) error`; SQL via `database/sql` with the module's existing `github.com/mattn/go-sqlite3` driver (import `_ "github.com/mattn/go-sqlite3"`).
- Produces: a markdown block printed to stdout that docs/benchmarks.md pastes verbatim; heading text in docs must be exactly `## BranchBench topologies (v0.2.11)` (Task 4 links `#branchbench-topologies-v0211`).

**Why this shape.** BranchBench (Ang, Kim, Weldon, Durand, Kaffes, Wu — Columbia DAPLab, arXiv:2604.17180, harness github.com/ElaineAng/db-fork) defines five macrobenchmark workflows as parameter tuples over one tree-building loop. The harness code has no license file, so we do NOT vendor any of its files or SQL; we re-implement the parameter tuples (numbers) and cite the paper. Our seed is our own CH-benCHmark-shaped generator, not their dump.

**Program behaviour (`cmd/branchbench`):**

Flags: `-store DIR` (default: a fresh `os.MkdirTemp` under `$TMPDIR`, removed at exit unless `-keep`), `-workflows simulation,data_cleaning,software_dev,mcts,failure_repro` (default all five), `-concurrency N` (default 8; 1 = sequential), `-quick` (scale every workflow down as in the table below), `-seed-warehouses W` (default 10), `-timeout 2h`.

Seed (`seed.go`): one SQLite file, WAL mode off, tables `warehouse, district, customer, item, stock, orders, order_line, new_order, history, region, nation, supplier` with TPC-C-style integer/real/text columns and primary keys; row counts per BranchBench's config comments "10 warehouses, 1K items, 100 cust/district": `W` warehouses × 10 districts × 100 customers = 10,000 customers; `stock` = W × 1,000; `item` = 1,000; `orders` = one per customer (10,000) with 5–15 `order_line` rows each (~100,000 lines, `ol_amount` real); `region` 5, `nation` 25, `supplier` 100. Deterministic (`math/rand` seeded 1). Print the seed's file size in MiB in the report header. Load it with `ws.Create("chbench")` + write the rows into the `main` checkout + `ws.Checkpoint("chbench","main","seed",nil)`.

Workflows (`workload.go`), exact BranchBench full-scale parameters (`macrobench/configs/*.textproto` in db-fork; the mcts config's own comment says these are Table 1 / Section 3.3 of the paper):

| Workflow | T workers | S steps/worker | F_r | F_i | D | C cross-branch | γ prune | M_s schema/step | M_d mutations/step | Q_v eval/step | `-quick` |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `simulation` (flat star) | 1000 | 1 | 1000 | – | 1 | 1 | 1.0 | 0 | 50 | 1 | T=F_r=20 |
| `data_cleaning` (wide shallow) | 10 | 20 | 10 | 3 | 3 | 2 | 0.0 | 1 | 1 | 1 | T=3,S=4 |
| `software_dev` (bushy) | 5 | 20 | 5 | 3 | 4 | 1 | 0.1 | 1 | 1 | 2 | T=2,S=4 |
| `mcts` (deep narrow) | 10 | 100 | 10 | 10 | 25 | 0 | 0.1 | 0 | 1 | 1 | T=2,S=6,D=5 |
| `failure_repro` (flat, 1 worker) | 1 | 10 | 10 | – | 1 | 0 | 1.0 | 5 | 45 | 1 | S=3 |

Tree loop, per worker, per step: (1) choose a parent: the worker's current node if its depth < D and its child count < fanout (F_r for root, F_i otherwise), else a uniformly random eligible node (depth < D, children < fanout), else the root; (2) `Fork("chbench", parent, child, "", 0, meta{"workflow","worker","step","depth"})`, timed as **fork**; (3) `Checkout("chbench", child)`, timed as **checkout**; (4) open the checkout with `database/sql` and run M_s schema changes (`ALTER TABLE customer ADD COLUMN c_x<n> INTEGER DEFAULT 0` for odd n, `CREATE INDEX idx_<n> ON order_line(ol_w_id, ol_d_id)` for even n — drop-and-recreate if it exists), M_d data mutations (each a "new order": one `INSERT INTO orders`, 5–15 `INSERT INTO order_line`, one `UPDATE stock SET s_quantity = s_quantity - 1`, in one transaction), Q_v eval queries (branch-local reads, timed as **eval**: `SELECT ol_w_id, SUM(ol_amount) FROM order_line GROUP BY ol_w_id`; software_dev's second query is an integrity check `SELECT COUNT(*) FROM order_line ol LEFT JOIN orders o ON o.o_w_id=ol.ol_w_id AND o.o_d_id=ol.ol_d_id AND o.o_id=ol.ol_o_id WHERE o.o_id IS NULL`); (5) close the SQL handle and `Checkpoint("chbench", child, "s", nil)` so children can fork from the published state, timed as **checkpoint**; (6) with probability γ, `Destroy("chbench", child, false)`, timed as **destroy** (a destroyed node is never chosen as a parent); (7) the worker's current node becomes the child (unless destroyed, then the parent). After all workers finish, run C cross-branch queries: for each live branch, `Checkout` it (read-only open with `?mode=ro`) and sum `ol_amount`; aggregate in the driver — BranchBench does the same driver-side aggregation (DoltHub's critique notes it), so say so in the docs. Workers run as goroutines through a semaphore of `-concurrency`; the tree is guarded by one mutex. Any `ops` error aborts the workflow with the step count reached so far (mirrors BranchBench's "completed steps" reporting).

Metrics per workflow: steps completed / total; wall time; **branching overhead ratio** = (fork + checkout + checkpoint + destroy time) / wall time; p50/p99 for fork, checkout, checkpoint, eval, each **at depth 1 and at the max depth reached** (report both so a depth penalty, if any, is visible); live branches at peak; store bytes delta (walk `-store` before and after, `filepath.WalkDir` summing sizes); concurrency used. Output: a header line `<GOOS>/<GOARCH>, <CPU model if readable from sysctl/lscpu else runtime.NumCPU()> cores, Go <version>, offshoot <git describe>, measured <YYYY-MM-DD>, seed <N> MiB, concurrency <N>`, then one markdown table with columns `Workflow | Steps | Wall | Branch overhead | Fork p50/p99 (d=1 → d=max) | Checkout p50/p99 (d=1 → d=max) | Checkpoint p50/p99 (d=1 → d=max) | Eval p50/p99 (d=1 → d=max) | Peak live | Store Δ`, then one line per workflow with the max depth reached and the cross-branch query time.

- [ ] **Step 1: Quick-scale test first**

`cmd/branchbench/main_test.go`:

```go
func TestEveryWorkflowRunsAtQuickScale(t *testing.T) {
	dir := t.TempDir()
	rep, err := run(config{store: dir, workflows: allWorkflowNames(), quick: true, concurrency: 2, warehouses: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range rep.workflows {
		if w.stepsDone != w.stepsTotal {
			t.Fatalf("%s: %d/%d steps", w.name, w.stepsDone, w.stepsTotal)
		}
		if w.evalP50[1] <= 0 {
			t.Fatalf("%s: no depth-1 eval sample", w.name)
		}
	}
	if !strings.Contains(rep.markdown(), "| mcts |") {
		t.Fatal("report lacks the mcts row")
	}
}
```

Run: `go test ./cmd/branchbench -run TestEveryWorkflowRunsAtQuickScale -v` → FAIL (package does not exist).

- [ ] **Step 2: Implement** `seed.go`, `workload.go`, `main.go` (`run(config) (*report, error)`, `main()` parses flags → `run` → prints `rep.markdown()`). Keep each file under ~300 lines. `go vet ./cmd/branchbench && gofmt -l cmd/branchbench` clean.

- [ ] **Step 3:** test passes: `go test ./cmd/branchbench -count=1 -v` (must finish in < 60 s).

- [ ] **Step 4: Makefile**

```make
# BranchBench's five agentic-workflow topologies (arXiv:2604.17180) re-run
# against a local offshoot store, full-scale parameters: ~2,300 forks in
# all. Prints one markdown table; docs/benchmarks.md pastes it verbatim.
bench-branchbench:
	go run ./cmd/branchbench
```

Add `bench-branchbench` to `.PHONY`.

- [ ] **Step 5: Run it for real** — `make bench-branchbench` on this machine with defaults (all five, concurrency 8, 10 warehouses). Record the full stdout. If any workflow exceeds 20 minutes, stop, report the partial table, and say which workflow and why (the docs then state the completed set honestly; do not shrink the parameters silently).

- [ ] **Step 6: Docs** — `docs/benchmarks.md`, new section `## BranchBench topologies (v0.2.11)` inserted after the isolation section and before `## Method`, containing in order: (a) two sentences on what BranchBench is with the arXiv id, authors' lab, and the harness repo link; (b) "What we ran": our re-implementation of the five parameter tuples (the table above, transcribed), our own CH-benCHmark-shaped seed with its MiB, the no-daemon path (fork → checkout → SQL → checkpoint) and why (children fork from published state), driver-side cross-branch aggregation as in the original; (c) the pasted stdout verbatim; (d) "What BranchBench found on hosted systems" — quote with attribution and topology: the abstract's "systems optimized for fast branching suffer up to 5-4000x slower reads as branches deepen, while systems optimized for fast data operations incur 25-1500x higher branch creation and switching latency"; Dolt's MCTS run "completed 170/1000" worker-steps before the 2-hour cap (`timed_out: true` at 7581.85 s in the harness's published `run_stats_final`); Neon's "does not support more than 20 concurrent 'live' branches" and its MCTS completion (DAPLab's blog says 33/1000; the repo's raw `neon_full` stats sum to 23 — cite the paper's figure and footnote the discrepancy); "no system was able to fully complete the five agentic applications within the 2 hours"; (e) "Not apples to apples" — those are the authors' runs on hosted Postgres systems; ours is a local file store with no network, no compute provisioning, no multi-tenant contention, a different seed, and a different concurrency model; we report our numbers next to theirs because the *shape* of the workload is the same, not because the systems are comparable; (f) "What the numbers do and do not show" — whether eval latency at max depth matches depth 1 (a checkout is a plain file), whether fork/checkout/checkpoint show a depth trend (chains and `compact`), and the O(size) checkout cost already documented in the isolation section.

- [ ] **Step 7: Commit** `"bench: BranchBench's five topologies against offshoot; measured table and caveats in docs"`.

---

### Task 4: First screen, the testing page as launch artifact, and the user-gated launch rows

**Files:**
- Modify: `README.md` (reorder the region between the "No merge" paragraph and the verb table; fix the Status version line)
- Modify: `docs/testing.md` (add "At a glance" table at top, "Signed releases" section, "What is not proven here" section, pointers to the two benchmark sections)
- Modify: `docs/status.md` (standing-nag table: two new rows)
- Create: `docs/superpowers/specs/2026-09-26-launch-announce-draft.md`

**README target order** (everything below the tree diagram and the "No merge" paragraph, in this order):

1. `## Install` — moved up from its current position. Lead with a three-line block before the table:
   ```sh
   brew tap sricola/offshoot https://github.com/sricola/offshoot && brew trust sricola/offshoot && brew install offshoot
   # or: go install github.com/sricola/offshoot/cmd/offshoot@latest
   # or: docker run --rm -v offshoot-data:/data ghcr.io/sricola/offshoot:latest init
   ```
   then the existing channel table (with Task 2's "signed and attested" note), then the existing platform paragraph.
2. `## See it run` — the existing "Runnable demo … Real recording … `<details>` transcript" block moved here verbatim, with the intro sentence: "Three agents race three migrations on three forks; one is right; it gets promoted and the others expire."
3. A short `## Stability, in one paragraph` — moved from the Status section's caveat paragraph ("The caveats, stated plainly …" through "1.0 is reserved …"), with one added sentence: "Releases are signed and carry SLSA provenance ([verify](…installation/#verify-what-you-downloaded)); [how offshoot is tested](docs/testing.md) shows the harnesses behind these claims."
4. `## Quickstart (60 seconds, no server, no bucket)` — unchanged except the first line becomes `offshoot init` (install is above now; keep "or `go build -o offshoot ./cmd/offshoot` from source" as a trailing comment line), followed by the verb table, then the eval-harness and pass^k paragraphs as today.
5. `## Why it's different`, `## What offshoot deliberately doesn't do`, `## Status` (version line corrected to **v0.2.10** now, bullets unchanged, caveat paragraph removed since it moved up), then the rest unchanged.

Update the nav line at the top (`[Quickstart] · [Install] · …`) to `[Install](#install) · [See it run](#see-it-run) · [Quickstart](#quickstart-60-seconds-no-server-no-bucket) · [Daemon] · [MCP] · [SDKs] · [Benchmarks] · [FAQ] · [Roadmap]`. Run `go -C site/gen run .` afterwards: it verifies internal links (README anchors included where linked).

**docs/testing.md additions** (keep all existing text):

- Directly under the intro paragraph, an "At a glance" table:

| Claim | Evidence | Where it runs |
|---|---|---|
| A `kill -9`'d writer never corrupts the replica | `TestTortureWriterKill`: ~3,500 rounds, ~1,700 SIGKILLs, dump-identical every round | nightly Linux, weekly macOS (`nightly.yml`) |
| One writer per lineage, always | lease epochs + create-only puts + CAS on every ref; CAS probe refuses stores without it | every `go test`, RustFS on every PR, AWS nightly |
| Storage backends behave identically | `storetest.RunConformance` | local + fake S3 every run; real RustFS every PR; real AWS nightly since 2026-09-25 |
| Per-test isolation is cheap and measured | `make bench-isolation`, one pasted run | [benchmarks.md](benchmarks.md#per-test-isolation-primitives-v0211) |
| Branch-heavy agent topologies hold up | `make bench-branchbench`, one pasted run | [benchmarks.md](benchmarks.md#branchbench-topologies-v0211) |
| What you download is what CI built | keyless cosign + SLSA provenance + SBOM on every tag | `release.yml`; verify per [installation](installation.md#verify-what-you-downloaded) |
| Regression tests fail on the bug they fix | mutation-verified against the pre-fix code | review policy, CONTRIBUTING.md |

- New section `## Signed releases` before "## See also": four sentences on keyless signing identity, provenance, SBOM, and where to verify.
- New section `## What is not proven here` before "## See also", consolidating the honesty notes already scattered in the page: capturer SIGKILL not exercised; macOS runs without `-race`; providers other than RustFS/AWS are same-code-path only; benchmark numbers are one machine, one run.

**docs/status.md standing-nag rows** (append to the table):

| Item | Ready since | Blocked on |
|---|---|---|
| Announce (HN "How offshoot is tested" post + Show HN with the parallel-attempts recording and the MCP walkthrough) | Tier 1 item 5 — copy drafted in [docs/superpowers/specs/2026-09-26-launch-announce-draft.md](../docs/superpowers/specs/2026-09-26-launch-announce-draft.md) | ROADMAP's own gate: one external person completes install → fork → promote without help; the maintainer presses post |
| Corruption bounty | Tier 1 item 5 — proposed terms: a reproducible case where a `kill -9`'d stock-`sqlite3` writer under `offshoot serve` leaves a checkout that fails `PRAGMA integrity_check` or diverges from `.dump` of the source, on a supported backend, with the run script attached; one payout per distinct root cause | the amount and the payout are the maintainer's call; goes live by moving these terms into docs/testing.md |

**Announce draft** (`docs/superpowers/specs/2026-09-26-launch-announce-draft.md`): a title ("How offshoot is tested: kill -9, CAS everywhere, and a benchmark nobody ran on SQLite"), a 250-word HN text post that leads with the torture-harness numbers, the BranchBench table's one-line takeaway (filled from Task 3's real result), the isolation numbers, and the signed-release line; then a Show HN one-liner; then the five predictable questions with one-paragraph answers (why not Dolt/Neon; why no merge; why SQLite only; is S3 really safe; will the format break). Mark the file "DRAFT — not posted".

- [ ] **Step 1:** README reorder exactly as above; `go -C site/gen run .` passes. **Step 2:** testing.md additions. **Step 3:** status.md rows and the announce draft. **Step 4: Commit** `"docs: first screen leads with install, the recording and the stability contract; testing page finished as the launch artifact; announce and bounty rows"`.

---

### Task 5: Status, changelog, roadmap

**Files:**
- Modify: `docs/status.md` (rows: OpenSSF Scorecard workflow; signed/attested releases; BranchBench harness — each "shipped-and-tested" naming the workflow/program and the validation run; "v0.2.11 (unreleased)")
- Modify: `CHANGELOG.md` (Unreleased → Added: Scorecard; cosign/SLSA/SBOM releases with the verification recipe; `cmd/branchbench` + `make bench-branchbench` + the docs section; README first-screen reorder; testing page "at a glance")
- Modify: `ROADMAP.md` Launch track (lines ~333-347): tick the items this plan completes (rigor artifacts, signed releases), leave "Announce" unticked with the pointer to the standing nag
- Verify: `go -C site/gen run .`; `make check-plugin`; `grep -n "v0.2.9" README.md` prints nothing.
- [ ] Commit `"docs: status, changelog and roadmap for launch rigor"`.

---

## Self-review notes

- Coverage vs the roadmap item: BranchBench run + table (T3); "how offshoot is tested" post (T4, canonical page); cosign/SLSA/SBOM + Scorecard (T1, T2); README reorder (T4); announce (user-gated row, draft in T4).
- Consistency: verification commands identical in installation.md, SECURITY.md and testing.md (T2 defines them; T4 links, does not restate); benchmark anchors `#per-test-isolation-primitives-v0211` and `#branchbench-topologies-v0211` must match the headings T3 creates.
- Honesty: BranchBench's hosted-Postgres numbers are quoted, never re-run by us; the Scorecard badge shows "no score" until the first main run; releases before v0.2.11 are checksum-only.
