#!/usr/bin/env bash
#
# ci-local.sh — runs .github/workflows/ci.yml's four jobs locally, in
# minutes instead of waiting on a runner. Invoked via `make ci-local` (all
# four, in sequence, with a summary table) or `make ci-local-<job>` for one
# job alone. See CONTRIBUTING.md's "Local CI" section for what this does and
# does NOT prove versus the real GitHub Actions run — short version: this is
# fast pre-merge signal, not a replacement for the actual CI gate. Two
# concrete gaps worth knowing before trusting a green ci-local over a red
# GHA run:
#
#   - GHA's macos-latest image is a moving target from under us: it dropped
#     the sqlite3 CLI from PATH the day it rolled to the "macos-26" image
#     (see ci.yml's "Install sqlite3 CLI (macos)" step comment) with no
#     version bump on our side to notice. ci-local-host runs on whatever
#     macOS/Homebrew state your machine already has — it can't catch a
#     runner image regression like that; only a real macos-latest run can.
#   - GHA runners are non-root/no-sudo with a runner-managed pip/HOME
#     layout, which is exactly what surfaced the pytester HOME/user-site
#     interaction ci.yml's sdks job comment documents at length (a
#     --break-system-packages install lands in ~/.local, invisible to
#     pytester's subprocess runs, which repoint HOME to a throwaway dir).
#     ci-local-sdks uses a venv specifically to sidestep that, so it can't
#     reproduce the failure mode it was written in response to.
#
# CI on GitHub remains the merge gate. ci-local is pre-merge signal, run
# locally, to shorten the loop before pushing.
#
# Jobs (mirroring ci.yml's job names):
#   host   -> the `test` job's ubuntu/macos matrix, run on THIS host
#             (go vet + go test -race)
#   linux  -> the same suite, inside a golang:<go.mod version>-bookworm
#             container, so Linux-only bugs (this repo has real history
#             there) surface without needing a Linux box. Module/build
#             caches live in named Docker volumes so repeat runs are fast.
#   minio  -> the `s3-conformance` job: MinIO in Docker, `make test-s3`
#             against it, always torn down after (trap, not just the happy
#             path).
#   sdks   -> the `sdks` job: `make test-sdks`, the pytest-plugin suite in
#             a venv, and `make dry-run-sdks` if its tooling (build, twine,
#             npm) is present — skipped loudly, not silently, if not.
#
# Usage: scripts/ci-local.sh [host|linux|minio|sdks|all]   (default: all)

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

# Derived from go.mod so this never drifts from the version the repo's own
# Dockerfile builds against (golang:1.24-bookworm as of this writing) —
# same major.minor, same -bookworm base ci.yml's setup-go step effectively
# gets from go.mod's go directive.
GO_MINOR="$(awk '/^go [0-9]+\.[0-9]+/ {print $2; exit}' go.mod | cut -d. -f1,2)"
DOCKER_IMAGE="golang:${GO_MINOR}-bookworm"

GOCACHE_VOLUME="ci-local-gocache"
GOMODCACHE_VOLUME="ci-local-gomodcache"
MINIO_CONTAINER="ci-local-minio"

log() { printf '\n--- ci-local: %s ---\n' "$*"; }
err() { printf 'ci-local: %s\n' "$*" >&2; }

need_cmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		err "'$1' not found on PATH — required for this job."
		return 1
	fi
}

# ---------------------------------------------------------------------------
# host — mirrors ci.yml's `test` job, run natively.
# ---------------------------------------------------------------------------
job_host() {
	log "verify sqlite3 CLI (ci.yml installs this on the runner; on your own"
	echo "  machine it's a CONTRIBUTING.md dev-setup prerequisite — ci-local"
	echo "  does not install it for you)"
	if ! command -v sqlite3 >/dev/null 2>&1; then
		err "sqlite3 not found on PATH."
		err "  macOS: brew install sqlite sqldiff"
		err "  Linux: sudo apt-get install -y sqlite3 sqlite3-tools"
		return 1
	fi
	sqlite3 --version
	if ! command -v sqldiff >/dev/null 2>&1; then
		err "sqldiff not found on PATH (needed by cmd/offshoot/diff_test.go's"
		err "default-mode tests, gated on exec.LookPath(\"sqldiff\"))."
		err "  macOS: brew install sqldiff"
		err "  Linux: sudo apt-get install -y sqlite3-tools"
		return 1
	fi
	sqldiff --help >/dev/null || return 1

	log "go vet ./..."
	go vet ./... || return 1

	log "go test ./... -count=1 -race"
	go test ./... -count=1 -race
}

# ---------------------------------------------------------------------------
# linux — mirrors ci.yml's `test` job's ubuntu leg, inside Docker.
# ---------------------------------------------------------------------------
job_linux() {
	need_cmd docker || return 1

	log "docker run ${DOCKER_IMAGE} (repo bind-mounted read-only; module +"
	echo "  build caches in named volumes ${GOMODCACHE_VOLUME}/${GOCACHE_VOLUME}"
	echo "  so repeat runs don't re-download/re-build from scratch)"

	docker run --rm \
		-e CGO_ENABLED=1 \
		-e GOMODCACHE=/gocache/mod \
		-e GOCACHE=/gocache/build \
		-e HOME=/tmp/ci-local-home \
		-v "${REPO_ROOT}:/src:ro" \
		-v "${GOMODCACHE_VOLUME}:/gocache/mod" \
		-v "${GOCACHE_VOLUME}:/gocache/build" \
		-w /src \
		"${DOCKER_IMAGE}" \
		bash -euc '
			mkdir -p "$HOME"
			echo "--- apt-get install sqlite3 sqlite3-tools (mirrors ci.ymls ubuntu install step) ---"
			apt-get update -qq
			apt-get install -y -qq --no-install-recommends sqlite3 sqlite3-tools
			sqlite3 --version
			sqldiff --help >/dev/null

			echo "--- go vet ./... ---"
			go vet ./...

			echo "--- go test ./... -count=1 -race ---"
			go test ./... -count=1 -race
		'
}

# ---------------------------------------------------------------------------
# minio — mirrors ci.yml's `s3-conformance` job.
# ---------------------------------------------------------------------------
job_minio() {
	need_cmd docker || return 1
	need_cmd curl || return 1

	# Runs in a subshell with its own EXIT trap so MinIO is torn down no
	# matter how this job ends (success, test failure, or us bailing out
	# early on a health-check timeout) — same intent as ci.yml's
	# `Stop MinIO` step, which runs under `if: always()`.
	(
		set -u
		trap 'docker rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true' EXIT

		docker rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true

		log "starting MinIO (docker run minio/minio:latest server /data)"
		docker run -d --name "${MINIO_CONTAINER}" \
			-p 9000:9000 \
			-e MINIO_ROOT_USER=minioadmin \
			-e MINIO_ROOT_PASSWORD=minioadmin \
			minio/minio:latest server /data >/dev/null || exit 1

		up=0
		for _ in $(seq 1 30); do
			if curl -sSf http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
				up=1
				break
			fi
			sleep 1
		done
		if [ "$up" -ne 1 ]; then
			err "MinIO never became healthy after 30s"
			exit 1
		fi
		echo "minio is up"

		log "create test bucket (offshoot-test)"
		# --network container:<name>, not ci.yml's --network host: shares the
		# MinIO container's network namespace directly, so `mc` can reach
		# 127.0.0.1:9000 without relying on Docker host networking, which
		# Docker Desktop (macOS) doesn't support the way it does on the
		# ubuntu-latest runner ci.yml targets. Same pattern already used by
		# this Makefile's `bench-s3` target.
		docker run --rm --network "container:${MINIO_CONTAINER}" \
			-e MC_HOST_local=http://minioadmin:minioadmin@127.0.0.1:9000 \
			minio/mc:latest mb -p local/offshoot-test || exit 1

		log "make test-s3"
		OFFSHOOT_S3_TEST_BUCKET=offshoot-test \
			OFFSHOOT_S3_ENDPOINT=http://127.0.0.1:9000 \
			OFFSHOOT_S3_PATH_STYLE=1 \
			AWS_ACCESS_KEY_ID=minioadmin \
			AWS_SECRET_ACCESS_KEY=minioadmin \
			AWS_REGION=us-east-1 \
			make test-s3
	)
}

# ---------------------------------------------------------------------------
# sdks — mirrors ci.yml's `sdks` job.
# ---------------------------------------------------------------------------
job_sdks() {
	need_cmd python3 || return 1
	need_cmd npm || return 1

	log "make test-sdks (python3 unittest + npm test)"
	make test-sdks || return 1

	# A venv, not --break-system-packages onto system python — see this
	# script's header and ci.yml's own comment on the `Install
	# offshoot-db[pytest] + pytest-xdist` step for why: pytest's `pytester`
	# fixture repoints HOME per nested subprocess run, so pytest must be
	# reachable independent of $HOME, which only a venv's baked-in
	# site-packages path guarantees.
	log "pytest fixture plugin suite (venv install, mirrors ci.yml)"
	venv=".venv-ci-local-pytest-plugin"
	rm -rf "$venv"
	python3 -m venv "$venv" || return 1
	"$venv/bin/pip" install --quiet -e "sdk/python[pytest]" pytest-xdist || {
		rm -rf "$venv"
		return 1
	}
	PATH="${REPO_ROOT}/${venv}/bin:${PATH}" make test-pytest-plugin
	status=$?
	rm -rf "$venv"
	if [ "$status" -ne 0 ]; then
		return "$status"
	fi

	log "make dry-run-sdks (needs python3 build+twine, npm — skipped loudly if absent)"
	if python3 -c "import build, twine" >/dev/null 2>&1; then
		make dry-run-sdks
	else
		err "SKIPPING make dry-run-sdks: python3 'build' and/or 'twine' packages"
		err "  not importable. Install with: python3 -m pip install build twine"
		err "  (see CONTRIBUTING.md's dev setup / ci.yml's sdks job)."
	fi
}

# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

# Bash on macOS ships at 3.2 (no associative arrays), so per-job
# status/duration live in plainly-named variables set via eval rather than
# a `declare -A` map.
run_job() {
	job_key="$1"
	job_label="$2"
	job_fn="$3"

	printf '\n============================================================\n'
	printf 'ci-local: %s\n' "$job_label"
	printf '============================================================\n'

	job_start=$SECONDS
	if "$job_fn"; then
		eval "${job_key}_status=pass"
	else
		eval "${job_key}_status=fail"
	fi
	eval "${job_key}_secs=\$(( SECONDS - job_start ))"
}

fmt_secs() {
	s="$1"
	printf '%dm%02ds' "$((s / 60))" "$((s % 60))"
}

cmd_all() {
	run_job host  "ci-local-host  (go vet + go test -race, this host)"        job_host
	run_job linux "ci-local-linux (go vet + go test -race, docker/linux)"     job_linux
	run_job minio "ci-local-minio (MinIO in docker + make test-s3)"           job_minio
	run_job sdks  "ci-local-sdks  (make test-sdks + pytest-plugin + dry-run)" job_sdks

	overall=0
	total_secs=0
	printf '\n============================================================\n'
	printf 'ci-local summary\n'
	printf '============================================================\n'
	printf '%-14s %-6s %10s\n' "job" "result" "time"
	for key in host linux minio sdks; do
		eval "st=\${${key}_status}"
		eval "sec=\${${key}_secs}"
		total_secs=$((total_secs + sec))
		printf '%-14s %-6s %10s\n' "$key" "$st" "$(fmt_secs "$sec")"
		[ "$st" = "fail" ] && overall=1
	done
	printf -- '------------------------------------------------------------\n'
	printf '%-14s %-6s %10s\n' "total" "-" "$(fmt_secs "$total_secs")"
	printf '============================================================\n'
	return "$overall"
}

main() {
	case "${1:-all}" in
	host) job_host ;;
	linux) job_linux ;;
	minio) job_minio ;;
	sdks) job_sdks ;;
	all) cmd_all ;;
	*)
		err "usage: $0 [host|linux|minio|sdks|all]"
		exit 2
		;;
	esac
}

main "$@"
