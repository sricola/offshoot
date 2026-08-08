.PHONY: test test-torture build test-s3 bench bench-s3 test-python-sdk test-ts-sdk test-sdks \
	check-sdk-versions dry-run-python-sdk dry-run-ts-sdk dry-run-sdks test-pytest-plugin \
	ci-local ci-local-host ci-local-linux ci-local-minio ci-local-sdks lint
test:
	go test ./... -count=1

# lint fails if any file is unformatted (gofmt -l prints offenders; the
# `(! read)` trick makes any output a nonzero exit) or go vet finds a
# problem; staticcheck runs best-effort last (`go run` fetches it, so it
# needs network — the || clause keeps an offline run from failing the two
# gates that already passed). ci.yml runs the same gofmt/vet pair on every
# PR so an unformatted non-test file can't ship silently again (audit §7).
lint:
	gofmt -l . | tee /dev/stderr | (! read)
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./... || echo "staticcheck failed or unavailable (non-blocking)"
test-torture:
	go test ./internal/capture -tags=torture -run TestTorture -count=1 -timeout 30m -v
build:
	go build -o bin/torture ./cmd/torture
test-s3:
	go test ./internal/store -run TestS3RealProvider -count=1 -v

# bench runs internal/ops's Fork/Checkout/session.Open benchmarks against a
# local store (see internal/ops/fork_bench_test.go and docs/benchmarks.md).
# -short skips the size=4GB Fork case: a multi-minute, multi-GB-RAM run,
# meant to be measured explicitly and rarely, not on every `make bench`. Run
# it directly when you want that number:
#   go test ./internal/ops -bench 'ForkAtHead/size=4GB' -benchmem -run '^$$' -benchtime=1x -timeout 20m
bench:
	go test ./internal/ops -bench . -benchmem -run '^$$' -count=3 -short

# bench-s3 runs the same benchmarks against a real S3-compatible backend
# (MinIO in Docker) instead of the local store, so docs/benchmarks.md's
# local-store numbers can be compared against the network-bound path.
# Requires Docker. Always tears the container down, even on failure.
bench-s3:
	docker rm -f offshoot-bench-minio >/dev/null 2>&1 || true
	docker run -d --name offshoot-bench-minio -p 9100:9000 \
		-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
		minio/minio:latest server /data
	for i in $$(seq 1 30); do \
		curl -sSf http://127.0.0.1:9100/minio/health/live >/dev/null 2>&1 && break; \
		sleep 1; \
	done
	curl -sSf http://127.0.0.1:9100/minio/health/live >/dev/null
	docker run --rm --network container:offshoot-bench-minio \
		-e MC_HOST_local=http://minioadmin:minioadmin@127.0.0.1:9000 \
		minio/mc:latest mb -p local/offshoot-bench
	OFFSHOOT_S3_TEST_BUCKET=offshoot-bench \
	OFFSHOOT_S3_ENDPOINT=http://127.0.0.1:9100 \
	OFFSHOOT_S3_PATH_STYLE=1 \
	AWS_ACCESS_KEY_ID=minioadmin \
	AWS_SECRET_ACCESS_KEY=minioadmin \
	go test ./internal/ops -bench . -benchmem -run '^$$' -count=1 -short; \
	status=$$?; \
	docker rm -f offshoot-bench-minio >/dev/null 2>&1; \
	exit $$status
# Named explicitly (not `discover -s tests`, which globs test_*.py) because
# tests/test_pytest_plugin.py — the offshoot.pytest_plugin fixture plugin's
# own suite — imports pytest at module level and must NOT be picked up here:
# this target proves the base SDK's plain-unittest suites pass with no
# pytest installed at all. See `make test-pytest-plugin` for that file.
test-python-sdk:
	cd sdk/python && python3 -m unittest tests.test_client tests.test_langgraph -v
test-ts-sdk:
	cd sdk/typescript && npm install --no-audit --no-fund && npm test
# test-sdks is deliberately not a dependency of the default `test` target:
# it needs python3 and node/npm on PATH, which the Go suite does not.
test-sdks: test-python-sdk test-ts-sdk

# test-pytest-plugin runs offshoot.pytest_plugin's own test suite
# (sdk/python/tests/test_pytest_plugin.py). Unlike test-python-sdk (plain
# unittest, deliberately pytest-free so `pip install offshoot-db` with no
# extra keeps working), this target legitimately needs pytest + pytest-xdist
# on PATH — it IS the test suite for the pytest fixture plugin (the
# `offshoot-db[pytest]` extra) and its xdist smoke scenario. Not a
# dependency of test-sdks/test: see CONTRIBUTING.md's dev setup for the
# `pip install "sdk/python[pytest]" pytest-xdist` this needs first. Builds
# the offshoot binary once and pins OFFSHOOT_BIN so both the daemon fixture
# and its nested pytester-driven runs reuse it instead of a repeat `go
# build`.
test-pytest-plugin:
	go build -o bin/offshoot-pytest-plugin-test ./cmd/offshoot
	OFFSHOOT_BIN=$(CURDIR)/bin/offshoot-pytest-plugin-test \
	  python3 -m pytest sdk/python/tests/test_pytest_plugin.py -v

# check-sdk-versions verifies sdk/VERSION (the single source of truth — see
# CONTRIBUTING.md's "Release process") agrees with the version literally
# spelled out in sdk/python/pyproject.toml and sdk/typescript/package.json.
check-sdk-versions:
	python3 scripts/check_sdk_versions.py

# dry-run-python-sdk builds the real sdist+wheel `offshoot-db` would publish,
# runs twine's metadata check against them, then installs the wheel into a
# throwaway venv and import-tests it. This is exactly what
# .github/workflows/publish.yml's PyPI job runs when the repository variable
# PUBLISH_ENABLED is not "true" instead of uploading anywhere; ci.yml's sdks
# job runs it on every PR so a manifest mistake (bad classifier, missing
# readme, broken package-discovery glob) fails long before a release tag
# does. Needs `python3 -m pip install build twine` on PATH — see
# CONTRIBUTING.md's dev setup.
dry-run-python-sdk: check-sdk-versions
	rm -rf sdk/python/dist
	cd sdk/python && python3 -m build
	python3 -m twine check sdk/python/dist/*
	rm -rf sdk/python/.dry-run-venv
	python3 -m venv sdk/python/.dry-run-venv
	sdk/python/.dry-run-venv/bin/pip install --quiet sdk/python/dist/*.whl
	sdk/python/.dry-run-venv/bin/python3 -c "import offshoot; print('import offshoot: ok,', offshoot.__doc__.splitlines()[0])"
	rm -rf sdk/python/.dry-run-venv

# dry-run-ts-sdk builds dist/ (from a clean slate — rm -rf first, so a stale
# dist/ left over from a different files-array/branch can never be what
# gets packed), packs the exact tarball `npm publish` would upload
# (respecting package.json's "files" whitelist), asserts the tarball's
# actual contents are EXACTLY the allowed set (not "at least" or "no worse
# than" — any addition or removal fails the build, so a loosened "files"
# array or a stray committed dist/ artifact fails CI, not a human's
# memory), then installs *that tarball* — not the source tree — into a
# throwaway project and import-tests the published entry point. This is
# exactly what .github/workflows/publish.yml's npm job runs when
# PUBLISH_ENABLED is not "true" instead of uploading anywhere; ci.yml's
# sdks job runs it on every PR. Needs `npm` and `tar` on PATH.
dry-run-ts-sdk:
	rm -rf sdk/typescript/dist
	cd sdk/typescript && npm install --no-audit --no-fund && npm run build
	rm -rf sdk/typescript/dry-run-pack sdk/typescript/dry-run-install
	mkdir -p sdk/typescript/dry-run-pack sdk/typescript/dry-run-install
	cd sdk/typescript && npm pack --silent --pack-destination dry-run-pack
	tar -tzf sdk/typescript/dry-run-pack/offshoot-db-client-*.tgz | sort > sdk/typescript/dry-run-pack/actual-tarball-contents.txt
	printf '%s\n' package/README.md package/dist/client.d.ts package/dist/client.js package/dist/testkit.d.ts package/dist/testkit.js package/package.json | sort > sdk/typescript/dry-run-pack/expected-tarball-contents.txt
	diff -u sdk/typescript/dry-run-pack/expected-tarball-contents.txt sdk/typescript/dry-run-pack/actual-tarball-contents.txt
	@echo "tarball contents: exact match (README.md, dist/client.d.ts, dist/client.js, dist/testkit.d.ts, dist/testkit.js, package.json) — nothing more, nothing less"
	cd sdk/typescript/dry-run-install && npm init -y --silent >/dev/null
	cd sdk/typescript/dry-run-install && npm install --no-audit --no-fund --silent ../dry-run-pack/offshoot-db-client-*.tgz
	cd sdk/typescript/dry-run-install && node -e "import('@offshoot-db/client').then((m) => { if (typeof m.connect !== 'function') throw new Error('connect export missing from published package'); console.log('import @offshoot-db/client: ok'); })"
	cd sdk/typescript/dry-run-install && node -e "import('@offshoot-db/client/testkit').then((m) => { if (typeof m.startDaemon !== 'function' || typeof m.seedOnce !== 'function' || typeof m.forkPerTest !== 'function' || typeof m.dump !== 'function') throw new Error('testkit exports missing from published package'); console.log('import @offshoot-db/client/testkit: ok'); })"
	rm -rf sdk/typescript/dry-run-pack sdk/typescript/dry-run-install

# dry-run-sdks runs both dry runs — the "does this build and install"
# tier that gates on nothing but PATH tools, run on every PR touching
# sdk/** (see ci.yml's sdks job) so PUBLISH_ENABLED flipping on is never the
# first time a manifest mistake is discovered.
dry-run-sdks: dry-run-python-sdk dry-run-ts-sdk

# ci-local mirrors .github/workflows/ci.yml's job matrix locally, so
# CI-equivalent signal lands in minutes instead of waiting on a runner. Each
# job is also runnable alone (`make ci-local-host`, etc.) for a tighter
# loop while iterating on one thing. `ci-local` runs all four in sequence
# and prints a pass/fail-per-job summary table; see scripts/ci-local.sh's
# header and CONTRIBUTING.md's "Local CI" section for exactly what this
# does and does NOT prove versus a real GitHub Actions run.
ci-local:
	./scripts/ci-local.sh all
ci-local-host:
	./scripts/ci-local.sh host
ci-local-linux:
	./scripts/ci-local.sh linux
ci-local-minio:
	./scripts/ci-local.sh minio
ci-local-sdks:
	./scripts/ci-local.sh sdks

.PHONY: third-party-licenses
# third-party-licenses regenerates THIRD_PARTY_LICENSES.csv from the current
# module graph. Runnable locally (network access required, to fetch
# go-licenses itself and consult per-dependency license URLs) and used by
# release.yml's collect job so a release always ships an up-to-date bundle.
third-party-licenses:
	go run github.com/google/go-licenses@latest report ./... > THIRD_PARTY_LICENSES.csv
