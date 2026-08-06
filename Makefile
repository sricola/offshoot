.PHONY: test test-torture build test-s3 bench bench-s3 test-python-sdk test-ts-sdk test-sdks
test:
	go test ./... -count=1
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
test-python-sdk:
	cd sdk/python && python3 -m unittest discover -s tests -v
test-ts-sdk:
	cd sdk/typescript && npm install --no-audit --no-fund && npm test
# test-sdks is deliberately not a dependency of the default `test` target:
# it needs python3 and node/npm on PATH, which the Go suite does not.
test-sdks: test-python-sdk test-ts-sdk

.PHONY: third-party-licenses
# third-party-licenses regenerates THIRD_PARTY_LICENSES.csv from the current
# module graph. Runnable locally (network access required, to fetch
# go-licenses itself and consult per-dependency license URLs) and used by
# release.yml's collect job so a release always ships an up-to-date bundle.
third-party-licenses:
	go run github.com/google/go-licenses@latest report ./... > THIRD_PARTY_LICENSES.csv
