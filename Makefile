.PHONY: test test-torture build test-s3 test-python-sdk
test:
	go test ./... -count=1
test-torture:
	go test ./internal/capture -tags=torture -run TestTorture -count=1 -timeout 30m -v
build:
	go build -o bin/torture ./cmd/torture
test-s3:
	go test ./internal/store -run TestS3RealProvider -count=1 -v
test-python-sdk:
	cd sdk/python && python3 -m unittest discover -s tests -v
