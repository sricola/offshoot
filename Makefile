.PHONY: test test-torture build
test:
	go test ./... -count=1
test-torture:
	go test ./internal/capture -tags=torture -run TestTorture -timeout 30m -v
build:
	go build -o bin/torture ./cmd/torture
