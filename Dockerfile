# syntax=docker/dockerfile:1

# offshoot ships as a single cgo binary (mattn/go-sqlite3) plus the sqlite3
# CLI, which the daemon and CLI both shell out to in a couple of code paths
# and which tests/examples expect on PATH. Build stage compiles the binary
# with cgo against glibc so it matches the runtime stage's glibc; runtime is
# a slim Debian image with just the sqlite3 CLI and CA certs (needed for the
# S3 backend's TLS calls) added on top.

FROM golang:1.24-bookworm AS build
WORKDIR /src

# Populate the module cache first so dependency downloads are cached
# independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/offshoot ./cmd/offshoot

FROM debian:bookworm-slim AS runtime

# sqlite3-tools carries sqldiff, which `offshoot diff` shells out to —
# Debian packages it separately from the sqlite3 CLI (see
# cmd/offshoot/diff.go for the per-distro breakdown).
RUN apt-get update \
    && apt-get install -y --no-install-recommends sqlite3 sqlite3-tools ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --home-dir /home/offshoot --shell /usr/sbin/nologin offshoot

COPY --from=build /out/offshoot /usr/local/bin/offshoot

# Store location defaults to ./.offshoot (see `offshoot` usage text), which
# resolves relative to WORKDIR below. Mount a volume at /data and point
# OFFSHOOT_STORE there (or pass -store) to persist state across container
# restarts, e.g.:
#
#   docker run -v offshoot-data:/data -e OFFSHOOT_STORE=/data ghcr.io/sricola/offshoot init
#
# /data is also set as VOLUME below so state is not silently lost even if
# the caller forgets -v; anonymous volumes still don't survive `docker rm
# -v`, so an explicit named volume or bind mount is recommended for real use.
ENV OFFSHOOT_STORE=/data
WORKDIR /data
VOLUME ["/data"]
RUN chown offshoot:offshoot /data

USER offshoot

ENTRYPOINT ["offshoot"]
# No default subcommand: `offshoot` with no args prints the usage text and
# exits 0 (there is no --help flag), so leave CMD empty rather than passing
# something offshoot would reject as an unknown command.
CMD []
