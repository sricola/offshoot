# offshoot

Branch SQLite like git: create, fork, checkpoint, rollback, and promote
SQLite databases — stock SQLite files, your storage, one binary.

**Status: pre-alpha — local mode working (Plan 2); capture spike GO (Plan 1).**
Requires Go 1.24+, cgo, and the `sqlite3` CLI for tests. Linux and macOS only.

## Quickstart (60 seconds, no server, no bucket)

    go build -o offshoot ./cmd/offshoot
    ./offshoot init
    ./offshoot create app
    sqlite3 "$(./offshoot path app)" "CREATE TABLE users (name); INSERT INTO users VALUES ('ada');"
    ./offshoot checkpoint app v1
    ./offshoot fork app attempt-1        # instant branch
    sqlite3 "$(./offshoot path app@attempt-1)" "DELETE FROM users;"   # destructive experiment
    ./offshoot rollback app@attempt-1 --to fork                        # undo it
    ./offshoot promote app@attempt-1 --onto main --force               # or ship it
    ./offshoot status

Plan-2 (local mode) notes: checkpoints are full snapshots; checkout paths are
fixed at `<store>/checkouts/{db}/{branch}.db`; operations require the
checkout to be quiescent (no live writers). Daemon mode with live capture,
incremental segments, and S3/R2/Tigris backends is Plan 3.

Design: docs/superpowers/specs/2026-07-29-offshoot-design.md
Capture-spike evidence: docs/superpowers/specs/2026-07-29-offshoot-spike-report.md
