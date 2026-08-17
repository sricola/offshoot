# Installation

One binary, no server required. Linux and macOS; requires Go 1.25+ and cgo
only if you build from source, and the `sqlite3` CLI only for running the
test suite. Pick whichever channel fits:

## Homebrew

```
brew tap sricola/offshoot https://github.com/sricola/offshoot
brew trust sricola/offshoot
brew install offshoot
```

Recent Homebrew requires the explicit `brew trust` step for any
third-party tap. The formula lives in-repo at
[`Formula/offshoot.rb`](../Formula/offshoot.rb).

## Prebuilt binaries

Tarballs (`offshoot_<tag>_<os>_<arch>.tar.gz`) publish to the
[releases page](https://github.com/sricola/offshoot/releases) for each
tagged release, each with a `.sha256` checksum file alongside it. For
example, for a Linux amd64 machine (substitute the tag you're
downloading):

```
shasum -a 256 -c offshoot_v0.2.9_linux_amd64.tar.gz.sha256
tar xzf offshoot_v0.2.9_linux_amd64.tar.gz
./offshoot version
```

## Docker

```
docker run --rm -v offshoot-data:/data ghcr.io/sricola/offshoot:latest init
```

Multi-architecture `linux/amd64` and `linux/arm64` images publish to GHCR on
every tagged release. The store lives in the `/data` volume, so reuse
`-v offshoot-data:/data` across commands
(`... offshoot:latest create app`, `... offshoot:latest serve`, and so
on).

## go install / from source

```
go install github.com/sricola/offshoot/cmd/offshoot@latest
```

Or from a clone:

```
git clone https://github.com/sricola/offshoot
cd offshoot
go build -o offshoot ./cmd/offshoot
```

Both need Go 1.25+ with cgo enabled (offshoot embeds SQLite via
`mattn/go-sqlite3`).

## Platform support

Linux and macOS only. **Windows:** use WSL2 — the Linux binaries, Docker
image, and build-from-source all work there as-is. Native Windows is
unsupported: offshoot leans on POSIX file semantics (unix sockets, POSIX
locks) that don't map cleanly to Windows
([why](faq.md#why-no-windows-support)).

## Initialize a store

Every offshoot command runs against a **store** — a local directory or an
S3-compatible bucket. Create one with `init` before anything else:

```
offshoot -store ./.offshoot init                 # local directory (the default)
offshoot -store s3://my-bucket/offshoot init     # S3-compatible bucket
```

`-store` can appear anywhere in the argument list. If omitted, offshoot
uses the `OFFSHOOT_STORE` environment variable, and falls back to
`./.offshoot` if that's unset too — so after `offshoot init` in a project
directory, bare commands just work. Running `init` against an
already-initialized store fails rather than silently succeeding, so don't
script it unconditionally before every command.

**The fail-closed probe:** offshoot's safety rests on compare-and-swap —
every branch ref update is a conditional write. At attach time, every
command probes the store and **refuses to run if conditional writes are
not enforced**, rather than silently degrading. The probe re-runs on every
CLI invocation (each command attaches fresh); a long-lived daemon
(`offshoot serve`) pays it once per process instead of once per command.

### S3 configuration

Credentials come from the AWS SDK's default chain (environment, shared
config/credentials file, IAM role) — never from an offshoot-specific
variable. Three variables are consulted for `s3://` specs:

| Variable | Meaning |
|---|---|
| `OFFSHOOT_S3_ENDPOINT` | Custom endpoint (MinIO, or any S3-compatible endpoint); unset means AWS's default endpoint |
| `OFFSHOOT_S3_REGION` | Region; defaults to `auto` when a custom endpoint is set |
| `OFFSHOOT_S3_PATH_STYLE` | `1` for path-style addressing (needed for MinIO) |

For a *remote* (`s3://`) store, `OFFSHOOT_CHECKOUTS` controls where
checkouts are materialized locally (default: a per-store directory under
the user cache dir); local stores always keep checkouts under the store
directory itself.

### Provider support

A provider is listed as supported only after the conformance suite and CAS
probe pass against it for real:

| Provider | Status |
|---|---|
| MinIO | verified in CI — the conformance suite runs against real MinIO on every PR and push to main |
| AWS S3 | verified — probe + conformance + multipart passed against a real bucket (us-east-1, 2026-08-13) |
| Google Cloud Storage (S3 interop) | **unsupported** — no conditional writes on the S3 API; the probe refuses it ([why](faq.md#why-no-google-cloud-storage)) |

See [Limitations](limitations.md#s3-compatible-means-conditional-writes)
for what "S3-compatible" concretely requires.

## Next

- New to offshoot? The [Quickstart](quickstart.md) is the five-minute
  fork/rollback tour.
- Every flag of every command: the [CLI reference](reference.md).
