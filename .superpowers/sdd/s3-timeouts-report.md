# S3 single-shot timeouts + GetReader progress watchdog

Closes the last tracked availability finding (pass-2 residual on the 0.2.6
LOW-1 fix): the single-shot S3 calls ran under bare `context.Background()`,
so a backend that accepted a request and then stalled mid-body (or stopped
reading an upload) wedged the caller forever — several of those calls sit on
the daemon's flush path under `flushMu`, so `Session.Close` hung too.

Branch: `s3-timeouts` (off `main`). No public signature changed; multipart
file untouched. internal/store only → **no torture run** (per policy).

## Wrapping approach (buffered single-shot calls)

One new package var, `singleShotRPCTimeout = 15 * time.Minute` (s3.go), as
the flat BASE, plus size scaling for calls whose payload size is known at
call time (review finding: a flat 15 min alone imposes an effective ~5.7
MiB/s sustained-throughput floor — a legitimate ~4 GiB below-threshold
snapshot flush over a ~20 Mbps uplink takes ~27 min and would have died at
15 min on every attempt, permanently):

```go
const singleShotFloorBytesPerSecond = 1 << 20 // same 1 MiB/s floor multipartRPCTimeout derives from

func singleShotDeadline(size int64) time.Duration {
    d := singleShotRPCTimeout
    if size > 0 {
        d += time.Duration(size/singleShotFloorBytesPerSecond) * time.Second
    }
    return d
}

func singleShotCtx(size int64) (context.Context, context.CancelFunc) {
    return context.WithTimeout(context.Background(), singleShotDeadline(size))
}
```

This mirrors `multipartRPCTimeout`'s per-part math at whole-object scale
(its 15 min = ~550 MiB worst-case part at a pessimistic 1 MiB/s + headroom;
the constants' comments cite each other so the floors can't silently
diverge). Sized calls: `Put`/`PutIf` (`len(data)`), the below-threshold
single `PutObject` in `PutReader`/`PutReaderIf` (`size` param), and
`CopyObject`'s single-request copy (the HEAD-reported source size —
server-side, normally fast, but a 5 GiB copy still gets transfer headroom).
Size-0 (flat base) calls, with the reasoning stated in the var comment:
`Get` (buffered Gets on this backend carry refs/manifests/tombstones —
large chain members stream via `GetReader`; the response size isn't known
until headers anyway), `List` pages, `Delete`, `DeleteObjects` batches, and
the `HeadObject` preflight. `singleShotDeadline(5 GiB)` = 15 min + ~85 min.

Wrapped call sites (all three-line, identical shape — obtain pair, call,
`cancel()` immediately after the result is fully consumed):

- `Get` — the one non-trivial case: the SDK call returns at headers but the
  body is read by `io.ReadAll` inside our method, so the pair is obtained at
  the top and the cancel is **deferred**, keeping the deadline alive through
  the ReadAll on every path.
- `Put`, `PutIf`, `Delete` — cancel right after the call.
- `PutReader` / `PutReaderIf` below-threshold single `PutObject` — same.
- `List` — fresh pair **per pagination page**, canceled before the next
  iteration (explicit cancel, no defer-in-loop accumulation).
- `DeleteObjects` — fresh pair per 1000-key batch, same discipline.
- `CopyObject` — separate pairs for the `HeadObject` preflight and the
  single-request copy; the >5 GiB path still delegates to
  `copyObjectMultipart`, which keeps its own per-RPC deadlines (untouched).

Test hooks: `SetSingleShotRPCTimeoutForTest` (replaces the BASE — test
payloads are far under the 1 MiB/s floor, so their size component is 0s
and the hook value is effectively the whole deadline; documented on the
hook) and `SetReadProgressTimeoutForTest`, in
`internal/store/export_test.go`, matching the existing
`SetMultipartRPCTimeoutForTest` pattern; plus `SingleShotDeadlineForTest`
exposing the scaling arithmetic for direct pinning.

## GetReader watchdog design

`GetReader`'s returned stream outlives the call, so a request-scoped total
deadline would kill legitimate long reads of large objects. Instead the
request runs under `context.WithCancel(context.Background())` and the Body
is wrapped in a `watchdogReader` (s3.go):

- The timer is created ONCE in the constructor via
  `time.AfterFunc(readProgressTimeout, func() { stalled.Store(true); cancel() })`
  and immediately stopped — created disarmed. Creating it eagerly (not
  lazily on first Read) means Read and Close never race on the field
  itself; only `Reset`/`Stop` calls race, which are safe.
- **Arm-only-during-Read**: each `Read` does `timer.Reset(window)` on
  entry and `timer.Stop()` on exit around the blocking `body.Read`. So the
  timer can only fire while a Read is actually blocked inside the body: a
  stalled PRODUCER leaves Read blocked until the timer cancels the request
  context, unblocking the Read with an error; a slow CONSUMER (arbitrary
  pauses BETWEEN Reads) never has an armed timer and is never killed. Any
  progress — even one byte — re-earns a full window on the next Read.
- **Close race**: timer-fire racing `Close` is harmless by construction —
  both just call `cancel()` (idempotent) and `Timer.Stop` (idempotent).
  `stalled` is an `atomic.Bool` so the fired-vs-not check in Read doesn't
  race the timer goroutine.
- **Recognizable error**: when the watchdog fired, the Read error (which
  carries `context.Canceled` via net/http's body-read error mapping) is
  wrapped as `store: s3 read <key>: read stalled for <window> with no
  progress: ...`. The only production consumer is ops' `lazyReader`
  (verified by grep — `internal/ops/materialize.go` is the sole non-test
  `GetReader` caller), whose error path treats any Read error as fatal:
  it propagates the error out of `MaterializeChain` and every stream is
  deferred-closed. No caller special-cases read errors, so no special
  handling is needed for the watchdog error.
- Known benign edge (documented in the type's comment): a timer that fires
  just as its Read returns successfully cancels the context, so the NEXT
  Read fails — only reachable when a single Read consumed the entire
  window anyway, i.e. the stream was already at the stall boundary.

`readProgressTimeout = 60s` — it bounds a *zero-progress* window, never
total stream lifetime, so it can be tight (same order of patience as the
transport's `ResponseHeaderTimeout`).

## Fake extensions (storetest/fakes3.go, minimal)

- `SetRequestStall(fn)` — parks a matching request BEFORE any processing
  (before `f.mu`, before the fault hook, zero response bytes) until the
  client abandons it (`<-r.Context().Done()`), so a stalled handler can
  never outlive the test into `srv.Close`'s drain. Gotcha found the hard
  way (first full run hung 10 minutes in `srv.Close`): net/http's server
  only detects a client disconnect — and cancels `r.Context()` — via its
  background read, which it arms only once the request BODY has been fully
  consumed; parking with the PUT's body unread left the handler
  permanently blind to the client giving up. The stall therefore drains
  `r.Body` before parking, and both parks carry a 30s `stallBackstop`
  select arm so any future detection regression degrades to a slow test,
  never a hung binary.
- `SetGetBodyStall(key, n)` — GET writes headers (full Content-Length) plus
  the first n body bytes, flushes, then parks until the client gives up
  (same backstop). `f.mu` is released for the duration of the park so a
  stalled GET never wedges the rest of the fake.

## Tests (internal/store/s3_timeout_test.go)

- `TestS3GetSingleShotTimeoutMidBodyStall` — headers + 1 KiB arrive, body
  stalls; with the hook at 100ms, `Get` fails with
  `context.DeadlineExceeded` (not ErrNotFound) instead of hanging — proves
  the deadline survives into the ReadAll.
- `TestS3PutIfSingleShotTimeoutStalledResponse` — response never begins;
  `PutIf` fails with `context.DeadlineExceeded`, never `ErrCAS`. Because
  every buffered call wraps through the one `singleShotCtx` helper, these
  two tests + inspection cover the class.
- `TestS3GetReaderWatchdogKillsStalledProducer` — 1 KiB then stall: Read
  returns exactly the 1024 delivered bytes plus an error wrapping
  `context.Canceled` and containing "read stalled", within a bounded
  window; `Close` afterwards returns promptly. All receives are
  timer-bounded (`waitOrFatal`), run under `-race`.
- `TestS3SingleShotDeadlineScalesWithSize` — pins the arithmetic:
  `singleShotDeadline(0)` == the flat base; `(5 GiB)` == base + 5120s
  (>= the ~85 min a worst-case legitimate transfer needs at the floor);
  `(64 KiB)` == flat base (sub-floor payloads add nothing, so the test
  hook governs every fixture-sized call).
- `TestS3GetReaderSlowConsumerNotKilled` — fast producer, consumer sleeps
  3x the (50ms) window between Reads: full payload reads back
  byte-identical with no error — proves the watchdog arms only during
  blocking Reads.

All existing tests pass unmodified.

## Results

- `go build ./... && go vet ./... && gofmt -l .` — clean.
- `go test ./internal/store/... ./internal/session/... ./internal/ops/... -count=1`
  — all PASS (store 38s, storetest 0.2s, session 64s, ops 45s, reflink
  0.3s), existing tests unmodified.
- `go test -race ./internal/store/... -count=1` — PASS (store 37s,
  storetest 1.3s), no races.
- `./scripts/ci-local.sh minio` — PASS (TestS3RealProvider: Probe,
  full Conformance, Multipart — the real-provider suite against MinIO,
  including streaming reads through the new watchdog).

## Concerns

- The watchdog wraps the stall error around whatever the transport surfaces;
  the `context.Canceled` chain relies on net/http's documented body-read
  error mapping (returning the request context's error). The test pins it,
  so a Go stdlib behavior change would surface as a test failure, not a
  silent mis-classification — the "read stalled" message is attached based
  on our own `stalled` flag, not on error-string matching, so the wrap
  itself can't misfire.
- `singleShotRPCTimeout` (15 min base) and `singleShotFloorBytesPerSecond`
  intentionally share `multipartRPCTimeout`'s derivation but are separate
  declarations: tightening one later can't silently change the other's
  semantics (their comments cross-reference).
- Review round 1 caught the flat-deadline undersizing (fixed with the
  size-scaled deadline above); the branch was also rebased onto current
  main (post-site-redesign) so `git diff main` is clean.
- The fake's GET-body-stall path briefly re-acquires `f.mu` after the park
  purely to balance `handle`'s deferred unlock; no state is touched.
