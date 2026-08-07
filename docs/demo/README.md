# Launch demo assets

Two demos of offshoot's pitch — fork before risky work, keep only what
passes — in the two forms the launch plan's phase-3 requirement asked for: a
real recording of the CLI demo, and a real transcript of the MCP-in-an-agent
story that can't be screen-recorded.

## What's here

| File | What it is |
| --- | --- |
| [`parallel-attempts.cast`](parallel-attempts.cast) | asciicast v3 recording of [`examples/parallel-attempts/run.sh`](../../examples/parallel-attempts/run.sh) running end to end: fork a database three ways, run three different migrations against the forks, promote the one that's actually correct, discard the other two. 100x30, idle time capped at 2s at record time so dead air doesn't drag playback. |
| [`parallel-attempts.txt`](parallel-attempts.txt) | Plain-text transcript of the exact same recording (`asciinema convert -f txt`, not a separate run) — for embedding in the repo README where a `.cast` player isn't available. |
| [`mcp-walkthrough.md`](mcp-walkthrough.md) | The MCP-in-Claude-Code story: `claude mcp add` setup, then a full transcript of an agent session — fork before a risky migration, roll back when tests are red, checkpoint and promote when they're green — with the real JSON-RPC and real SQL captured by driving `offshoot mcp` over stdio myself. Every tool call and output in it is real; the "agent" narrating between calls is illustrative (clearly labeled inline). |
| [`mcp-session-driver.sh`](mcp-session-driver.sh) | The script that produced `mcp-walkthrough.md`'s transcript. Re-runnable: builds `offshoot`, spins up a throwaway store, and drives the real MCP subprocess end to end. |

## Re-recording

### `parallel-attempts.cast` / `.txt`

```
asciinema rec docs/demo/parallel-attempts.cast \
  --command "bash examples/parallel-attempts/run.sh" \
  --idle-time-limit 2 \
  --window-size 100x30 \
  --headless \
  --overwrite \
  --return \
  -t "offshoot: parallel migration attempts on forks"

asciinema convert docs/demo/parallel-attempts.cast docs/demo/parallel-attempts.txt --overwrite
```

`--headless` records without needing a real attached terminal (works over
SSH / in CI / from an agent's own shell); drop it if you're recording
interactively at a real terminal and want to watch it happen live. The demo
itself (`run.sh`) is self-contained — it builds `offshoot` fresh, creates its
own temp store, and cleans up after itself — so re-recording needs nothing
but `go`, `sqlite3`, and `asciinema` on `PATH`.

To play it back locally: `asciinema play docs/demo/parallel-attempts.cast`
(needs a real terminal — `Device not configured` if stdout isn't a tty, e.g.
piped through another tool).

### `mcp-walkthrough.md`

```
docs/demo/mcp-session-driver.sh
```

Prints a `LOGFILE=` path when it's done; that log is the literal source the
walkthrough's JSON-RPC and SQL blocks were copied from. Needs `go` and
`sqlite3` on `PATH`. Re-run it (it was run three times against three
independent fresh stores while writing the walkthrough, byte-identical each
time modulo the random store path) if you want to confirm the transcript
still matches current behavior before re-publishing the doc.

## Where these are meant to be embedded

- **Repo README** (`README.md`, root): `parallel-attempts.txt` inline as a
  collapsible `<details>` block near the top (the pitch, shown running), plus
  a link to `parallel-attempts.cast` for anyone who wants the real timing —
  GitHub doesn't render `.cast` files natively, so either link to an
  asciinema.org upload of it or point at the file for local playback via
  `asciinema play`. `mcp-walkthrough.md` linked from the MCP section
  (`README.md`'s existing `claude mcp add` mention) as "see it work end to
  end."
- **Launch site** (`site/`): `parallel-attempts.cast` embedded live via
  asciinema's JS player (asciinema.org upload or self-hosted player pointed
  at the `.cast` file) as the above-the-fold demo; `mcp-walkthrough.md`
  rendered as its own page linked from wherever the site pitches agent/MCP
  usage.

## Honesty notes

- Every byte of terminal output in `parallel-attempts.cast`/`.txt` is from a
  real run of `run.sh` — no doctored timings, no edited output. `attempt-1`
  and `attempt-2` genuinely fail their own checks; `attempt-3` genuinely
  passes and is what gets promoted.
- Every JSON-RPC request/response and every `sqlite3` command/output in
  `mcp-walkthrough.md` is real, captured by driving a real `offshoot mcp`
  subprocess over stdio against a real store — see that doc's own labeling
  section for exactly what's real vs. narrated.
- `asciinema` was already installed (`/opt/homebrew/bin/asciinema`,
  v3.2.1) in the environment this was produced in; no install step was
  needed. If it's ever missing, `brew install asciinema` (macOS) is the
  documented path — see asciinema's own docs for other platforms.
