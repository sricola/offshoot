# LangGraph community listing — DRAFT, NOT SUBMITTED

**Status: draft only.** This is prepared text for a future PR against
LangGraph's community-integrations docs, so it's ready to go the moment
`offshoot-db` is on PyPI (see [ROADMAP.md](../../ROADMAP.md)'s Milestone 3
listings bullet and [status.md](../status.md) — actual PyPI publication is
user-gated on claiming the `offshoot-db` name, per this milestone's plan).
**Nobody should open this PR yet.** When it's time: confirm LangGraph's
current community-integration contribution process (template, target repo/
directory — their docs site has moved before), swap in the real PyPI badge/
install command, and submit from there.

---

## Suggested PR title

`docs: add offshoot (branchable SQLite) to community integrations`

## Suggested PR description

> Adds `offshoot`'s `ThreadForks` to the community integrations list.
>
> `offshoot` (https://github.com/offshoot-db/offshoot) is git-like branching
> for SQLite: fork a database instantly, checkpoint it, roll back, promote a
> winning attempt — all as CAS-safe operations over a plain SQLite file, with
> a small Python/TypeScript client and a Rust-free, dependency-light Go
> daemon behind it.
>
> `offshoot.langgraph.ThreadForks` is a **companion**, not a
> `BaseCheckpointSaver` — it doesn't replace or wrap whatever checkpointer a
> graph is already compiled with. It solves a narrower, adjacent problem:
> LangGraph can rewind a *conversation* to an earlier checkpoint, but it has
> no idea a tool node wrote rows into a database along the way, so
> rewinding the conversation doesn't rewind that database. `ThreadForks`
> maps each LangGraph thread id to its own `offshoot` branch and each
> LangGraph checkpoint id to a named `offshoot` checkpoint on that branch,
> so forking/resuming a thread at an earlier point can fork the database at
> that exact same point too — the retried path starts from precisely the
> data the original attempt had there, not from whatever the first attempt
> left behind.
>
> Six lines of integration (from the runnable example):
>
> ```python
> forks = ThreadForks(client, db="agent")           # once, at startup
>
> path = forks.path(thread_id)                       # before graph.stream(...)
> # ... graph.stream(...) / graph.invoke(...) runs, tool nodes write to `path` ...
> forks.checkpoint(thread_id, checkpoint_id)          # after the step, name it
>
> # later, resuming from a historical checkpoint:
> new_path = forks.fork_thread(thread_id, checkpoint_id, new_thread_id)
> ```
>
> A full runnable example (simulated flow by default, `--real` flag runs it
> through an actual compiled `StateGraph`) lives at
> `examples/langgraph-rewind/` in the offshoot repo.

## Suggested listing entry (short form, for a table/index of integrations)

| Name | What it does | Install | Links |
|---|---|---|---|
| `offshoot` (`ThreadForks`) | Forks the SQLite database a thread's tools wrote to whenever the thread itself is rewound/forked, so a retried path starts from the exact data the original had at that checkpoint. | `pip install offshoot-db` | [Repo](https://github.com/offshoot-db/offshoot) · [Example](https://github.com/offshoot-db/offshoot/tree/main/examples/langgraph-rewind) · [Docs](https://github.com/offshoot-db/offshoot#readme) |

## Pre-submission checklist (fill in when this actually ships)

- [ ] `offshoot-db` is live on PyPI (`pip install offshoot-db` actually
      works) — this is the hard blocker; see status.md.
- [ ] Re-verify LangGraph's current contribution process for community
      integrations (template file, target repo, review owners) — do not
      assume the process described above is still current.
- [ ] Re-run `examples/langgraph-rewind/agent.py --real` against whatever
      `langgraph` version is current at submission time and update the
      pinned version note if it drifted from `1.2.10`.
- [ ] Link check every URL in this draft once PyPI + the listing PR both
      exist (this draft's links point at the intended locations, not
      confirmed-live ones).
