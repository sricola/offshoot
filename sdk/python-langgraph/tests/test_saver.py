"""End-to-end tests against a real offshoot daemon + real LangGraph.

Every test drives the actual stack: `go build`-built offshoot binary,
`offshoot serve` on a temp store, OffshootSaver.session() on it, and a real
compiled StateGraph (or raw BaseCheckpointSaver calls with LangGraph's own
checkpoint types). Nothing is mocked. Skips (locally) / fails (under CI /
OFFSHOOT_REQUIRE_LANGGRAPH) when langgraph isn't installed — see conftest.
"""
from __future__ import annotations

from typing import TypedDict

import pytest

# Skip the whole module (locally) when langgraph isn't installed; under
# CI / OFFSHOOT_REQUIRE_LANGGRAPH conftest.pytest_configure has already
# turned that into a hard failure before collection got this far.
pytest.importorskip("langgraph")
pytest.importorskip("langgraph.checkpoint.sqlite")

from langgraph_checkpoint_offshoot import OffshootSaver  # noqa: E402


class S(TypedDict):
    n: int


def compile_graph(saver: OffshootSaver):
    from langgraph.graph import END, START, StateGraph

    def add_one(state: S) -> dict:
        return {"n": state["n"] + 1}

    g = StateGraph(S)
    g.add_node("add_one", add_one)
    g.add_edge(START, "add_one")
    g.add_edge("add_one", END)
    return g.compile(checkpointer=saver)


def latest_n(saver: OffshootSaver, thread_id: str) -> int:
    tup = saver.get_tuple({"configurable": {"thread_id": thread_id}})
    assert tup is not None
    return tup.checkpoint["channel_values"]["n"]


def checkpoint_count(saver: OffshootSaver, thread_id: str) -> int:
    return len(list(saver.list({"configurable": {"thread_id": thread_id}})))


def test_raw_put_get_tuple_round_trip(daemon):
    """The BaseCheckpointSaver contract itself, with LangGraph's own types:
    put a real (empty) Checkpoint, read it back via get_tuple and list."""
    from langgraph.checkpoint.base import empty_checkpoint

    with OffshootSaver.session(daemon.sock, "raw") as saver:
        config = {"configurable": {"thread_id": "t-raw", "checkpoint_ns": ""}}
        cp = empty_checkpoint()
        metadata = {"source": "input", "step": -1, "parents": {}}
        saved = saver.put(config, cp, metadata, {})
        assert saved["configurable"]["checkpoint_id"] == cp["id"]

        tup = saver.get_tuple(saved)
        assert tup is not None
        assert tup.checkpoint["id"] == cp["id"]
        assert tup.metadata["source"] == "input"

        tups = list(saver.list({"configurable": {"thread_id": "t-raw"}}))
        assert [t.checkpoint["id"] for t in tups] == [cp["id"]]


def test_graph_round_trip_and_survives_checkpoint_reopen(daemon):
    """A real StateGraph runs against the saver; a named offshoot
    checkpoint persists the state so a fresh session sees it."""
    with OffshootSaver.session(daemon.sock, "rt") as saver:
        app = compile_graph(saver)
        config = {"configurable": {"thread_id": "t1"}}
        out = app.invoke({"n": 0}, config)
        assert out == {"n": 1}
        assert latest_n(saver, "t1") == 1
        saver.checkpoint("after-first-run")

    # A brand-new session on the same branch reads the flushed state back.
    with OffshootSaver.session(daemon.sock, "rt", create=False) as saver2:
        assert latest_n(saver2, "t1") == 1
        app2 = compile_graph(saver2)
        out2 = app2.invoke({"n": out["n"]}, {"configurable": {"thread_id": "t1"}})
        assert out2 == {"n": 2}


def test_fork_thread_isolates_parent_from_child(daemon):
    """fork_thread returns a saver on its own branch; the child's writes
    never leak into the parent's state (and vice versa)."""
    parent = OffshootSaver.session(daemon.sock, "forkdb")
    try:
        app = compile_graph(parent)
        config = {"configurable": {"thread_id": "t1"}}
        app.invoke({"n": 0}, config)
        assert latest_n(parent, "t1") == 1
        parent_count = checkpoint_count(parent, "t1")

        child = parent.fork_thread("attempt-1", ttl="1h")
        try:
            assert child.branch == "attempt-1"
            assert child.db == "forkdb"
            assert child.path != parent.path
            # The fork starts from the parent's state...
            assert latest_n(child, "t1") == 1

            # ...and diverges without touching the parent.
            child_app = compile_graph(child)
            child_app.invoke({"n": 1}, config)
            child_app.invoke({"n": 2}, config)
            assert latest_n(child, "t1") == 3
            assert latest_n(parent, "t1") == 1
            assert checkpoint_count(parent, "t1") == parent_count
            assert checkpoint_count(child, "t1") > parent_count

            # The fork carries its TTL (self-reaping attempt branch).
            import offshoot
            with offshoot.connect(daemon.sock) as c:
                info = {b.branch: b for b in c.branches("forkdb")}
            assert info["attempt-1"].ttl == "1h0m0s"
        finally:
            child.destroy()
    finally:
        parent.close()


def test_rollback_restores_named_checkpoint(daemon):
    """rollback(to) discards everything after the named checkpoint and the
    same saver object keeps working on the restored state."""
    with OffshootSaver.session(daemon.sock, "rbdb") as saver:
        app = compile_graph(saver)
        config = {"configurable": {"thread_id": "t1"}}
        app.invoke({"n": 0}, config)
        assert latest_n(saver, "t1") == 1
        saver.checkpoint("good")

        app.invoke({"n": 1}, config)
        app.invoke({"n": 2}, config)
        assert latest_n(saver, "t1") == 3

        saver.rollback("good")
        assert latest_n(saver, "t1") == 1

        # The saver still works after the in-place reopen: run again.
        app2 = compile_graph(saver)
        assert app2.invoke({"n": 1}, config) == {"n": 2}


def test_promote_copies_winner_onto_target(daemon):
    """Eval-loop ending: the winning attempt's state is promoted onto main."""
    parent = OffshootSaver.session(daemon.sock, "promdb")
    app = compile_graph(parent)
    config = {"configurable": {"thread_id": "t1"}}
    app.invoke({"n": 0}, config)

    winner = parent.fork_thread("attempt-win", ttl="1h")
    compile_graph(winner).invoke({"n": 1}, config)
    assert latest_n(winner, "t1") == 2

    # main's session must be closed before promote repoints its checkout
    # (same rule as rollback/compact: the session owns the checkout).
    parent.close()
    winner.promote("main", force=True)  # main is protected by default
    winner.destroy()

    with OffshootSaver.session(daemon.sock, "promdb", create=False) as after:
        assert latest_n(after, "t1") == 2


def test_destroy_deletes_branch(daemon):
    parent = OffshootSaver.session(daemon.sock, "destroydb")
    child = parent.fork_thread("doomed")
    child.destroy()
    import offshoot
    with offshoot.connect(daemon.sock) as c:
        names = {b.branch for b in c.branches("destroydb")}
    assert "doomed" not in names
    parent.close()
    # Idempotent / closed-saver guard.
    from offshoot.client import OffshootError
    with pytest.raises(OffshootError):
        parent.checkpoint("nope")


def test_at_rest_is_stubbed():
    with pytest.raises(NotImplementedError):
        OffshootSaver.at_rest("/tmp/store", "db")
