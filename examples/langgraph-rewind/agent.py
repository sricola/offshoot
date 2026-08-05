#!/usr/bin/env python3
"""LangGraph can rewind the conversation; it can't rewind the database the
agent wrote to — this example rewinds both.

Simulates a LangGraph-style agent loop: an agent thread writes orders into a
sqlite database across three turns, checkpointing after each one, then a
user asks to rewind to turn 1 and try something else. That rewind forks the
thread's *database*, not just its conversation history, so the retried turn
starts from exactly the turn-1 world and the two paths' data actually
diverges. Both final world states are printed.

Needs only the offshoot Python SDK (../../sdk/python) and the standard
library — no LangGraph import, by default. Comments below mark exactly
where a real `graph.stream(...)` / `graph.invoke(...)` call would sit if
this were wired into an actual LangGraph agent.

With --real, if `langgraph` is importable (`pip install langgraph`), the
same flow runs through an actual compiled `StateGraph` with a sqlite-writing
tool node, instead of the plain-Python stand-in.

Usage (see README.md in this directory for the full copy-paste recipe):
    python3 agent.py
    python3 agent.py --real
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "sdk" / "python"))

import offshoot  # noqa: E402  (path must be set up first)
from offshoot.langgraph import ThreadForks  # noqa: E402


class _TempDaemon:
    """Builds offshoot and starts a private daemon on a fresh temp store, so
    this example is runnable standalone with no daemon already running."""

    def __init__(self):
        self.dir = Path(tempfile.mkdtemp(prefix="offshoot-langgraph-demo-"))
        self.bin = self.dir / "offshoot"
        subprocess.run(["go", "build", "-o", str(self.bin), "./cmd/offshoot"],
                        cwd=REPO, check=True)
        self.store = self.dir / "store"
        subprocess.run([str(self.bin), "-store", str(self.store), "init"],
                        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.sock = str(self.dir / "d.sock")
        self.proc = subprocess.Popen(
            [str(self.bin), "-store", str(self.store), "serve", "-socket", self.sock],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 10
        while not os.path.exists(self.sock):
            if time.time() > deadline:
                raise RuntimeError("daemon did not start: " +
                                    self.proc.stderr.peek().decode(errors="replace"))
            if self.proc.poll() is not None:
                raise RuntimeError(self.proc.stderr.read().decode(errors="replace"))
            time.sleep(0.05)

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.proc.stderr.close()
        shutil.rmtree(self.dir, ignore_errors=True)


def write_order(db_path: str, item: str) -> None:
    """What a LangGraph tool node's body would do: write to the thread's
    live db. In a real graph this runs inside a node function; here it's
    called directly to stand in for the `graph.stream(...)` step."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, item TEXT)")
        conn.execute("INSERT INTO orders (item) VALUES (?)", (item,))
        conn.commit()
    finally:
        conn.close()


def print_world(label: str, db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT item FROM orders ORDER BY id").fetchall()
    finally:
        conn.close()
    print(f"{label}: {[r[0] for r in rows]}")


def run_simulated(client, db: str) -> None:
    forks = ThreadForks(client, db)
    thread = f"conv-{uuid.uuid4()}"
    try:
        # --- turn 1 ---
        path = forks.path(thread)                    # graph.stream(...) would run here
        write_order(path, "widget")                   # the tool node's write
        forks.checkpoint(thread, "turn-1")             # after the step: name the checkpoint

        # --- turn 2 ---
        write_order(path, "gadget")                    # graph.stream(...) would run here
        forks.checkpoint(thread, "turn-2")

        # --- turn 3: a mistake the user wants to undo ---
        write_order(path, "gizmo-oops")                 # graph.stream(...) would run here
        forks.checkpoint(thread, "turn-3")

        print_world("original thread, after 3 turns", path)

        # --- the user rewinds to turn 1 and retries with a different action ---
        retry_thread = f"{thread}-retry"
        retry_path = forks.fork_thread(thread, "turn-1", retry_thread)
        write_order(retry_path, "sprocket")             # graph.stream(...) would run here
        forks.checkpoint(retry_thread, "turn-2")

        print_world("rewound thread, after retrying turn 2", retry_path)
    finally:
        forks.close()


def run_real(client, db: str) -> None:
    from typing import TypedDict
    from langgraph.graph import StateGraph, START, END

    class State(TypedDict):
        db_path: str
        item: str

    def write_order_node(state: State) -> State:
        write_order(state["db_path"], state["item"])
        return state

    g = StateGraph(State)
    g.add_node("write_order", write_order_node)
    g.add_edge(START, "write_order")
    g.add_edge("write_order", END)
    graph = g.compile()

    forks = ThreadForks(client, db)
    thread = f"conv-{uuid.uuid4()}"
    try:
        path = forks.path(thread)
        graph.invoke({"db_path": path, "item": "widget"})
        forks.checkpoint(thread, "turn-1")

        graph.invoke({"db_path": path, "item": "gadget"})
        forks.checkpoint(thread, "turn-2")

        graph.invoke({"db_path": path, "item": "gizmo-oops"})
        forks.checkpoint(thread, "turn-3")

        print_world("original thread, after 3 turns", path)

        retry_thread = f"{thread}-retry"
        retry_path = forks.fork_thread(thread, "turn-1", retry_thread)
        graph.invoke({"db_path": retry_path, "item": "sprocket"})
        forks.checkpoint(retry_thread, "turn-2")

        print_world("rewound thread, after retrying turn 2", retry_path)
    finally:
        forks.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--real", action="store_true",
        help="run the flow through an actual LangGraph StateGraph "
             "(requires `pip install langgraph`)")
    parser.add_argument(
        "--socket", default=None,
        help="unix socket of an already-running offshoot daemon "
             "(default: build offshoot and start a private one)")
    parser.add_argument(
        "--db", default=f"langgraph-demo-{uuid.uuid4().hex[:8]}",
        help="database name to create for this run (default: a fresh random name)")
    args = parser.parse_args()

    temp_daemon = None
    if args.socket:
        client = offshoot.connect(args.socket)
    else:
        temp_daemon = _TempDaemon()
        client = offshoot.connect(temp_daemon.sock)

    try:
        client.create(args.db)
        if args.real:
            try:
                import langgraph  # noqa: F401
            except ImportError as e:
                print(f"--real requires `pip install langgraph`: {e}", file=sys.stderr)
                return 1
            run_real(client, args.db)
        else:
            run_simulated(client, args.db)
    finally:
        client.close()
        if temp_daemon is not None:
            temp_daemon.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
