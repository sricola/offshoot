"""OffshootSaver: a LangGraph checkpointer whose SQLite file is an offshoot
branch — forkable, rollbackable, promotable, TTL-reaped.

Design (deliberately thin):

- ``OffshootSaver`` subclasses ``BaseCheckpointSaver`` (LangGraph
  ``isinstance``-checks the checkpointer, so subclassing is required) but
  does **no checkpoint I/O of its own**: every LangGraph-facing method
  delegates to a stock ``langgraph-checkpoint-sqlite`` ``SqliteSaver``
  pointed at an offshoot **checkout** — the writable SQLite file the
  offshoot daemon keeps under continuous capture. LangGraph's own
  battle-tested serialization does the checkpoint I/O; offshoot manages the
  file underneath it.

- The offshoot value-add is the handful of extra methods, each mapping 1:1
  to one offshoot daemon op:

  ===================  =====================================================
  saver method         offshoot op (sdk/python ``offshoot.client``)
  ===================  =====================================================
  ``checkpoint(name)``  ``Session.flush(name)`` — durable named checkpoint
  ``fork_thread(new)``  ``Client.fork`` — new branch, returns a NEW saver
  ``rollback(to)``      ``Client.rollback`` — repoint branch at checkpoint
  ``promote(onto)``     ``Client.promote`` — repoint ``onto`` at this head
  ``destroy()``         ``Client.destroy`` — delete the branch
  ===================  =====================================================

Two construction modes were designed; ONE ships:

- ``OffshootSaver.session(socket_path, db, branch)`` — **shipped.** A live
  daemon session: the daemon leases the branch, keeps the checkout under
  continuous capture, and ``checkpoint(name)``/``fork_thread``/``promote``
  flush through it. This is the correct mode: durability and branch ops go
  through one arbiter (the daemon), and every op the saver needs exists on
  the daemon's wire protocol.

- ``OffshootSaver.at_rest(store, db, branch)`` — **stubbed**
  (``NotImplementedError``). CLI-mode would checkout at rest and persist via
  ``offshoot checkpoint``, but the daemon wire protocol has no
  "commit an at-rest checkout" op (``flush`` is session-only), so a correct
  at-rest mode would have to shell out to the ``offshoot`` binary. A thin
  correct adapter beats a broad fragile one; use ``session`` mode.
"""
from __future__ import annotations

import copy
import sqlite3
from collections.abc import AsyncIterator, Collection, Iterator, Mapping, Sequence
from typing import Any, TYPE_CHECKING

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.sqlite import SqliteSaver

from offshoot.client import Client, OffshootError, Session, _TTL

if TYPE_CHECKING:
    from langgraph.checkpoint.base import (
        ChannelVersions,
        Checkpoint,
        CheckpointMetadata,
        CheckpointTuple,
        DeltaChannelHistory,
    )
    from langchain_core.runnables import RunnableConfig

__all__ = ["OffshootSaver"]


class OffshootSaver(BaseCheckpointSaver):
    """A ``BaseCheckpointSaver`` backed by an offshoot-managed SQLite file.

    Construct via :meth:`session` (or :meth:`at_rest` — currently stubbed).
    LangGraph checkpoint I/O is delegated wholesale to an inner
    ``SqliteSaver`` on the branch's checkout; the extra methods
    (:meth:`checkpoint`, :meth:`fork_thread`, :meth:`rollback`,
    :meth:`promote`, :meth:`destroy`) map 1:1 to offshoot ops — see the
    module docstring's table.

    Use as a context manager, or call :meth:`close`, to release the SQLite
    connection, the daemon session (lease), and the daemon socket.

    Threading: same stance as ``SqliteSaver`` (the SQLite connection is
    opened with ``check_same_thread=False`` and ``SqliteSaver`` serializes
    access with its own lock). The offshoot-op methods themselves are NOT
    thread-safe (the daemon client is one-request-one-response on one
    socket); call them from one thread at a time.
    """

    def __init__(self, client: Client, session: Session, db: str, branch: str,
                 socket_path: str) -> None:
        # Private constructor — use OffshootSaver.session() / .at_rest().
        super().__init__()
        self._client = client
        self._session = session
        self._db = db
        self._branch = branch
        self._socket_path = socket_path
        self._conn = sqlite3.connect(
            session.path,
            # Same stance as SqliteSaver.from_conn_string: LangGraph may
            # call the checkpointer from worker threads; SqliteSaver holds
            # its own lock around every cursor.
            check_same_thread=False,
        )
        self._inner = SqliteSaver(self._conn)
        self.serde = self._inner.serde
        self._closed = False

    # ------------------------------------------------------------------
    # Construction modes
    # ------------------------------------------------------------------

    @classmethod
    def session(cls, socket_path: str, db: str, branch: str = "main", *,
                create: bool = True) -> "OffshootSaver":
        """Open a live daemon session on ``db@branch`` and return a saver on
        its checkout.

        The daemon keeps the checkout under continuous capture; nothing is
        durable in the offshoot store until :meth:`checkpoint` (or an op
        that flushes internally — :meth:`fork_thread`, :meth:`promote`)
        runs. ``create=True`` (default) creates ``db`` first if the store
        doesn't have it; the branch itself must already exist (``main``
        exists from creation; other branches come from :meth:`fork_thread`
        or ``Client.fork``).
        """
        client = Client(socket_path)
        try:
            if create and db not in client.dbs():
                client.create(db)
            session = client.open(db, branch)
        except BaseException:
            client.close()
            raise
        return cls(client, session, db, branch, socket_path)

    @classmethod
    def at_rest(cls, store: str, db: str, branch: str = "main") -> "OffshootSaver":
        """CLI-mode (at-rest checkout + explicit ``offshoot checkpoint``).

        **Not implemented.** The daemon wire protocol has no "commit an
        at-rest checkout" op (``flush`` is session-only), so a correct
        at-rest mode would have to shell out to the ``offshoot`` binary per
        checkpoint. Use :meth:`session` — it covers the same workflows with
        one arbiter for durability and branch ops.
        """
        raise NotImplementedError(
            "OffshootSaver.at_rest is not implemented: persisting an "
            "at-rest checkout requires the `offshoot checkpoint` CLI (the "
            "daemon protocol's flush op is session-only). Use "
            "OffshootSaver.session(socket_path, db, branch) instead.")

    # ------------------------------------------------------------------
    # Offshoot value-add — each maps 1:1 to one offshoot op
    # ------------------------------------------------------------------

    @property
    def db(self) -> str:
        """The offshoot database this saver's branch belongs to."""
        return self._db

    @property
    def branch(self) -> str:
        """The offshoot branch this saver writes to."""
        return self._branch

    @property
    def path(self) -> str:
        """The writable SQLite checkout path the inner SqliteSaver uses."""
        return self._session.path

    def checkpoint(self, name: str = "", meta: dict[str, str] | None = None) -> int:
        """Flush the live checkout to a durable offshoot snapshot
        (``Session.flush``), optionally as a named checkpoint; returns the
        flush's txid. A named checkpoint is what :meth:`rollback` and
        ``fork_thread(from_checkpoint=...)`` can later target."""
        self._check_open()
        return self._session.flush(name, meta)

    def fork_thread(self, new_branch: str, *, ttl: _TTL = None,
                    from_checkpoint: str | None = None,
                    meta: dict[str, str] | None = None) -> "OffshootSaver":
        """Fork this branch (``Client.fork``) and return a NEW saver on the
        fork's own checkout, leaving this saver untouched.

        With ``from_checkpoint=None`` the fork is taken at this branch's
        head — the live checkout is flushed first so the head includes
        everything LangGraph has written up to now. With a named
        checkpoint (previously created via :meth:`checkpoint`), the fork is
        taken at exactly that state and no flush happens.

        ``ttl`` (e.g. ``"1h"`` or a ``timedelta``) makes the fork
        self-reaping — the natural default for per-attempt / per-trial
        branches nobody promotes. The returned saver owns its own daemon
        connection and session; close it (or let TTL reaping collect the
        branch) independently of this one.
        """
        self._check_open()
        if from_checkpoint is None:
            self._session.flush()
        self._client.fork(self._db, self._branch, new_branch,
                          from_checkpoint=from_checkpoint, ttl=ttl, meta=meta)
        return OffshootSaver.session(self._socket_path, self._db, new_branch,
                                     create=False)

    def rollback(self, to: str) -> None:
        """Repoint this branch at named checkpoint ``to``
        (``Client.rollback``) and reopen the saver on the restored state.

        Everything after ``to`` — including unflushed live writes — is
        discarded (checkpoints at or before ``to`` survive). The daemon
        refuses to roll back a branch with an open session, so this closes
        the session (and the SQLite connection) around the rollback and
        reopens both; the inner ``SqliteSaver`` is replaced. In-place: the
        same saver object continues to work afterwards.
        """
        self._check_open()
        self._conn.close()
        self._session.close()
        try:
            self._client.rollback(self._db, self._branch, to)
        finally:
            # Reopen even if rollback failed (e.g. unknown checkpoint):
            # the session/conn were already closed above, and the branch
            # state is unchanged on failure, so reopening restores this
            # saver to a working state either way.
            self._session = self._client.open(self._db, self._branch)
            self._conn = sqlite3.connect(self._session.path, check_same_thread=False)
            self._inner = SqliteSaver(self._conn)
            self.serde = self._inner.serde

    def promote(self, onto: str, force: bool = False) -> int:
        """Flush, then repoint branch ``onto`` at this branch's head
        (``Client.promote``); returns the promoted txid. ``force`` is
        required when ``onto`` is protected (``main`` is, by default).
        This saver keeps writing to its own branch afterwards — promote
        copies state onto ``onto``; it does not move this saver."""
        self._check_open()
        self._session.flush()
        return self._client.promote(self._db, self._branch, onto, force=force)

    def destroy(self, force: bool = True) -> None:
        """Close this saver and delete its branch (``Client.destroy``).
        ``force=True`` (default) also overrides the protected-branch
        refusal — pass ``force=False`` to keep that guard."""
        if self._closed:
            return
        self._conn.close()
        self._session.close()
        try:
            self._client.destroy(self._db, self._branch, force=force)
        finally:
            self._client.close()
            self._closed = True

    def close(self) -> None:
        """Release the SQLite connection, the daemon session (lease), and
        the daemon socket. The branch itself survives (its last flushed
        state; TTL-reaped later if it was forked with a TTL)."""
        if self._closed:
            return
        self._closed = True
        try:
            self._conn.close()
        finally:
            try:
                self._session.close()
            finally:
                self._client.close()

    def __enter__(self) -> "OffshootSaver":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise OffshootError(
                f"OffshootSaver on {self._db}@{self._branch} is closed")

    # ------------------------------------------------------------------
    # BaseCheckpointSaver: pure delegation to the inner SqliteSaver.
    # (Pinned against langgraph-checkpoint 4.2.0 / langgraph-checkpoint-
    # sqlite 3.1.1 — signatures verified by introspection; see README.)
    # ------------------------------------------------------------------

    @property
    def config_specs(self) -> list:  # type: ignore[override]
        return self._inner.config_specs

    def get(self, config: "RunnableConfig") -> "Checkpoint | None":
        return self._inner.get(config)

    def get_tuple(self, config: "RunnableConfig") -> "CheckpointTuple | None":
        return self._inner.get_tuple(config)

    def list(self, config: "RunnableConfig | None", *,
             filter: dict[str, Any] | None = None,
             before: "RunnableConfig | None" = None,
             limit: int | None = None) -> "Iterator[CheckpointTuple]":
        return self._inner.list(config, filter=filter, before=before, limit=limit)

    def put(self, config: "RunnableConfig", checkpoint: "Checkpoint",
            metadata: "CheckpointMetadata",
            new_versions: "ChannelVersions") -> "RunnableConfig":
        return self._inner.put(config, checkpoint, metadata, new_versions)

    def put_writes(self, config: "RunnableConfig",
                   writes: Sequence[tuple[str, Any]], task_id: str,
                   task_path: str = "") -> None:
        return self._inner.put_writes(config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        return self._inner.delete_thread(thread_id)

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        return self._inner.delete_for_runs(run_ids)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        return self._inner.copy_thread(source_thread_id, target_thread_id)

    def prune(self, thread_ids: Sequence[str], *,
              strategy: str = "keep_latest") -> None:
        return self._inner.prune(thread_ids, strategy=strategy)

    def get_delta_channel_history(
            self, *, config: "RunnableConfig",
            channels: Sequence[str]) -> "Mapping[str, DeltaChannelHistory]":
        return self._inner.get_delta_channel_history(config=config, channels=channels)

    def get_next_version(self, current: Any, channel: None = None) -> Any:
        return self._inner.get_next_version(current, channel)

    def with_allowlist(
            self, extra_allowlist: Collection[tuple[str, ...]]) -> "OffshootSaver":
        # Base's with_allowlist would clone self but leave the INNER
        # saver's serde (the one that actually serializes) untouched;
        # instead derive a new inner and wrap it. The clone shares this
        # saver's daemon session/connection — close whichever one you
        # like; they are the same underlying resources.
        new_inner = self._inner.with_allowlist(extra_allowlist)
        if new_inner is self._inner:
            return self
        clone = copy.copy(self)
        clone._inner = new_inner
        clone.serde = new_inner.serde
        return clone

    # Async variants: delegated as-is. SqliteSaver's async methods raise
    # NotImplementedError pointing at AsyncSqliteSaver — that behavior is
    # deliberately preserved: an async OffshootSaver (wrapping
    # AsyncSqliteSaver) is future work, and silently running sync I/O on
    # the event loop would be worse than the honest error.

    async def aget(self, config: "RunnableConfig") -> "Checkpoint | None":
        return await self._inner.aget(config)

    async def aget_tuple(self, config: "RunnableConfig") -> "CheckpointTuple | None":
        return await self._inner.aget_tuple(config)

    async def alist(self, config: "RunnableConfig | None", *,
                    filter: dict[str, Any] | None = None,
                    before: "RunnableConfig | None" = None,
                    limit: int | None = None) -> "AsyncIterator[CheckpointTuple]":
        async for item in self._inner.alist(config, filter=filter, before=before,
                                            limit=limit):
            yield item

    async def aput(self, config: "RunnableConfig", checkpoint: "Checkpoint",
                   metadata: "CheckpointMetadata",
                   new_versions: "ChannelVersions") -> "RunnableConfig":
        return await self._inner.aput(config, checkpoint, metadata, new_versions)

    async def aput_writes(self, config: "RunnableConfig",
                          writes: Sequence[tuple[str, Any]], task_id: str,
                          task_path: str = "") -> None:
        return await self._inner.aput_writes(config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        return await self._inner.adelete_thread(thread_id)

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        return await self._inner.adelete_for_runs(run_ids)

    async def acopy_thread(self, source_thread_id: str,
                           target_thread_id: str) -> None:
        return await self._inner.acopy_thread(source_thread_id, target_thread_id)

    async def aprune(self, thread_ids: Sequence[str], *,
                     strategy: str = "keep_latest") -> None:
        return await self._inner.aprune(thread_ids, strategy=strategy)

    async def aget_delta_channel_history(
            self, *, config: "RunnableConfig",
            channels: Sequence[str]) -> "Mapping[str, DeltaChannelHistory]":
        return await self._inner.aget_delta_channel_history(
            config=config, channels=channels)
