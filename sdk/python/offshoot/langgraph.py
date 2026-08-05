"""A companion for LangGraph agents: fork the database when the thread forks.

LangGraph can rewind a conversation to an earlier checkpoint and resume from
there. It has no idea the agent's tool calls wrote rows into a database along
the way — rewinding the conversation doesn't rewind that database. This
module maps each LangGraph thread id to its own offshoot branch, so that
rewinding a thread (forking it, in LangGraph's own vocabulary) can fork the
database it wrote to at exactly the point the conversation is rewound to.

This is a **companion**, not a ``BaseCheckpointSaver`` subclass, and it does
not persist LangGraph's own checkpoint state (messages, node outputs, etc.) —
that's still LangGraph's job, via whatever checkpointer the graph is
compiled with. :class:`ThreadForks` only tracks *offshoot* branches, keyed by
the same thread and checkpoint ids the agent is already using. There is no
upstream LangGraph integration here (no registered checkpointer, no node
wrapper) — call ``checkpoint()`` and ``fork_thread()`` yourself at the right
points in your own loop; see ``examples/langgraph-rewind`` for the six-line
version.

Design rules:

1. **One offshoot branch per thread.** :meth:`ThreadForks.path` maps a
   thread id to a branch named ``prefix + sanitized(thread_id)``, forking it
   from ``base_branch`` the first time that thread is seen. Every such
   branch is forked with a TTL (default 24h) so abandoned threads reap
   themselves — a thread a user opens and never returns to, or a forked
   "what if" attempt nobody promotes, is exactly the workload TTLs exist
   for. Reused (already-forked) threads are detected by branch name and are
   not re-forked.

2. **Checkpoints are named after LangGraph's own checkpoint ids.**
   :meth:`ThreadForks.checkpoint` flushes the thread's session under a name
   derived from the caller-supplied ``checkpoint_id`` — the same id
   LangGraph hands back for a step in the thread's history. That means
   :meth:`ThreadForks.fork_thread` can later fork the database at exactly
   the state the conversation is being rewound to, by naming the same
   checkpoint id as the fork point.

3. **Deterministic sanitization, not validation.** Thread ids and checkpoint
   ids come from LangGraph (or an app's own id scheme) and may contain
   characters ``store.ValidateName`` rejects — LangGraph thread ids are
   commonly UUIDs (fine as-is) but nothing stops an app from using request
   ids, emails, or arbitrary strings that fall well outside
   ``[a-z0-9-_.]``. Every id run through this module is lowercased, has any
   run of characters outside ``[a-z0-9-]`` collapsed to a single ``-``, is
   truncated to a bounded length, and always has a short deterministic hash
   of the *original, untruncated* id appended. The hash suffix is what keeps
   two different ids from colliding on the same sanitized name — whether
   they collide because sanitization maps them to the same characters (e.g.
   two ids that differ only in punctuation) or because truncation would
   otherwise cut them down to the same prefix. Same input always produces
   the same output; sanitization is not validation, and does not attempt to
   preserve readability beyond a best-effort prefix.

4. **Errors name the missing piece.** An unknown checkpoint id passed to
   ``fork_thread`` (never named via ``checkpoint()``) raises a clear
   :class:`~offshoot.client.OffshootError`, not a raw daemon message about a
   branch name the caller never chose.

Precondition: the underlying db (the ``db`` argument to ``ThreadForks``)
must already exist — create it once with ``client.create(db)`` before
constructing a :class:`ThreadForks` over it, the same way any other offshoot
db is created. ``base_branch`` (default ``"main"``) must exist on it too;
``client.create`` makes that branch automatically.
"""
from __future__ import annotations

import hashlib
import re

from .client import Client, OffshootError, Session

__all__ = ["ThreadForks"]

# Anything outside this set gets collapsed to a single '-' by _sanitize.
_UNSAFE_RUN = re.compile(r"[^a-z0-9-]+")

# Keep sanitized bodies short; the hash suffix (not the body) is what
# guarantees distinctness, so the body only needs to be a readable hint.
_MAX_BODY = 40
_HASH_LEN = 8


def _sanitize(raw) -> str:
    """Map an arbitrary id to a `store.ValidateName`-safe, [a-z0-9-] string.

    Deterministic: the same raw id always sanitizes to the same output. A
    short hash of the full, untruncated raw id is always appended, so two
    ids that collide after lowercasing/collapsing/truncation still produce
    distinct names.
    """
    s = str(raw)
    lowered = s.lower()
    body = _UNSAFE_RUN.sub("-", lowered).strip("-")
    if not body:
        body = "id"
    body = body[:_MAX_BODY].strip("-") or "id"
    digest = hashlib.sha256(s.encode("utf-8")).hexdigest()[:_HASH_LEN]
    return f"{body}-{digest}"


class ThreadForks:
    """Maps agent thread ids to offshoot branches so rewinding a thread can
    rewind the database it wrote to.

    See the module docstring for the design rules this class follows.
    """

    def __init__(self, client: Client, db: str, base_branch: str = "main",
                 ttl="24h", prefix: str = "thread-"):
        self._client = client
        self._db = db
        self._base_branch = base_branch
        self._ttl = ttl
        self._prefix = prefix
        self._sessions: dict[object, Session] = {}

    def branch_for(self, thread_id) -> str:
        """The deterministic, sanitized offshoot branch name for thread_id."""
        return self._prefix + _sanitize(thread_id)

    def _branch_names(self) -> set[str]:
        return {b.branch for b in self._client.branches(self._db)}

    def _session(self, thread_id) -> Session:
        session = self._sessions.get(thread_id)
        if session is not None:
            return session
        branch = self.branch_for(thread_id)
        if branch not in self._branch_names():
            self._client.fork(self._db, self._base_branch, branch, ttl=self._ttl)
        session = self._client.open(self._db, branch)
        self._sessions[thread_id] = session
        return session

    def path(self, thread_id) -> str:
        """Open (forking from base_branch if this thread is new) and return
        the writable sqlite path for thread_id."""
        return self._session(thread_id).path

    def checkpoint(self, thread_id, checkpoint_id) -> int:
        """Flush thread_id's session to a checkpoint named after
        checkpoint_id; returns the flush's txid."""
        name = _sanitize(checkpoint_id)
        return self._session(thread_id).flush(name=name)

    def fork_thread(self, from_thread, at_checkpoint, new_thread) -> str:
        """Fork new_thread's database off from_thread's, at at_checkpoint.

        at_checkpoint must have previously been named via
        :meth:`checkpoint(from_thread, at_checkpoint) <checkpoint>`. Returns
        new_thread's writable sqlite path.
        """
        from_branch = self.branch_for(from_thread)
        new_branch = self.branch_for(new_thread)
        # One lookup up front distinguishes the three ways this can fail so
        # the error names the actual missing/colliding piece, instead of
        # guessing "must be a missing checkpoint" for any OffshootError that
        # client.fork() happens to raise (that guess is wrong, and
        # misleading, when new_thread's branch already exists — e.g. a
        # retried resume with the same new_thread id — since the daemon's
        # real "compare-and-swap conflict: key exists" gets buried behind
        # advice to re-checkpoint, which can never fix a name collision).
        info = {b.branch: b for b in self._client.branches(self._db)}
        if from_branch not in info:
            raise OffshootError(
                f"fork_thread: unknown thread {from_thread!r} (no branch "
                f"{from_branch!r} on db {self._db!r}); call "
                f"path({from_thread!r}) before forking from it")
        if new_branch in info:
            raise OffshootError(
                f"fork_thread: thread {new_thread!r} already has a branch "
                f"({new_branch!r} on db {self._db!r}); close it and destroy "
                f"the branch first, or pick a different new_thread id")
        ckpt_name = _sanitize(at_checkpoint)
        if ckpt_name not in info[from_branch].checkpoints:
            raise OffshootError(
                f"fork_thread: checkpoint {at_checkpoint!r} was never "
                f"recorded on thread {from_thread!r} (looked for checkpoint "
                f"{ckpt_name!r} on branch {from_branch!r}); call "
                f"checkpoint({from_thread!r}, {at_checkpoint!r}) before "
                f"forking from it")
        # Anything client.fork() itself raises past this point (e.g. a
        # racing creator winning the compare-and-swap on new_branch between
        # the check above and here) passes through with the daemon's own
        # message intact, unembellished — this code has nothing more
        # specific to say about it.
        self._client.fork(self._db, from_branch, new_branch,
                           from_checkpoint=ckpt_name, ttl=self._ttl)
        return self._session(new_thread).path

    def close(self, thread_id=None) -> None:
        """Close thread_id's session, or every open session if thread_id is
        None."""
        if thread_id is None:
            for tid in list(self._sessions):
                self._sessions.pop(tid).close()
            return
        session = self._sessions.pop(thread_id, None)
        if session is not None:
            session.close()
