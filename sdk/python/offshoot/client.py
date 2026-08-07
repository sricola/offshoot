"""Thin client for the offshoot daemon's lifecycle API.

Wire protocol: newline-delimited JSON over a unix socket; one request, one
response, no pipelining (matches internal/daemon/protocol.go).
Stdlib only — no dependencies.
"""
from __future__ import annotations

import json
import socket
from dataclasses import dataclass, field
from datetime import timedelta


class OffshootError(Exception):
    """An error returned by the daemon."""


@dataclass
class CheckpointInfo:
    """One checkpoint's entry within :attr:`Branch.checkpoints_v2`.

    Mirrors ``internal/daemon/protocol.go``'s ``CheckpointInfo``.
    ``created_at`` is RFC3339 UTC, empty for a checkpoint that predates
    per-checkpoint timestamps.
    """

    name: str
    txid: int
    created_at: str = ""


@dataclass
class Branch:
    """One branch of one db, as returned by :meth:`Client.branches`.

    Mirrors ``internal/daemon/protocol.go``'s ``BranchInfo``. ``ttl`` is the
    canonical ``time.Duration.String()`` re-render (e.g. a fork requested
    with ttl "1h" reads back here as "1h0m0s") — safe to echo straight back
    into a future :meth:`Client.fork`/:meth:`Client.touch` call.

    ``checkpoints`` (bare names) and ``checkpoints_v2`` (name/txid/
    created_at) describe the exact same set of checkpoints — ``checkpoints``
    stays for wire/API compat with code written before ``checkpoints_v2``
    existed; new code should prefer ``checkpoints_v2``.
    """

    branch: str
    head_txid: int
    protected: bool = False
    ttl: str = ""
    ttl_remaining: str = ""
    lease_holder: str = ""
    checkpoints: list[str] = field(default_factory=list)
    touched_at: str = ""
    checkpoints_v2: list[CheckpointInfo] = field(default_factory=list)


def _ttl_str(ttl) -> str:
    """Render a Python TTL value into the wire's Go-duration-string form.

    None means "no change" (fork: no TTL; touch: keep the current TTL); 0 or
    the literal string "none" clears a TTL (touch only); a timedelta or a Go
    duration string like "2h" sets it. A nonzero int is rejected — it's
    ambiguous (seconds? a bare number the daemon won't parse?) — pass a
    timedelta or a Go duration string like "3600s" instead.
    """
    if ttl is None:
        return ""
    # timedelta(0) == 0 is False in Python (timedelta only compares equal to
    # another timedelta), so this must be checked before the "ttl == 0"
    # literal-zero check below, or a caller passing timedelta(0) to mean
    # "clear the TTL" (per this function's own docstring) would instead fall
    # through to the timedelta branch and render "0s" — a TTL of zero
    # seconds, not "no TTL".
    if isinstance(ttl, timedelta):
        return "none" if ttl == timedelta(0) else f"{int(ttl.total_seconds())}s"
    if ttl == 0 or ttl == "none":
        return "none"
    if isinstance(ttl, int) and not isinstance(ttl, bool):
        raise TypeError(
            f"ttl={ttl!r}: a nonzero int is ambiguous; use a timedelta or a "
            'Go duration string (e.g. "1h", "3600s")')
    return str(ttl)  # a Go duration string like "2h"


def connect(socket_path: str) -> "Client":
    """Open a connection to the offshoot daemon listening on socket_path."""
    return Client(socket_path)


class Client:
    """A connection to one offshoot daemon.

    Use as a context manager, or call :meth:`close` explicitly, to release
    the underlying unix socket.

    Not thread-safe: the wire protocol is one request, one response, no
    pipelining, and this class keeps no lock around its socket and read
    buffer, so concurrent calls from multiple threads can interleave and
    corrupt each other's request/response framing. Use one Client per
    thread.
    """

    def __init__(self, socket_path: str):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self._sock.connect(socket_path)
            self._rfile = self._sock.makefile("rb")
        except OSError:
            self._sock.close()
            raise

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def _call(self, op: str, **fields) -> dict:
        req = {"op": op, **{k: v for k, v in fields.items() if v not in ("", None, False)}}
        try:
            self._sock.sendall(json.dumps(req).encode() + b"\n")
            line = self._rfile.readline()
        except OSError as e:
            raise OffshootError(f"daemon connection failed: {e}") from e
        if not line:
            raise OffshootError("daemon closed the connection")
        try:
            resp = json.loads(line)
        except json.JSONDecodeError as e:
            raise OffshootError(f"daemon sent a malformed response: {e}") from e
        if not resp.get("ok", False):
            raise OffshootError(resp.get("error", "unknown daemon error"))
        return resp

    def create(self, db: str) -> None:
        """Create a fresh db (branch main at txid 1)."""
        self._call("create", db=db)

    def open(self, db: str, branch: str = "main") -> "Session":
        """Open a live session on db@branch; returns its Session."""
        resp = self._call("open", db=db, branch=branch)
        return Session(self, resp["checkout"], db, branch)

    def checkout(self, db: str, branch: str) -> str:
        """Materialize db@branch's head snapshot at rest; returns its path."""
        resp = self._call("checkout", db=db, branch=branch)
        return resp["checkout"]

    def fork(self, db: str, source: str, new: str, from_checkpoint: str | None = None,
              ttl=None, meta: dict[str, str] | None = None) -> int:
        """Branch `new` off db@source (at from_checkpoint, or source's head).

        meta (None = no metadata) is a small string->string map describing
        the new branch's lineage (e.g. eval run id, git SHA, agent id),
        capped server-side (ops.ValidateMeta: at most 32 keys, keys <= 64
        bytes, values <= 512 bytes) and stored on the new branch's ref.

        Returns the fork point's txid.
        """
        resp = self._call("fork", db=db, branch=source, name=new, ttl=_ttl_str(ttl),
                            meta=meta or None, **{"from": from_checkpoint or ""})
        return resp.get("txid", 0)

    def destroy(self, db: str, branch: str, force: bool = False) -> None:
        """Delete db@branch. force overrides the protected-branch refusal."""
        self._call("destroy", db=db, branch=branch, force=force)

    def rollback(self, db: str, branch: str, to: str) -> str:
        """Repoint db@branch at checkpoint `to`; returns the refreshed checkout path."""
        resp = self._call("rollback", db=db, branch=branch, name=to)
        return resp.get("checkout", "")

    def promote(self, db: str, source: str, onto: str, force: bool = False) -> int:
        """Repoint db@onto at db@source's head; returns the promoted txid."""
        resp = self._call("promote", db=db, branch=source, name=onto, force=force)
        return resp.get("txid", 0)

    def touch(self, db: str, branch: str, ttl=None) -> None:
        """Reset db@branch's activity clock, optionally setting/clearing its TTL.

        ttl: None keeps the current TTL, a timedelta/duration-string sets it,
        0 or "none" clears it.
        """
        self._call("touch", db=db, branch=branch, ttl=_ttl_str(ttl))

    def branches(self, db: str) -> list[Branch]:
        """List every branch of db."""
        resp = self._call("branches", db=db)
        return [
            Branch(
                branch=b["branch"],
                head_txid=b.get("head_txid", 0),
                protected=b.get("protected", False),
                ttl=b.get("ttl", ""),
                ttl_remaining=b.get("ttl_remaining", ""),
                lease_holder=b.get("lease_holder", ""),
                checkpoints=b.get("checkpoints", []),
                touched_at=b.get("touched_at", ""),
                checkpoints_v2=[
                    CheckpointInfo(
                        name=cp["name"],
                        txid=cp.get("txid", 0),
                        created_at=cp.get("created_at", ""),
                    )
                    for cp in b.get("checkpoints_v2", [])
                ],
            )
            for b in resp.get("branches", [])
        ]

    def dbs(self) -> list[str]:
        """List every database this store has at least one ref for, sorted."""
        resp = self._call("dbs")
        return resp.get("databases", [])

    def export(self, db: str, branch: str, out_path: str,
               checkpoint: str | None = None, force: bool = False) -> None:
        """Materialize db@branch's state at checkpoint (None = head) to a
        plain SQLite file at out_path, server-side, on the daemon's own
        host/filesystem — out_path must be an ABSOLUTE path (same-host/
        same-user unix-socket trust model; see
        internal/daemon/protocol.go's ``Request.Path``). Refuses to
        overwrite an existing out_path unless force=True. No sidecar, no
        lease — out_path has no ongoing relationship to the store.

        This reads the branch's last DURABLE state from the store, never a
        live session's checkout: if a session on db@branch has unflushed
        writes, they are NOT in the export. Flush (or checkpoint) first if
        you need them included.
        """
        self._call("export", db=db, branch=branch, name=checkpoint or "",
                    path=out_path, force=force)

    def checkout_at(self, db: str, branch: str, checkpoint: str, force: bool = False) -> str:
        """Materialize db@branch's state at checkpoint into a dedicated
        read-only cache file, distinct from (and never touching) the
        branch's writable checkout — safe to call alongside an open
        session on the same branch. Repeat calls with force=False return
        the cached path as-is (a checkpoint's content is immutable, so no
        store access is needed); force=True re-materializes.

        Returns the read-only cache file's path.
        """
        resp = self._call("checkout-at", db=db, branch=branch, name=checkpoint, force=force)
        return resp.get("checkout", "")

    def status(self) -> list[dict]:
        """List every session open in the daemon, as raw dicts."""
        resp = self._call("status")
        return resp.get("sessions", [])

    def close(self) -> None:
        """Close the connection to the daemon."""
        self._rfile.close()
        self._sock.close()


class Session:
    """A live daemon session: a lease plus a checkout under continuous capture."""

    def __init__(self, client: Client, path: str, db: str, branch: str):
        self._client, self.path, self._db, self._branch = client, path, db, branch

    def flush(self, name: str = "", meta: dict[str, str] | None = None) -> int:
        """Flush the checkout to a durable snapshot; returns its txid.

        meta (None = no metadata) is only meaningful alongside a non-empty
        name: it is stored on the resulting named checkpoint (capped
        server-side, same limits as :meth:`Client.fork`'s meta). Passing
        meta with an empty name is rejected by the daemon — there is no
        checkpoint for it to attach to.
        """
        resp = self._client._call("flush", db=self._db, branch=self._branch, name=name,
                                    meta=meta or None)
        return resp.get("txid", 0)

    def close(self) -> None:
        """Close the session, releasing its lease."""
        self._client._call("close", db=self._db, branch=self._branch)
