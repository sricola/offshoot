"""Delegation-drift tripwire for OffshootSaver's hand-written surface.

OffshootSaver delegates every LangGraph-facing method to an inner
SqliteSaver by hand (saver.py's "pure delegation" block), pinned against
langgraph-checkpoint 4.2 / langgraph-checkpoint-sqlite 3.1 by out-of-band
introspection. Nothing else notices when a future langgraph-checkpoint
(or -sqlite) release ADDS a public method: the subclass would silently
inherit the base-class implementation, bypassing the inner saver that
actually holds the data.

This test recomputes the installed packages' public surface at run time
and asserts every method is explicitly overridden on OffshootSaver
(present in OffshootSaver's own __dict__, not inherited), so an upgrade
that grows the surface fails HERE, loudly, instead of corrupting
delegation silently. Same skip-locally/fail-under-CI discipline as the
rest of the suite (see conftest).
"""
from __future__ import annotations

import pytest

# Skip the whole module (locally) when langgraph isn't installed; under
# CI / OFFSHOOT_REQUIRE_LANGGRAPH conftest.pytest_configure has already
# turned that into a hard failure before collection got this far.
pytest.importorskip("langgraph")
pytest.importorskip("langgraph.checkpoint.sqlite")

from langgraph.checkpoint.base import BaseCheckpointSaver  # noqa: E402
from langgraph.checkpoint.sqlite import SqliteSaver  # noqa: E402

from langgraph_checkpoint_offshoot import OffshootSaver  # noqa: E402

# Names deliberately NOT overridden on OffshootSaver, each with a reason.
# Every entry must still exist on the introspected surface — a stale entry
# (name removed upstream) fails the test too, so this list can't rot.
EXPECTED_INHERITED = {
    # Alternate constructor: a classmethod contextmanager that builds a
    # saver from a sqlite connection string. It never touches an existing
    # instance's inner delegate, so inheriting it cannot bypass delegation
    # — and OffshootSaver's own constructors are session()/at_rest().
    "from_conn_string",
}


def _public_surface(cls: type) -> set[str]:
    """Public callables/properties defined anywhere on cls's MRO (excluding
    object/typing machinery) — the surface a delegating subclass must
    explicitly cover."""
    names: set[str] = set()
    for klass in cls.__mro__:
        if klass.__module__ in ("builtins", "typing"):
            continue
        for name, value in vars(klass).items():
            if name.startswith("_"):
                continue
            if callable(value) or isinstance(
                    value, (property, classmethod, staticmethod)):
                names.add(name)
    return names


def test_every_public_method_is_explicitly_overridden():
    surface = _public_surface(SqliteSaver) | _public_surface(BaseCheckpointSaver)

    stale = EXPECTED_INHERITED - surface
    assert not stale, (
        "EXPECTED_INHERITED lists names no longer on the installed "
        f"langgraph surface — prune them: {sorted(stale)}")

    missing = sorted(
        name for name in surface - EXPECTED_INHERITED
        if name not in vars(OffshootSaver))
    assert not missing, (
        "OffshootSaver inherits these public methods instead of explicitly "
        f"delegating them to the inner SqliteSaver: {missing}. A newer "
        "langgraph-checkpoint(-sqlite) likely added them. Add explicit "
        "delegating overrides in saver.py's 'pure delegation' block (and "
        "update the pin comment/README), or — only if inheriting is "
        "provably safe — add the name to EXPECTED_INHERITED with a reason.")
