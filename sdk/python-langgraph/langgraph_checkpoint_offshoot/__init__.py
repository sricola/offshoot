"""langgraph-checkpoint-offshoot: offshoot-backed LangGraph checkpointing
with branching semantics (fork / rollback / promote / TTL-reap).

See :mod:`langgraph_checkpoint_offshoot.saver` for the design notes.
"""
from .saver import OffshootSaver

__all__ = ["OffshootSaver"]

# No __version__ literal here, on purpose: pyproject.toml is the single
# source of the version (same convention as sdk/python, which defines no
# __version__ either). Runtime lookup, if ever needed:
# importlib.metadata.version("langgraph-checkpoint-offshoot").
