"""langgraph-checkpoint-offshoot: offshoot-backed LangGraph checkpointing
with branching semantics (fork / rollback / promote / TTL-reap).

See :mod:`langgraph_checkpoint_offshoot.saver` for the design notes.
"""
from .saver import OffshootSaver

__all__ = ["OffshootSaver"]
__version__ = "0.1.0"
