"""Raft-backed distributed key-value store."""

from .raft import RaftNode, Role

__all__ = ["RaftNode", "Role"]
