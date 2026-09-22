"""Raft leader election, replication, and commit logic.

The transport is injected, making consensus code independently testable.
"""
from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Awaitable, Callable

from .log import DurableLog, LogEntry
from .store import KeyValueStore


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


Rpc = Callable[[str, dict], Awaitable[dict]]


@dataclass
class RaftStatus:
    node_id: str
    role: Role
    term: int
    leader_id: str | None
    commit_index: int


class RaftNode:
    def __init__(self, node_id: str, peers: dict[str, str], data_dir: str | Path,
                 rpc: Rpc, election_timeout: tuple[float, float] = (0.45, 0.8),
                 heartbeat_interval: float = 0.12) -> None:
        self.node_id, self.peers, self.rpc = node_id, peers, rpc
        self.log = DurableLog(Path(data_dir) / "raft.log")
        self.store = KeyValueStore()
        self.role = Role.FOLLOWER
        self.current_term = 0
        self.voted_for: str | None = None
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._stopped = asyncio.Event()
        self._wakeup = asyncio.Event()
        self._election_timeout = election_timeout
        self._heartbeat_interval = heartbeat_interval
        self._tasks: list[asyncio.Task] = []
        self._load_state()
        self._apply_committed(self.commit_index)

    def _load_state(self) -> None:
        state_path = self.log.path.with_name("raft-state.json")
        self._state_path = state_path
        if state_path.exists():
            state = json.loads(state_path.read_text())
            self.current_term = state.get("term", 0)
            self.voted_for = state.get("voted_for")
            self.commit_index = min(state.get("commit_index", 0), len(self.log.entries))

    def _persist_state(self) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"term": self.current_term, "voted_for": self.voted_for,
                                         "commit_index": self.commit_index}))
        temporary.replace(self._state_path)

    async def start(self) -> None:
        self._tasks = [asyncio.create_task(self._election_loop()), asyncio.create_task(self._heartbeat_loop())]

    async def stop(self) -> None:
        self._stopped.set()
        self._wakeup.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def status(self) -> RaftStatus:
        return RaftStatus(self.node_id, self.role, self.current_term, self.leader_id, self.commit_index)

    async def handle_rpc(self, message: dict) -> dict:
        kind = message.get("type")
        if kind == "request_vote":
            return await self._request_vote(message)
        if kind == "append_entries":
            return await self._append_entries(message)
        return {"error": "unknown raft RPC"}

    async def _request_vote(self, request: dict) -> dict:
        async with self._lock:
            term = request["term"]
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}
            if term > self.current_term:
                self._become_follower(term)
            candidate_is_current = (request["last_log_term"], request["last_log_index"]) >= (self.log.term_at(len(self.log.entries)), len(self.log.entries))
            grant = candidate_is_current and self.voted_for in (None, request["candidate_id"])
            if grant:
                self.voted_for = request["candidate_id"]
                self._persist_state()
                self._wakeup.set()
            return {"term": self.current_term, "vote_granted": grant}

    async def _append_entries(self, request: dict) -> dict:
        async with self._lock:
            if request["term"] < self.current_term:
                return {"term": self.current_term, "success": False}
            if request["term"] > self.current_term or self.role != Role.FOLLOWER:
                self._become_follower(request["term"])
            self.leader_id = request["leader_id"]
            self._wakeup.set()
            previous = request["prev_log_index"]
            if previous > len(self.log.entries) or (previous and self.log.term_at(previous) != request["prev_log_term"]):
                return {"term": self.current_term, "success": False, "match_index": len(self.log.entries)}
            incoming = [LogEntry(**entry) for entry in request["entries"]]
            for offset, entry in enumerate(incoming):
                index = previous + offset + 1
                if index <= len(self.log.entries) and self.log.term_at(index) != entry.term:
                    self.log.truncate_and_append(index - 1, incoming[offset:])
                    break
                if index > len(self.log.entries):
                    for suffix in incoming[offset:]:
                        self.log.append(suffix)
                    break
            if request["leader_commit"] > self.commit_index:
                self.commit_index = min(request["leader_commit"], len(self.log.entries))
                self._persist_state()
                self._apply_committed(self.commit_index)
            return {"term": self.current_term, "success": True, "match_index": len(self.log.entries)}

    async def propose(self, command: dict[str, str]) -> bool:
        async with self._lock:
            if self.role != Role.LEADER:
                return False
            self.log.append(LogEntry(self.current_term, command))
            index = len(self.log.entries)
        if not self.peers:
            async with self._lock:
                self.commit_index = index
                self._persist_state(); self._apply_committed(index)
            return True
        results = await asyncio.gather(*(self._replicate_to(peer) for peer in self.peers), return_exceptions=True)
        async with self._lock:
            replicated = 1 + sum(result is True for result in results)
            if self.role == Role.LEADER and replicated >= self._majority():
                self.commit_index = index
                self._persist_state(); self._apply_committed(index)
                return True
            return False

    async def _replicate_to(self, peer: str) -> bool:
        for _ in range(8):
            async with self._lock:
                if self.role != Role.LEADER: return False
                next_index = self.next_index.get(peer, len(self.log.entries) + 1)
                previous = next_index - 1
                request = {"type": "append_entries", "term": self.current_term, "leader_id": self.node_id,
                           "prev_log_index": previous, "prev_log_term": self.log.term_at(previous),
                           "entries": [entry.__dict__ for entry in self.log.entries[previous:]], "leader_commit": self.commit_index}
            try: response = await self.rpc(peer, request)
            except (OSError, asyncio.TimeoutError): return False
            async with self._lock:
                if response.get("term", 0) > self.current_term:
                    self._become_follower(response["term"]); return False
                if response.get("success"):
                    self.match_index[peer] = response["match_index"]
                    self.next_index[peer] = response["match_index"] + 1
                    return True
                self.next_index[peer] = max(1, next_index - 1)
        return False

    async def _election_loop(self) -> None:
        while not self._stopped.is_set():
            self._wakeup.clear()
            try: await asyncio.wait_for(self._wakeup.wait(), timeout=random.uniform(*self._election_timeout))
            except asyncio.TimeoutError:
                if self.role != Role.LEADER: await self._start_election()

    async def _start_election(self) -> None:
        async with self._lock:
            self.role = Role.CANDIDATE; self.current_term += 1; self.voted_for = self.node_id; self.leader_id = None; self._persist_state()
            request = {"type": "request_vote", "term": self.current_term, "candidate_id": self.node_id,
                       "last_log_index": len(self.log.entries), "last_log_term": self.log.term_at(len(self.log.entries))}
        replies = await asyncio.gather(*(self.rpc(peer, request) for peer in self.peers), return_exceptions=True)
        async with self._lock:
            if self.role != Role.CANDIDATE: return
            for reply in replies:
                if isinstance(reply, dict) and reply.get("term", 0) > self.current_term:
                    self._become_follower(reply["term"]); return
            if 1 + sum(isinstance(reply, dict) and reply.get("vote_granted") for reply in replies) >= self._majority():
                self.role = Role.LEADER; self.leader_id = self.node_id
                self.next_index = {peer: len(self.log.entries) + 1 for peer in self.peers}
                self.match_index = {peer: 0 for peer in self.peers}

    async def _heartbeat_loop(self) -> None:
        while not self._stopped.is_set():
            await asyncio.sleep(self._heartbeat_interval)
            if self.role == Role.LEADER:
                await asyncio.gather(*(self._replicate_to(peer) for peer in self.peers), return_exceptions=True)

    def _become_follower(self, term: int) -> None:
        self.role = Role.FOLLOWER; self.current_term = term; self.voted_for = None; self.leader_id = None; self._persist_state()

    def _apply_committed(self, upto: int) -> None:
        while self.last_applied < upto:
            self.last_applied += 1
            self.store.apply(self.log.entries[self.last_applied - 1].command)

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1
