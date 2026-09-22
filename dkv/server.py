"""Async TCP server for client commands and Raft RPCs."""
from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

from .raft import RaftNode, Role


class KVServer:
    def __init__(self, raft: RaftNode, host: str, port: int) -> None:
        self.raft, self.host, self.port = raft, host, port
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        await self.raft.start()

    @property
    def address(self) -> tuple[str, int]:
        """Actual bound address; useful when the server was started on port 0."""
        if not self._server or not self._server.sockets:
            raise RuntimeError("server has not started")
        host, port = self._server.sockets[0].getsockname()[:2]
        return host, port

    async def stop(self) -> None:
        await self.raft.stop()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while raw := await reader.readline():
                request = json.loads(raw)
                if request.get("type") in {"request_vote", "append_entries"}:
                    response = await self.raft.handle_rpc(request)
                else:
                    response = await self._client_command(request)
                writer.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
                await writer.drain()
        except (ConnectionError, json.JSONDecodeError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def _client_command(self, request: dict) -> dict:
        operation = request.get("op", "").lower()
        if operation == "status":
            return {"ok": True, "status": asdict(self.raft.status())}
        if operation == "get":
            # Reads are served only by the leader to avoid stale follower reads.
            if self.raft.role != Role.LEADER:
                return self._redirect()
            value = self.raft.store.get(request.get("key", ""))
            return {"ok": value is not None, "value": value, "error": None if value is not None else "key not found"}
        if operation in {"set", "delete"}:
            if self.raft.role != Role.LEADER:
                return self._redirect()
            command = {"op": operation, "key": request.get("key", "")}
            if operation == "set":
                if "value" not in request: return {"ok": False, "error": "SET requires value"}
                command["value"] = str(request["value"])
            if not command["key"]: return {"ok": False, "error": "key is required"}
            committed = await self.raft.propose(command)
            return {"ok": committed, "error": None if committed else "quorum unavailable"}
        return {"ok": False, "error": "unsupported operation"}

    def _redirect(self) -> dict:
        leader = self.raft.peers.get(self.raft.leader_id or "")
        return {"ok": False, "error": "not leader", "leader_id": self.raft.leader_id, "leader_addr": leader}


async def tcp_rpc(address: str, request: dict) -> dict:
    host, port = address.rsplit(":", 1)
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=0.4)
    try:
        writer.write(json.dumps(request, separators=(",", ":")).encode() + b"\n")
        await writer.drain()
        return json.loads(await asyncio.wait_for(reader.readline(), timeout=0.4))
    finally:
        writer.close()
        await writer.wait_closed()
