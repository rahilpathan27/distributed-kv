"""Server executable."""
from __future__ import annotations

import argparse
import asyncio
import signal

from .raft import RaftNode
from .server import KVServer, tcp_rpc


async def run(args: argparse.Namespace) -> None:
    peers = {}
    for item in args.peer:
        node_id, address = item.split("=", 1)
        if node_id != args.node_id: peers[node_id] = address
    raft = RaftNode(args.node_id, peers, args.data_dir, lambda peer, request: tcp_rpc(peers[peer], request))
    server = KVServer(raft, args.host, args.port)
    await server.start()
    print(f"dkv node={args.node_id} listening on {args.host}:{args.port}", flush=True)
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, shutdown.set)
    await shutdown.wait()
    await server.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Raft distributed key-value node")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--peer", action="append", default=[], metavar="ID=HOST:PORT")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
