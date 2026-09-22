import asyncio
import tempfile
import unittest
from pathlib import Path

from dkv.raft import RaftNode, Role
from dkv.server import KVServer


class RaftTests(unittest.TestCase):
    def test_successful_write_has_no_error(self):
        with tempfile.TemporaryDirectory() as directory:
            async def scenario():
                node = RaftNode("a", {}, directory, lambda *_: None)
                node.role = Role.LEADER
                server = KVServer(node, "127.0.0.1", 0)
                response = await server._client_command({"op": "set", "key": "ok", "value": "yes"})
                self.assertEqual(response, {"ok": True, "error": None})
            asyncio.run(scenario())

    def test_single_node_elects_and_persists(self):
        with tempfile.TemporaryDirectory() as directory:
            async def scenario():
                node = RaftNode("a", {}, directory, lambda *_: None, election_timeout=(.01, .02))
                await node.start(); await asyncio.sleep(.04)
                self.assertEqual(node.role, Role.LEADER)
                self.assertTrue(await node.propose({"op": "set", "key": "color", "value": "blue"}))
                await node.stop()
            asyncio.run(scenario())
            async def restore():
                restored = RaftNode("a", {}, directory, lambda *_: None)
                self.assertEqual(restored.store.get("color"), "blue")
            asyncio.run(restore())

    def test_three_nodes_elect_and_replicate(self):
        with tempfile.TemporaryDirectory() as directory:
            async def scenario():
                nodes = {}
                async def rpc(peer, message): return await nodes[peer].handle_rpc(message)
                for node_id in "abc":
                    nodes[node_id] = RaftNode(node_id, {other: other for other in "abc" if other != node_id}, Path(directory) / node_id, rpc, (.02, .04), .01)
                await asyncio.gather(*(node.start() for node in nodes.values()))
                await asyncio.sleep(.12)
                leaders = [node for node in nodes.values() if node.role == Role.LEADER]
                self.assertEqual(len(leaders), 1)
                self.assertTrue(await leaders[0].propose({"op": "set", "key": "answer", "value": "42"}))
                await asyncio.sleep(.03)
                self.assertTrue(all(node.store.get("answer") == "42" for node in nodes.values()))
                await asyncio.gather(*(node.stop() for node in nodes.values()))
            asyncio.run(scenario())
