# Raft KV

> A pure-Python distributed key-value store that uses the Raft consensus algorithm for leader election, replicated writes, failover, and durable recovery.

Raft KV is a compact systems-engineering project. It demonstrates how a cluster turns a client mutation into a durable, majority-agreed state-machine update. The implementation uses only the Python standard library at runtime—`asyncio`, TCP sockets, JSON, and filesystem primitives—and is designed to be easy to inspect and run locally or through Docker Compose.

## Contents

- [What it provides](#what-it-provides)
- [Architecture](#architecture)
- [Raft lifecycle](#raft-lifecycle)
- [Requirements and setup](#requirements-and-setup)
- [Run locally](#run-locally)
- [Run with Docker](#run-with-docker)
- [Client protocol](#client-protocol)
- [Failure testing](#failure-testing)
- [Tests and continuous integration](#tests-and-continuous-integration)
- [Persistence](#persistence)
- [Repository layout](#repository-layout)
- [Operational notes and limitations](#operational-notes-and-limitations)
- [Contributing and license](#contributing-and-license)

## What it provides

| Capability | Behavior |
| --- | --- |
| Leader election | Followers use randomized election timeouts and `RequestVote` RPCs. A candidate becomes leader only with a majority vote. |
| Failure detection | A leader sends periodic `AppendEntries` heartbeats. Missing heartbeats trigger a new election. |
| Replicated writes | The leader appends a command locally, replicates it to peers, and acknowledges it only after a quorum accepts it. |
| Consistent reads | Client reads are served by the current leader only; followers provide redirect information instead of returning possibly stale values. |
| Durable log | Each Raft entry is checksummed, appended to disk, flushed, and `fsync`ed before it is considered locally durable. |
| Recovery | Term, vote, commit index, and committed commands are restored from disk when a node restarts. |
| Catch-up | A rejoining follower receives missing entries through standard `AppendEntries` replication. |
| Containers | A three-node Compose deployment uses isolated networking and persistent named volumes. |

## Architecture

```text
                     client JSON request
                              |
                              v
                 +------------------------+
                 |      elected leader     |
                 |  client API + Raft node |
                 +-----------+------------+
                             |
       durable append         | AppendEntries / heartbeats
            +----------------+----------------+
            |                                 |
            v                                 v
  +--------------------+             +--------------------+
  | follower node 2    |             | follower node 3    |
  | durable Raft log   |             | durable Raft log   |
  | committed KV state |             | committed KV state |
  +--------------------+             +--------------------+
```

Each node has the same components:

1. `KVServer` accepts newline-delimited JSON over TCP.
2. `RaftNode` owns membership, role, term, voting, replication, commit, and apply state.
3. `DurableLog` persists Raft entries as checksummed JSON records.
4. `KeyValueStore` is the deterministic state machine. It changes only when an entry is committed.

Client and Raft RPC traffic share a TCP listener. Messages are distinguished by the JSON `type` field: Raft uses `request_vote` and `append_entries`; clients use `op`.

## Raft lifecycle

### Election

1. A follower receives no valid leader heartbeat for a randomized timeout.
2. It increments its term, votes for itself, and becomes a candidate.
3. It sends `RequestVote` to every peer, including its last-log term/index.
4. Peers grant at most one vote per term, and only to candidates whose logs are at least as up to date as their own.
5. A majority of votes promotes the candidate to leader. Any RPC containing a newer term causes a node to step down to follower.

### Write path

1. A client sends `SET` or `DELETE` to the leader.
2. The leader appends the command to its local durable log.
3. The leader sends `AppendEntries` to followers. Followers verify the previous log index and term, repair conflicts if needed, then persist the entries.
4. When a majority has replicated the entry, the leader advances its commit index and applies the command to the key-value store.
5. The next heartbeat propagates the commit index to followers, which apply the same state transition.

### Failure path

If a leader stops, the remaining nodes elect a new leader. A three-node cluster needs two live nodes to form a quorum. A single remaining node may keep serving status but cannot safely acknowledge new writes; it returns `quorum unavailable`.

## Requirements and setup

- Python **3.11+** for local development, or Docker Desktop for the container deployment.
- No runtime Python dependencies beyond the standard library.

Optional editable install, which provides `dkv-server` and `dkv-client` executables:

```bash
python3 -m pip install -e .
```

All examples below assume you are at the repository root:

```bash
cd ~/Desktop/distributed-kv
```

## Run locally

### One-node development mode

Run the server:

```bash
python3 -m dkv.cli --node-id node1 --port 7070 --data-dir ./data/node1
```

After its election timeout, the only node elects itself leader. In another terminal:

```bash
python3 -m dkv.client --addr 127.0.0.1:7070
```

```text
dkv> STATUS
dkv> SET user:42 Rahil
dkv> GET user:42
dkv> DELETE user:42
dkv> QUIT
```

### Three-node cluster

Start each node in a separate terminal. Using separate terminals makes failure testing explicit and prevents background processes from being left behind.

```bash
# Terminal 1
python3 -m dkv.cli --node-id node1 --port 7070 --data-dir ./data/node1 \
  --peer node2=127.0.0.1:7080 --peer node3=127.0.0.1:7090

# Terminal 2
python3 -m dkv.cli --node-id node2 --port 7080 --data-dir ./data/node2 \
  --peer node1=127.0.0.1:7070 --peer node3=127.0.0.1:7090

# Terminal 3
python3 -m dkv.cli --node-id node3 --port 7090 --data-dir ./data/node3 \
  --peer node1=127.0.0.1:7070 --peer node2=127.0.0.1:7080
```

Wait two seconds, then inspect each port with the client. Exactly one node should report `"role": "leader"`.

```bash
python3 -m dkv.client --addr 127.0.0.1:7070
python3 -m dkv.client --addr 127.0.0.1:7080
python3 -m dkv.client --addr 127.0.0.1:7090
```

The `make cluster` target is also available, but the three-terminal setup is better for observing a particular node’s output and using `Ctrl+C` to simulate failure.

## Run with Docker

Docker Compose starts three containers on an internal Docker network. Each container uses port `7070` internally—the containers have isolated network namespaces, so this is correct. They are published to different host ports for convenient testing.

| Container | Internal endpoint | Host endpoint |
| --- | --- | --- |
| `node1` | `node1:7070` | `127.0.0.1:7070` |
| `node2` | `node2:7070` | `127.0.0.1:7080` |
| `node3` | `node3:7070` | `127.0.0.1:7090` |

Start the cluster:

```bash
docker compose -f docker/docker-compose.yml up --build
```

From another terminal, use the local client against a published port:

```bash
python3 -m dkv.client --addr 127.0.0.1:7070
```

Or run the bundled client inside a particular container:

```bash
docker compose -f docker/docker-compose.yml exec node2 dkv-client --addr 127.0.0.1:7070
```

Stop the environment while retaining named-volume data:

```bash
docker compose -f docker/docker-compose.yml down
```

Remove containers **and persisted cluster data**:

```bash
docker compose -f docker/docker-compose.yml down -v
```

## Client protocol

The interactive client accepts these commands:

| Command | Example | Result |
| --- | --- | --- |
| `SET key value` | `SET language Python` | Replicated write on the leader. |
| `GET key` | `GET language` | Leader-only read. |
| `DELETE key` | `DELETE language` | Replicated deletion on the leader. |
| `STATUS` | `STATUS` | Node role, term, leader ID, and commit index. |
| `QUIT` | `QUIT` | Ends the client session. |

The transport uses one JSON object per line. Example client request:

```json
{"op":"set","key":"language","value":"Python"}
```

Example success response:

```json
{"ok":true,"error":null}
```

Example follower response:

```json
{"ok":false,"error":"not leader","leader_id":"node2","leader_addr":"127.0.0.1:7080"}
```

## Failure testing

Run these scenarios against a three-node local or Docker cluster.

| Scenario | Procedure | Expected result |
| --- | --- | --- |
| Election | Start all nodes; run `STATUS` on each. | One leader, two followers, shared leader ID. |
| Basic replication | Write a value on the leader, wait one second, then fail the leader. | The new leader reads the value. |
| Follower redirect | Send `SET` or `GET` to a follower. | `not leader` plus leader details. |
| Leader failure | Stop the current leader; wait 1–2 seconds. | A survivor is elected with a larger term. |
| Follower catch-up | Stop one follower, write with the remaining quorum, restart it, then fail the leader. | The rejoined node has the missing committed values. |
| Quorum loss | Stop both followers; write to the remaining leader. | `quorum unavailable`; no successful acknowledgement. |
| Quorum recovery | Restart one follower and wait for replication. | New writes succeed again. |
| Durability | Commit a value, stop all nodes, restart with the same data directories/volumes. | A leader can read the committed value. |

Docker examples:

```bash
# Replace node1 with the actual current leader when testing failover.
docker compose -f docker/docker-compose.yml stop node1
docker compose -f docker/docker-compose.yml start node1
docker compose -f docker/docker-compose.yml logs --follow node2
```

## Tests and continuous integration

Run the deterministic test suite:

```bash
python3 -m unittest discover -s tests -v
```

Current coverage includes:

- Single-node election and committed-state recovery after restart.
- Three-node election with exactly one leader.
- Majority replication and application of a committed mutation.
- Successful write responses do not expose a false error value.

GitHub Actions runs the suite on Python 3.11 and 3.12 for pushes and pull requests targeting `main`. Its workflow is at [`.github/workflows/tests.yml`](.github/workflows/tests.yml).

## Persistence

Each `--data-dir` contains:

| File | Contents |
| --- | --- |
| `raft.log` | Append-only JSON records containing term, command, and SHA-256 checksum. Every append is flushed and fsynced. |
| `raft-state.json` | Current term, voted-for node, and commit index. Updates are written to a temporary file and atomically replaced. |

The in-memory KV store is rebuilt by applying only the persisted committed prefix of the log. This ensures an entry that was appended but never reached quorum is not applied after a restart.

## Repository layout

```text
.
├── dkv/
│   ├── raft.py               # Raft roles, elections, replication, commits
│   ├── log.py                # durable checksum-protected Raft log
│   ├── store.py              # committed key-value state machine
│   ├── server.py             # async TCP JSON server and RPC transport
│   ├── cli.py                # node process entry point
│   └── client.py             # interactive terminal client
├── tests/test_raft.py        # deterministic consensus and recovery tests
├── docker/docker-compose.yml # three-node container topology
├── .github/workflows/tests.yml
├── Dockerfile
├── pyproject.toml
├── CONTRIBUTING.md
└── LICENSE
```

## Operational notes and limitations

This repository is an educational, portfolio-quality implementation—not a production database. It intentionally keeps the design small and readable. In particular, it does **not** yet provide:

- Log compaction or snapshotting; logs grow indefinitely.
- Dynamic membership changes; peers are configured at process start.
- TLS, authentication, authorization, encryption at rest, metrics, tracing, or rate limits.
- Linearizable read-index/lease reads; it uses leader-only reads as the simpler safety boundary.
- Sophisticated replication optimizations such as conflict-term jumps, batching limits, or backpressure.
- A client retry/idempotency protocol. A client that loses a response after sending a write should treat the operation as uncertain and read/retry carefully.

Do not expose this service directly to an untrusted network without adding those operational controls.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for local checks and Raft change expectations.

Raft KV is released under the [MIT License](LICENSE).
