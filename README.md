# Distributed Key-Value Store

A production-quality distributed key-value store built in Go from scratch, featuring TCP networking, Write-Ahead Logging, leader-follower replication, failure detection, and crash recovery.

```
                     +----------------+
                     |     Client     |
                     +-------+--------+
                             |
                             | TCP
                             v
                     +---------------+
                     |    Leader     |
                     |               |
                     | Concurrent KV |
                     |      +        |
                     |    WAL        |
                     +--+---+----+---+
                        |        |
                 Replication     Replication
                        |        |
                        v        v
                  +---------+ +---------+
                  |Follower | |Follower |
                  |    A    | |    B    |
                  +---------+ +---------+
                       ^          ^
                       |          |
                    heartbeat / recovery
```

## Features

| Component | What it demonstrates |
|-----------|---------------------|
| TCP server | Networking, custom protocol |
| Goroutines | Concurrency (per-connection) |
| Concurrent KV store | Thread safety with `sync.RWMutex` |
| Write-Ahead Log | Persistence, `fsync`, binary encoding |
| Log replay | Crash recovery |
| Leader/follower | Replication protocol |
| Heartbeats | Failure detection |
| Recovery sync | Incremental state synchronization |
| Leader election | Distributed consensus (Bully algorithm) |
| Docker | Container deployment |
| Tests | 50+ tests with race detector |

## Quick Start

### Build

```bash
make build
```

### Run a single node

```bash
./bin/dkv-server --port 7070 --data-dir ./data/node1
```

### Connect with the client

```bash
./bin/dkv-client --addr localhost:7070
```

```
Connected to localhost:7070
Type commands: SET key value | GET key | DELETE key | PING | QUIT

dkv> SET user:42 Rahil
+OK
dkv> GET user:42
+Rahil
dkv> SET language Go
+OK
dkv> DELETE user:42
+OK
dkv> PING
+PONG
```

### Run a 3-node cluster

```bash
make cluster
```

Or manually:

```bash
# Terminal 1 - Leader
./bin/dkv-server --port 7070 --repl-port 7071 --data-dir ./data/leader \
  --role leader --node-id leader

# Terminal 2 - Follower 1
./bin/dkv-server --port 7080 --repl-port 7081 --data-dir ./data/follower1 \
  --role follower --node-id follower1 --leader-addr localhost:7071

# Terminal 3 - Follower 2
./bin/dkv-server --port 7090 --repl-port 7091 --data-dir ./data/follower2 \
  --role follower --node-id follower2 --leader-addr localhost:7071
```

### Docker

```bash
make docker-up     # Start 3-node cluster
make docker-down   # Stop cluster
```

## Protocol

Simple text-based protocol over TCP (newline-delimited):

```
SET key value   →  +OK
GET key         →  +value  or  -ERR key not found
DELETE key      →  +OK     or  -ERR key not found
PING            →  +PONG
```

## Architecture

### Write Path (Leader)

```
Client → TCP → Parse Command → WAL (fsync) → In-Memory Store → Replicate to Followers
```

### Read Path (Any Node)

```
Client → TCP → Parse Command → In-Memory Store → Response
```

### Crash Recovery

```
Server Start → Open WAL → Replay Entries → Reconstruct Store → Accept Connections
```

### Follower Sync

```
Follower Connects → SYNC <last_index> → Leader streams missing entries → Follower applies
```

## Testing

```bash
# All unit tests with race detector
make test

# Verbose output
make test-v

# Integration tests
go test ./tests/... -v -race

# Failover testing
./scripts/test_failover.sh
```

### Persistence Test

```bash
# 1. Start server, write data
./bin/dkv-server --port 7070 --data-dir ./data/test &
echo -e "SET name Rahil\nSET language Go" | ./bin/dkv-client

# 2. Kill server
kill %1

# 3. Restart and verify
./bin/dkv-server --port 7070 --data-dir ./data/test &
echo "GET name" | ./bin/dkv-client      # → +Rahil
echo "GET language" | ./bin/dkv-client   # → +Go
```

## Project Structure

```
distributed-kv/
├── cmd/
│   ├── server/main.go          # Server binary
│   └── client/main.go          # CLI client
├── internal/
│   ├── store/                  # Concurrent KV engine (sync.RWMutex)
│   ├── protocol/               # TCP protocol parser
│   ├── server/                 # TCP server (goroutine per connection)
│   ├── wal/                    # Write-Ahead Log (binary + CRC32)
│   ├── replication/            # Leader-follower replication
│   ├── cluster/                # Heartbeats, recovery, election
│   └── config/                 # Configuration
├── tests/                      # Integration tests
├── scripts/                    # Cluster & failover scripts
├── docker/                     # Dockerfile + docker-compose
├── Makefile
└── README.md
```

## Built With

- **Go** — standard library only, zero external dependencies
- **TCP** — custom text-based protocol
- **Goroutines** — concurrent connection handling
- **sync.RWMutex** — thread-safe store
- **encoding/binary** — WAL binary format
- **hash/crc32** — corruption detection
- **Docker** — containerized deployment
