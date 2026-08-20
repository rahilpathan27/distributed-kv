#!/bin/bash
# run_cluster.sh - Launch a 3-node distributed KV cluster locally
set -e

BINARY="./bin/dkv-server"
BASE_DIR="./data"

# Build if not already built
if [ ! -f "$BINARY" ]; then
    echo "Building server..."
    make build
fi

# Clean up data directories
rm -rf "$BASE_DIR/leader" "$BASE_DIR/follower1" "$BASE_DIR/follower2"
mkdir -p "$BASE_DIR/leader" "$BASE_DIR/follower1" "$BASE_DIR/follower2"

# Trap to clean up on exit
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    kill $LEADER_PID $F1_PID $F2_PID 2>/dev/null || true
    wait $LEADER_PID $F1_PID $F2_PID 2>/dev/null || true
    echo "Cluster stopped."
}
trap cleanup EXIT INT TERM

# Start leader
echo "Starting leader on :7070 (repl :7071)..."
$BINARY --port 7070 --repl-port 7071 --data-dir "$BASE_DIR/leader" --role leader --node-id leader &
LEADER_PID=$!
sleep 1

# Start follower 1
echo "Starting follower 1 on :7080 (repl :7081)..."
$BINARY --port 7080 --repl-port 7081 --data-dir "$BASE_DIR/follower1" --role follower --node-id follower1 --leader-addr localhost:7071 &
F1_PID=$!
sleep 1

# Start follower 2
echo "Starting follower 2 on :7090 (repl :7091)..."
$BINARY --port 7090 --repl-port 7091 --data-dir "$BASE_DIR/follower2" --role follower --node-id follower2 --leader-addr localhost:7071 &
F2_PID=$!
sleep 1

echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║         Distributed KV Cluster Running            ║"
echo "╠════════════════════════════════════════════════════╣"
echo "║  Leader:     localhost:7070 (repl: 7071)          ║"
echo "║  Follower 1: localhost:7080 (repl: 7081)          ║"
echo "║  Follower 2: localhost:7090 (repl: 7091)          ║"
echo "╠════════════════════════════════════════════════════╣"
echo "║  Connect:  ./bin/dkv-client --addr localhost:7070 ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "Press Ctrl+C to stop the cluster."

# Wait for any child to exit
wait
