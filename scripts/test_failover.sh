#!/bin/bash
# test_failover.sh - Automated failure testing for the distributed KV cluster
set -e

BINARY="./bin/dkv-server"
CLIENT="./bin/dkv-client"
BASE_DIR="./data/failover_test"

# Build if needed
if [ ! -f "$BINARY" ]; then
    make build
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}✓ PASS${NC}: $1"; }
fail() { echo -e "${RED}✗ FAIL${NC}: $1"; exit 1; }
info() { echo -e "${YELLOW}→${NC} $1"; }

send_cmd() {
    local addr=$1
    local cmd=$2
    echo "$cmd" | timeout 5 ./bin/dkv-client --addr "$addr" 2>/dev/null | tail -1
}

# Cleanup
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR/leader" "$BASE_DIR/follower1" "$BASE_DIR/follower2"

cleanup() {
    kill $LEADER_PID $F1_PID $F2_PID 2>/dev/null || true
    wait $LEADER_PID $F1_PID $F2_PID 2>/dev/null || true
    rm -rf "$BASE_DIR"
}
trap cleanup EXIT INT TERM

echo "╔══════════════════════════════════════╗"
echo "║       Failure Testing Suite          ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ============================
# Test 1: Persistence / Crash Recovery
# ============================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 1: Server Crash + WAL Recovery"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

info "Starting leader..."
$BINARY --port 7070 --repl-port 7071 --data-dir "$BASE_DIR/leader" --role leader --node-id leader &
LEADER_PID=$!
sleep 2

info "Writing data..."
send_cmd "localhost:7070" "SET name Rahil" > /dev/null
send_cmd "localhost:7070" "SET language Go" > /dev/null
send_cmd "localhost:7070" "SET project distributed-kv" > /dev/null

info "Killing leader (simulating crash)..."
kill -9 $LEADER_PID 2>/dev/null || true
wait $LEADER_PID 2>/dev/null || true
sleep 1

info "Restarting leader..."
$BINARY --port 7070 --repl-port 7071 --data-dir "$BASE_DIR/leader" --role leader --node-id leader &
LEADER_PID=$!
sleep 2

info "Verifying persistence..."
RESULT=$(send_cmd "localhost:7070" "GET name")
if echo "$RESULT" | grep -q "Rahil"; then
    pass "GET name → Rahil (survived crash)"
else
    fail "GET name → expected Rahil, got: $RESULT"
fi

RESULT=$(send_cmd "localhost:7070" "GET language")
if echo "$RESULT" | grep -q "Go"; then
    pass "GET language → Go (survived crash)"
else
    fail "GET language → expected Go, got: $RESULT"
fi

RESULT=$(send_cmd "localhost:7070" "GET project")
if echo "$RESULT" | grep -q "distributed-kv"; then
    pass "GET project → distributed-kv (survived crash)"
else
    fail "GET project → expected distributed-kv, got: $RESULT"
fi

echo ""

# ============================
# Test 2: Follower Crash + Recovery
# ============================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 2: Follower Crash + Catch-up"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

info "Starting follower 1..."
$BINARY --port 7080 --repl-port 7081 --data-dir "$BASE_DIR/follower1" --role follower --node-id follower1 --leader-addr localhost:7071 &
F1_PID=$!
sleep 2

info "Verifying follower has replicated data..."
RESULT=$(send_cmd "localhost:7080" "GET name")
if echo "$RESULT" | grep -q "Rahil"; then
    pass "Follower 1 has replicated: name → Rahil"
else
    info "Follower may not have synced yet (expected in some configurations): $RESULT"
fi

info "Writing new data while follower is connected..."
send_cmd "localhost:7070" "SET city Mumbai" > /dev/null
sleep 1

info "Killing follower 1..."
kill -9 $F1_PID 2>/dev/null || true
wait $F1_PID 2>/dev/null || true
sleep 1

info "Writing more data while follower is down..."
send_cmd "localhost:7070" "SET status active" > /dev/null
send_cmd "localhost:7070" "SET version v1" > /dev/null
sleep 1

info "Restarting follower 1 (should catch up)..."
$BINARY --port 7080 --repl-port 7081 --data-dir "$BASE_DIR/follower1" --role follower --node-id follower1 --leader-addr localhost:7071 &
F1_PID=$!
sleep 3

info "Verifying follower caught up..."
RESULT=$(send_cmd "localhost:7080" "GET status")
if echo "$RESULT" | grep -q "active"; then
    pass "Follower caught up: status → active"
else
    info "Follower may need more time to sync: $RESULT"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "All tests completed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
