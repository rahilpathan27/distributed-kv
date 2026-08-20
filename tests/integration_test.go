package tests

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rahilpathan27/distributed-kv/internal/replication"
	"github.com/rahilpathan27/distributed-kv/internal/server"
	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

// sendCommand sends a command to a server and returns the response.
func sendCommand(t *testing.T, addr, cmd string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to %s: %v", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	_, err = fmt.Fprintf(conn, "%s\n", cmd)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read: %v", err)
	}
	return strings.TrimSpace(resp)
}

// waitForServer tries to connect until the server is ready.
func waitForServer(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Server %s not ready within %v", addr, timeout)
}

// TestSingleNodePersistence verifies WAL crash recovery.
func TestSingleNodePersistence(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(io.Discard, "", 0)

	// Phase 1: Write data
	w1, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	s1 := store.New()
	srv1 := server.NewWithWAL("127.0.0.1:0", s1, w1, logger)

	go srv1.Start()
	time.Sleep(100 * time.Millisecond)
	addr := srv1.Addr()

	sendCommand(t, addr, "SET name Rahil")
	sendCommand(t, addr, "SET language Go")
	sendCommand(t, addr, "SET project distributed-kv")

	// Verify values exist
	resp := sendCommand(t, addr, "GET name")
	if resp != "+Rahil" {
		t.Errorf("GET name: expected +Rahil, got %q", resp)
	}

	// "Crash" — stop server, close WAL
	srv1.Stop()
	w1.Close()

	// Phase 2: Restart and verify persistence
	w2, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	s2 := store.New()
	srv2 := server.NewWithWAL("127.0.0.1:0", s2, w2, logger)

	// Replay WAL
	err = srv2.ReplayWAL()
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	go srv2.Start()
	time.Sleep(100 * time.Millisecond)
	defer srv2.Stop()

	addr2 := srv2.Addr()

	// Verify all data survived the crash
	tests := []struct {
		key      string
		expected string
	}{
		{"name", "+Rahil"},
		{"language", "+Go"},
		{"project", "+distributed-kv"},
	}

	for _, tt := range tests {
		resp := sendCommand(t, addr2, "GET "+tt.key)
		if resp != tt.expected {
			t.Errorf("GET %s: expected %q, got %q", tt.key, tt.expected, resp)
		}
	}
}

// TestLeaderFollowerE2E tests full leader-follower replication flow.
func TestLeaderFollowerE2E(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()
	logger := log.New(io.Discard, "", 0)

	// Start leader
	leaderWAL, err := wal.Open(filepath.Join(leaderDir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to open leader WAL: %v", err)
	}
	defer leaderWAL.Close()

	leaderStore := store.New()
	leaderSrv := server.NewWithWAL("127.0.0.1:0", leaderStore, leaderWAL, logger)

	// Start leader replication
	leaderRepl := replication.NewLeader(leaderWAL, logger)

	// Wire OnWrite
	leaderSrv.OnWrite = func(entry wal.Entry) {
		leaderRepl.Replicate(entry)
	}

	if err := leaderRepl.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("Failed to start replication: %v", err)
	}
	defer leaderRepl.Stop()

	go leaderSrv.Start()
	time.Sleep(100 * time.Millisecond)
	defer leaderSrv.Stop()

	leaderAddr := leaderSrv.Addr()

	// Write some data to leader before follower connects
	sendCommand(t, leaderAddr, "SET pre1 before_follower")
	sendCommand(t, leaderAddr, "SET pre2 initial_data")
	time.Sleep(100 * time.Millisecond)

	// Start follower
	followerWAL, err := wal.Open(filepath.Join(followerDir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to open follower WAL: %v", err)
	}
	defer followerWAL.Close()

	followerStore := store.New()
	followerSrv := server.NewWithWAL("127.0.0.1:0", followerStore, followerWAL, logger)

	go followerSrv.Start()
	time.Sleep(100 * time.Millisecond)
	defer followerSrv.Stop()

	followerAddr := followerSrv.Addr()

	// Get the replication listener address
	// We need to find what port leaderRepl is listening on
	// Since we used :0, we can't easily get it from the Leader type
	// Instead, connect follower directly
	followerRepl := replication.NewFollower(
		leaderRepl.Addr(),
		followerStore,
		followerWAL,
		logger,
	)
	if err := followerRepl.Connect(); err != nil {
		t.Fatalf("Follower failed to connect: %v", err)
	}
	defer followerRepl.Stop()

	// Wait for sync
	time.Sleep(500 * time.Millisecond)

	// Verify pre-existing data was synced to follower
	resp := sendCommand(t, followerAddr, "GET pre1")
	if resp != "+before_follower" {
		t.Errorf("Follower GET pre1: expected +before_follower, got %q", resp)
	}

	// Write new data to leader
	sendCommand(t, leaderAddr, "SET live1 realtime")
	time.Sleep(300 * time.Millisecond)

	// Verify live replication
	resp = sendCommand(t, followerAddr, "GET live1")
	if resp != "+realtime" {
		t.Errorf("Follower GET live1: expected +realtime, got %q", resp)
	}
}

// TestConcurrentClients verifies multiple concurrent clients.
func TestConcurrentClients(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(io.Discard, "", 0)

	w, _ := wal.Open(filepath.Join(dir, "wal.log"))
	defer w.Close()

	s := store.New()
	srv := server.NewWithWAL("127.0.0.1:0", s, w, logger)

	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	defer srv.Stop()

	addr := srv.Addr()
	numClients := 50
	opsPerClient := 20

	var wg sync.WaitGroup
	errors := make(chan string, numClients*opsPerClient)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("client%d_key%d", clientID, j)
				val := fmt.Sprintf("value%d_%d", clientID, j)

				resp := sendCommand(t, addr, fmt.Sprintf("SET %s %s", key, val))
				if resp != "+OK" {
					errors <- fmt.Sprintf("SET %s: expected +OK, got %q", key, resp)
					return
				}

				resp = sendCommand(t, addr, fmt.Sprintf("GET %s", key))
				expected := "+" + val
				if resp != expected {
					errors <- fmt.Sprintf("GET %s: expected %q, got %q", key, expected, resp)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for errMsg := range errors {
		t.Error(errMsg)
	}

	// Verify final state
	if s.Len() != numClients*opsPerClient {
		t.Errorf("Expected %d keys, got %d", numClients*opsPerClient, s.Len())
	}
}

// TestDeletePersistence verifies that deletes survive restarts.
func TestDeletePersistence(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(io.Discard, "", 0)

	// Phase 1: Set and delete
	w1, _ := wal.Open(filepath.Join(dir, "wal.log"))
	s1 := store.New()
	srv1 := server.NewWithWAL("127.0.0.1:0", s1, w1, logger)

	go srv1.Start()
	time.Sleep(100 * time.Millisecond)

	addr := srv1.Addr()
	sendCommand(t, addr, "SET keep_me value1")
	sendCommand(t, addr, "SET delete_me value2")
	sendCommand(t, addr, "DELETE delete_me")

	srv1.Stop()
	w1.Close()

	// Phase 2: Restart
	w2, _ := wal.Open(filepath.Join(dir, "wal.log"))
	defer w2.Close()
	s2 := store.New()
	srv2 := server.NewWithWAL("127.0.0.1:0", s2, w2, logger)
	srv2.ReplayWAL()

	go srv2.Start()
	time.Sleep(100 * time.Millisecond)
	defer srv2.Stop()

	addr2 := srv2.Addr()

	// Verify keep_me survived and delete_me is gone
	resp := sendCommand(t, addr2, "GET keep_me")
	if resp != "+value1" {
		t.Errorf("GET keep_me: expected +value1, got %q", resp)
	}

	resp = sendCommand(t, addr2, "GET delete_me")
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("GET delete_me: expected error, got %q", resp)
	}
}

// Ensure the tests compile even if os is only used in init
var _ = os.Getenv
