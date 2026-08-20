package cluster

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

	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

func TestHeartbeatManagerAlive(t *testing.T) {
	t.Parallel()
	logger := log.New(io.Discard, "", 0)
	hm := NewHeartbeatManager(100*time.Millisecond, 200*time.Millisecond, logger)
	hm.Start()
	defer hm.Stop()

	addr := "node1"
	hm.RegisterNode(addr)

	for i := 0; i < 10; i++ {
		hm.RecordHeartbeat(addr)
		time.Sleep(50 * time.Millisecond)
	}

	states := hm.GetNodeStates()
	if states[addr] != StateAlive {
		t.Errorf("expected node to be ALIVE, got %s", states[addr])
	}
}

func TestHeartbeatManagerDead(t *testing.T) {
	t.Parallel()
	logger := log.New(io.Discard, "", 0)
	hm := NewHeartbeatManager(100*time.Millisecond, 200*time.Millisecond, logger)
	hm.maxMisses = 3
	
	var downCalled bool
	var mu sync.Mutex
	hm.OnNodeDown = func(addr string) {
		mu.Lock()
		downCalled = true
		mu.Unlock()
	}

	hm.Start()
	defer hm.Stop()

	addr := "node2"
	hm.RegisterNode(addr)
	hm.RecordHeartbeat(addr)

	time.Sleep(1 * time.Second)

	states := hm.GetNodeStates()
	if states[addr] != StateDead {
		t.Errorf("expected node to be DEAD, got %s", states[addr])
	}

	mu.Lock()
	called := downCalled
	mu.Unlock()
	if !called {
		t.Errorf("expected OnNodeDown to be called")
	}
}

func TestHeartbeatManagerRecovery(t *testing.T) {
	t.Parallel()
	logger := log.New(io.Discard, "", 0)
	hm := NewHeartbeatManager(100*time.Millisecond, 200*time.Millisecond, logger)
	hm.maxMisses = 1
	
	var upCalled bool
	var mu sync.Mutex
	hm.OnNodeUp = func(addr string) {
		mu.Lock()
		upCalled = true
		mu.Unlock()
	}

	hm.Start()
	defer hm.Stop()

	addr := "node3"
	hm.RegisterNode(addr)
	
	time.Sleep(500 * time.Millisecond)
	
	states := hm.GetNodeStates()
	if states[addr] != StateDead {
		t.Fatalf("expected node to be DEAD, got %s", states[addr])
	}

	hm.RecordHeartbeat(addr)
	time.Sleep(50 * time.Millisecond)
	
	states = hm.GetNodeStates()
	if states[addr] != StateAlive {
		t.Errorf("expected node to be ALIVE, got %s", states[addr])
	}

	mu.Lock()
	called := upCalled
	mu.Unlock()
	if !called {
		t.Errorf("expected OnNodeUp to be called")
	}
}

func TestRecoveryManagerSync(t *testing.T) {
	t.Parallel()
	
	// Create leader WAL
	leaderDir := t.TempDir()
	leaderWAL, err := wal.Open(filepath.Join(leaderDir, "wal.log"))
	if err != nil {
		t.Fatalf("failed to open leader wal: %v", err)
	}
	defer leaderWAL.Close()
	
	for i := 1; i <= 5; i++ {
		_, err := leaderWAL.Append(wal.OpSet, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
		if err != nil {
			t.Fatalf("failed to append to leader wal: %v", err)
		}
	}

	c1, c2 := net.Pipe()
	go func() {
		defer c2.Close()
		reader := bufio.NewReader(c2)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		
		if strings.HasPrefix(line, "SYNC") {
			entries, _ := leaderWAL.EntriesFrom(1)
			for _, entry := range entries {
				opStr := "SET"
				if entry.Op == wal.OpDelete {
					opStr = "DELETE"
				}
				fmt.Fprintf(c2, "REPLICATE %d %s %s %s\n", entry.Index, opStr, entry.Key, entry.Value)
				reader.ReadString('\n') // read ACK
			}
		}
	}()

	followerDir := t.TempDir()
	followerWAL, err := wal.Open(filepath.Join(followerDir, "wal.log"))
	if err != nil {
		t.Fatalf("failed to open follower wal: %v", err)
	}
	defer followerWAL.Close()
	
	s := store.New()
	logger := log.New(os.Stdout, "", 0)
	rm := NewRecoveryManager(s, followerWAL, logger)
	rm.dialer = func(network, address string) (net.Conn, error) {
		return c1, nil
	}

	synced, err := rm.RequestSync("dummy")
	if err != nil {
		t.Fatalf("RequestSync failed: %v", err)
	}
	if synced != 5 {
		t.Errorf("expected to sync 5 entries, got %d", synced)
	}
	
	val, ok := s.Get("key5")
	if !ok || val != "val5" {
		t.Errorf("expected key5=val5, got %s, ok=%v", val, ok)
	}
}

func TestNodeStateString(t *testing.T) {
	t.Parallel()
	if StateAlive.String() != "ALIVE" {
		t.Errorf("expected ALIVE")
	}
	if StateSuspect.String() != "SUSPECT" {
		t.Errorf("expected SUSPECT")
	}
	if StateDead.String() != "DEAD" {
		t.Errorf("expected DEAD")
	}
	if NodeState(99).String() != "UNKNOWN" {
		t.Errorf("expected UNKNOWN")
	}
}
