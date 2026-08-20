package replication

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

func setupTestWAL(t *testing.T, name string) (*wal.WAL, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	return w, func() { w.Close() }
}

func TestLeaderFollowerReplication(t *testing.T) {
	t.Parallel()

	leaderWAL, cleanup1 := setupTestWAL(t, "leader.wal")
	defer cleanup1()

	leader := NewLeader(leaderWAL, log.New(os.Stderr, "LEADER: ", log.Ltime|log.Lshortfile))
	
	err := leader.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("leader.Start failed: %v", err)
	}
	defer leader.Stop()

	addr := leader.listener.Addr().String()

	followerWAL, cleanup2 := setupTestWAL(t, "follower.wal")
	defer cleanup2()

	followerStore := store.New()
	follower := NewFollower(addr, followerStore, followerWAL, log.New(os.Stderr, "FOLLOWER: ", log.Ltime|log.Lshortfile))

	err = follower.Connect()
	if err != nil {
		t.Fatalf("follower.Connect failed: %v", err)
	}
	defer follower.Stop()

	time.Sleep(50 * time.Millisecond)

	entries := []wal.Entry{
		{Index: 1, Op: wal.OpSet, Key: "k1", Value: "v1"},
		{Index: 2, Op: wal.OpSet, Key: "k2", Value: "v2"},
		{Index: 3, Op: wal.OpDelete, Key: "k1", Value: ""},
		{Index: 4, Op: wal.OpSet, Key: "k3", Value: "v3"},
		{Index: 5, Op: wal.OpSet, Key: "k2", Value: "v2_updated"},
	}

	for _, entry := range entries {
		leader.Replicate(entry)
	}

	time.Sleep(200 * time.Millisecond)

	if val, ok := followerStore.Get("k1"); ok {
		t.Errorf("k1 should be deleted, got %s", val)
	}
	if val, ok := followerStore.Get("k2"); !ok || val != "v2_updated" {
		t.Errorf("expected k2=v2_updated, got %v (ok=%v)", val, ok)
	}
	if val, ok := followerStore.Get("k3"); !ok || val != "v3" {
		t.Errorf("expected k3=v3, got %v (ok=%v)", val, ok)
	}
}

func TestFollowerSyncOnConnect(t *testing.T) {
	t.Parallel()

	leaderWAL, cleanup1 := setupTestWAL(t, "leader.wal")
	defer cleanup1()

	leaderWAL.Append(wal.OpSet, "k1", "v1")
	leaderWAL.Append(wal.OpSet, "k2", "v2")
	leaderWAL.Append(wal.OpDelete, "k1", "")
	leaderWAL.Append(wal.OpSet, "k3", "v3")
	leaderWAL.Append(wal.OpSet, "k2", "v2_updated")

	leader := NewLeader(leaderWAL, log.New(os.Stderr, "LEADER: ", log.Ltime|log.Lshortfile))
	
	err := leader.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("leader.Start failed: %v", err)
	}
	defer leader.Stop()

	addr := leader.listener.Addr().String()

	followerWAL, cleanup2 := setupTestWAL(t, "follower.wal")
	defer cleanup2()

	followerStore := store.New()
	follower := NewFollower(addr, followerStore, followerWAL, log.New(os.Stderr, "FOLLOWER: ", log.Ltime|log.Lshortfile))

	err = follower.Connect()
	if err != nil {
		t.Fatalf("follower.Connect failed: %v", err)
	}
	defer follower.Stop()

	time.Sleep(200 * time.Millisecond)

	if val, ok := followerStore.Get("k1"); ok {
		t.Errorf("k1 should be deleted, got %s", val)
	}
	if val, ok := followerStore.Get("k2"); !ok || val != "v2_updated" {
		t.Errorf("expected k2=v2_updated, got %v (ok=%v)", val, ok)
	}
	if val, ok := followerStore.Get("k3"); !ok || val != "v3" {
		t.Errorf("expected k3=v3, got %v (ok=%v)", val, ok)
	}
}

func TestMultipleFollowers(t *testing.T) {
	t.Parallel()

	leaderWAL, cleanup1 := setupTestWAL(t, "leader.wal")
	defer cleanup1()

	leader := NewLeader(leaderWAL, log.New(os.Stderr, "LEADER: ", log.Ltime|log.Lshortfile))
	
	err := leader.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("leader.Start failed: %v", err)
	}
	defer leader.Stop()

	addr := leader.listener.Addr().String()

	fWAL1, cleanupF1 := setupTestWAL(t, "f1.wal")
	defer cleanupF1()
	store1 := store.New()
	f1 := NewFollower(addr, store1, fWAL1, log.New(os.Stderr, "F1: ", log.Ltime|log.Lshortfile))
	
	err = f1.Connect()
	if err != nil {
		t.Fatalf("f1.Connect failed: %v", err)
	}
	defer f1.Stop()

	fWAL2, cleanupF2 := setupTestWAL(t, "f2.wal")
	defer cleanupF2()
	store2 := store.New()
	f2 := NewFollower(addr, store2, fWAL2, log.New(os.Stderr, "F2: ", log.Ltime|log.Lshortfile))

	err = f2.Connect()
	if err != nil {
		t.Fatalf("f2.Connect failed: %v", err)
	}
	defer f2.Stop()

	time.Sleep(50 * time.Millisecond)

	entries := []wal.Entry{
		{Index: 1, Op: wal.OpSet, Key: "common", Value: "data"},
	}

	for _, entry := range entries {
		leader.Replicate(entry)
	}

	time.Sleep(200 * time.Millisecond)

	for i, s := range []*store.Store{store1, store2} {
		if val, ok := s.Get("common"); !ok || val != "data" {
			t.Errorf("follower %d: expected common=data, got %v (ok=%v)", i+1, val, ok)
		}
	}
}
