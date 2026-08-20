package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rahilpathan27/distributed-kv/internal/cluster"
	"github.com/rahilpathan27/distributed-kv/internal/config"
	"github.com/rahilpathan27/distributed-kv/internal/replication"
	"github.com/rahilpathan27/distributed-kv/internal/server"
	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

func main() {
	cfg := config.ParseFlags()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Create data dir if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	logger := log.New(os.Stdout, fmt.Sprintf("[dkv:%s] ", cfg.NodeID), log.LstdFlags|log.Lmicroseconds)

	// Open WAL
	walPath := filepath.Join(cfg.DataDir, "wal.log")
	w, err := wal.Open(walPath)
	if err != nil {
		logger.Fatalf("Failed to open WAL: %v", err)
	}
	defer w.Close()

	// Create store
	s := store.New()

	// Replay WAL to reconstruct state
	replayedCount := 0
	if err := w.Replay(func(entry wal.Entry) error {
		if entry.Op == wal.OpSet {
			s.Set(entry.Key, entry.Value)
		} else if entry.Op == wal.OpDelete {
			s.Delete(entry.Key)
		}
		replayedCount++
		return nil
	}); err != nil {
		logger.Fatalf("Failed to replay WAL: %v", err)
	}
	if replayedCount > 0 {
		logger.Printf("Replayed %d WAL entries (last index: %d)", replayedCount, w.LastIndex())
	}

	// Create server with WAL
	clientAddr := fmt.Sprintf(":%d", cfg.Port)
	srv := server.NewWithWAL(clientAddr, s, w, logger)

	replAddr := fmt.Sprintf(":%d", cfg.ReplPort)

	if cfg.Role == "leader" {
		startLeader(cfg, srv, w, s, logger, clientAddr, replAddr)
	} else {
		startFollower(cfg, srv, w, s, logger, clientAddr, replAddr)
	}
}

func startLeader(cfg *config.Config, srv *server.Server, w *wal.WAL, s *store.Store, logger *log.Logger, clientAddr, replAddr string) {
	// Create leader replication manager
	leader := replication.NewLeader(w, logger)

	// Wire the server's OnWrite callback to replicate to followers
	srv.OnWrite = func(entry wal.Entry) {
		leader.Replicate(entry)
	}

	// Start replication listener
	if err := leader.Start(replAddr); err != nil {
		logger.Fatalf("Failed to start replication listener: %v", err)
	}
	logger.Printf("Replication listening on %s", replAddr)

	// Create heartbeat manager for monitoring followers
	hb := cluster.NewHeartbeatManager(1*time.Second, 3*time.Second, logger)
	hb.OnNodeDown = func(addr string) {
		logger.Printf("⚠ Follower %s is DOWN", addr)
	}
	hb.OnNodeUp = func(addr string) {
		logger.Printf("✓ Follower %s is back UP", addr)
	}
	hb.Start()

	// Start TCP server for client connections
	go func() {
		if err := srv.Start(); err != nil {
			logger.Fatalf("Server error: %v", err)
		}
	}()

	printBanner(clientAddr, replAddr, "LEADER", cfg.NodeID)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Println("Shutting down leader...")
	hb.Stop()
	leader.Stop()
	srv.Stop()
}

func startFollower(cfg *config.Config, srv *server.Server, w *wal.WAL, s *store.Store, logger *log.Logger, clientAddr, replAddr string) {
	// Create follower replication (connects to leader)
	follower := replication.NewFollower(cfg.LeaderAddr, s, w, logger)

	// Connect to leader (sends SYNC to catch up)
	if err := follower.Connect(); err != nil {
		logger.Printf("WARNING: Failed to connect to leader at %s: %v (will retry)", cfg.LeaderAddr, err)
	} else {
		logger.Printf("Connected to leader at %s (synced from index %d)", cfg.LeaderAddr, w.LastIndex())
	}

	// Start TCP server for client read connections
	go func() {
		if err := srv.Start(); err != nil {
			logger.Fatalf("Server error: %v", err)
		}
	}()

	printBanner(clientAddr, replAddr, "FOLLOWER", cfg.NodeID)
	logger.Printf("Leader: %s", cfg.LeaderAddr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Println("Shutting down follower...")
	follower.Stop()
	srv.Stop()
}

func printBanner(clientAddr, replAddr, role, nodeID string) {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║   Distributed KV Store v1.0          ║")
	fmt.Printf("║   Role: %-28s ║\n", role)
	fmt.Printf("║   Node: %-28s ║\n", nodeID)
	fmt.Printf("║   Client:  %-25s ║\n", clientAddr)
	fmt.Printf("║   Repl:    %-25s ║\n", replAddr)
	fmt.Println("╚══════════════════════════════════════╝")
}
