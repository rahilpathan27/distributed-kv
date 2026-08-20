package cluster

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

// RecoveryManager handles follower reconnection and catch-up.
type RecoveryManager struct {
	store  *store.Store
	walLog *wal.WAL
	logger *log.Logger
	dialer func(network, address string) (net.Conn, error)
}

// NewRecoveryManager creates a new RecoveryManager.
func NewRecoveryManager(s *store.Store, w *wal.WAL, logger *log.Logger) *RecoveryManager {
	if logger == nil {
		logger = log.New(io.Discard, "", log.LstdFlags)
	}
	return &RecoveryManager{
		store:  s,
		walLog: w,
		logger: logger,
		dialer: net.Dial,
	}
}

// RequestSync connects to the leader and requests missing entries.
// It sends SYNC <lastIndex> and reads REPLICATE messages until caught up.
// Returns the number of entries synced.
func (rm *RecoveryManager) RequestSync(leaderAddr string) (int, error) {
	conn, err := rm.dialer("tcp", leaderAddr)
	if err != nil {
		return 0, fmt.Errorf("failed to dial leader: %w", err)
	}
	defer conn.Close()

	lastIndex := rm.walLog.LastIndex()
	if _, err := fmt.Fprintf(conn, "SYNC %d\n", lastIndex); err != nil {
		return 0, fmt.Errorf("failed to send SYNC: %w", err)
	}

	reader := bufio.NewReader(conn)
	synced := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return synced, fmt.Errorf("error reading from leader: %w", err)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 5)
		if len(parts) < 4 || parts[0] != "REPLICATE" {
			rm.logger.Printf("Unexpected message format: %s", line)
			continue
		}

		index, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			rm.logger.Printf("Invalid index: %s", parts[1])
			continue
		}

		var op wal.OpType
		switch parts[2] {
		case "SET":
			op = wal.OpSet
		case "DELETE":
			op = wal.OpDelete
		default:
			rm.logger.Printf("Invalid op type: %s", parts[2])
			continue
		}
		key := parts[3]
		var value string
		if len(parts) == 5 {
			value = parts[4]
		}

		// Append to WAL
		_, err = rm.walLog.Append(op, key, value)
		if err != nil {
			return synced, fmt.Errorf("failed to append to WAL: %w", err)
		}

		// Apply to store
		switch op {
		case wal.OpSet:
			rm.store.Set(key, value)
		case wal.OpDelete:
			rm.store.Delete(key)
		}

		synced++

		// Send ACK
		if _, err := fmt.Fprintf(conn, "ACK %d\n", index); err != nil {
			return synced, fmt.Errorf("failed to send ACK: %w", err)
		}
	}

	return synced, nil
}
