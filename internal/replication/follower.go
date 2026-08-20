package replication

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

// Follower connects to a leader and receives replicated entries.
type Follower struct {
	leaderAddr string
	store      *store.Store
	walLog     *wal.WAL
	conn       net.Conn
	logger     *log.Logger
	quit       chan struct{}
	wg         sync.WaitGroup
	mu         sync.Mutex
}

// NewFollower creates a new Follower.
func NewFollower(leaderAddr string, s *store.Store, w *wal.WAL, logger *log.Logger) *Follower {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Follower{
		leaderAddr: leaderAddr,
		store:      s,
		walLog:     w,
		logger:     logger,
		quit:       make(chan struct{}),
	}
}

// Connect establishes a connection to the leader and starts receiving entries.
// It sends SYNC <lastIndex> to request missing entries.
func (f *Follower) Connect() error {
	conn, err := net.Dial("tcp", f.leaderAddr)
	if err != nil {
		return err
	}
	f.conn = conn

	lastIndex := f.walLog.LastIndex()
	_, err = fmt.Fprintf(conn, "SYNC %d\n", lastIndex)
	if err != nil {
		conn.Close()
		return err
	}

	f.wg.Add(1)
	go f.receiveLoop()

	return nil
}

// Stop disconnects from the leader.
func (f *Follower) Stop() error {
	close(f.quit)
	if f.conn != nil {
		f.conn.Close()
	}
	f.wg.Wait()
	return nil
}

// receiveLoop reads REPLICATE messages from the leader.
func (f *Follower) receiveLoop() {
	defer f.wg.Done()
	scanner := bufio.NewScanner(f.conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 5)
		if len(parts) < 4 || parts[0] != "REPLICATE" {
			f.logger.Printf("Invalid message from leader: %s", line)
			continue
		}

		indexStr := parts[1]
		opStr := parts[2]
		key := parts[3]
		var value string
		if len(parts) == 5 {
			value = parts[4]
		}

		index, err := strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			f.logger.Printf("Invalid index: %v", err)
			continue
		}

		var op wal.OpType
		if opStr == "SET" {
			op = wal.OpSet
		} else if opStr == "DELETE" {
			op = wal.OpDelete
		} else {
			f.logger.Printf("Invalid operation: %s", opStr)
			continue
		}

		entry := wal.Entry{
			Index: index,
			Op:    op,
			Key:   key,
			Value: value,
		}

		err = f.applyEntry(entry)
		if err != nil {
			f.logger.Printf("Failed to apply entry: %v", err)
		}

		fmt.Fprintf(f.conn, "ACK %d\n", index)
	}

	if err := scanner.Err(); err != nil {
		f.logger.Printf("Error reading from leader: %v", err)
	}
}

// applyEntry applies a WAL entry to the local store and WAL.
func (f *Follower) applyEntry(entry wal.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.walLog.Append(entry.Op, entry.Key, entry.Value)
	if err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	if entry.Op == wal.OpSet {
		f.store.Set(entry.Key, entry.Value)
	} else if entry.Op == wal.OpDelete {
		f.store.Delete(entry.Key)
	}

	return nil
}
