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

	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

// FollowerState tracks a connected follower's state.
type FollowerState struct {
	Addr      string
	Conn      net.Conn
	LastIndex uint64
	mu        sync.Mutex
	writer    *bufio.Writer
}

// Leader manages replication to follower nodes.
type Leader struct {
	mu        sync.RWMutex
	followers map[string]*FollowerState
	walLog    *wal.WAL
	listener  net.Listener
	logger    *log.Logger
	quit      chan struct{}
	wg        sync.WaitGroup
}

// NewLeader creates a new Leader replication manager.
func NewLeader(w *wal.WAL, logger *log.Logger) *Leader {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Leader{
		followers: make(map[string]*FollowerState),
		walLog:    w,
		logger:    logger,
		quit:      make(chan struct{}),
	}
}

// Start begins listening for follower connections on the given address.
func (l *Leader) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	l.listener = listener
	l.wg.Add(1)
	go l.acceptLoop()
	return nil
}

// Addr returns the leader's replication listener address.
func (l *Leader) Addr() string {
	if l.listener != nil {
		return l.listener.Addr().String()
	}
	return ""
}

// Stop gracefully shuts down the leader replication.
func (l *Leader) Stop() error {
	close(l.quit)
	if l.listener != nil {
		l.listener.Close()
	}
	l.mu.Lock()
	for _, f := range l.followers {
		f.Conn.Close()
	}
	l.mu.Unlock()
	l.wg.Wait()
	return nil
}

func (l *Leader) acceptLoop() {
	defer l.wg.Done()
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			select {
			case <-l.quit:
				return
			default:
				l.logger.Printf("Failed to accept connection: %v", err)
				continue
			}
		}
		l.wg.Add(1)
		go l.handleFollower(conn)
	}
}

// Replicate sends a WAL entry to all connected followers.
// This is called by the server's OnWrite callback.
func (l *Leader) Replicate(entry wal.Entry) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	opStr := "SET"
	if entry.Op == wal.OpDelete {
		opStr = "DELETE"
	}
	
	var msg string
	if entry.Op == wal.OpDelete {
		msg = fmt.Sprintf("REPLICATE %d %s %s\n", entry.Index, opStr, entry.Key)
	} else {
		msg = fmt.Sprintf("REPLICATE %d %s %s %s\n", entry.Index, opStr, entry.Key, entry.Value)
	}

	for _, f := range l.followers {
		f.mu.Lock()
		_, err := f.writer.WriteString(msg)
		if err == nil {
			err = f.writer.Flush()
		}
		f.mu.Unlock()
		if err != nil {
			l.logger.Printf("Failed to replicate to follower %s: %v", f.Addr, err)
			f.Conn.Close()
		}
	}
}

// handleFollower handles a single follower connection.
func (l *Leader) handleFollower(conn net.Conn) {
	defer l.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	
	line, err := reader.ReadString('\n')
	if err != nil {
		l.logger.Printf("Failed to read SYNC from follower: %v", err)
		return
	}
	line = strings.TrimSpace(line)
	parts := strings.Split(line, " ")
	if len(parts) != 2 || parts[0] != "SYNC" {
		l.logger.Printf("Invalid SYNC command: %s", line)
		return
	}
	lastIndex, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		l.logger.Printf("Invalid SYNC index: %v", err)
		return
	}

	addr := conn.RemoteAddr().String()
	follower := &FollowerState{
		Addr:      addr,
		Conn:      conn,
		LastIndex: lastIndex,
		writer:    bufio.NewWriter(conn),
	}

	entries, err := l.walLog.EntriesFrom(lastIndex + 1)
	if err != nil {
		l.logger.Printf("Failed to get entries for follower: %v", err)
		return
	}

	follower.mu.Lock()
	for _, entry := range entries {
		opStr := "SET"
		if entry.Op == wal.OpDelete {
			opStr = "DELETE"
		}
		var msg string
		if entry.Op == wal.OpDelete {
			msg = fmt.Sprintf("REPLICATE %d %s %s\n", entry.Index, opStr, entry.Key)
		} else {
			msg = fmt.Sprintf("REPLICATE %d %s %s %s\n", entry.Index, opStr, entry.Key, entry.Value)
		}
		follower.writer.WriteString(msg)
	}
	follower.writer.Flush()
	follower.mu.Unlock()

	l.mu.Lock()
	l.followers[addr] = follower
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		delete(l.followers, addr)
		l.mu.Unlock()
	}()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				l.logger.Printf("Error reading from follower %s: %v", addr, err)
			}
			return
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ACK ") {
			ackIndexStr := strings.TrimPrefix(line, "ACK ")
			ackIndex, err := strconv.ParseUint(ackIndexStr, 10, 64)
			if err == nil {
				follower.mu.Lock()
				if ackIndex > follower.LastIndex {
					follower.LastIndex = ackIndex
				}
				follower.mu.Unlock()
			}
		}
	}
}
