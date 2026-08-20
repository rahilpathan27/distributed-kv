package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/rahilpathan27/distributed-kv/internal/protocol"
	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

// Server is a TCP server that handles client connections and
// executes key-value store commands.
type Server struct {
	addr     string
	store    *store.Store
	wal      *wal.WAL
	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
	logger   *log.Logger
	OnWrite  func(entry wal.Entry)
}

// New creates a new Server listening on the given address.
// The address should be in the format "host:port" (e.g., ":7070").
func New(addr string, s *store.Store, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{
		addr:   addr,
		store:  s,
		quit:   make(chan struct{}),
		logger: logger,
	}
}

// NewWithWAL creates a new Server with WAL integration.
func NewWithWAL(addr string, s *store.Store, w *wal.WAL, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{
		addr:   addr,
		store:  s,
		wal:    w,
		quit:   make(chan struct{}),
		logger: logger,
	}
}

// ReplayWAL replays WAL entries into the store.
func (srv *Server) ReplayWAL() error {
	if srv.wal == nil {
		return nil
	}
	return srv.wal.Replay(func(entry wal.Entry) error {
		if entry.Op == wal.OpSet {
			srv.store.Set(entry.Key, entry.Value)
		} else if entry.Op == wal.OpDelete {
			srv.store.Delete(entry.Key)
		}
		return nil
	})
}

// Start begins accepting TCP connections. It blocks until the server
// is stopped via Stop(). Each connection is handled in a separate goroutine.
func (srv *Server) Start() error {
	listener, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", srv.addr, err)
	}
	
	srv.mu.Lock()
	srv.listener = listener
	srv.mu.Unlock()
	
	srv.logger.Printf("Server listening on %s", srv.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-srv.quit:
				return nil
			default:
				srv.logger.Printf("Failed to accept connection: %v", err)
				continue
			}
		}

		srv.wg.Add(1)
		go func(c net.Conn) {
			defer srv.wg.Done()
			srv.handleConn(c)
		}(conn)
	}
}

// Stop gracefully shuts down the server. It stops accepting new connections
// and waits for all existing connections to finish.
func (srv *Server) Stop() error {
	close(srv.quit)
	
	srv.mu.Lock()
	if srv.listener != nil {
		err := srv.listener.Close()
		srv.mu.Unlock()
		if err != nil {
			return err
		}
	} else {
		srv.mu.Unlock()
	}
	
	srv.wg.Wait()
	return nil
}

// Addr returns the server's listening address. Useful when using port 0.
func (srv *Server) Addr() string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.listener != nil {
		return srv.listener.Addr().String()
	}
	return srv.addr
}

// handleConn handles a single client connection. It reads commands line by line,
// parses them, executes them against the store, and writes responses.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	srv.logger.Printf("Accepted connection from %s", conn.RemoteAddr())
	defer srv.logger.Printf("Closed connection from %s", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		cmd, err := protocol.ParseCommand(line)
		if err != nil {
			conn.Write([]byte(protocol.FormatError(err.Error())))
			continue
		}

		resp := srv.executeCommand(cmd)
		conn.Write([]byte(resp))
	}
	
	if err := scanner.Err(); err != nil {
		srv.logger.Printf("Error reading from connection %s: %v", conn.RemoteAddr(), err)
	}
}

// executeCommand executes a parsed command against the store and returns
// the response string.
func (srv *Server) executeCommand(cmd protocol.Command) string {
	switch cmd.Type {
	case protocol.CmdSet:
		if srv.wal != nil {
			index, err := srv.wal.Append(wal.OpSet, cmd.Key, cmd.Value)
			if err != nil {
				return protocol.FormatError(fmt.Sprintf("wal append error: %v", err))
			}
			if srv.OnWrite != nil {
				srv.OnWrite(wal.Entry{Index: index, Op: wal.OpSet, Key: cmd.Key, Value: cmd.Value})
			}
		}
		srv.store.Set(cmd.Key, cmd.Value)
		return protocol.FormatOK()
	case protocol.CmdGet:
		val, ok := srv.store.Get(cmd.Key)
		if !ok {
			return protocol.FormatError("key not found")
		}
		return protocol.FormatValue(val)
	case protocol.CmdDelete:
		if srv.wal != nil {
			index, err := srv.wal.Append(wal.OpDelete, cmd.Key, "")
			if err != nil {
				return protocol.FormatError(fmt.Sprintf("wal append error: %v", err))
			}
			if srv.OnWrite != nil {
				srv.OnWrite(wal.Entry{Index: index, Op: wal.OpDelete, Key: cmd.Key, Value: ""})
			}
		}
		ok := srv.store.Delete(cmd.Key)
		if !ok {
			return protocol.FormatError("key not found")
		}
		return protocol.FormatOK()
	case protocol.CmdPing:
		return protocol.FormatPong()
	default:
		return protocol.FormatError("unknown command type")
	}
}
