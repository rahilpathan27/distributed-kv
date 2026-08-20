import os
import sys

# server.go
server_file = "internal/server/server.go"
with open(server_file, "r") as f:
    server_content = f.read()

server_content = server_content.replace(
"""	"github.com/rahilpathan/distributed-kv/internal/protocol"
	"github.com/rahilpathan/distributed-kv/internal/store"
)""",
"""	"github.com/rahilpathan/distributed-kv/internal/protocol"
	"github.com/rahilpathan/distributed-kv/internal/store"
	"github.com/rahilpathan/distributed-kv/internal/wal"
)"""
)

server_content = server_content.replace(
"""type Server struct {
	addr     string
	store    *store.Store
	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
	logger   *log.Logger
}""",
"""type Server struct {
	addr     string
	store    *store.Store
	wal      *wal.WAL
	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
	logger   *log.Logger
	OnWrite  func(entry wal.Entry)
}"""
)

server_content = server_content.replace(
"""func New(addr string, s *store.Store, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{
		addr:   addr,
		store:  s,
		quit:   make(chan struct{}),
		logger: logger,
	}
}""",
"""func New(addr string, s *store.Store, logger *log.Logger) *Server {
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
}"""
)

server_content = server_content.replace(
"""	case protocol.CmdSet:
		srv.store.Set(cmd.Key, cmd.Value)
		return protocol.FormatOK()""",
"""	case protocol.CmdSet:
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
		return protocol.FormatOK()"""
)

server_content = server_content.replace(
"""	case protocol.CmdDelete:
		ok := srv.store.Delete(cmd.Key)
		if !ok {
			return protocol.FormatError("key not found")
		}
		return protocol.FormatOK()""",
"""	case protocol.CmdDelete:
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
		return protocol.FormatOK()"""
)

with open(server_file, "w") as f:
    f.write(server_content)

# server_test.go
server_test_file = "internal/server/server_test.go"
with open(server_test_file, "r") as f:
    test_content = f.read()

test_content = test_content.replace(
"""	"github.com/rahilpathan/distributed-kv/internal/store"
)""",
"""	"os"
	"path/filepath"
	"github.com/rahilpathan/distributed-kv/internal/store"
	"github.com/rahilpathan/distributed-kv/internal/wal"
)"""
)

test_content += """
func TestServerWALIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "wal.log")
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	s := store.New()
	logger := log.New(io.Discard, "", 0)
	srv := NewWithWAL("127.0.0.1:0", s, w, logger)

	go func() {
		srv.Start()
	}()

	var addr string
	for i := 0; i < 50; i++ {
		addr = srv.Addr()
		if addr != "127.0.0.1:0" && addr != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "127.0.0.1:0" || addr == "" {
		t.Fatalf("Server address not bound")
	}

	resp := sendCommand(t, addr, "SET key1 val1\\n")
	if resp != "+OK\\n" {
		t.Fatalf("Expected +OK\\n, got %q", resp)
	}

	resp = sendCommand(t, addr, "DELETE key1\\n")
	if resp != "+OK\\n" {
		t.Fatalf("Expected +OK\\n, got %q", resp)
	}

	err = srv.Stop()
	if err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
	w.Close()

	w2, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.EntriesFrom(1)
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0].Op != wal.OpSet || entries[0].Key != "key1" || entries[0].Value != "val1" {
		t.Errorf("Unexpected entry 1: %+v", entries[0])
	}
	if entries[1].Op != wal.OpDelete || entries[1].Key != "key1" {
		t.Errorf("Unexpected entry 2: %+v", entries[1])
	}
}
"""

with open(server_test_file, "w") as f:
    f.write(test_content)

print("Modified server.go and server_test.go")
