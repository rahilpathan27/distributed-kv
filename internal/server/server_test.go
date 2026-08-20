package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"os"
	"path/filepath"
	"github.com/rahilpathan27/distributed-kv/internal/store"
	"github.com/rahilpathan27/distributed-kv/internal/wal"
)

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	s := store.New()
	logger := log.New(io.Discard, "", 0)
	srv := New("127.0.0.1:0", s, logger)
	
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start listening
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

	return srv, addr
}

func sendCommand(t *testing.T, addr, cmd string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(cmd))
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read: %v", err)
	}
	return resp
}

func TestServerStartStop(t *testing.T) {
	srv, _ := startTestServer(t)
	err := srv.Stop()
	if err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}
}

func TestServerPing(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	resp := sendCommand(t, addr, "PING\n")
	if resp != "+PONG\n" {
		t.Errorf("Expected +PONG\\n, got %q", resp)
	}
}

func TestServerSetGet(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	resp1 := sendCommand(t, addr, "SET key1 value1\n")
	if resp1 != "+OK\n" {
		t.Errorf("Expected +OK\\n, got %q", resp1)
	}

	resp2 := sendCommand(t, addr, "GET key1\n")
	if resp2 != "+value1\n" {
		t.Errorf("Expected +value1\\n, got %q", resp2)
	}
}

func TestServerDelete(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	sendCommand(t, addr, "SET key2 val\n")
	resp1 := sendCommand(t, addr, "DELETE key2\n")
	if resp1 != "+OK\n" {
		t.Errorf("Expected +OK\\n, got %q", resp1)
	}

	resp2 := sendCommand(t, addr, "GET key2\n")
	if resp2 != "-ERR key not found\n" {
		t.Errorf("Expected -ERR key not found\\n, got %q", resp2)
	}
}

func TestServerGetMissing(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	resp := sendCommand(t, addr, "GET nonexisting\n")
	if resp != "-ERR key not found\n" {
		t.Errorf("Expected -ERR key not found\\n, got %q", resp)
	}
}

func TestServerDeleteMissing(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	resp := sendCommand(t, addr, "DELETE nonexisting\n")
	if resp != "-ERR key not found\n" {
		t.Errorf("Expected -ERR key not found\\n, got %q", resp)
	}
}

func TestServerInvalidCommand(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	resp := sendCommand(t, addr, "GARBAGECMD foo\n")
	if !strings.HasPrefix(resp, "-ERR ") {
		t.Errorf("Expected -ERR prefix, got %q", resp)
	}
}

func TestServerMultipleClients(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			key := fmt.Sprintf("k%d", id)
			val := fmt.Sprintf("v%d", id)
			
			setResp := sendCommand(t, addr, fmt.Sprintf("SET %s %s\n", key, val))
			if setResp != "+OK\n" {
				t.Errorf("Client %d expected +OK\\n, got %q", id, setResp)
			}
			
			getResp := sendCommand(t, addr, fmt.Sprintf("GET %s\n", key))
			if getResp != fmt.Sprintf("+%s\n", val) {
				t.Errorf("Client %d expected +%s\\n, got %q", id, val, getResp)
			}
		}(i)
	}
	wg.Wait()
}

func TestServerMultipleCommands(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	commands := []string{
		"SET k1 v1\n",
		"GET k1\n",
		"PING\n",
	}
	expected := []string{
		"+OK\n",
		"+v1\n",
		"+PONG\n",
	}

	for i, cmd := range commands {
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to write cmd %d: %v", i, err)
		}

		reader := bufio.NewReader(conn)
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read resp %d: %v", i, err)
		}

		if resp != expected[i] {
			t.Errorf("Cmd %d expected %q, got %q", i, expected[i], resp)
		}
	}
}

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

	resp := sendCommand(t, addr, "SET key1 val1\n")
	if resp != "+OK\n" {
		t.Fatalf("Expected +OK\n, got %q", resp)
	}

	resp = sendCommand(t, addr, "DELETE key1\n")
	if resp != "+OK\n" {
		t.Fatalf("Expected +OK\n, got %q", resp)
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
