package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestOpenNew(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	if w.LastIndex() != 0 {
		t.Errorf("Expected LastIndex 0, got %d", w.LastIndex())
	}
}

func TestAppendAndReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		idx, err := w.Append(OpSet, key, val)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if idx != uint64(i) {
			t.Errorf("Expected index %d, got %d", i, idx)
		}
	}
	w.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open existing failed: %v", err)
	}
	defer w2.Close()

	if w2.LastIndex() != 5 {
		t.Errorf("Expected LastIndex 5, got %d", w2.LastIndex())
	}

	count := 0
	err = w2.Replay(func(e Entry) error {
		count++
		if e.Index != uint64(count) {
			t.Errorf("Expected index %d, got %d", count, e.Index)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected 5 entries, got %d", count)
	}
}

func TestAppendAutoIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 1; i <= 3; i++ {
		idx, err := w.Append(OpSet, "k", "v")
		if err != nil {
			t.Fatal(err)
		}
		if idx != uint64(i) {
			t.Errorf("Expected index %d, got %d", i, idx)
		}
	}
}

func TestReplayEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	count := 0
	err = w.Replay(func(e Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay empty WAL failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 entries, got %d", count)
	}
}

func TestPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(OpSet, "k1", "v1")
	w.Append(OpSet, "k2", "v2")
	w.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.LastIndex() != 2 {
		t.Errorf("Expected last index 2, got %d", w2.LastIndex())
	}
}

func TestCorruptionDetection(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(OpSet, "k1", "v1")
	w.Close()

	// Corrupt file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// flip a byte in the payload
	data[10] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Expected error on Open due to corruption")
	}
	if !strings.Contains(err.Error(), "crc mismatch") {
		t.Errorf("Expected CRC mismatch error, got %v", err)
	}
}

func TestEntriesFrom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 1; i <= 10; i++ {
		w.Append(OpSet, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	entries, err := w.EntriesFrom(6)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("Expected 5 entries, got %d", len(entries))
	}
	if entries[0].Index != 6 || entries[4].Index != 10 {
		t.Errorf("Unexpected entries: %v", entries)
	}
}

func TestConcurrentAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := w.Append(OpSet, fmt.Sprintf("k-%d-%d", id, j), "v")
				if err != nil {
					t.Errorf("Append failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	if w.LastIndex() != 1000 {
		t.Errorf("Expected last index 1000, got %d", w.LastIndex())
	}
}

func TestDeleteEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Append(OpDelete, "k1", "")

	entries, err := w.EntriesFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].Op != OpDelete {
		t.Errorf("Expected OpDelete, got %v", entries[0].Op)
	}
	if entries[0].Value != "" {
		t.Errorf("Expected empty value, got %s", entries[0].Value)
	}
}

func TestLargeValues(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	largeKey := strings.Repeat("K", 1024)
	largeVal := strings.Repeat("V", 1024)

	_, err = w.Append(OpSet, largeKey, largeVal)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := w.EntriesFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].Key != largeKey || entries[0].Value != largeVal {
		t.Error("Large key/value mismatch")
	}
}
