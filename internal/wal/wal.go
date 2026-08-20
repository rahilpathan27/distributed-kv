package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// OpType represents a WAL operation type.
type OpType uint8

const (
	OpSet    OpType = 1
	OpDelete OpType = 2
)

// Entry represents a single WAL entry.
type Entry struct {
	Index uint64
	Op    OpType
	Key   string
	Value string // empty for delete operations
}

// WAL is a write-ahead log that persists operations to disk before
// they are applied to the in-memory store.
type WAL struct {
	mu        sync.Mutex
	file      *os.File
	lastIndex uint64
}

func encodeEntry(e Entry) []byte {
	// [8 bytes: log index]
	// [1 byte: op type]
	// [2 bytes: key length]
	// [N bytes: key]
	// [4 bytes: value length]
	// [M bytes: value]
	// [4 bytes: CRC32 checksum]
	
	payloadLen := 8 + 1 + 2 + len(e.Key) + 4 + len(e.Value)
	buf := make([]byte, payloadLen+4) // +4 for CRC
	
	binary.BigEndian.PutUint64(buf[0:8], e.Index)
	buf[8] = byte(e.Op)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(e.Key)))
	copy(buf[11:11+len(e.Key)], e.Key)
	
	offset := 11 + len(e.Key)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(e.Value)))
	copy(buf[offset+4:offset+4+len(e.Value)], e.Value)
	
	crcOffset := payloadLen
	crc := crc32.ChecksumIEEE(buf[:crcOffset])
	binary.BigEndian.PutUint32(buf[crcOffset:crcOffset+4], crc)
	
	totalBuf := make([]byte, 4+len(buf))
	binary.BigEndian.PutUint32(totalBuf[0:4], uint32(len(buf)))
	copy(totalBuf[4:], buf)
	
	return totalBuf
}

func decodeEntry(data []byte) (Entry, error) {
	if len(data) < 19 {
		return Entry{}, errors.New("data too short")
	}
	
	crcExpected := binary.BigEndian.Uint32(data[len(data)-4:])
	crcActual := crc32.ChecksumIEEE(data[:len(data)-4])
	if crcExpected != crcActual {
		return Entry{}, errors.New("crc mismatch")
	}
	
	index := binary.BigEndian.Uint64(data[0:8])
	op := OpType(data[8])
	
	keyLen := int(binary.BigEndian.Uint16(data[9:11]))
	if 11+keyLen > len(data)-8 {
		return Entry{}, errors.New("invalid key length")
	}
	key := string(data[11 : 11+keyLen])
	
	offset := 11 + keyLen
	valLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	if offset+4+valLen != len(data)-4 {
		return Entry{}, errors.New("invalid value length")
	}
	val := string(data[offset+4 : offset+4+valLen])
	
	return Entry{
		Index: index,
		Op:    op,
		Key:   key,
		Value: val,
	}, nil
}

// Open opens or creates a WAL file at the given path.
// If the file already exists, it replays it to determine the last log index.
func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	w := &WAL{
		file: file,
	}

	var lastIndex uint64
	err = w.Replay(func(e Entry) error {
		lastIndex = e.Index
		return nil
	})
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to replay wal: %w", err)
	}

	w.lastIndex = lastIndex
	return w, nil
}

// Append writes an entry to the WAL and calls fsync.
// It assigns the next sequential log index to the entry.
// Returns the assigned log index.
func (w *WAL) Append(op OpType, key, value string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastIndex++
	entry := Entry{
		Index: w.lastIndex,
		Op:    op,
		Key:   key,
		Value: value,
	}

	data := encodeEntry(entry)
	if _, err := w.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync wal: %w", err)
	}

	return entry.Index, nil
}

// Replay reads all entries from the WAL and calls fn for each one.
// It validates CRC32 checksums and stops at the first corrupted entry.
// Returns nil if all entries are valid (including if the file is empty).
func (w *WAL) Replay(fn func(Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	// Make sure we are at the end for further appends
	defer func() {
		w.file.Seek(0, io.SeekEnd)
	}()

	var lengthBuf [4]byte
	for {
		_, err := io.ReadFull(w.file, lengthBuf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read entry length: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBuf[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			return fmt.Errorf("failed to read entry data (len=%d): %w", length, err)
		}

		entry, err := decodeEntry(data)
		if err != nil {
			return fmt.Errorf("corrupted entry: %w", err)
		}

		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}

// LastIndex returns the index of the last entry in the WAL.
// Returns 0 if the WAL is empty.
func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex
}

// EntriesFrom reads and returns all entries with index >= fromIndex.
// This is used for replica recovery/synchronization.
func (w *WAL) EntriesFrom(fromIndex uint64) ([]Entry, error) {
	var entries []Entry
	err := w.Replay(func(e Entry) error {
		if e.Index >= fromIndex {
			entries = append(entries, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
