// db.go — DB type, in-memory index, Get, ForEachSorted, Close, error codes.

package kvdb

import (
	"errors"
	"os"
	"sort"
	"sync"
)

// Error sentinels
var (
	ErrNotFound   = errors.New("kvdb: key not found")
	ErrDiskFull   = errors.New("kvdb: max file size reached")
	ErrPoisoned   = errors.New("kvdb: db is in error state, please close and reopen")
	ErrLocked     = errors.New("kvdb: database file is locked by another process")
	ErrBadFile    = errors.New("kvdb: file is not a valid KIPI database")
)

// indexEntry tracks where a key's value lives in the log.
type indexEntry struct {
	offset int64 // byte offset of the record header in the file
	valLen uint32
	keyLen uint32
	// For delete tombstones the entry is simply removed from the map.
}

// Options configure the database.
type Options struct {
	MaxBytes int64 // 0 = unlimited
}

// DB is the main database handle. All exported methods are safe to call
// concurrently from multiple goroutines.
type DB struct {
	mu       sync.RWMutex
	file     *os.File
	lockFile *os.File
	path     string
	opts     Options

	// In-memory index: key -> position in log
	index    map[string]indexEntry
	fileSize int64
	liveBytes int64 // bytes used by live (non-compacted) records

	// Auto-compaction threshold: compact when dead ratio > 50% and dead > 1MB
	poisoned bool
	closed   bool

	// nextBatchID is incremented for each committed batch.
	nextBatchID uint64
}

// Get returns the value for key, or ErrNotFound.
// Safe to call concurrently with writes.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, errors.New("kvdb: db is closed")
	}
	e, ok := db.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return db.readValue(e)
}

// readValue reads a value from the file given an index entry.
// Caller must hold at least a read lock.
func (db *DB) readValue(e indexEntry) ([]byte, error) {
	// Record layout: RecHeaderSize + keyLen + valLen
	offset := e.offset + int64(RecHeaderSize) + int64(e.keyLen)
	val := make([]byte, e.valLen)
	_, err := db.file.ReadAt(val, offset)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// ForEachSorted calls fn for each key-value pair in sorted key order.
// Caller must not modify the db inside fn.
func (db *DB) ForEachSorted(fn func(key, val []byte) error) error {
	db.mu.RLock()
	keys := make([]string, 0, len(db.index))
	entries := make(map[string]indexEntry, len(db.index))
	for k, e := range db.index {
		keys = append(keys, k)
		entries[k] = e
	}
	db.mu.RUnlock()

	sort.Strings(keys)
	for _, k := range keys {
		db.mu.RLock()
		e, ok := db.index[k]
		db.mu.RUnlock()
		if !ok {
			continue // was deleted concurrently
		}
		_ = entries[k]
		val, err := db.readValueUnlocked(e)
		if err != nil {
			return err
		}
		if err := fn([]byte(k), val); err != nil {
			return err
		}
	}
	return nil
}

// readValueUnlocked reads without taking any lock (must be safe; file is not truncated).
func (db *DB) readValueUnlocked(e indexEntry) ([]byte, error) {
	offset := e.offset + int64(RecHeaderSize) + int64(e.keyLen)
	val := make([]byte, e.valLen)
	_, err := db.file.ReadAt(val, offset)
	return val, err
}

// KeyCount returns the number of live keys.
func (db *DB) KeyCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.index)
}

// Close flushes and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	var errs []error
	if err := db.file.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := db.file.Close(); err != nil {
		errs = append(errs, err)
	}
	if db.lockFile != nil {
		unlockFile(db.lockFile)
		db.lockFile.Close()
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// shouldAutoCompact returns true when compaction would be beneficial.
// Must be called with write lock held.
func (db *DB) shouldAutoCompact(compactMinBytes int64) bool {
	if compactMinBytes <= 0 {
		compactMinBytes = 4 * 1024 * 1024 // 4 MB default
	}
	dead := db.fileSize - db.liveBytes
	return dead > compactMinBytes && db.fileSize > 0 && dead*2 > db.fileSize
}

