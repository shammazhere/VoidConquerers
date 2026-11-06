package kvdb

import (
	"errors"
	"os"
	"sort"
	"sync"
	"time"
)

var (
	ErrDiskFull   = errors.New("DISK_FULL")
	ErrIO         = errors.New("IO_ERROR")
	ErrBadRequest = errors.New("BAD_REQUEST")
	ErrLocked     = errors.New("DB_LOCKED")
	ErrClosed     = errors.New("DB_CLOSED")
	ErrNotFound   = errors.New("NOTFOUND")
	ErrPoisoned   = errors.New("POISONED")
)

// OpType distinguishes puts from deletes.
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Options configure Open.
type Options struct {
	// MaxBytes caps the data file size. 0 means unlimited.
	// Batches that contain a PUT may only grow the file to MaxBytes-MaxBytes/8;
	// the last 1/8 is a reserve for delete-only batches, so a full database
	// can always delete (and then compact) its way out (scenario E5).
	MaxBytes int64
	// CompactMinBytes is the floor for automatic compaction: compact when
	// reclaimable > max(CompactMinBytes, liveBytes). 0 means 64 MiB,
	// negative disables automatic compaction.
	CompactMinBytes int64
	// Trace, if set, receives phase events ("scan-start", "compact-start", ...).
	// The torture tool uses them to report where its kills landed.
	Trace func(event string)
	// StepDelay (test only) sleeps between the I/O steps of the write path,
	// compaction and recovery truncation. It never changes the order of
	// any step; it only widens the windows between them so that random
	// kills land inside them even on disks whose fsync is very fast.
	StepDelay time.Duration
	// CrashAt (test only) names a crash point ("CP1".."CP14"). When the
	// engine reaches that line it kills its own process (see crash.go).
	// CrashAfter skips that many hits first. Empty means never.
	CrashAt    string
	CrashAfter int
}

// Op is one operation inside a batch.
type Op struct {
	Del   bool
	Type  OpType
	Key   []byte
	Value []byte
}

func (op Op) IsDel() bool {
	return op.Del || op.Type == OpDelete
}

// entry is what the in-memory index stores per live key. Values stay on disk.
type entry struct {
	off     int64  // file offset of the PUT record
	recSize uint32 // 12 + KeyLen + ValLen
	vlen    uint32
}

// RecoveryStats describes what the last Open had to do.
type RecoveryStats struct {
	Keys           int
	Duration       time.Duration
	TruncatedBytes int64 // bytes after the last valid commit marker that were cut off
	TmpRemoved     bool  // a leftover db.tmp from a killed compaction was deleted
	Created        bool
	FileBytes      int64
}

// DB is an open database. All methods are safe for concurrent use.
type DB struct {
	path, tmpPath, lockPath string
	opts                    Options

	lock *os.File // holds the OS file lock for the process lifetime

	// mu protects everything below. Writers (Apply, Compact) take it
	// exclusively for the whole commit; Get takes it shared.
	mu        sync.RWMutex
	f         *os.File
	index     map[string]entry
	fileSize  int64 // end of the last committed byte; the next append goes here
	liveBytes int64 // sum of recSize over all indexed records
	poisoned  bool
	closed    bool
	crashHits int // test only: hits of Options.CrashAt so far

	stats RecoveryStats
}

func (db *DB) pause() {
	if db.opts.StepDelay > 0 {
		time.Sleep(db.opts.StepDelay)
	}
}

func (db *DB) trace(ev string) {
	if db.opts.Trace != nil {
		db.opts.Trace(ev)
	}
}

// poison stops all further writes. It is called when the file may hold bytes
// we could not roll back or whose durability is unknown. Recovery on the next
// Open decides what is really on disk.
func (db *DB) poison() { db.poisoned = true }

// Get returns the value for key. found=false means "not found".
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, false, ErrClosed
	}
	e, ok := db.index[string(key)]
	if !ok {
		return nil, false, nil
	}
	if db.f == nil {
		return nil, false, ErrIO
	}
	rec := make([]byte, e.recSize)
	if _, err := db.f.ReadAt(rec, e.off); err != nil {
		return nil, false, ErrIO
	}
	v, err := verifyPut(rec, string(key))
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// ForEachSorted calls fn for every live key in ascending byte order, under
// the read lock, so the whole iteration sees one consistent state.
func (db *DB) ForEachSorted(fn func(key, value []byte) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.f == nil {
		return ErrIO
	}
	keys := make([]string, 0, len(db.index))
	for k := range db.index {
		keys = append(keys, k)
	}
	sort.Strings(keys) // Go string comparison is bytewise
	var rec []byte
	for _, k := range keys {
		e := db.index[k]
		if cap(rec) < int(e.recSize) {
			rec = make([]byte, e.recSize)
		}
		rec = rec[:e.recSize]
		if _, err := db.f.ReadAt(rec, e.off); err != nil {
			return ErrIO
		}
		v, err := verifyPut(rec, k)
		if err != nil {
			return err
		}
		if err := fn([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

// Stats is a snapshot of the engine counters.
type Stats struct {
	Keys        int
	FileBytes   int64
	LiveBytes   int64
	Reclaimable int64
	Poisoned    bool
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Stats{
		Keys:        len(db.index),
		FileBytes:   db.fileSize,
		LiveBytes:   db.liveBytes,
		Reclaimable: db.fileSize - HeaderSize - db.liveBytes,
		Poisoned:    db.poisoned,
	}
}

// KeyCount returns the number of live keys in the database.
func (db *DB) KeyCount() int {
	return db.Stats().Keys
}

// RecoveryStats returns what the Open that produced db had to repair.
func (db *DB) RecoveryStats() RecoveryStats { return db.stats }

// Close releases the file and the lock. Every commit was already fsynced
// before it returned, so Close writes nothing (scenario C6: a kill inside
// Close is like a kill at any other time).
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	var err error
	if db.f != nil {
		err = db.f.Close()
	}
	unlockFile(db.lock)
	db.lock.Close()
	return err
}
