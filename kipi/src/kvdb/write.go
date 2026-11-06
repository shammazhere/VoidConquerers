// write.go — Apply: the single write path implementing steps 1-12.
//
// Crash points CP1-CP7 are documented inline.
//
// Write ordering for a batch:
//   1. Validate inputs (key/value length limits, disk-full guard)
//   2. For each op: append record bytes to buffer
//   3. Append commit marker to buffer
//   4. Write entire buffer to file (single write syscall where possible)
//   5. fsync (barrier 1) — CP5: killed here, marker MAY be on disk; recovery handles it
//   6. Update in-memory index
//   7. ACK to caller
//   8. Auto-compact if needed

package kvdb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	MaxKeyLen   = 1024
	MaxValLen   = 1024 * 1024 // 1 MB
	MaxBatchOps = 10000
)

// Op is a single operation in a batch.
type Op struct {
	Type  OpType
	Key   []byte
	Value []byte
}

// OpType distinguishes puts from deletes.
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Apply atomically applies one or more operations.
// This is the single write path. CP1-CP7 are crash points.
//
// Step 1: validate
// Step 2: disk-full guard
// Step 3: encode all records + marker into one buffer
// Step 4: write buffer to file
// Step 5: fsync (barrier — CP5: if killed here marker may be on disk)
// Step 6: update index
// Step 7: return nil (ACK happens in caller)
func (db *DB) Apply(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	if len(ops) > MaxBatchOps {
		return fmt.Errorf("kvdb: batch too large: %d ops (max %d)", len(ops), MaxBatchOps)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return errors.New("kvdb: db is closed")
	}
	if db.poisoned {
		return ErrPoisoned
	}

	// Step 1: Validate all ops before touching the file (CP1: nothing on disk yet)
	for _, op := range ops {
		if len(op.Key) == 0 || len(op.Key) > MaxKeyLen {
			return fmt.Errorf("kvdb: key length %d out of range [1,%d]", len(op.Key), MaxKeyLen)
		}
		if op.Type == OpPut && len(op.Value) > MaxValLen {
			return fmt.Errorf("kvdb: value length %d exceeds max %d", len(op.Value), MaxValLen)
		}
	}

	// Step 2: Estimate required bytes and check against limit
	needed := int64(0)
	for _, op := range ops {
		needed += int64(RecHeaderSize) + int64(len(op.Key))
		if op.Type == OpPut {
			needed += int64(len(op.Value))
		}
	}
	needed += int64(RecHeaderSize) + 8 // commit marker: keyLen=8, valLen=0

	// CP2: nothing written yet
	if db.opts.MaxBytes > 0 && db.fileSize+needed > db.opts.MaxBytes {
		return ErrDiskFull
	}

	// Step 3: Encode all records + marker into one buffer
	batchID := db.nextBatchID
	db.nextBatchID++

	buf := make([]byte, 0, int(needed))
	offsets := make([]int64, len(ops)) // file offsets of each record
	curOffset := db.fileSize

	for i, op := range ops {
		offsets[i] = curOffset
		switch op.Type {
		case OpPut:
			buf = appendRecord(buf, typeRecord, op.Key, op.Value)
		case OpDelete:
			buf = appendRecord(buf, typeDelete, op.Key, nil)
		}
		curOffset += int64(RecHeaderSize) + int64(len(op.Key))
		if op.Type == OpPut {
			curOffset += int64(len(op.Value))
		}
	}
	// Append commit marker
	markerBytes := encodeMarker(batchID)
	buf = append(buf, markerBytes...)

	// Step 4: Write to file (CP3: some bytes written, no marker yet)
	// CP4: all records written, marker not yet
	n, err := db.file.WriteAt(buf, db.fileSize)
	if err != nil || n != len(buf) {
		// Rollback: truncate to original size
		db.rollback(db.fileSize)
		if err != nil {
			return fmt.Errorf("kvdb: write error: %w", err)
		}
		return fmt.Errorf("kvdb: short write: wrote %d of %d bytes", n, len(buf))
	}
	// CP3/CP4 handled: if killed here, marker is not fsynced; recovery will truncate.

	// Step 5: fsync — barrier 1 (CP5: killed during fsync)
	// After fsync returns, all bytes including the marker are on disk.
	if err := db.file.Sync(); err != nil {
		db.rollback(db.fileSize)
		return fmt.Errorf("kvdb: fsync error: %w", err)
	}
	// CP6: fsync returned, we have not updated index yet.
	// Recovery: marker is on disk → batch will be replayed. Correct.

	// Step 6: Update in-memory index (atomic from Go's perspective under write lock)
	newFileSize := db.fileSize + int64(len(buf))
	for i, op := range ops {
		switch op.Type {
		case OpPut:
			db.index[string(op.Key)] = indexEntry{
				offset: offsets[i],
				keyLen: uint32(len(op.Key)),
				valLen: uint32(len(op.Value)),
			}
			db.liveBytes += int64(RecHeaderSize) + int64(len(op.Key)) + int64(len(op.Value))
		case OpDelete:
			if _, existed := db.index[string(op.Key)]; existed {
				old := db.index[string(op.Key)]
				db.liveBytes -= int64(RecHeaderSize) + int64(old.keyLen) + int64(old.valLen)
			}
			delete(db.index, string(op.Key))
		}
	}
	db.fileSize = newFileSize
	// CP7: index updated. ACK will be returned.

	return nil
}

// Put is a convenience wrapper for a single put.
func (db *DB) Put(key, value []byte) error {
	return db.Apply([]Op{{Type: OpPut, Key: key, Value: value}})
}

// Delete is a convenience wrapper for a single delete.
func (db *DB) Delete(key []byte) error {
	return db.Apply([]Op{{Type: OpDelete, Key: key}})
}

// rollback truncates the file to the given size.
// Should be called when a write fails partway through.
// Caller must hold write lock.
func (db *DB) rollback(size int64) {
	_ = db.file.Truncate(size)
}

// batchIDBytes encodes a uint64 as little-endian bytes.
func batchIDBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, id)
	return b
}

