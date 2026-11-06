package kvdb

import (
	"fmt"
	"os"
)

// writeChunk is the largest single write() we issue. Big records (1 MB
// values, 10,000-op batches) are written in several pieces, so a kill can
// genuinely land in the middle of a record (scenarios A2, A7, B1, B6).
const writeChunk = 256 << 10

// Put stores value under key.
func (db *DB) Put(key, value []byte) error {
	return db.Apply([]Op{{Key: key, Value: value}})
}

// Delete removes key. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	return db.Apply([]Op{{Del: true, Key: key}})
}

// Batch applies ops atomically, in listed order. For a repeated key the last
// op wins (put X, del X -> X absent; del X, put X -> X present).
func (db *DB) Batch(ops []Op) error { return db.Apply(ops) }

func validate(ops []Op) error {
	if len(ops) == 0 || len(ops) > MaxBatchOps {
		return fmt.Errorf("%w: batch must have 1..%d ops, got %d", ErrBadRequest, MaxBatchOps, len(ops))
	}
	for _, op := range ops {
		if len(op.Key) < 1 || len(op.Key) > MaxKeyLen {
			return fmt.Errorf("%w: key length %d not in 1..%d", ErrBadRequest, len(op.Key), MaxKeyLen)
		}
		if op.IsDel() && len(op.Value) != 0 {
			return fmt.Errorf("%w: delete with a value", ErrBadRequest)
		}
		if len(op.Value) > MaxValLen {
			return fmt.Errorf("%w: value length %d > %d", ErrBadRequest, len(op.Value), MaxValLen)
		}
	}
	return nil
}

func recSizeOf(op Op) int64 {
	if op.IsDel() {
		return int64(RecHdrSize + len(op.Key))
	}
	return int64(RecHdrSize + len(op.Key) + len(op.Value))
}

// Apply is the single write path used by Put, Delete and Batch. A single put
// is a batch of one. onCommit, if not nil, runs after the commit is durable
// and the index is updated, while the write lock is still held: serve prints
// its ACK there, so ACK lines come out in commit order.
//
// Step numbers match ARCHITECTURE.md section 5 and the diagram.
func (db *DB) Apply(ops []Op, onCommit ...func()) error {
	// Step 2: validate. Nothing is written for a bad request.
	if err := validate(ops); err != nil {
		return err
	}

	var cb func()
	if len(onCommit) > 0 {
		cb = onCommit[0]
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Step 1: reject if closed or poisoned.
	if db.closed {
		return ErrClosed
	}
	if db.poisoned || db.f == nil {
		return fmt.Errorf("%w: database is poisoned after an earlier I/O failure; restart to recover", ErrIO)
	}

	// Step 3: write_size = all data records + one 12-byte marker.
	var writeSize int64
	hasPut := false
	for _, op := range ops {
		writeSize += recSizeOf(op)
		if !op.IsDel() {
			hasPut = true
		}
	}
	writeSize += RecHdrSize

	// Step 4: space guard. May run compaction. Returns DISK_FULL before a
	// single byte is written, so a rejected batch leaves the file untouched
	// (E1, E2, E3).
	if err := db.spaceGuard(writeSize, hasPut); err != nil {
		return err
	}

	start := db.fileSize
	if sabotage == "ack-before-fsync" && cb != nil { // planted bug, dead code in normal builds
		cb()
		cb = nil
	}
	// CP1 - CRASH HERE: nothing of this commit is in the file yet. The file
	// ends at db.fileSize on a committed marker. Next open(): scans to the
	// same end, truncates nothing, the op is absent (A1).
	db.crashPoint("CP1")

	// Step 5: append the data records at the explicit offset `start`
	// (pwrite), never with O_APPEND, so a failed write cannot shift anything.
	offs, err := db.appendRecords(start, ops)
	if err != nil {
		db.rollback(start)
		return fmt.Errorf("%w: write: %v", ErrIO, err)
	}
	db.pause()
	// CP3 - CRASH HERE: all data records are in the page cache, not fsynced,
	// no marker. Next open(): the scan stages them, reaches EOF without a
	// marker, discards them and truncates to `start` (A3, B2).
	db.crashPoint("CP3")

	// Step 6: BARRIER 1. After this the data records are on disk.
	if err := db.f.Sync(); err != nil {
		// Never retry a failed fsync: the kernel may have dropped the dirty
		// pages and a retry can falsely succeed. Poison and let recovery decide.
		db.poison()
		return fmt.Errorf("%w: fsync 1: %v", ErrIO, err)
	}
	db.pause()
	// CP4 - CRASH HERE: data records durable, marker not written. Next
	// open(): same as CP3, the batch is discarded, file truncated to `start`.
	db.crashPoint("CP4")

	// Step 7: append the commit marker (ValLen = number of data records).
	markerOff := start + writeSize - RecHdrSize
	if err := writeAtChunked(db.f, encodeMarker(len(ops)), markerOff); err != nil {
		db.rollback(start)
		return fmt.Errorf("%w: write marker: %v", ErrIO, err)
	}
	db.pause()
	// CP5 - CRASH HERE: marker is in the page cache, not fsynced. A process
	// kill cannot lose page-cache bytes, so the marker is complete and the
	// next open() commits the batch; if it were torn, its CRC fails and the
	// batch is discarded. Either way all-or-nothing (A3, B3).
	db.crashPoint("CP5")

	// Step 8: BARRIER 2. After this the commit is durable.
	if err := db.f.Sync(); err != nil {
		db.poison()
		return fmt.Errorf("%w: fsync 2: %v", ErrIO, err)
	}
	db.pause()
	// CP6 - CRASH HERE: commit durable, index not updated, no ACK sent. Next
	// open(): finds the valid marker, applies the batch. The data is present
	// although the caller never heard back (A4).
	db.crashPoint("CP6")

	// Step 9: update the in-memory index, ops in listed order (last op wins).
	db.fileSize = start + writeSize
	for i, op := range ops {
		e := entry{off: offs[i], recSize: uint32(recSizeOf(op)), vlen: uint32(len(op.Value))}
		db.applyToIndex(op.IsDel(), string(op.Key), e)
	}

	// Step 10: ACK, still under the write lock.
	if cb != nil {
		cb()
	}
	// CP7 - CRASH HERE: ACK sent. Same disk state as CP6: present (A5).
	db.crashPoint("CP7")

	// Step 11: automatic compaction. Its failure does not change this
	// commit's result: the commit is already durable and acknowledged.
	db.maybeAutoCompact()
	return nil
	// Step 12: the deferred Unlock releases the write lock.
}

// applyToIndex applies one op to the index and keeps liveBytes exact.
// Recovery uses the same function, so replay == the original write.
func (db *DB) applyToIndex(del bool, key string, e entry) {
	if old, ok := db.index[key]; ok {
		db.liveBytes -= int64(old.recSize)
	}
	if del {
		delete(db.index, key)
		return
	}
	db.index[key] = e
	db.liveBytes += int64(e.recSize)
}

// appendRecords encodes the data records of ops and writes them starting at
// `start`. It returns the offset of each record.
func (db *DB) appendRecords(start int64, ops []Op) ([]int64, error) {
	offs := make([]int64, len(ops))
	buf := make([]byte, 0, writeChunk+RecHdrSize+MaxKeyLen)
	bufStart := start
	pos := start
	for i, op := range ops {
		offs[i] = pos
		if op.IsDel() {
			buf = appendRecord(buf, FlagDel|FlagBatch, op.Key, nil)
		} else {
			buf = appendRecord(buf, FlagPut|FlagBatch, op.Key, op.Value)
		}
		pos += recSizeOf(op)
		if len(buf) >= writeChunk {
			// CP2 - CRASH HERE (any iteration): a prefix of this batch is in
			// the file, possibly ending in the middle of a record. Next
			// open(): the torn record fails its length/EOF or CRC check, the
			// whole uncommitted run is discarded and the file is truncated to
			// `start` (A2, A7, B1, B6).
			if err := writeAtChunked(db.f, buf, bufStart); err != nil {
				return nil, err
			}
			db.crashPoint("CP2") // a prefix of the batch is now in the file
			db.pause()
			bufStart += int64(len(buf))
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		if err := writeAtChunked(db.f, buf, bufStart); err != nil {
			return nil, err
		}
	}
	return offs, nil
}

// writeAtChunked writes b at off in pieces of at most writeChunk bytes.
func writeAtChunked(f *os.File, b []byte, off int64) error {
	for len(b) > 0 {
		n := len(b)
		if n > writeChunk {
			n = writeChunk
		}
		if _, err := f.WriteAt(b[:n], off); err != nil {
			return err
		}
		b = b[n:]
		off += int64(n)
	}
	return nil
}

// rollback removes the partial bytes of a failed commit: ftruncate back to
// the last committed end, then fsync. If either fails we poison, because the
// file may now hold garbage after the committed end and the next append must
// not land behind it.
func (db *DB) rollback(start int64) {
	if err := db.f.Truncate(start); err != nil {
		db.poison()
		return
	}
	if err := db.f.Sync(); err != nil {
		db.poison()
	}
}

// reserve is the part of MaxBytes that only delete-only batches may use.
func (db *DB) reserve() int64 { return db.opts.MaxBytes / 8 }

// spaceGuard implements step 4. The limit is MaxBytes for delete-only
// batches and MaxBytes-reserve for batches containing a put.
func (db *DB) spaceGuard(writeSize int64, hasPut bool) error {
	if db.opts.MaxBytes <= 0 {
		return nil
	}
	limit := db.opts.MaxBytes
	if hasPut {
		limit -= db.reserve()
	}
	if db.fileSize+writeSize <= limit {
		return nil
	}
	// projected = file size right after a compaction: header + live records + one marker.
	projected := int64(HeaderSize) + db.liveBytes + RecHdrSize
	if projected+writeSize > limit {
		return fmt.Errorf("%w: write of %d bytes does not fit in max_bytes=%d even after compaction (file %d bytes, live %d bytes)",
			ErrDiskFull, writeSize, db.opts.MaxBytes, db.fileSize, db.liveBytes)
	}
	if err := db.compactLocked(); err != nil {
		return err
	}
	if db.fileSize+writeSize <= limit {
		return nil
	}
	return fmt.Errorf("%w: write of %d bytes does not fit in max_bytes=%d", ErrDiskFull, writeSize, db.opts.MaxBytes)
}

// maybeAutoCompact implements step 11: compact when the dead bytes exceed
// max(CompactMinBytes, liveBytes). That bounds the log to about
// 2 x live + CompactMinBytes, which bounds recovery time.
func (db *DB) maybeAutoCompact() {
	if db.opts.CompactMinBytes < 0 {
		return
	}
	reclaimable := db.fileSize - HeaderSize - db.liveBytes
	floor := db.opts.CompactMinBytes
	if db.liveBytes > floor {
		floor = db.liveBytes
	}
	if reclaimable > floor {
		if err := db.compactLocked(); err != nil {
			db.trace("compact-failed " + err.Error())
		}
	}
}
