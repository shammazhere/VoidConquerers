package kvdb

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Compact rewrites the file with only the live records. It holds the write
// lock for its whole duration (writers wait, readers wait).
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.poisoned || db.f == nil {
		return fmt.Errorf("%w: database is poisoned", ErrIO)
	}
	return db.compactLocked()
}

// compactLocked must be called with db.mu held for writing.
// Step numbers match ARCHITECTURE.md section 8.
func (db *DB) compactLocked() error {
	// Step 1: the compacted file is header + live records + one marker. If
	// that does not fit under max_bytes, abort before touching anything (E4).
	projected := int64(HeaderSize) + db.liveBytes + RecHdrSize
	if db.opts.MaxBytes > 0 && projected > db.opts.MaxBytes {
		return fmt.Errorf("%w: compacted size %d > max_bytes %d", ErrDiskFull, projected, db.opts.MaxBytes)
	}
	db.trace("compact-start")

	// Step 2: create db.tmp exclusively. We hold the process lock, so a file
	// with that name can only be a leftover of an earlier failed compaction
	// in this process; remove it first.
	_ = os.Remove(db.tmpPath)
	tmp, err := os.OpenFile(db.tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("%w: create %s: %v", ErrIO, db.tmpPath, err)
	}
	abort := func(e error) error {
		tmp.Close()
		os.Remove(db.tmpPath)
		db.trace("compact-aborted")
		return e
	}

	// Step 3: copy every live record, in file-offset order (sequential reads).
	// Each record is verified (CRC + key) and copied byte for byte: the
	// record format has no offsets in it, so the bytes stay valid at the new
	// position and recSize / liveBytes do not change.
	type item struct {
		key string
		e   entry
	}
	items := make([]item, 0, len(db.index))
	for k, e := range db.index {
		items = append(items, item{k, e})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].e.off < items[j].e.off })

	bw := bufio.NewWriterSize(tmp, 4<<20)
	if _, err := bw.Write(encodeHeader()); err != nil {
		return abort(fmt.Errorf("%w: %v", ErrIO, err))
	}
	newOffs := make([]int64, len(items))
	pos := int64(HeaderSize)
	var rec []byte
	for i, it := range items {
		if cap(rec) < int(it.e.recSize) {
			rec = make([]byte, it.e.recSize)
		}
		rec = rec[:it.e.recSize]
		if _, err := db.f.ReadAt(rec, it.e.off); err != nil {
			return abort(fmt.Errorf("%w: read: %v", ErrIO, err))
		}
		if _, err := verifyPut(rec, it.key); err != nil {
			return abort(err)
		}
		// CP11 - CRASH HERE (any iteration): db.tmp is partial and
		// unsynced; db.kv is untouched and authoritative. Next open():
		// Phase 0 sees both files and deletes db.tmp (C3).
		if _, err := bw.Write(rec); err != nil {
			return abort(fmt.Errorf("%w: write: %v", ErrIO, err))
		}
		if i == len(items)/2 {
			db.crashPoint("CP11")
		}
		newOffs[i] = pos
		pos += int64(it.e.recSize)
	}

	// Step 4: one commit marker for all live records.
	if _, err := bw.Write(encodeMarker(len(items))); err != nil {
		return abort(fmt.Errorf("%w: %v", ErrIO, err))
	}
	if err := bw.Flush(); err != nil {
		return abort(fmt.Errorf("%w: %v", ErrIO, err))
	}
	db.pause()
	if pos+RecHdrSize != projected {
		return abort(fmt.Errorf("%w: compaction size mismatch %d != %d", ErrCorrupt, pos+RecHdrSize, projected))
	}

	// Step 5: fsync the temp file. After this it is a complete, durable file.
	if err := tmp.Sync(); err != nil {
		return abort(fmt.Errorf("%w: fsync tmp: %v", ErrIO, err))
	}
	db.pause()
	// CP12 - CRASH HERE: db.tmp is complete and durable but has not replaced
	// db.kv. db.kv is still authoritative and holds everything db.tmp holds
	// (we hold the write lock, so nothing new was acknowledged meanwhile).
	// Next open(): deletes db.tmp (C3).
	db.crashPoint("CP12")

	// Steps 6 and 7: rename db.tmp over db.kv and swap the file handle.
	// Platform specific (swap_posix.go / swap_windows.go).
	if err := db.installCompacted(tmp); err != nil {
		return err
	}
	// CP13 - CRASH HERE: the rename is done; db.kv is the compacted file,
	// db.tmp no longer exists. Next open(): normal scan of the new file (C4, C5).
	db.crashPoint("CP13")

	// Step 7 (cont.): point the index at the new offsets before any further I/O.
	for i, it := range items {
		e := it.e
		e.off = newOffs[i]
		db.index[it.key] = e
	}
	db.fileSize = projected
	db.pause()

	// Step 8: fsync the directory so the rename itself is durable (this only
	// matters for power loss; a process kill cannot undo a completed rename).
	if err := syncDir(filepath.Dir(db.path)); err != nil {
		db.poison()
		return fmt.Errorf("%w: fsync dir: %v", ErrIO, err)
	}
	db.trace("compact-end")
	return nil
}
