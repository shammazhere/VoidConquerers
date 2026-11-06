// compact.go — Crash-safe compaction: write live records to .tmp, fsync, rename.
//
// Crash points CP11-CP13:
//   CP11: killed while writing .tmp  → tmp is incomplete; next open deletes it, old file intact
//   CP12: killed during rename       → either old or new wins; both are complete
//   CP13: killed after rename, before dir fsync → new file is authoritative; dir fsync
//         on Windows is omitted (rename is durable via NTFS journal)
//
// Compaction steps:
//   1. Write all live records to path.tmp (new file header + records + markers)
//   2. fsync path.tmp
//   3. Rename path.tmp → path   (atomic on POSIX; handled by installCompacted on Windows)
//   4. fsync directory (POSIX only)
//   5. Update in-memory index with new offsets
//   6. Update fileSize and liveBytes

package kvdb

import (
	"fmt"
	"os"
	"sort"
)

// Compact rewrites the database, dropping dead records.
// Must be called with no concurrent writers (the caller ensures this via
// the write lock in CompactLocked, or externally).
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compactLocked()
}

// compactLocked is the core compaction logic.
// Caller must hold the write lock.
func (db *DB) compactLocked() error {
	if db.closed {
		return fmt.Errorf("kvdb: db is closed")
	}

	tmpPath := db.path + ".tmp"

	// Step 1: Collect live keys in sorted order
	keys := make([]string, 0, len(db.index))
	for k := range db.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// CP11: Create and write the tmp file
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("kvdb: compact: could not create tmp file: %w", err)
	}

	// Write file header
	hdr := encodeFileHeader()
	if _, err := tmp.Write(hdr); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("kvdb: compact: could not write tmp header: %w", err)
	}

	// Write each live record as a single-op batch (record + marker)
	newIndex := make(map[string]indexEntry, len(keys))
	offset := int64(FileHeaderSize)
	batchID := uint64(0)
	var newLiveBytes int64

	for _, k := range keys {
		e, ok := db.index[k]
		if !ok {
			continue
		}
		val, err := db.readValueUnlocked(e)
		if err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("kvdb: compact: could not read value for key %q: %w", k, err)
		}

		key := []byte(k)
		recBuf := appendRecord(nil, typeRecord, key, val)
		markerBuf := encodeMarker(batchID)
		batchID++

		recOffset := offset
		payload := append(recBuf, markerBuf...)
		if _, err := tmp.Write(payload); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("kvdb: compact: write error: %w", err)
		}

		newIndex[k] = indexEntry{
			offset: recOffset,
			keyLen: uint32(len(key)),
			valLen: uint32(len(val)),
		}
		newLiveBytes += int64(RecHeaderSize) + int64(len(key)) + int64(len(val))
		offset += int64(len(payload))
	}

	// Step 2: fsync the tmp file
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("kvdb: compact: fsync tmp: %w", err)
	}

	// Step 3+4+5: Platform-specific: close old file descriptor, rename, reopen.
	// CP12: if killed during rename, either old or new wins.
	// CP13: after rename, dir fsync (POSIX) or no-op (Windows).
	newFile, newSize, err := installCompacted(tmp, tmpPath, db.path, db.file, db.lockFile)
	if err != nil {
		tmp.Close()
		return fmt.Errorf("kvdb: compact: install error: %w", err)
	}

	// Step 6: Update in-memory state
	db.file = newFile
	db.index = newIndex
	db.fileSize = newSize
	db.liveBytes = newLiveBytes
	db.nextBatchID = batchID

	return nil
}

