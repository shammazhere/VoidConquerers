// recovery.go — Open(): lock, tmp cleanup, scan log, truncate torn tail, build index.
//
// Crash points CP8-CP10 and CP14 are documented inline.
//
// Recovery phases:
//   Phase 0: Acquire file lock (prevent concurrent opens)
//   Phase 1: Create file if it does not exist; validate header
//   Phase 2: Delete any leftover .tmp file (from a previous aborted compaction)
//   Phase 3: Scan the log forward; collect all fully committed batches;
//             truncate any torn tail at the first invalid record.
//
// A "torn tail" is any sequence of bytes at the end of the file that does
// not form a complete, CRC-valid, committed record.  We truncate to the
// byte just after the last valid commit marker.

package kvdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Open opens or creates a KIPI database at path.
// It acquires an exclusive lock, recovers from any crash, and returns a ready DB.
func Open(path string, opts Options) (*DB, error) {
	start := time.Now()

	// Phase 0: Acquire exclusive lock (CP14: if killed during lock, next open succeeds)
	lockPath := path + ".lock"
	lockFile, err := lockFile(lockPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrLocked, err)
	}

	// Phase 1: Create or open the data file
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		unlockFile(lockFile)
		lockFile.Close()
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		unlockFile(lockFile)
		lockFile.Close()
		return nil, err
	}

	if fi.Size() == 0 {
		// New file: write header
		hdr := encodeFileHeader()
		if _, err := f.Write(hdr); err != nil {
			f.Close()
			unlockFile(lockFile)
			lockFile.Close()
			return nil, fmt.Errorf("kvdb: could not write file header: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			unlockFile(lockFile)
			lockFile.Close()
			return nil, fmt.Errorf("kvdb: could not fsync new file: %w", err)
		}
	} else {
		// Existing file: validate header
		hdrBuf := make([]byte, FileHeaderSize)
		if _, err := f.ReadAt(hdrBuf, 0); err != nil {
			f.Close()
			unlockFile(lockFile)
			lockFile.Close()
			return nil, fmt.Errorf("%w: could not read header", ErrBadFile)
		}
		if !verifyFileHeader(hdrBuf) {
			f.Close()
			unlockFile(lockFile)
			lockFile.Close()
			return nil, ErrBadFile
		}
	}

	// Phase 2: Clean up leftover .tmp file (C3/C4/C5 compaction crash recovery)
	// CP8: If we were killed while cleaning tmp, we just try again next open.
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath) // ignore error; it may not exist

	// Phase 3: Scan the log to build the in-memory index
	// CP9: If killed during scan, the next open re-scans from scratch. Idempotent.
	db := &DB{
		file:     f,
		lockFile: lockFile,
		path:     path,
		opts:     opts,
		index:    make(map[string]indexEntry),
	}

	validEnd, nextBatchID, err := db.scanLog()
	if err != nil {
		f.Close()
		unlockFile(lockFile)
		lockFile.Close()
		return nil, fmt.Errorf("kvdb: scan error: %w", err)
	}

	// CP10: Truncate torn tail to validEnd.
	// If killed during truncation, next open will truncate again (idempotent).
	fi2, _ := f.Stat()
	if fi2.Size() > validEnd {
		if err := f.Truncate(validEnd); err != nil {
			f.Close()
			unlockFile(lockFile)
			lockFile.Close()
			return nil, fmt.Errorf("kvdb: could not truncate torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			unlockFile(lockFile)
			lockFile.Close()
			return nil, fmt.Errorf("kvdb: could not fsync after truncate: %w", err)
		}
	}

	db.fileSize = validEnd
	db.nextBatchID = nextBatchID

	// Compute liveBytes from index
	for _, e := range db.index {
		db.liveBytes += int64(RecHeaderSize) + int64(e.keyLen) + int64(e.valLen)
	}

	elapsed := time.Since(start)
	_ = elapsed // available for callers that want to report recovery time
	_ = filepath.Base(path)

	return db, nil
}

// scanLog reads the log forward from byte FileHeaderSize.
// It applies committed batches to the index and returns:
//   - validEnd: file offset up to and including the last valid commit marker
//   - nextBatchID: one greater than the highest committed batchID seen
//   - err: any I/O error
//
// Records not followed by a matching commit marker are simply ignored (torn tail).
// The in-memory index is populated from committed records only.
func (db *DB) scanLog() (validEnd int64, nextBatchID uint64, err error) {
	fi, err := db.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	fileSize := fi.Size()
	if fileSize < int64(FileHeaderSize) {
		return int64(FileHeaderSize), 0, nil
	}

	// We scan in two stages:
	// 1. Read all records into uncommitted batches
	// 2. When we see a commit marker, commit that batch to the index

	// pending accumulates records for the current (uncommitted) batch
	type pendingRec struct {
		rtype  uint8
		key    []byte
		val    []byte
		offset int64
	}
	var pending []pendingRec

	pos := int64(FileHeaderSize)
	validEnd = int64(FileHeaderSize)
	hdr := make([]byte, RecHeaderSize)

	for pos+int64(RecHeaderSize) <= fileSize {
		if _, err := db.file.ReadAt(hdr, pos); err != nil {
			break // treat as torn tail
		}
		rtype := hdr[0]
		keyLen := binary.LittleEndian.Uint32(hdr[1:5])
		valLen := binary.LittleEndian.Uint32(hdr[5:9])
		storedCRC := binary.LittleEndian.Uint32(hdr[9:13])

		// Sanity-check sizes to prevent OOM on garbage data
		if keyLen > MaxKeyLen || valLen > MaxValLen {
			break // torn or garbage tail
		}
		totalPayload := int64(keyLen) + int64(valLen)
		if pos+int64(RecHeaderSize)+totalPayload > fileSize {
			break // truncated record (torn tail)
		}

		// Read key + val
		payload := make([]byte, totalPayload)
		if totalPayload > 0 {
			if _, err := db.file.ReadAt(payload, pos+int64(RecHeaderSize)); err != nil {
				break
			}
		}
		key := payload[:keyLen]
		val := payload[keyLen:]

		// Verify CRC
		if !verifyRecord(rtype, key, val, storedCRC) {
			break // corrupted record — treat as torn tail
		}

		recEnd := pos + int64(RecHeaderSize) + totalPayload

		switch rtype {
		case typeRecord:
			pending = append(pending, pendingRec{rtype: typeRecord, key: key, val: val, offset: pos})
		case typeDelete:
			pending = append(pending, pendingRec{rtype: typeDelete, key: key, offset: pos})
		case typeMarker:
			// A commit marker: commit all pending records to index
			batchID := binary.LittleEndian.Uint64(key)
			for _, r := range pending {
				switch r.rtype {
				case typeRecord:
					db.index[string(r.key)] = indexEntry{
						offset: r.offset,
						keyLen: uint32(len(r.key)),
						valLen: uint32(len(r.val)),
					}
				case typeDelete:
					delete(db.index, string(r.key))
				}
			}
			pending = pending[:0]
			validEnd = recEnd
			if batchID >= nextBatchID {
				nextBatchID = batchID + 1
			}
		default:
			break // unknown record type — treat as torn tail
		}

		pos = recEnd
	}

	// Any pending (uncommitted) records are discarded — they form the torn tail.
	return validEnd, nextBatchID, nil
}

