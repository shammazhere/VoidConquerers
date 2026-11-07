package kvdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Open opens or creates the database at path and recovers from any previous
// crash. Recovery is idempotent: its only change to db.kv is one
// ftruncate+fsync to a length that is already a valid committed prefix.
//
// Phase numbers match ARCHITECTURE.md section 9.
func Open(path string, opts Options) (*DB, error) {
	t0 := time.Now()
	if opts.CompactMinBytes == 0 {
		opts.CompactMinBytes = 1 << 20 // 1 MiB
	}
	db := &DB{
		path:     path,
		tmpPath:  path + ".tmp",
		lockPath: path + ".lock",
		opts:     opts,
		index:    make(map[string]entry),
	}
	dir := filepath.Dir(path)

	// ---- Phase 0: lock and setup ----

	// 0.1 Take the exclusive lock FIRST. Nothing else is touched before this,
	// so a second process can never delete a live compaction's temp file.
	lf, err := os.OpenFile(db.lockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("%w: open lock file: %v", ErrIO, err)
	}
	if err := lockWithRetry(lf); err != nil {
		lf.Close()
		return nil, fmt.Errorf("%w: %s is in use by another process", ErrLocked, path)
	}
	db.lock = lf
	fail := func(err error) (*DB, error) {
		if db.f != nil {
			db.f.Close()
		}
		unlockFile(lf)
		lf.Close()
		return nil, err
	}

	_, statErr := os.Stat(path)
	switch {
	case errors.Is(statErr, fs.ErrNotExist):
		// 0.2 No database yet: create it via tmp + rename, so db.kv either
		// does not exist or has a complete header.
		db.trace("create")
		if err := db.createFile(path, db.tmpPath, dir); err != nil {
			return fail(err)
		}
		db.stats.Created = true
	case statErr != nil:
		return fail(fmt.Errorf("%w: stat: %v", ErrIO, statErr))
	default:
		// 0.3 db.kv exists. A db.tmp next to it is a compaction that never
		// completed its rename (or whose cleanup was cut short). db.kv is
		// authoritative, so the temp file is garbage (C3, C5).
		// A ".rejected.part" is a quarantine copy cut short by a crash.
		_ = os.Remove(db.path + ".rejected.part")
		if _, err := os.Stat(db.tmpPath); err == nil {
			// CP8 - CRASH HERE (before or after the Remove, before
			// syncDir): db.kv is authoritative; db.tmp may still exist.
			// Next open(): repeats this step if it does, then scans db.kv.
			db.crashPoint("CP8")
			if err := os.Remove(db.tmpPath); err != nil {
				return fail(fmt.Errorf("%w: remove stale tmp: %v", ErrIO, err))
			}
			if err := syncDir(dir); err != nil {
				return fail(fmt.Errorf("%w: fsync dir: %v", ErrIO, err))
			}
			db.stats.TmpRemoved = true
			db.trace("tmp-removed")
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fail(fmt.Errorf("%w: open: %v", ErrIO, err))
	}
	db.f = f

	// ---- Phase 1: sequential scan (writes nothing) ----
	db.trace("scan-start")
	res, err := scan(f, db.applyToIndex)
	if err != nil {
		return fail(err)
	}
	// CP9 - CRASH HERE (anywhere in the scan, up to this line): nothing has
	// been written by this open(). The file is exactly as the previous crash
	// left it, so the next open() runs the same scan and reaches the same
	// state (C1, F2).
	db.crashPoint("CP9")

	// ---- Phase 2: truncate everything after the last valid marker ----
	if res.size > res.lastSafe && sabotage != "no-truncate" { // sabotage: planted bug, dead code in normal builds
		// 2.0 Before cutting, keep a copy of the cut bytes for inspection
		// (rejected.go). Best effort, never applied, never touches db.kv.
		if saved, err := db.saveRejected(f, res.lastSafe, res.size); err == nil {
			db.stats.RejectedFile = saved
		} else {
			db.trace("rejected-copy-failed " + err.Error())
		}
		db.trace("truncate-start")
		db.pause()
		if err := f.Truncate(res.lastSafe); err != nil {
			return fail(fmt.Errorf("%w: truncate: %v", ErrIO, err))
		}
		db.pause()
		// CP10 - CRASH HERE: the file length is either the old length or
		// lastSafe. Both scan to the same lastSafe, so the next open()
		// reaches the same state and repeats the (now no-op or same) truncate (C2).
		db.crashPoint("CP10")
		if err := f.Sync(); err != nil {
			return fail(fmt.Errorf("%w: fsync after truncate: %v", ErrIO, err))
		}
		db.stats.TruncatedBytes = res.size - res.lastSafe
	}

	// ---- Phase 2b: complete a commit marker that lost its constant tail ----
	// (F4). Appending the missing constant bytes is idempotent: a crash
	// before, during or after leaves a file whose next scan completes the
	// same marker again.
	if len(res.markerFix) > 0 && res.lastSafe == res.size {
		if _, err := f.WriteAt(res.markerFix, res.size); err != nil {
			return fail(fmt.Errorf("%w: restore marker: %v", ErrIO, err))
		}
		if err := f.Sync(); err != nil {
			return fail(fmt.Errorf("%w: fsync after restoring marker: %v", ErrIO, err))
		}
		res.lastSafe += int64(len(res.markerFix))
		db.stats.MarkerRestored = len(res.markerFix)
	}

	// ---- Phase 3: ready ----
	db.fileSize = res.lastSafe
	if sabotage == "no-truncate" { // planted bug: append behind the garbage, dead code in normal builds
		db.fileSize = res.size
	}
	db.stats.Keys = len(db.index)
	db.stats.FileBytes = res.lastSafe
	db.stats.ScannedBytes = res.size
	db.stats.Records = res.applied
	db.stats.Batches = res.commits
	db.stats.Discarded = res.discarded
	if res.size > res.lastSafe {
		db.stats.StopReason = res.stopReason
	}
	db.stats.Duration = time.Since(t0)
	db.trace("ready")
	return db, nil
}

func lockWithRetry(lf *os.File) error {
	// Linux/macOS release a flock the instant the killed process is gone.
	// Windows may release a LockFileEx lock a moment after TerminateProcess,
	// so retry briefly before reporting DB_LOCKED.
	deadline := time.Now().Add(time.Second)
	for {
		err := lockFile(lf)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// createFile writes a fresh header to tmp, fsyncs it, renames it to path and
// fsyncs the directory. A crash leaves either no db.kv or a complete header.
func (db *DB) createFile(path, tmpPath, dir string) error {
	_ = os.Remove(tmpPath)
	t, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("%w: create: %v", ErrIO, err)
	}
	if _, err := t.Write(encodeHeader()); err != nil {
		t.Close()
		return fmt.Errorf("%w: write header: %v", ErrIO, err)
	}
	if err := t.Sync(); err != nil {
		t.Close()
		return fmt.Errorf("%w: fsync header: %v", ErrIO, err)
	}
	t.Close()
	// CP14 - CRASH HERE: db.tmp has a complete header, db.kv does not exist.
	// Next open(): "db.kv missing" path deletes db.tmp and creates again.
	db.crashPoint("CP14")
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("%w: rename: %v", ErrIO, err)
	}
	return syncDir(dir)
}

// scanResult describes one sequential pass over the file.
type scanResult struct {
	size       int64  // file size when the scan started
	lastSafe   int64  // end of the last valid commit marker (>= HeaderSize)
	records    int    // valid data records seen (committed or not)
	commits    int    // valid commit markers seen
	applied    int    // data records committed by a marker (accepted)
	discarded  int    // complete, valid data records with no marker after them (rejected)
	stopOff    int64  // where the scan stopped
	stopReason string // why it stopped
	// markerFix: the file ends in a commit marker that is missing 1-4 of its
	// last bytes. Those bytes are constants, so the marker was completed in
	// memory, verified by its CRC and committed; markerFix holds the missing
	// bytes, which open() appends (F4).
	markerFix []byte
}

// scan reads the whole file once. Data records are staged; when a valid
// marker whose count matches arrives, the staged ops are passed to apply in
// file order. It stops at EOF or at the first invalid record. It never writes.
func scan(f *os.File, apply func(del bool, key string, e entry)) (scanResult, error) {
	var res scanResult
	st, err := f.Stat()
	if err != nil {
		return res, fmt.Errorf("%w: stat: %v", ErrIO, err)
	}
	res.size = st.Size()

	hdr := make([]byte, HeaderSize)
	if _, err := f.ReadAt(hdr, 0); err != nil && !errors.Is(err, io.EOF) {
		return res, fmt.Errorf("%w: read header: %v", ErrIO, err)
	}
	if res.size < HeaderSize {
		return res, fmt.Errorf("%w: file is %d bytes, shorter than the header", ErrCorrupt, res.size)
	}
	if err := checkHeader(hdr); err != nil {
		return res, err
	}

	type staged struct {
		del bool
		key string
		e   entry
	}
	var pending []staged

	r := bufio.NewReaderSize(io.NewSectionReader(f, HeaderSize, res.size-HeaderSize), 8<<20)
	off := int64(HeaderSize)
	res.lastSafe = HeaderSize
	var rh [RecHdrSize]byte
	payload := make([]byte, 0, 64<<10)

scanLoop:
	for {
		// READ_HEADER
		n, err := io.ReadFull(r, rh[:])
		if err == io.EOF && n == 0 {
			res.stopReason = "end of file"
			break
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// The last 4 bytes of a commit marker are always the same
				// (KeyLen 0, Flags 0x08, Pad 0). If the file ends 8-11 bytes
				// into a marker (its tail cut off by hand, scenario F4, or a
				// kill while the marker was being written), complete it in
				// memory. It counts only if its CRC verifies and its count
				// equals the staged records, exactly like a whole marker. Its
				// records were fsynced before the marker was written, so
				// committing them is as safe as committing a whole marker.
				if n >= 8 && len(pending) > 0 && sabotage != "no-marker" {
					var full [RecHdrSize]byte
					copy(full[:], rh[:n])
					full[8], full[9], full[10], full[11] = 0, 0, FlagMarker, 0
					mh := parseRecHdr(full[:])
					if string(rh[8:n]) == string(full[8:n]) && recordCRC(full[:], nil) == mh.crc && int(mh.vlen) == len(pending) {
						for _, p := range pending {
							apply(p.del, p.key, p.e)
						}
						res.applied += len(pending)
						pending = pending[:0]
						res.commits++
						res.lastSafe = off + int64(n)
						res.markerFix = append([]byte(nil), full[n:]...)
						off += int64(n)
						res.stopReason = fmt.Sprintf("last commit marker was missing its last %d bytes (constants); restored", RecHdrSize-n)
						break
					}
				}
				res.stopReason = "torn record header (fewer than 12 bytes)"
				break
			}
			return res, fmt.Errorf("%w: read: %v", ErrIO, err)
		}
		// VALIDATE (lengths before reading anything else)
		h := parseRecHdr(rh[:])
		plen, ok := h.payloadLen()
		if !ok {
			res.stopReason = "invalid record header (flags/lengths)"
			break
		}
		if off+RecHdrSize+int64(plen) > res.size {
			res.stopReason = "record extends past end of file (torn write)"
			break
		}
		if cap(payload) < plen {
			payload = make([]byte, plen)
		}
		payload = payload[:plen]
		if _, err := io.ReadFull(r, payload); err != nil {
			return res, fmt.Errorf("%w: read payload: %v", ErrIO, err)
		}
		if recordCRC(rh[:], payload) != h.crc {
			// The whole record lies inside the file but its bytes are wrong.
			// A kill cannot cause this (a kill leaves a prefix of what was
			// written, so a torn record always runs past end of file). If a
			// valid commit marker follows, this is corruption in the middle
			// of committed data: truncating would destroy it, so refuse.
			if at := markerAfter(f, off+RecHdrSize+int64(plen), res.size); at >= 0 {
				return res, fmt.Errorf("%w: record at offset %d fails its CRC but a valid commit marker follows at offset %d; refusing to truncate committed data", ErrCorrupt, off, at)
			}
			res.stopReason = "CRC mismatch (torn or garbage record)"
			break
		}
		recSize := int64(RecHdrSize + plen)

		switch h.flags {
		case FlagPut | FlagBatch, FlagDel | FlagBatch:
			// STAGE
			pending = append(pending, staged{
				del: h.flags&FlagDel != 0,
				key: string(payload[:h.klen]),
				e:   entry{off: off, recSize: uint32(recSize), vlen: h.vlen},
			})
			res.records++
			if sabotage == "no-marker" { // planted bug, dead code in normal builds
				for _, p := range pending {
					apply(p.del, p.key, p.e)
				}
				pending = pending[:0]
				res.lastSafe = off + recSize
			}
		case FlagMarker:
			if sabotage == "no-marker" {
				res.lastSafe = off + recSize
				break
			}
			// COMMIT: only if the marker commits exactly the staged records.
			if int(h.vlen) != len(pending) {
				// Also impossible after a kill: the marker always carries the
				// count of the records written just before it.
				if at := markerAfter(f, off+recSize, res.size); at >= 0 {
					return res, fmt.Errorf("%w: commit marker at offset %d has the wrong count but a valid commit marker follows at offset %d; refusing to truncate committed data", ErrCorrupt, off, at)
				}
				res.stopReason = "commit marker count does not match staged records"
				break scanLoop
			}
			for _, p := range pending {
				apply(p.del, p.key, p.e)
			}
			res.applied += len(pending)
			pending = pending[:0]
			res.commits++
			res.lastSafe = off + recSize
		}
		off += recSize
	}
	res.stopOff = off // offset of the first byte that is not a valid record
	res.discarded = len(pending)
	return res, nil
}

// markerAfter walks valid records starting at `from` and returns the offset
// of the first CRC-valid commit marker it reaches, or -1 if it reaches end of
// file or an invalid record first. Used only after a record that is complete
// but damaged, to tell corruption (committed data follows) from a torn tail
// (nothing valid follows).
func markerAfter(f *os.File, from, size int64) int64 {
	if from >= size {
		return -1
	}
	r := bufio.NewReaderSize(io.NewSectionReader(f, from, size-from), 1<<20)
	off := from
	var rh [RecHdrSize]byte
	payload := make([]byte, 0, 64<<10)
	for {
		if _, err := io.ReadFull(r, rh[:]); err != nil {
			return -1
		}
		h := parseRecHdr(rh[:])
		plen, ok := h.payloadLen()
		if !ok || off+RecHdrSize+int64(plen) > size {
			return -1
		}
		if cap(payload) < plen {
			payload = make([]byte, plen)
		}
		payload = payload[:plen]
		if _, err := io.ReadFull(r, payload); err != nil {
			return -1
		}
		if recordCRC(rh[:], payload) != h.crc {
			return -1
		}
		if h.flags == FlagMarker && h.vlen > 0 {
			return off
		}
		off += RecHdrSize + int64(plen)
	}
}
