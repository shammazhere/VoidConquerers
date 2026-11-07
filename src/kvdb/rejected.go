package kvdb

// Rejected-write quarantine.
//
// When recovery finds an uncommitted tail (the write that was in progress
// when the process died), it must cut that tail off: those bytes were never
// committed and the client never got an ACK. Before cutting, recovery copies
// the tail into a separate file next to the database:
//
//	db.kv.rejected-<offset>-<crc>
//
// so a person can see exactly which writes were lost and send them again.
//
// The copy is for INSPECTION ONLY. Nothing in it is ever applied to the
// database, automatically or otherwise, because:
//   - the batch may be incomplete (applying part of it breaks atomicity);
//   - the client got no ACK and may already have retried (applying it again
//     would apply the write twice, possibly after newer writes);
//   - the bytes may be torn, so even a single record may be half there.
//
// Saving the copy is best effort: if it fails (for example the disk is full)
// recovery still cuts the tail and opens normally. It never changes the
// database file, so it cannot affect any crash guarantee. The copy is written
// to a ".part" file, fsynced and then renamed, so a crash leaves either no
// copy or a complete one; the name is derived from the tail's offset and CRC,
// so running recovery again after a crash does not create a duplicate.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// maxRejectedCopy caps how much of one tail is copied (a tail is at most one
// batch, but a batch of 10,000 x 1 MiB values could be very large).
const maxRejectedCopy = 64 << 20

// defaultKeepRejected is how many rejected-tail files are kept.
const defaultKeepRejected = 10

func rejectedPrefix(path string) string { return filepath.Base(path) + ".rejected-" }

// saveRejected copies file bytes [from, to) into a quarantine file and
// returns its path. It never modifies the database file.
func (db *DB) saveRejected(f *os.File, from, to int64) (string, error) {
	keep := db.opts.KeepRejected
	if keep < 0 {
		return "", nil
	}
	if keep == 0 {
		keep = defaultKeepRejected
	}
	n := to - from
	if n > maxRejectedCopy {
		n = maxRejectedCopy
	}
	dir := filepath.Dir(db.path)
	part := db.path + ".rejected.part"
	out, err := os.OpenFile(part, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	// The copy starts with a normal KIPI header so it can be read with the
	// same record format; the original offset is kept in the file name.
	crc := crc32.New(castagnoli)
	w := io.MultiWriter(out, crc)
	if _, err := out.Write(encodeHeader()); err == nil {
		_, err = io.Copy(w, io.NewSectionReader(f, from, n))
	}
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(part)
		return "", err
	}
	final := filepath.Join(dir, fmt.Sprintf("%s%d-%08x", rejectedPrefix(db.path), from, crc.Sum32()))
	if _, err := os.Stat(final); err == nil {
		// Same tail already saved by an earlier, interrupted recovery.
		_ = os.Remove(part)
		return final, nil
	}
	if err := os.Rename(part, final); err != nil {
		_ = os.Remove(part)
		return "", err
	}
	pruneRejected(db.path, keep)
	return final, nil
}

// RejectedFiles returns the quarantine files of the database at path, newest first.
func RejectedFiles(path string) ([]string, error) {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type fi struct {
		path string
		mod  time.Time
	}
	var files []fi
	prefix := rejectedPrefix(path)
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fi{filepath.Join(dir, e.Name()), info.ModTime()})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].mod.Equal(files[j].mod) {
			return files[i].path > files[j].path
		}
		return files[i].mod.After(files[j].mod)
	})
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.path
	}
	return out, nil
}

func pruneRejected(path string, keep int) {
	files, err := RejectedFiles(path)
	if err != nil {
		return
	}
	for i := keep; i < len(files); i++ {
		_ = os.Remove(files[i])
	}
}

// RejectedRecord is one entry found in a quarantine file.
type RejectedRecord struct {
	Kind  string // "PUT", "DEL", "MARKER" or "TORN"
	Key   []byte
	Value []byte
	Count int  // MARKER: number of records it commits
	Bytes int  // size of this entry in the file
	Valid bool // the record is complete and its CRC matches
}

// RejectedInfo describes one quarantine file.
type RejectedInfo struct {
	Path    string
	Offset  int64 // where the cut tail started in the database file
	Bytes   int64 // bytes of tail saved
	Saved   time.Time
	Records []RejectedRecord
}

// ReadRejected parses a quarantine file. A torn last record is reported as
// Kind "TORN" with its size.
func ReadRejected(file string) (RejectedInfo, error) {
	info := RejectedInfo{Path: file}
	st, err := os.Stat(file)
	if err != nil {
		return info, err
	}
	info.Saved = st.ModTime()
	name := filepath.Base(file)
	if i := strings.LastIndex(name, ".rejected-"); i >= 0 {
		fmt.Sscanf(name[i+len(".rejected-"):], "%d-", &info.Offset)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return info, err
	}
	if err := checkHeader(b); err != nil {
		return info, err
	}
	b = b[HeaderSize:]
	info.Bytes = int64(len(b))
	for len(b) > 0 {
		if len(b) < RecHdrSize {
			info.Records = append(info.Records, RejectedRecord{Kind: "TORN", Bytes: len(b)})
			break
		}
		h := parseRecHdr(b)
		n, ok := h.payloadLen()
		if !ok || RecHdrSize+n > len(b) {
			info.Records = append(info.Records, RejectedRecord{Kind: "TORN", Bytes: len(b)})
			break
		}
		rec := RejectedRecord{Bytes: RecHdrSize + n, Valid: recordCRC(b[:RecHdrSize], b[RecHdrSize:RecHdrSize+n]) == h.crc}
		switch {
		case h.flags == FlagMarker:
			rec.Kind, rec.Count = "MARKER", int(binary.LittleEndian.Uint32(b[4:8]))
		case h.flags&FlagDel != 0:
			rec.Kind, rec.Key = "DEL", append([]byte(nil), b[RecHdrSize:RecHdrSize+int(h.klen)]...)
		default:
			rec.Kind = "PUT"
			rec.Key = append([]byte(nil), b[RecHdrSize:RecHdrSize+int(h.klen)]...)
			rec.Value = append([]byte(nil), b[RecHdrSize+int(h.klen):RecHdrSize+n]...)
		}
		if !rec.Valid {
			info.Records = append(info.Records, RejectedRecord{Kind: "TORN", Bytes: len(b)})
			break
		}
		info.Records = append(info.Records, rec)
		b = b[RecHdrSize+n:]
	}
	return info, nil
}
