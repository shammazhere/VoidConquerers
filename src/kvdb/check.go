// check.go — read-only integrity verification for `./kvdb check`.

package kvdb

import (
	"encoding/binary"
	"fmt"
	"os"
)

// CheckResult is returned by Check.
type CheckResult struct {
	OK            bool
	KeyCount      int
	UncommittedBytes int64 // bytes in uncommitted tail (not necessarily an error)
	Message       string
}

// Check opens the file at path read-only and verifies its integrity.
// It does NOT acquire a write lock (read-only).
func Check(path string) (*CheckResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()

	if fileSize == 0 {
		return &CheckResult{OK: false, Message: "file is empty"}, nil
	}

	// Validate header
	hdrBuf := make([]byte, FileHeaderSize)
	if _, err := f.ReadAt(hdrBuf, 0); err != nil {
		return &CheckResult{OK: false, Message: "cannot read header"}, nil
	}
	if !verifyFileHeader(hdrBuf) {
		return &CheckResult{OK: false, Message: "invalid file header (not a KIPI file)"}, nil
	}

	// Scan records
	pos := int64(FileHeaderSize)
	validEnd := int64(FileHeaderSize)
	keysSeen := make(map[string]bool)
	hdr := make([]byte, RecHeaderSize)
	batchOpen := false

	for pos+int64(RecHeaderSize) <= fileSize {
		if _, err := f.ReadAt(hdr, pos); err != nil {
			break
		}
		rtype := hdr[0]
		keyLen := binary.LittleEndian.Uint32(hdr[1:5])
		valLen := binary.LittleEndian.Uint32(hdr[5:9])
		storedCRC := binary.LittleEndian.Uint32(hdr[9:13])

		if keyLen > MaxKeyLen || valLen > MaxValLen {
			break
		}
		totalPayload := int64(keyLen) + int64(valLen)
		if pos+int64(RecHeaderSize)+totalPayload > fileSize {
			break
		}

		payload := make([]byte, totalPayload)
		if totalPayload > 0 {
			if _, err := f.ReadAt(payload, pos+int64(RecHeaderSize)); err != nil {
				break
			}
		}
		key := payload[:keyLen]
		val := payload[keyLen:]

		if !verifyRecord(rtype, key, val, storedCRC) {
			break
		}

		recEnd := pos + int64(RecHeaderSize) + totalPayload

		switch rtype {
		case typeRecord:
			keysSeen[string(key)] = true
			batchOpen = true
		case typeDelete:
			delete(keysSeen, string(key))
			batchOpen = true
		case typeMarker:
			validEnd = recEnd
			batchOpen = false
		default:
			break
		}

		pos = recEnd
	}

	uncommittedBytes := fileSize - validEnd
	if batchOpen {
		// There are uncommitted records in the tail — this is a torn tail.
		return &CheckResult{
			OK:               true, // recoverable
			KeyCount:         len(keysSeen),
			UncommittedBytes: uncommittedBytes,
			Message: fmt.Sprintf("OK (uncommitted tail: %d bytes; will be truncated on open)", uncommittedBytes),
		}, nil
	}

	return &CheckResult{
		OK:       true,
		KeyCount: len(keysSeen),
		Message:  "OK",
	}, nil
}

