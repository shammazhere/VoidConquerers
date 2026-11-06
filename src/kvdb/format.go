// Package kvdb is a single-file, crash-safe key-value store.
//
// On-disk layout (all integers little-endian):
//
//	File   = Header(16 B) | Record | Record | ...        (append-only)
//	Header = "KIPI"(4) | Version u16 = 1 | Flags u16 = 0 | Reserved u64 = 0
//	Record = CRC32C u32 | ValLen u32 | KeyLen u16 | Flags u8 | Pad u8 | Key | Value
//
// CRC32C (Castagnoli) covers record bytes 4..end (ValLen, KeyLen, Flags, Pad,
// Key, Value). A commit marker is a 12-byte record with Flags = 0x08,
// KeyLen = 0 and ValLen = number of data records it commits. It has no payload.
package kvdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	HeaderSize  = 16
	RecHdrSize  = 12
	MaxKeyLen   = 1024
	MaxValLen   = 1 << 20 // 1 MiB
	MaxBatchOps = 10000
	Version     = 1

	FlagPut    = 0x01
	FlagDel    = 0x02
	FlagBatch  = 0x04 // set on every data record: "belongs to the batch the next marker commits"
	FlagMarker = 0x08
)

var magic = [4]byte{'K', 'I', 'P', 'I'}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means the file is not a file this database produced (bad
// header) or a committed record failed verification.
var ErrCorrupt = errors.New("CORRUPT")

func encodeHeader() []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], magic[:])
	binary.LittleEndian.PutUint16(b[4:6], Version)
	// b[6:8] flags = 0, b[8:16] reserved = 0
	return b
}

func checkHeader(b []byte) error {
	if len(b) < HeaderSize {
		return fmt.Errorf("%w: file shorter than the 16-byte header", ErrCorrupt)
	}
	if b[0] != magic[0] || b[1] != magic[1] || b[2] != magic[2] || b[3] != magic[3] {
		return fmt.Errorf("%w: bad magic (not a KIPI file)", ErrCorrupt)
	}
	if v := binary.LittleEndian.Uint16(b[4:6]); v != Version {
		return fmt.Errorf("%w: unsupported version", ErrCorrupt)
	}
	return nil
}

// appendRecord appends one data record (PUT or DEL) to dst and returns it.
// The CRC is computed last, over bytes 4..end of the record.
func appendRecord(dst []byte, flags byte, key, val []byte) []byte {
	start := len(dst)
	var h [RecHdrSize]byte
	binary.LittleEndian.PutUint32(h[4:8], uint32(len(val)))
	binary.LittleEndian.PutUint16(h[8:10], uint16(len(key)))
	h[10] = flags
	h[11] = 0
	dst = append(dst, h[:]...)
	dst = append(dst, key...)
	dst = append(dst, val...)
	crc := crc32.Checksum(dst[start+4:], castagnoli)
	binary.LittleEndian.PutUint32(dst[start:start+4], crc)
	return dst
}

// encodeMarker returns the 12-byte commit marker for a batch of n records.
func encodeMarker(n int) []byte {
	h := make([]byte, RecHdrSize)
	binary.LittleEndian.PutUint32(h[4:8], uint32(n))
	binary.LittleEndian.PutUint16(h[8:10], 0)
	h[10] = FlagMarker
	h[11] = 0
	binary.LittleEndian.PutUint32(h[0:4], crc32.Checksum(h[4:], castagnoli))
	return h
}

type recHdr struct {
	crc   uint32
	vlen  uint32
	klen  uint16
	flags byte
	pad   byte
}

func parseRecHdr(b []byte) recHdr {
	return recHdr{
		crc:   binary.LittleEndian.Uint32(b[0:4]),
		vlen:  binary.LittleEndian.Uint32(b[4:8]),
		klen:  binary.LittleEndian.Uint16(b[8:10]),
		flags: b[10],
		pad:   b[11],
	}
}

// payloadLen validates the header fields BEFORE any payload is read (so a
// torn or garbage length can never cause a huge allocation) and returns how
// many bytes follow the 12-byte header. ok=false means "not a valid record".
func (h recHdr) payloadLen() (n int, ok bool) {
	if h.pad != 0 {
		return 0, false
	}
	switch h.flags {
	case FlagPut | FlagBatch:
		if h.klen < 1 || h.klen > MaxKeyLen || h.vlen > MaxValLen {
			return 0, false
		}
		return int(h.klen) + int(h.vlen), true
	case FlagDel | FlagBatch:
		if h.klen < 1 || h.klen > MaxKeyLen || h.vlen != 0 {
			return 0, false
		}
		return int(h.klen), true
	case FlagMarker:
		if h.klen != 0 {
			return 0, false
		}
		return 0, true // ValLen is the op count, there is no payload
	default:
		return 0, false
	}
}

// recordCRC computes the CRC of a record from its 12-byte header and payload.
func recordCRC(hdr, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, hdr[4:RecHdrSize])
	return crc32.Update(c, castagnoli, payload)
}

// verifyPut checks a whole PUT record read back from disk (used by Get,
// Check and Compact). It returns the value slice.
func verifyPut(rec []byte, key string) ([]byte, error) {
	if len(rec) < RecHdrSize {
		return nil, ErrCorrupt
	}
	h := parseRecHdr(rec)
	n, ok := h.payloadLen()
	if !ok || h.flags != FlagPut|FlagBatch || RecHdrSize+n != len(rec) {
		return nil, ErrCorrupt
	}
	if recordCRC(rec[:RecHdrSize], rec[RecHdrSize:]) != h.crc {
		return nil, ErrCorrupt
	}
	k := rec[RecHdrSize : RecHdrSize+int(h.klen)]
	if string(k) != key {
		return nil, ErrCorrupt
	}
	return rec[RecHdrSize+int(h.klen):], nil
}
