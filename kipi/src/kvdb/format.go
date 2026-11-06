// format.go — on-disk encoding and decoding for KIPI records.
//
// On-disk layout (append-only log):
//
//   File header (16 bytes):
//     [0:4]   magic   "KIPI"
//     [4:8]   version uint32 LE = 1
//     [8:16]  reserved (zeros)
//
//   Each record (12-byte header + payload):
//     [0:1]   recType  uint8  (typeRecord=1, typeDelete=2, typeMarker=3)
//     [1:5]   keyLen   uint32 LE
//     [5:9]   valLen   uint32 LE  (0 for delete / marker)
//     [9:13]  crc32c   uint32 LE  — CRC32C of (recType||keyLen||valLen||key||val)
//     [13:]   key bytes
//     [13+keyLen:] value bytes
//
//   Commit marker record:
//     recType = typeMarker, keyLen = 8, valLen = 0
//     key bytes = batchID uint64 LE
//     crc32c covers the above
//
// A batch is "committed" only when its marker record is fully written AND fsynced.
// Recovery scans from the start and collects committed batches only.

package kvdb

import (
	"encoding/binary"
	"hash/crc32"
)

// Record types
const (
	typeRecord uint8 = 1
	typeDelete uint8 = 2
	typeMarker uint8 = 3
)

// Sizes
const (
	FileHeaderSize = 16
	RecHeaderSize  = 13 // recType(1) + keyLen(4) + valLen(4) + crc32c(4)
)

// File magic
var fileMagic = [4]byte{'K', 'I', 'P', 'I'}

// crcTable uses Castagnoli polynomial (CRC32C).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// crc32c computes a CRC32C checksum over data.
func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// encodeFileHeader returns the 16-byte file header.
func encodeFileHeader() []byte {
	buf := make([]byte, FileHeaderSize)
	copy(buf[0:4], fileMagic[:])
	binary.LittleEndian.PutUint32(buf[4:8], 1) // version
	return buf
}

// verifyFileHeader returns true if buf starts with a valid KIPI header.
func verifyFileHeader(buf []byte) bool {
	if len(buf) < FileHeaderSize {
		return false
	}
	if buf[0] != 'K' || buf[1] != 'I' || buf[2] != 'P' || buf[3] != 'I' {
		return false
	}
	ver := binary.LittleEndian.Uint32(buf[4:8])
	return ver == 1
}

// appendRecord encodes a put/delete record and appends it to dst.
// For deletes, val must be nil/empty and rtype must be typeDelete.
func appendRecord(dst []byte, rtype uint8, key, val []byte) []byte {
	kl := uint32(len(key))
	vl := uint32(len(val))

	// Compute CRC over header fields + key + val
	crcBuf := make([]byte, 1+4+4+int(kl)+int(vl))
	crcBuf[0] = rtype
	binary.LittleEndian.PutUint32(crcBuf[1:5], kl)
	binary.LittleEndian.PutUint32(crcBuf[5:9], vl)
	copy(crcBuf[9:9+kl], key)
	copy(crcBuf[9+kl:], val)
	crc := crc32c(crcBuf)

	hdr := make([]byte, RecHeaderSize)
	hdr[0] = rtype
	binary.LittleEndian.PutUint32(hdr[1:5], kl)
	binary.LittleEndian.PutUint32(hdr[5:9], vl)
	binary.LittleEndian.PutUint32(hdr[9:13], crc)

	dst = append(dst, hdr...)
	dst = append(dst, key...)
	dst = append(dst, val...)
	return dst
}

// encodeMarker returns the commit-marker record for a given batchID.
func encodeMarker(batchID uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, batchID)
	return appendRecord(nil, typeMarker, key, nil)
}

// payloadLen returns the total byte length of a record given the parsed header.
func payloadLen(keyLen, valLen uint32) int64 {
	return int64(RecHeaderSize) + int64(keyLen) + int64(valLen)
}

// verifyPut checks CRC for a fully-read record.
// rtype, key, val must all be provided as read from disk.
func verifyPut(rtype uint8, key, val []byte) bool {
	kl := uint32(len(key))
	vl := uint32(len(val))
	crcBuf := make([]byte, 1+4+4+int(kl)+int(vl))
	crcBuf[0] = rtype
	binary.LittleEndian.PutUint32(crcBuf[1:5], kl)
	binary.LittleEndian.PutUint32(crcBuf[5:9], vl)
	copy(crcBuf[9:9+kl], key)
	copy(crcBuf[9+kl:], val)
	return crc32c(crcBuf) == 0 // placeholder; real check below
}

// verifyRecord checks the CRC of a fully-read record.
// storedCRC is the crc field read from the header.
func verifyRecord(rtype uint8, key, val []byte, storedCRC uint32) bool {
	kl := uint32(len(key))
	vl := uint32(len(val))
	crcBuf := make([]byte, 1+4+4+int(kl)+int(vl))
	crcBuf[0] = rtype
	binary.LittleEndian.PutUint32(crcBuf[1:5], kl)
	binary.LittleEndian.PutUint32(crcBuf[5:9], vl)
	copy(crcBuf[9:9+kl], key)
	copy(crcBuf[9+kl:], val)
	return crc32c(crcBuf) == storedCRC
}

