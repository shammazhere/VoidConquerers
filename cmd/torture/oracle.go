package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// ServerErrorCode represents a typed error response from the database server.
type ServerErrorCode string

const (
	ErrPoisoned   ServerErrorCode = "POISONED"
	ErrDiskFull   ServerErrorCode = "DISK_FULL"
	ErrBadRequest ServerErrorCode = "BAD_REQUEST"
	ErrDBLocked   ServerErrorCode = "DB_LOCKED"
	ErrIOError    ServerErrorCode = "IO_ERROR"
)

// OpItem represents a single PUT or DEL within an operation or batch.
type OpItem struct {
	Key   string
	Val   []byte
	IsDel bool
}

// Operation represents an atomic write submitted to the server (single op or batch).
type Operation struct {
	OpID  string
	Lane  string
	Items []OpItem
}

// Keys returns the unique set of keys touched by this operation.
func (op Operation) Keys() []string {
	seen := make(map[string]struct{}, len(op.Items))
	keys := make([]string, 0, len(op.Items))
	for _, item := range op.Items {
		if _, ok := seen[item.Key]; !ok {
			seen[item.Key] = struct{}{}
			keys = append(keys, item.Key)
		}
	}
	return keys
}

// Oracle maintains the independent authoritative truth of committed data
// and verifies crash-recovery dumps against allowable in-flight states.
type Oracle struct {
	mu            sync.Mutex
	maxInFlight   int                  // W: maximum allowed in-flight operations
	committed     map[string][]byte    // Key -> Value
	inFlight      map[string]Operation // OpID -> Operation
	inFlightLanes map[string]string    // Lane -> OpID
	inFlightKeys  map[string]int       // Key -> reference count across in-flight ops
	seenOpIDs     map[string]struct{}  // All OpIDs ever dispatched (for duplicate rejection)
	logFile              *os.File             // oracle.log writer
	logErr               error                // First sticky log write error
	allowSameKeyInFlight bool                 // Allow same key concurrently in-flight (for scenario D2)
}

// NewOracle creates or opens an independent oracle instance.
func NewOracle(logPath string, maxInFlightOps int) (*Oracle, error) {
	if maxInFlightOps <= 0 {
		return nil, errors.New("maxInFlightOps must be > 0")
	}

	var f *os.File
	var err error
	if logPath != "" {
		f, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("open oracle log: %w", err)
		}
	}

	return &Oracle{
		maxInFlight:   maxInFlightOps,
		committed:     make(map[string][]byte),
		inFlight:      make(map[string]Operation),
		inFlightLanes: make(map[string]string),
		inFlightKeys:  make(map[string]int),
		seenOpIDs:     make(map[string]struct{}),
		logFile:       f,
	}, nil
}

// writeLog writes to oracle.log if open, recording the first sticky error.
// Caller must hold o.mu.
func (o *Oracle) writeLog(format string, args ...any) error {
	if o.logFile == nil {
		return nil
	}
	if o.logErr != nil {
		return o.logErr
	}
	_, err := fmt.Fprintf(o.logFile, format, args...)
	if err != nil {
		o.logErr = fmt.Errorf("oracle log write error: %w", err)
		return o.logErr
	}
	return nil
}

// Flush explicitly syncs the oracle log file to disk.
func (o *Oracle) Flush() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.logErr != nil {
		return o.logErr
	}
	if o.logFile != nil {
		if err := o.logFile.Sync(); err != nil {
			o.logErr = fmt.Errorf("oracle log sync error: %w", err)
			return o.logErr
		}
	}
	return nil
}

// Close flushes and closes the oracle log file if open.
func (o *Oracle) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var firstErr error
	if o.logErr != nil {
		firstErr = o.logErr
	}

	if o.logFile != nil {
		if err := o.logFile.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := o.logFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		o.logFile = nil
	}
	return firstErr
}

// SetAllowSameKeyInFlight toggles key-disjoint enforcement (disabled for scenario D2).
func (o *Oracle) SetAllowSameKeyInFlight(allow bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.allowSameKeyInFlight = allow
}

// CanDispatch checks whether an operation can be dispatched:
// 1. Lane must not have an active in-flight operation.
// 2. Total in-flight count must be < W.
// 3. None of its keys may intersect currently in-flight keys (unless allowSameKeyInFlight is set).
func (o *Oracle) CanDispatch(op Operation) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, busy := o.inFlightLanes[op.Lane]; busy {
		return false
	}
	if len(o.inFlight) >= o.maxInFlight {
		return false
	}
	if !o.allowSameKeyInFlight {
		for _, k := range op.Keys() {
			if o.inFlightKeys[k] > 0 {
				return false
			}
		}
	}
	return true
}

// Dispatch registers an operation as in-flight before sending it to the server.
// Strictly validates op limits, lane availability, and key disjointness.
func (o *Oracle) Dispatch(op Operation) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.logErr != nil {
		return o.logErr
	}

	// Validate OpID
	if op.OpID == "" {
		return errors.New("op-id cannot be empty")
	}
	if strings.Contains(op.OpID, ":") {
		return fmt.Errorf("op-id %q contains invalid character ':'", op.OpID)
	}
	if _, seen := o.seenOpIDs[op.OpID]; seen {
		return fmt.Errorf("duplicate op-id: %q already dispatched", op.OpID)
	}

	// Validate Lane
	if op.Lane == "" {
		return errors.New("lane cannot be empty")
	}
	if existingOpID, busy := o.inFlightLanes[op.Lane]; busy {
		return fmt.Errorf("lane %q already has in-flight op %q", op.Lane, existingOpID)
	}

	// Validate Total In-Flight Limit W
	if len(o.inFlight) >= o.maxInFlight {
		return fmt.Errorf("total in-flight limit of %d ops reached", o.maxInFlight)
	}

	// Validate Batch Items
	if len(op.Items) == 0 {
		return errors.New("batch cannot be empty")
	}
	if len(op.Items) > 10000 {
		return fmt.Errorf("batch size %d exceeds limit of 10,000 operations", len(op.Items))
	}

	for idx, item := range op.Items {
		if len(item.Key) == 0 || len(item.Key) > 1024 {
			return fmt.Errorf("item %d: invalid key length %d: must be 1..1024 bytes", idx, len(item.Key))
		}
		if len(item.Val) > 1048576 {
			return fmt.Errorf("item %d: value length %d exceeds max 1 MiB (1,048,576 bytes)", idx, len(item.Val))
		}
	}

	// Validate Key Disjointness against currently in-flight keys (unless in D2 mode)
	if !o.allowSameKeyInFlight {
		for _, k := range op.Keys() {
			if o.inFlightKeys[k] > 0 {
				return fmt.Errorf("key %q already in flight (disjoint violation)", k)
			}
		}
	}

	// Record in-flight
	o.seenOpIDs[op.OpID] = struct{}{}
	o.inFlight[op.OpID] = op
	o.inFlightLanes[op.Lane] = op.OpID
	for _, k := range op.Keys() {
		o.inFlightKeys[k]++
	}

	if err := o.writeLog("DISPATCH %s lane=%s ops=%d\n", op.OpID, op.Lane, len(op.Items)); err != nil {
		return err
	}
	return nil
}

// RecordAck records that an operation received an ACK from the server.
// The operation is immediately applied to the committed oracle state.
func (o *Oracle) RecordAck(opID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.logErr != nil {
		return o.logErr
	}

	op, exists := o.inFlight[opID]
	if !exists {
		return fmt.Errorf("ACK for unknown or already handled op-id: %s", opID)
	}

	// Apply items in listed order
	for _, item := range op.Items {
		if item.IsDel {
			delete(o.committed, item.Key)
		} else {
			valCopy := make([]byte, len(item.Val))
			copy(valCopy, item.Val)
			o.committed[item.Key] = valCopy
		}
	}

	// Release in-flight tracking
	delete(o.inFlightLanes, op.Lane)
	for _, k := range op.Keys() {
		o.inFlightKeys[k]--
		if o.inFlightKeys[k] <= 0 {
			delete(o.inFlightKeys, k)
		}
	}
	delete(o.inFlight, opID)

	if err := o.writeLog("ACK %s\n", opID); err != nil {
		return err
	}
	return nil
}

// RecordErr records a typed error response from the server for an in-flight op.
// Validates error codes:
// - POISONED, DISK_FULL, BAD_REQUEST, DB_LOCKED: definitively uncommitted; removed from in-flight.
// - IO_ERROR: ambiguous fsync failure; kept in-flight for recovery resolution.
func (o *Oracle) RecordErr(opID string, errCode ServerErrorCode) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.logErr != nil {
		return o.logErr
	}

	switch errCode {
	case ErrPoisoned, ErrDiskFull, ErrBadRequest, ErrDBLocked, ErrIOError:
		// Valid code
	default:
		return fmt.Errorf("unknown server error code: %q", errCode)
	}

	op, exists := o.inFlight[opID]
	if !exists {
		return fmt.Errorf("ERR for unknown op-id: %s", opID)
	}

	if err := o.writeLog("ERR %s %s\n", opID, errCode); err != nil {
		return err
	}

	if errCode != ErrIOError {
		// Definitively not committed: discard from in-flight
		delete(o.inFlightLanes, op.Lane)
		for _, k := range op.Keys() {
			o.inFlightKeys[k]--
			if o.inFlightKeys[k] <= 0 {
				delete(o.inFlightKeys, k)
			}
		}
		delete(o.inFlight, opID)
	}
	// If ErrIOError: stays in o.inFlight, resolved during Verify()
	return nil
}

// VerifyResult captures the outcome of an oracle dump verification.
type VerifyResult struct {
	MatchedCandidate []string // OpIDs of in-flight ops that were committed in the winning candidate
	DroppedOps       []string // OpIDs of in-flight ops that were discarded
	TotalKeys        int
}

// Verify checks the post-recovery dump against all 2^m allowable in-flight states.
//
// Scalable Verification Algorithm:
// 1. Checks all keys not touched by any in-flight op once against `committed`.
// 2. Enumerates 2^m subsets ONLY over keys touched by in-flight ops (without cloning full committed map).
// 3. On failure: leaves in-flight state unchanged and returns detailed error.
// 4. On match: selects winning candidate via deterministic tie-break, applies changes,
//    logs resolutions, and clears in-flight state.
func (o *Oracle) Verify(dump map[string][]byte) (*VerifyResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.logErr != nil {
		return nil, o.logErr
	}

	// 1. Identify all keys touched by active in-flight operations (U)
	inFlightList := make([]Operation, 0, len(o.inFlight))
	for _, op := range o.inFlight {
		inFlightList = append(inFlightList, op)
	}
	sort.Slice(inFlightList, func(i, j int) bool {
		return inFlightList[i].OpID < inFlightList[j].OpID
	})

	touchedKeys := make(map[string]struct{})
	for _, op := range inFlightList {
		for _, k := range op.Keys() {
			touchedKeys[k] = struct{}{}
		}
	}

	// 2. Check keys NOT touched by in-flight ops once (O(|committed| + |dump|))
	// 2a. Every non-touched key in committed must exist in dump with identical value
	for k, commVal := range o.committed {
		if _, isTouched := touchedKeys[k]; isTouched {
			continue
		}
		dumpVal, exists := dump[k]
		if !exists {
			diff := o.buildDiffReport(dump, inFlightList)
			return nil, fmt.Errorf("invariant violation: committed non-in-flight key %q missing in dump.\n%s", k, diff)
		}
		if !bytes.Equal(commVal, dumpVal) {
			diff := o.buildDiffReport(dump, inFlightList)
			return nil, fmt.Errorf("invariant violation: value mismatch for non-in-flight key %q.\n%s", k, diff)
		}
	}

	// 2b. Every non-touched key in dump must exist in committed
	for k := range dump {
		if _, isTouched := touchedKeys[k]; isTouched {
			continue
		}
		if _, exists := o.committed[k]; !exists {
			diff := o.buildDiffReport(dump, inFlightList)
			return nil, fmt.Errorf("invariant violation: unexpected phantom key %q in dump.\n%s", k, diff)
		}
	}

	// 3. Enumerate 2^m subsets ONLY over touched keys
	m := len(inFlightList)
	totalSubsets := 1 << m

	type candidateMatch struct {
		subsetIdx  int
		appliedOps []string
		droppedOps []string
	}

	var matches []candidateMatch

	for mask := 0; mask < totalSubsets; mask++ {
		// Base touched state for keys in touchedKeys
		candTouched := make(map[string][]byte, len(touchedKeys))
		for k := range touchedKeys {
			if commVal, exists := o.committed[k]; exists {
				candTouched[k] = commVal
			}
		}

		var applied []string
		var dropped []string

		// Apply operations in subset mask
		for i := 0; i < m; i++ {
			op := inFlightList[i]
			if (mask & (1 << i)) != 0 {
				applied = append(applied, op.OpID)
				for _, item := range op.Items {
					if item.IsDel {
						delete(candTouched, item.Key)
					} else {
						candTouched[item.Key] = item.Val
					}
				}
			} else {
				dropped = append(dropped, op.OpID)
			}
		}

		// Compare candTouched with dump strictly on touchedKeys
		subsetMatches := true
		for k := range touchedKeys {
			expectedVal, shouldExist := candTouched[k]
			dumpVal, dumpExists := dump[k]
			if shouldExist != dumpExists {
				subsetMatches = false
				break
			}
			if shouldExist && !bytes.Equal(expectedVal, dumpVal) {
				subsetMatches = false
				break
			}
		}

		if subsetMatches {
			matches = append(matches, candidateMatch{
				subsetIdx:  mask,
				appliedOps: applied,
				droppedOps: dropped,
			})
		}
	}

	if len(matches) == 0 {
		diff := o.buildDiffReport(dump, inFlightList)
		return nil, fmt.Errorf("invariant violation: dump matched 0 of %d in-flight candidates.\n%s", totalSubsets, diff)
	}

	// 4. Deterministic Tie-Break:
	// Select the SMALLEST subset (fewest in-flight applied), then lexicographically lowest OpID sequence
	sort.Slice(matches, func(i, j int) bool {
		if len(matches[i].appliedOps) != len(matches[j].appliedOps) {
			return len(matches[i].appliedOps) < len(matches[j].appliedOps)
		}
		return strings.Join(matches[i].appliedOps, ",") < strings.Join(matches[j].appliedOps, ",")
	})

	winner := matches[0]

	// 5. Apply winning candidate to committed state and log resolutions
	for _, opID := range winner.appliedOps {
		op := o.inFlight[opID]
		for _, item := range op.Items {
			if item.IsDel {
				delete(o.committed, item.Key)
			} else {
				valCopy := make([]byte, len(item.Val))
				copy(valCopy, item.Val)
				o.committed[item.Key] = valCopy
			}
		}
		if err := o.writeLog("RESOLVED_COMMITTED %s\n", opID); err != nil {
			return nil, err
		}
	}

	for _, opID := range winner.droppedOps {
		if err := o.writeLog("RESOLVED_DROPPED %s\n", opID); err != nil {
			return nil, err
		}
	}

	// Clear in-flight state
	o.inFlight = make(map[string]Operation)
	o.inFlightLanes = make(map[string]string)
	o.inFlightKeys = make(map[string]int)

	return &VerifyResult{
		MatchedCandidate: winner.appliedOps,
		DroppedOps:       winner.droppedOps,
		TotalKeys:        len(dump),
	}, nil
}

// GetLastAcked returns the last acknowledged value for a key (or nil if deleted or not yet set).
func (o *Oracle) GetLastAcked(key string) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	val, ok := o.committed[key]
	if !ok {
		return nil, false
	}
	res := make([]byte, len(val))
	copy(res, val)
	return res, true
}

// GetInFlightValues returns a slice of values (or nil for DEL) currently in flight for the given key.
func (o *Oracle) GetInFlightValues(key string) [][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	var vals [][]byte
	for _, op := range o.inFlight {
		for _, item := range op.Items {
			if item.Key == key {
				if item.IsDel {
					vals = append(vals, nil)
				} else {
					valCopy := make([]byte, len(item.Val))
					copy(valCopy, item.Val)
					vals = append(vals, valCopy)
				}
			}
		}
	}
	return vals
}

// SetCommitted updates the oracle's committed value for a key.
func (o *Oracle) SetCommitted(key string, val []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if val == nil {
		delete(o.committed, key)
	} else {
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		o.committed[key] = valCopy
	}
}

// ClearInFlight clears the in-flight state.
func (o *Oracle) ClearInFlight() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inFlight = make(map[string]Operation)
	o.inFlightLanes = make(map[string]string)
	o.inFlightKeys = make(map[string]int)
}

// VerifyD2 verifies Scenario D2:
// Two lanes had concurrent in-flight ops on the same key.
// - lastAcked == nil means the key was last acknowledged as absent.
// - An inFlight entry of nil means an in-flight DEL.
// Accept the dump only if it matches lastAcked or one inFlight value. Reject stale values.
func VerifyD2(dump map[string][]byte, key string, lastAcked []byte, inFlight [][]byte) error {
	actual, exists := dump[key]

	// 1. Check if matches lastAcked
	if lastAcked == nil && !exists {
		return nil
	}
	if lastAcked != nil && exists && bytes.Equal(actual, lastAcked) {
		return nil
	}

	// 2. Check if matches any in-flight value
	for _, cand := range inFlight {
		if cand == nil && !exists {
			return nil
		}
		if cand != nil && exists && bytes.Equal(actual, cand) {
			return nil
		}
	}

	return fmt.Errorf("D2 violation for key %q: dump value matches neither lastAcked nor any in-flight value", key)
}

func (o *Oracle) buildDiffReport(dump map[string][]byte, inFlight []Operation) string {
	var b strings.Builder
	b.WriteString("=== DUMP DIFF REPORT ===\n")
	b.WriteString(fmt.Sprintf("Committed keys: %d | Dump keys: %d | In-flight ops: %d\n",
		len(o.committed), len(dump), len(inFlight)))

	missingCount := 0
	for k := range o.committed {
		if _, exists := dump[k]; !exists {
			if missingCount < 10 {
				b.WriteString(fmt.Sprintf("MISSING KEY in dump: %s\n", k))
			}
			missingCount++
		}
	}
	if missingCount > 10 {
		b.WriteString(fmt.Sprintf("... and %d more missing keys\n", missingCount-10))
	}

	extraCount := 0
	for k := range dump {
		if _, exists := o.committed[k]; !exists {
			if extraCount < 10 {
				b.WriteString(fmt.Sprintf("EXTRA KEY in dump: %s (len=%d)\n", k, len(dump[k])))
			}
			extraCount++
		}
	}
	if extraCount > 10 {
		b.WriteString(fmt.Sprintf("... and %d more extra keys\n", extraCount-10))
	}

	mismatchCount := 0
	for k, commVal := range o.committed {
		if dumpVal, exists := dump[k]; exists {
			if !bytes.Equal(commVal, dumpVal) {
				if mismatchCount < 10 {
					b.WriteString(fmt.Sprintf("VALUE MISMATCH on key %s: expected %d bytes (CRC %08x), got %d bytes (CRC %08x)\n",
						k, len(commVal), crc32.Checksum(commVal, castagnoliTable),
						len(dumpVal), crc32.Checksum(dumpVal, castagnoliTable)))
				}
				mismatchCount++
			}
		}
	}
	return b.String()
}

// -----------------------------------------------------------------------------
// Value Envelope & CRC Validation
// -----------------------------------------------------------------------------

// MinEnvelopeLen calculates the minimum target length required to store an envelope
// for a given key, opID, and seq (with a 0-byte payload).
func MinEnvelopeLen(key []byte, opID string, seq uint64) int {
	keyHex := hex.EncodeToString(key)
	fixedPrefix := fmt.Sprintf("VAL:v1:%s:%s:%d:", keyHex, opID, seq)
	return len(fixedPrefix) + 1 + 1 + 8 + 1 // +1 digit payload_len +1 colon +8 hex CRC +1 colon
}

// BuildValueEnvelope constructs the self-describing value envelope:
// VAL:v1:<key_hex>:<op_id>:<seq>:<payload_len>:<crc32c_hex>:<payload>
// - Rejects op-IDs containing ':'
// - Rejects totalTargetLen > 1 MiB (1,048,576 bytes) or too small for overhead
// - Rejects payloads longer than computed payload length (returns error instead of truncating)
func BuildValueEnvelope(key []byte, opID string, seq uint64, totalTargetLen int, payload []byte) ([]byte, error) {
	const maxValLen = 1048576
	if totalTargetLen > maxValLen {
		return nil, fmt.Errorf("target length %d exceeds max 1 MiB (%d)", totalTargetLen, maxValLen)
	}
	if strings.Contains(opID, ":") {
		return nil, fmt.Errorf("op-id %q cannot contain ':'", opID)
	}

	keyHex := hex.EncodeToString(key)
	fixedPrefix := fmt.Sprintf("VAL:v1:%s:%s:%d:", keyHex, opID, seq)

	payloadLen := -1
	formatDigits := 0
	for digits := 1; digits <= 7; digits++ {
		overhead := len(fixedPrefix) + digits + 1 + 8 + 1
		candidateLen := totalTargetLen - overhead
		if candidateLen < 0 {
			continue
		}
		if len(strconv.Itoa(candidateLen)) <= digits {
			payloadLen = candidateLen
			formatDigits = digits
			break
		}
	}

	if payloadLen < 0 {
		return nil, fmt.Errorf("target length %d is too small for envelope overhead", totalTargetLen)
	}

	if len(payload) > payloadLen {
		return nil, fmt.Errorf("payload length %d exceeds computed payload capacity %d", len(payload), payloadLen)
	}

	headerPrefix := fmt.Sprintf("%s%0*d:", fixedPrefix, formatDigits, payloadLen)

	finalPayload := make([]byte, payloadLen)
	if len(payload) > 0 {
		copy(finalPayload, payload)
		for i := len(payload); i < payloadLen; i++ {
			finalPayload[i] = byte(i % 256)
		}
	} else {
		for i := 0; i < payloadLen; i++ {
			finalPayload[i] = byte((i*31 + 17) % 256)
		}
	}

	// Compute CRC32C over raw headerPrefix bytes + finalPayload
	crcInput := append([]byte(headerPrefix), finalPayload...)
	checksum := crc32.Checksum(crcInput, castagnoliTable)
	crcHex := fmt.Sprintf("%08x", checksum)

	var buf bytes.Buffer
	buf.WriteString(headerPrefix)
	buf.WriteString(crcHex)
	buf.WriteByte(':')
	buf.Write(finalPayload)

	res := buf.Bytes()
	if len(res) != totalTargetLen {
		return nil, fmt.Errorf("internal sizing mismatch: got %d, expected %d", len(res), totalTargetLen)
	}
	return res, nil
}

// ParsedEnvelope holds the validated components of an envelope.
type ParsedEnvelope struct {
	KeyHex     string
	OpID       string
	Seq        uint64
	PayloadLen int
	CRC32C     uint32
	Payload    []byte
}

// ParseAndVerifyEnvelope decodes and validates a self-describing value envelope.
// Recomputes CRC32C over the exact raw header prefix bytes and payload.
func ParseAndVerifyEnvelope(expectedKey []byte, value []byte) (*ParsedEnvelope, error) {
	if !bytes.HasPrefix(value, []byte("VAL:v1:")) {
		return nil, errors.New("not a VAL:v1 envelope")
	}

	parts := bytes.SplitN(value, []byte(":"), 7)
	if len(parts) < 7 {
		return nil, fmt.Errorf("malformed envelope: expected 7 colon-separated sections, got %d", len(parts))
	}

	keyHex := string(parts[2])
	opID := string(parts[3])
	seq, err := strconv.ParseUint(string(parts[4]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid seq: %w", err)
	}

	payloadLen, err := strconv.Atoi(string(parts[5]))
	if err != nil {
		return nil, fmt.Errorf("invalid payload_len: %w", err)
	}

	rem := parts[6]
	if len(rem) < 9 || rem[8] != ':' {
		return nil, errors.New("malformed CRC separator in envelope")
	}

	crcHex := string(rem[:8])
	payload := rem[9:]

	if len(payload) != payloadLen {
		return nil, fmt.Errorf("payload length mismatch: header says %d, got %d", payloadLen, len(payload))
	}

	if expectedKey != nil {
		if hex.EncodeToString(expectedKey) != keyHex {
			return nil, fmt.Errorf("cross-key violation: envelope key_hex=%s does not match expected key=%s",
				keyHex, hex.EncodeToString(expectedKey))
		}
	}

	headerPrefixLen := len(value) - len(rem)
	headerPrefix := value[:headerPrefixLen]

	crcInput := append([]byte(nil), headerPrefix...)
	crcInput = append(crcInput, payload...)
	expectedCRC := crc32.Checksum(crcInput, castagnoliTable)

	parsedCRC, err := strconv.ParseUint(crcHex, 16, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid CRC hex %q: %w", crcHex, err)
	}

	if uint32(parsedCRC) != expectedCRC {
		return nil, fmt.Errorf("CRC32C mismatch: envelope has %08x, recomputed is %08x", uint32(parsedCRC), expectedCRC)
	}

	return &ParsedEnvelope{
		KeyHex:     keyHex,
		OpID:       opID,
		Seq:        seq,
		PayloadLen: payloadLen,
		CRC32C:     expectedCRC,
		Payload:    payload,
	}, nil
}

// -----------------------------------------------------------------------------
// Dump Escaping & Parsing
// -----------------------------------------------------------------------------

// EscapeDumpString escapes a byte slice according to the engine specification:
// Printable ASCII (0x20..0x7E, except \ and \t) emitted as-is.
// \t -> \t, \n -> \n, \r -> \r, \\ -> \\
// Other bytes -> \xHH
func EscapeDumpString(b []byte) string {
	var buf strings.Builder
	buf.Grow(len(b))
	for _, c := range b {
		switch c {
		case '\\':
			buf.WriteString(`\\`)
		case '\t':
			buf.WriteString(`\t`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		default:
			if c >= 0x20 && c <= 0x7E {
				buf.WriteByte(c)
			} else {
				fmt.Fprintf(&buf, `\x%02x`, c)
			}
		}
	}
	return buf.String()
}

// UnescapeDumpString inverts EscapeDumpString, recovering exact raw bytes.
// Rejects truncated \x escapes, unknown escapes like \q, and trailing backslashes.
func UnescapeDumpString(s string) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' {
			if i+1 >= len(s) {
				return nil, errors.New("trailing backslash in escape sequence")
			}
			switch s[i+1] {
			case '\\':
				buf.WriteByte('\\')
				i += 2
			case 't':
				buf.WriteByte('\t')
				i += 2
			case 'n':
				buf.WriteByte('\n')
				i += 2
			case 'r':
				buf.WriteByte('\r')
				i += 2
			case 'x':
				if i+3 >= len(s) {
					return nil, errors.New("truncated \\x escape sequence")
				}
				hexByte, err := hex.DecodeString(s[i+2 : i+4])
				if err != nil {
					return nil, fmt.Errorf("invalid \\xHH escape at index %d: %w", i, err)
				}
				buf.WriteByte(hexByte[0])
				i += 4
			default:
				return nil, fmt.Errorf("unknown escape sequence \\%c at index %d", s[i+1], i)
			}
		} else {
			buf.WriteByte(s[i])
			i++
		}
	}
	return buf.Bytes(), nil
}

// ParseDump parses lines formatted as `<escaped_key>\t<escaped_val>\n` from an io.Reader.
// Returns an error if any duplicate key is encountered.
func ParseDump(r io.Reader) (map[string][]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read dump: %w", err)
	}

	result := make(map[string][]byte)
	lines := strings.Split(string(data), "\n")
	for lineNum, line := range lines {
		if line == "" {
			continue
		}
		// Split on the first unescaped tab
		tabIdx := -1
		for i := 0; i < len(line); i++ {
			if line[i] == '\t' {
				tabIdx = i
				break
			}
			if line[i] == '\\' {
				i++ // skip next character
			}
		}

		if tabIdx == -1 {
			return nil, fmt.Errorf("line %d: missing tab separator between key and value: %q", lineNum+1, line)
		}

		rawKey, err := UnescapeDumpString(line[:tabIdx])
		if err != nil {
			return nil, fmt.Errorf("line %d: unescape key: %w", lineNum+1, err)
		}

		keyStr := string(rawKey)
		if _, exists := result[keyStr]; exists {
			return nil, fmt.Errorf("duplicate key %q at line %d in dump", keyStr, lineNum+1)
		}

		rawVal, err := UnescapeDumpString(line[tabIdx+1:])
		if err != nil {
			return nil, fmt.Errorf("line %d: unescape val: %w", lineNum+1, err)
		}

		result[keyStr] = rawVal
	}
	return result, nil
}
