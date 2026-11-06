package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestEscapeUnescapeRoundTrip(t *testing.T) {
	testCases := [][]byte{
		[]byte("hello world"),
		[]byte("key\twith\ttabs"),
		[]byte("value\nwith\nnewlines\r\nand\rcarriage_returns"),
		[]byte("backslashes\\\\and\\more"),
		{0x00, 0x01, 0x02, 0x1F, 0x20, 0x7E, 0x7F, 0x80, 0xFE, 0xFF},
		bytes.Repeat([]byte{0x00, 0xFF, '\t', '\n', '\\'}, 100),
	}

	for i, tc := range testCases {
		escaped := EscapeDumpString(tc)
		unescaped, err := UnescapeDumpString(escaped)
		if err != nil {
			t.Fatalf("case %d: unexpected unescape error: %v", i, err)
		}
		if !bytes.Equal(tc, unescaped) {
			t.Fatalf("case %d: round trip mismatch!\norig: %x\ngot:  %x", i, tc, unescaped)
		}
	}
}

func TestUnescapeDumpStringErrors(t *testing.T) {
	// Truncated \x escape
	if _, err := UnescapeDumpString(`key\x`); err == nil {
		t.Fatal("expected error on truncated \\x, got nil")
	}
	if _, err := UnescapeDumpString(`key\x1`); err == nil {
		t.Fatal("expected error on truncated \\x1, got nil")
	}

	// Unknown \q escape
	if _, err := UnescapeDumpString(`key\q`); err == nil {
		t.Fatal("expected error on unknown \\q, got nil")
	}

	// Trailing backslash
	if _, err := UnescapeDumpString(`key\`); err == nil {
		t.Fatal("expected error on trailing backslash, got nil")
	}
}

func TestParseDump(t *testing.T) {
	rawInput := "user\\t1\tAlice\\nSmith\nkey2\t\\x00\\x01\\x02\\xff\n"
	dump, err := ParseDump(strings.NewReader(rawInput))
	if err != nil {
		t.Fatalf("ParseDump failed: %v", err)
	}

	if val, ok := dump["user\t1"]; !ok || string(val) != "Alice\nSmith" {
		t.Fatalf("parsed user\\t1 wrong: %q", val)
	}

	if val, ok := dump["key2"]; !ok || !bytes.Equal(val, []byte{0x00, 0x01, 0x02, 0xff}) {
		t.Fatalf("parsed key2 wrong: %x", val)
	}
}

func TestParseDumpDuplicateKey(t *testing.T) {
	rawInput := "key1\tval1\nkey1\tval2\n"
	if _, err := ParseDump(strings.NewReader(rawInput)); err == nil {
		t.Fatal("expected duplicate key error from ParseDump, got nil")
	}
}

func TestValueEnvelopeArithmeticAndIntegrity(t *testing.T) {
	key := []byte("test_key_123")
	opID := "w1_1042"
	seq := uint64(1042)

	// Test exact 1 MiB value
	const oneMiB = 1048576
	env, err := BuildValueEnvelope(key, opID, seq, oneMiB, nil)
	if err != nil {
		t.Fatalf("BuildValueEnvelope 1MiB failed: %v", err)
	}

	if len(env) != oneMiB {
		t.Fatalf("expected envelope len %d, got %d", oneMiB, len(env))
	}

	parsed, err := ParseAndVerifyEnvelope(key, env)
	if err != nil {
		t.Fatalf("ParseAndVerifyEnvelope failed: %v", err)
	}

	if parsed.OpID != opID || parsed.Seq != seq {
		t.Fatalf("envelope field mismatch: got opID=%s seq=%d", parsed.OpID, parsed.Seq)
	}

	// Cross-key mismatch detection
	if _, err = ParseAndVerifyEnvelope([]byte("different_key"), env); err == nil {
		t.Fatal("expected cross-key verification error, got nil")
	}

	// Payload corruption detection
	corrupted := make([]byte, len(env))
	copy(corrupted, env)
	corrupted[len(corrupted)-10] ^= 0x01
	if _, err = ParseAndVerifyEnvelope(key, corrupted); err == nil {
		t.Fatal("expected CRC error on corrupted payload, got nil")
	}
}

func TestValueEnvelopeHeaderCorruption(t *testing.T) {
	key := []byte("k1")
	opID := "w1_99"
	seq := uint64(99)

	env, err := BuildValueEnvelope(key, opID, seq, 120, []byte("short_payload"))
	if err != nil {
		t.Fatalf("BuildValueEnvelope failed: %v", err)
	}

	// Corrupt opID byte in the header
	corruptOp := make([]byte, len(env))
	copy(corruptOp, env)
	idx := bytes.Index(corruptOp, []byte("w1_99"))
	if idx == -1 {
		t.Fatal("opID not found in envelope")
	}
	corruptOp[idx] = 'x' // flip 'w' to 'x'

	if _, err := ParseAndVerifyEnvelope(key, corruptOp); err == nil {
		t.Fatal("expected error on header opID corruption, got nil")
	}

	// Corrupt sequence number byte in the header
	corruptSeq := make([]byte, len(env))
	copy(corruptSeq, env)
	seqIdx := bytes.Index(corruptSeq, []byte(":99:"))
	if seqIdx == -1 {
		t.Fatal("seq not found in envelope")
	}
	corruptSeq[seqIdx+1] = '8' // flip '9' to '8'

	if _, err := ParseAndVerifyEnvelope(key, corruptSeq); err == nil {
		t.Fatal("expected error on header seq corruption, got nil")
	}
}

func TestBuildValueEnvelopeLimits(t *testing.T) {
	key := []byte("k1")

	// Target above 1 MiB
	if _, err := BuildValueEnvelope(key, "op1", 1, 1048577, nil); err == nil {
		t.Fatal("expected error for target > 1 MiB, got nil")
	}

	// Target too small for overhead
	if _, err := BuildValueEnvelope(key, "op1", 1, 15, nil); err == nil {
		t.Fatal("expected error for target too small, got nil")
	}

	// OpID containing colon
	if _, err := BuildValueEnvelope(key, "bad:op", 1, 100, nil); err == nil {
		t.Fatal("expected error for opID containing ':', got nil")
	}

	// Payload larger than computed capacity
	largePayload := bytes.Repeat([]byte("A"), 200)
	if _, err := BuildValueEnvelope(key, "op1", 1, 100, largePayload); err == nil {
		t.Fatal("expected error for payload exceeding target capacity, got nil")
	}
}

func TestOracleDispatchLimits(t *testing.T) {
	o, err := NewOracle("", 2) // max 2 in-flight ops
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	// Empty op-id
	if err := o.Dispatch(Operation{OpID: "", Lane: "w1", Items: []OpItem{{Key: "k", Val: []byte("v")}}}); err == nil {
		t.Fatal("expected error on empty op-id")
	}

	// Op-id containing colon
	if err := o.Dispatch(Operation{OpID: "bad:id", Lane: "w1", Items: []OpItem{{Key: "k", Val: []byte("v")}}}); err == nil {
		t.Fatal("expected error on op-id with ':'")
	}

	// Empty lane
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "", Items: []OpItem{{Key: "k", Val: []byte("v")}}}); err == nil {
		t.Fatal("expected error on empty lane")
	}

	// Empty batch
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "w1", Items: nil}); err == nil {
		t.Fatal("expected error on empty batch")
	}

	// Key len 0
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "w1", Items: []OpItem{{Key: "", Val: []byte("v")}}}); err == nil {
		t.Fatal("expected error on empty key")
	}

	// Key len > 1024
	longKey := strings.Repeat("k", 1025)
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "w1", Items: []OpItem{{Key: longKey, Val: []byte("v")}}}); err == nil {
		t.Fatal("expected error on key > 1024 bytes")
	}

	// Value len > 1 MiB
	longVal := make([]byte, 1048577)
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "w1", Items: []OpItem{{Key: "k", Val: longVal}}}); err == nil {
		t.Fatal("expected error on val > 1 MiB")
	}

	// Valid op on lane w1
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}); err != nil {
		t.Fatalf("dispatch op1 failed: %v", err)
	}

	// Duplicate op-id
	if err := o.Dispatch(Operation{OpID: "op1", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2")}}}); err == nil {
		t.Fatal("expected error on duplicate op-id")
	}

	// Lane w1 already busy
	if err := o.Dispatch(Operation{OpID: "op2", Lane: "w1", Items: []OpItem{{Key: "k2", Val: []byte("v2")}}}); err == nil {
		t.Fatal("expected error on busy lane w1")
	}

	// Valid op on lane w2
	if err := o.Dispatch(Operation{OpID: "op2", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2")}}}); err != nil {
		t.Fatalf("dispatch op2 failed: %v", err)
	}

	// Max in-flight limit reached (W=2)
	if err := o.Dispatch(Operation{OpID: "op3", Lane: "w3", Items: []OpItem{{Key: "k3", Val: []byte("v3")}}}); err == nil {
		t.Fatal("expected error when exceeding W max in-flight limit")
	}
}

func TestOracleRecordErrPoisonedVsIOError(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	// 1. POISONED: definitively uncommitted -> op removed, lane freed
	op1 := Operation{OpID: "op1", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}
	if err := o.Dispatch(op1); err != nil {
		t.Fatalf("dispatch op1 failed: %v", err)
	}

	if err := o.RecordErr("op1", ErrPoisoned); err != nil {
		t.Fatalf("RecordErr ErrPoisoned failed: %v", err)
	}

	// Lane w1 should now be free
	op2 := Operation{OpID: "op2", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v2")}}}
	if err := o.Dispatch(op2); err != nil {
		t.Fatalf("expected lane w1 to be free after POISONED: %v", err)
	}

	// 2. Unknown error code rejected
	if err := o.RecordErr("op2", ServerErrorCode("UNKNOWN_CODE")); err == nil {
		t.Fatal("expected error on unknown ServerErrorCode, got nil")
	}

	// 3. IO_ERROR: ambiguous -> op remains in-flight, lane remains busy
	if err := o.RecordErr("op2", ErrIOError); err != nil {
		t.Fatalf("RecordErr ErrIOError failed: %v", err)
	}

	// Lane w1 should still be busy because op2 is still in-flight
	op3 := Operation{OpID: "op3", Lane: "w1", Items: []OpItem{{Key: "k3", Val: []byte("v3")}}}
	if err := o.Dispatch(op3); err == nil {
		t.Fatal("expected lane w1 to remain busy after IO_ERROR")
	}

	// Verify resolves op2 if it committed
	dump := map[string][]byte{"k1": []byte("v2")}
	res, err := o.Verify(dump)
	if err != nil {
		t.Fatalf("Verify failed to resolve IO_ERROR in-flight op: %v", err)
	}
	if len(res.MatchedCandidate) != 1 || res.MatchedCandidate[0] != "op2" {
		t.Fatalf("expected op2 committed, got: %+v", res)
	}
}

func TestOracleFailedVerifyPreservesInFlight(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	op := Operation{OpID: "op_inflight", Lane: "w1", Items: []OpItem{{Key: "target", Val: []byte("val")}}}
	if err := o.Dispatch(op); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	// Corrupt dump that matches no candidate
	badDump := map[string][]byte{"phantom_key": []byte("boo")}
	if _, err := o.Verify(badDump); err == nil {
		t.Fatal("expected Verify to fail on bad dump")
	}

	// Assert in-flight state was preserved!
	if o.CanDispatch(Operation{Lane: "w1", Items: []OpItem{{Key: "other"}}}) {
		t.Fatal("lane w1 should still be busy after failed Verify")
	}
	if o.CanDispatch(Operation{Lane: "w2", Items: []OpItem{{Key: "target"}}}) {
		t.Fatal("key target should still be in-flight after failed Verify")
	}
}

func TestOracleTieBreakSmallestSubset(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	// Base state: k1 -> v1
	baseOp := Operation{OpID: "base", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}
	if err := o.Dispatch(baseOp); err != nil {
		t.Fatalf("dispatch base failed: %v", err)
	}
	if err := o.RecordAck("base"); err != nil {
		t.Fatalf("ack base failed: %v", err)
	}

	// In-flight op 1: PUT k1 -> v1 (no-op overwrite)
	inFlight1 := Operation{OpID: "noop_put", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}
	if err := o.Dispatch(inFlight1); err != nil {
		t.Fatalf("dispatch noop_put failed: %v", err)
	}

	// In-flight op 2: PUT k2 -> v2
	inFlight2 := Operation{OpID: "new_put", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2")}}}
	if err := o.Dispatch(inFlight2); err != nil {
		t.Fatalf("dispatch new_put failed: %v", err)
	}

	// Dump has only k1 -> v1
	// Candidate {} (0 ops) matches!
	// Candidate {noop_put} (1 op) matches!
	// Tie-break MUST pick {} (0 ops applied)
	dump := map[string][]byte{"k1": []byte("v1")}
	res, err := o.Verify(dump)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if len(res.MatchedCandidate) != 0 {
		t.Fatalf("expected tie-break to pick smallest subset (0 applied), got %d applied: %+v",
			len(res.MatchedCandidate), res.MatchedCandidate)
	}
	if len(res.DroppedOps) != 2 {
		t.Fatalf("expected both ops dropped, got: %+v", res.DroppedOps)
	}
}

func TestOracleLaneWaitFakeScheduler(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	// Lane 1 has ops: [OP1_K1]
	// Lane 2 has ops: [OP2_K1, OP2_K2]
	op1_k1 := Operation{OpID: "op1_k1", Lane: "w1", Items: []OpItem{{Key: "shared", Val: []byte("val1")}}}
	op2_k1 := Operation{OpID: "op2_k1", Lane: "w2", Items: []OpItem{{Key: "shared", Val: []byte("val2")}}}
	op2_k2 := Operation{OpID: "op2_k2", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("val3")}}}

	// Lane 1 dispatches op1_k1
	if !o.CanDispatch(op1_k1) {
		t.Fatal("Lane 1 should be able to dispatch")
	}
	if err := o.Dispatch(op1_k1); err != nil {
		t.Fatalf("dispatch op1_k1 failed: %v", err)
	}

	// Lane 2 attempts op2_k1 -> must WAIT because "shared" key is in flight
	if o.CanDispatch(op2_k1) {
		t.Fatal("Lane 2 should block on shared key while op1_k1 is in flight")
	}

	// Lane 1 receives ACK
	if err := o.RecordAck("op1_k1"); err != nil {
		t.Fatalf("ack op1_k1 failed: %v", err)
	}

	// Now Lane 2 unblocks and can dispatch op2_k1!
	if !o.CanDispatch(op2_k1) {
		t.Fatal("Lane 2 should now be able to dispatch op2_k1")
	}
	if err := o.Dispatch(op2_k1); err != nil {
		t.Fatalf("dispatch op2_k1 failed: %v", err)
	}

	// Lane 2 receives ACK for op2_k1
	if err := o.RecordAck("op2_k1"); err != nil {
		t.Fatalf("ack op2_k1 failed: %v", err)
	}

	// Now Lane 2 dispatches op2_k2 in exact sequence
	if !o.CanDispatch(op2_k2) {
		t.Fatal("Lane 2 should be able to dispatch op2_k2")
	}
	if err := o.Dispatch(op2_k2); err != nil {
		t.Fatalf("dispatch op2_k2 failed: %v", err)
	}
	if err := o.RecordAck("op2_k2"); err != nil {
		t.Fatalf("ack op2_k2 failed: %v", err)
	}
}

func TestVerifyD2(t *testing.T) {
	key := "shared_d2"
	v0 := []byte("v0_initial")
	v1 := []byte("v1_writer1")
	v2 := []byte("v2_writer2")

	// 1. When lastAcked is v0, inFlight has v1 and v2:
	inFlight := [][]byte{v1, v2}

	// Dump with lastAcked (v0) -> PASS
	if err := VerifyD2(map[string][]byte{key: v0}, key, v0, inFlight); err != nil {
		t.Fatalf("VerifyD2 lastAcked failed: %v", err)
	}

	// Dump with inFlight v1 -> PASS
	if err := VerifyD2(map[string][]byte{key: v1}, key, v0, inFlight); err != nil {
		t.Fatalf("VerifyD2 inFlight v1 failed: %v", err)
	}

	// Dump with inFlight v2 -> PASS
	if err := VerifyD2(map[string][]byte{key: v2}, key, v0, inFlight); err != nil {
		t.Fatalf("VerifyD2 inFlight v2 failed: %v", err)
	}

	// 2. Both writers are ACKed in order (writer 1 then writer 2):
	// Now lastAcked is v2! inFlight is empty.
	// Dump with stale v1 MUST FAIL!
	if err := VerifyD2(map[string][]byte{key: v1}, key, v2, nil); err == nil {
		t.Fatal("expected VerifyD2 to reject stale value v1 after v2 ACKed, got nil")
	}

	// Dump with v2 passes
	if err := VerifyD2(map[string][]byte{key: v2}, key, v2, nil); err != nil {
		t.Fatalf("VerifyD2 v2 failed: %v", err)
	}

	// 3. In-flight DEL (represented as nil entry in inFlight)
	inFlightDel := [][]byte{nil}
	// Dump where key is absent matches in-flight DEL -> PASS
	if err := VerifyD2(map[string][]byte{}, key, v2, inFlightDel); err != nil {
		t.Fatalf("VerifyD2 inFlight DEL absent failed: %v", err)
	}

	// Dump where key holds stale value v1 -> FAIL
	if err := VerifyD2(map[string][]byte{key: v1}, key, v2, inFlightDel); err == nil {
		t.Fatal("expected VerifyD2 failure on stale v1 with inFlight DEL, got nil")
	}

	// 4. lastAcked == nil (key acknowledged as absent)
	if err := VerifyD2(map[string][]byte{}, key, nil, nil); err != nil {
		t.Fatalf("VerifyD2 absent key with nil lastAcked failed: %v", err)
	}
}

func TestOracleLoggingAndFlush(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "oracle.log")

	o, err := NewOracle(logPath, 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	op := Operation{OpID: "op_log", Lane: "w1", Items: []OpItem{{Key: "k", Val: []byte("v")}}}
	if err := o.Dispatch(op); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if err := o.RecordAck("op_log"); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if err := o.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if err := o.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestOracleCleanMatch(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	op1 := Operation{OpID: "op1", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}
	op2 := Operation{OpID: "op2", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2")}}}

	if err := o.Dispatch(op1); err != nil {
		t.Fatalf("dispatch op1 failed: %v", err)
	}
	if err := o.RecordAck("op1"); err != nil {
		t.Fatalf("ack op1 failed: %v", err)
	}

	if err := o.Dispatch(op2); err != nil {
		t.Fatalf("dispatch op2 failed: %v", err)
	}
	if err := o.RecordAck("op2"); err != nil {
		t.Fatalf("ack op2 failed: %v", err)
	}

	dump := map[string][]byte{
		"k1": []byte("v1"),
		"k2": []byte("v2"),
	}

	res, err := o.Verify(dump)
	if err != nil {
		t.Fatalf("Verify failed on clean match: %v", err)
	}
	if res.TotalKeys != 2 || len(res.MatchedCandidate) != 0 {
		t.Fatalf("unexpected res: %+v", res)
	}
}

func TestOracleBatchAllOrNothing(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	batchOp := Operation{
		OpID: "batch1",
		Lane: "w1",
		Items: []OpItem{
			{Key: "b1", Val: []byte("v1")},
			{Key: "b2", Val: []byte("v2")},
			{Key: "b3", Val: []byte("v3")},
		},
	}
	if err := o.Dispatch(batchOp); err != nil {
		t.Fatalf("dispatch batch1 failed: %v", err)
	}

	// Partially applied dump (only b1 and b2) must FAIL
	partialDump := map[string][]byte{
		"b1": []byte("v1"),
		"b2": []byte("v2"),
	}
	if _, err := o.Verify(partialDump); err == nil {
		t.Fatal("expected failure on partial batch, got nil")
	}

	// All applied dump must PASS
	fullDump := map[string][]byte{
		"b1": []byte("v1"),
		"b2": []byte("v2"),
		"b3": []byte("v3"),
	}
	if _, err := o.Verify(fullDump); err != nil {
		t.Fatalf("full batch should pass: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Restored Oracle Tests: Subsets, In-Flight Delete, Negative Cases
// -----------------------------------------------------------------------------

func TestOracleInFlightSubsets(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	// Base committed op
	baseOp := Operation{OpID: "sub_base", Lane: "w1", Items: []OpItem{{Key: "k0", Val: []byte("v0")}}}
	if err := o.Dispatch(baseOp); err != nil {
		t.Fatalf("dispatch baseOp failed: %v", err)
	}
	if err := o.RecordAck("sub_base"); err != nil {
		t.Fatalf("ack baseOp failed: %v", err)
	}

	// Two in-flight ops across disjoint keys
	inFlight1 := Operation{OpID: "sub_w1_1", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}
	inFlight2 := Operation{OpID: "sub_w2_1", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2")}}}

	if err := o.Dispatch(inFlight1); err != nil {
		t.Fatalf("dispatch inFlight1 failed: %v", err)
	}
	if err := o.Dispatch(inFlight2); err != nil {
		t.Fatalf("dispatch inFlight2 failed: %v", err)
	}

	// 1. Candidate: neither committed -> dump has only k0
	dumpNeither := map[string][]byte{"k0": []byte("v0")}
	res, err := o.Verify(dumpNeither)
	if err != nil {
		t.Fatalf("dumpNeither failed: %v", err)
	}
	if len(res.MatchedCandidate) != 0 || len(res.DroppedOps) != 2 {
		t.Fatalf("expected 0 applied, 2 dropped, got: %+v", res)
	}

	// Dispatch next unique in-flight ops for subsequent subset test
	inFlight3 := Operation{OpID: "sub_w1_2", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1_new")}}}
	inFlight4 := Operation{OpID: "sub_w2_2", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2_new")}}}

	if err := o.Dispatch(inFlight3); err != nil {
		t.Fatalf("dispatch inFlight3 failed: %v", err)
	}
	if err := o.Dispatch(inFlight4); err != nil {
		t.Fatalf("dispatch inFlight4 failed: %v", err)
	}

	// 2. Candidate: sub_w1_2 committed, sub_w2_2 dropped -> dump has k0, k1
	dumpW1 := map[string][]byte{
		"k0": []byte("v0"),
		"k1": []byte("v1_new"),
	}
	res, err = o.Verify(dumpW1)
	if err != nil {
		t.Fatalf("dumpW1 failed: %v", err)
	}
	if len(res.MatchedCandidate) != 1 || res.MatchedCandidate[0] != "sub_w1_2" {
		t.Fatalf("expected sub_w1_2 applied, got: %+v", res)
	}

	// Dispatch next op on lane w2
	inFlight5 := Operation{OpID: "sub_w2_3", Lane: "w2", Items: []OpItem{{Key: "k2", Val: []byte("v2_final")}}}
	if err := o.Dispatch(inFlight5); err != nil {
		t.Fatalf("dispatch inFlight5 failed: %v", err)
	}

	// 3. Candidate: sub_w2_3 committed -> dump has k0, k1 (from previous carry-forward), k2
	dumpBoth := map[string][]byte{
		"k0": []byte("v0"),
		"k1": []byte("v1_new"),
		"k2": []byte("v2_final"),
	}
	res, err = o.Verify(dumpBoth)
	if err != nil {
		t.Fatalf("dumpBoth failed: %v", err)
	}
	if len(res.MatchedCandidate) != 1 || res.MatchedCandidate[0] != "sub_w2_3" {
		t.Fatalf("expected sub_w2_3 applied, got: %+v", res)
	}
}

func TestOracleInFlightDelete(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	// 1. Base key exists and is ACKed
	baseOp := Operation{OpID: "del_base_1", Lane: "w1", Items: []OpItem{{Key: "target_del", Val: []byte("initial")}}}
	if err := o.Dispatch(baseOp); err != nil {
		t.Fatalf("dispatch del_base_1 failed: %v", err)
	}
	if err := o.RecordAck("del_base_1"); err != nil {
		t.Fatalf("ack del_base_1 failed: %v", err)
	}

	// Dispatch in-flight DEL
	delOp1 := Operation{OpID: "del_inflight_1", Lane: "w1", Items: []OpItem{{Key: "target_del", IsDel: true}}}
	if err := o.Dispatch(delOp1); err != nil {
		t.Fatalf("dispatch del_inflight_1 failed: %v", err)
	}

	// Case A: Key is absent in dump -> DEL committed -> PASS
	dumpAbsent := map[string][]byte{}
	res, err := o.Verify(dumpAbsent)
	if err != nil {
		t.Fatalf("expected absent key to pass with in-flight DEL: %v", err)
	}
	if len(res.MatchedCandidate) != 1 || res.MatchedCandidate[0] != "del_inflight_1" {
		t.Fatalf("expected del_inflight_1 applied, got: %+v", res)
	}

	// 2. Re-add key and dispatch another in-flight delete
	addOp := Operation{OpID: "del_base_2", Lane: "w1", Items: []OpItem{{Key: "target_del", Val: []byte("val2")}}}
	if err := o.Dispatch(addOp); err != nil {
		t.Fatalf("dispatch del_base_2 failed: %v", err)
	}
	if err := o.RecordAck("del_base_2"); err != nil {
		t.Fatalf("ack del_base_2 failed: %v", err)
	}

	delOp2 := Operation{OpID: "del_inflight_2", Lane: "w1", Items: []OpItem{{Key: "target_del", IsDel: true}}}
	if err := o.Dispatch(delOp2); err != nil {
		t.Fatalf("dispatch del_inflight_2 failed: %v", err)
	}

	// Case B: Key is present with val2 -> DEL dropped -> PASS
	dumpPresent := map[string][]byte{"target_del": []byte("val2")}
	res, err = o.Verify(dumpPresent)
	if err != nil {
		t.Fatalf("expected present key with prior value to pass: %v", err)
	}
	if len(res.MatchedCandidate) != 0 || len(res.DroppedOps) != 1 {
		t.Fatalf("expected del_inflight_2 dropped, got: %+v", res)
	}

	// Case C: Key holds corrupted value -> FAIL
	delOp3 := Operation{OpID: "del_inflight_3", Lane: "w1", Items: []OpItem{{Key: "target_del", IsDel: true}}}
	if err := o.Dispatch(delOp3); err != nil {
		t.Fatalf("dispatch del_inflight_3 failed: %v", err)
	}
	dumpCorrupt := map[string][]byte{"target_del": []byte("corrupted_val")}
	if _, err := o.Verify(dumpCorrupt); err == nil {
		t.Fatal("expected failure on corrupted value during in-flight DEL, got nil")
	}
}

func TestOracleNegativeCases(t *testing.T) {
	o, err := NewOracle("", 2)
	if err != nil {
		t.Fatalf("NewOracle failed: %v", err)
	}

	op := Operation{OpID: "neg_op1", Lane: "w1", Items: []OpItem{{Key: "k1", Val: []byte("v1")}}}
	if err := o.Dispatch(op); err != nil {
		t.Fatalf("dispatch neg_op1 failed: %v", err)
	}
	if err := o.RecordAck("neg_op1"); err != nil {
		t.Fatalf("ack neg_op1 failed: %v", err)
	}

	// Negative 1: Missing committed key
	emptyDump := map[string][]byte{}
	if _, err := o.Verify(emptyDump); err == nil {
		t.Fatal("expected failure on missing committed key")
	}

	// Negative 2: Extra unknown phantom key
	extraDump := map[string][]byte{
		"k1":      []byte("v1"),
		"phantom": []byte("boo"),
	}
	if _, err := o.Verify(extraDump); err == nil {
		t.Fatal("expected failure on phantom extra key")
	}

	// Negative 3: Value byte mismatch
	mismatchDump := map[string][]byte{
		"k1": []byte("wrong_value"),
	}
	if _, err := o.Verify(mismatchDump); err == nil {
		t.Fatal("expected failure on value mismatch")
	}
}

