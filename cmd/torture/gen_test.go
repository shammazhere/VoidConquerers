package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestGenDeterminism(t *testing.T) {
	cfg := GenConfig{
		Seed:       42,
		Mode:       ModeDefault,
		Lanes:      2,
		Iteration:  1,
		OpsPerLane: 25,
	}

	run1, err := GenerateLaneQueues(cfg)
	if err != nil {
		t.Fatalf("first generate failed: %v", err)
	}

	run2, err := GenerateLaneQueues(cfg)
	if err != nil {
		t.Fatalf("second generate failed: %v", err)
	}

	if len(run1) != len(run2) {
		t.Fatalf("lane count mismatch: %d vs %d", len(run1), len(run2))
	}

	for l := 0; l < len(run1); l++ {
		l1 := run1[l]
		l2 := run2[l]

		if l1.Lane != l2.Lane {
			t.Fatalf("lane name mismatch at %d: %s vs %s", l, l1.Lane, l2.Lane)
		}
		if len(l1.Ops) != len(l2.Ops) {
			t.Fatalf("ops count mismatch on lane %s: %d vs %d", l1.Lane, len(l1.Ops), len(l2.Ops))
		}

		for i := 0; i < len(l1.Ops); i++ {
			op1 := l1.Ops[i]
			op2 := l2.Ops[i]

			if op1.OpID != op2.OpID {
				t.Fatalf("opID mismatch at lane %s op %d: %s vs %s", l1.Lane, i, op1.OpID, op2.OpID)
			}
			if len(op1.Items) != len(op2.Items) {
				t.Fatalf("items count mismatch at %s: %d vs %d", op1.OpID, len(op1.Items), len(op2.Items))
			}
			for j := 0; j < len(op1.Items); j++ {
				it1 := op1.Items[j]
				it2 := op2.Items[j]
				if it1.Key != it2.Key || it1.IsDel != it2.IsDel || !bytes.Equal(it1.Val, it2.Val) {
					t.Fatalf("item mismatch in op %s item %d: %+v vs %+v", op1.OpID, j, it1, it2)
				}
			}
		}
	}
}

func TestGenDifferentSeeds(t *testing.T) {
	cfg1 := GenConfig{Seed: 100, Mode: ModeDefault, Lanes: 2, Iteration: 1, OpsPerLane: 10}
	cfg2 := GenConfig{Seed: 200, Mode: ModeDefault, Lanes: 2, Iteration: 1, OpsPerLane: 10}

	run1, err := GenerateLaneQueues(cfg1)
	if err != nil {
		t.Fatalf("generate cfg1 failed: %v", err)
	}
	run2, err := GenerateLaneQueues(cfg2)
	if err != nil {
		t.Fatalf("generate cfg2 failed: %v", err)
	}

	// At least one op payload or key should differ between different seeds
	allIdentical := true
	for l := 0; l < len(run1); l++ {
		for i := 0; i < len(run1[l].Ops); i++ {
			it1 := run1[l].Ops[i].Items[0]
			it2 := run2[l].Ops[i].Items[0]
			if it1.Key != it2.Key || !bytes.Equal(it1.Val, it2.Val) {
				allIdentical = false
				break
			}
		}
	}
	if allIdentical {
		t.Fatal("expected different workloads from different seeds, but they were identical")
	}
}

func TestOpIDUniquenessAcrossIterationsAndLanes(t *testing.T) {
	seen := make(map[string]struct{})
	lanes := 3
	iterations := 5
	opsPerLane := 20

	for iter := 1; iter <= iterations; iter++ {
		cfg := GenConfig{
			Seed:       12345,
			Mode:       ModeDefault,
			Lanes:      lanes,
			Iteration:  iter,
			OpsPerLane: opsPerLane,
		}
		queues, err := GenerateLaneQueues(cfg)
		if err != nil {
			t.Fatalf("generate iter %d failed: %v", iter, err)
		}

		for _, q := range queues {
			for _, op := range q.Ops {
				if strings.Contains(op.OpID, ":") {
					t.Fatalf("opID %q contains forbidden colon ':'", op.OpID)
				}
				if _, exists := seen[op.OpID]; exists {
					t.Fatalf("duplicate opID detected: %q", op.OpID)
				}
				seen[op.OpID] = struct{}{}
			}
		}
	}

	expectedTotal := lanes * iterations * opsPerLane
	if len(seen) != expectedTotal {
		t.Fatalf("expected %d unique opIDs, got %d", expectedTotal, len(seen))
	}
}

func TestModeProfilesGeneration(t *testing.T) {
	modes := []Mode{
		ModeDefault,
		ModeLargeValues,
		ModeLargeBatches,
		ModeCompaction,
		ModeRecoveryCrash,
		ModeFullDisk,
		ModeDiskFull,
		ModeD2SameKey,
	}

	for _, m := range modes {
		cfg := GenConfig{
			Seed:       777,
			Mode:       m,
			Lanes:      2,
			Iteration:  1,
			OpsPerLane: 5,
		}
		queues, err := GenerateLaneQueues(cfg)
		if err != nil {
			t.Fatalf("mode %s generation failed: %v", m, err)
		}
		if len(queues) != 2 {
			t.Fatalf("mode %s: expected 2 queues, got %d", m, len(queues))
		}
	}

	// Specifically verify large-values generates valid envelopes up to 1 MiB
	cfgLarge := GenConfig{
		Seed:       999,
		Mode:       ModeLargeValues,
		Lanes:      1,
		Iteration:  1,
		OpsPerLane: 10,
	}
	largeQueues, err := GenerateLaneQueues(cfgLarge)
	if err != nil {
		t.Fatalf("generate large-values failed: %v", err)
	}

	found1MiB := false
	for _, op := range largeQueues[0].Ops {
		for _, item := range op.Items {
			if len(item.Val) == 1048576 {
				found1MiB = true
			}
			if len(item.Val) > 0 {
				_, err := ParseAndVerifyEnvelope([]byte(item.Key), item.Val)
				if err != nil {
					t.Fatalf("envelope validation failed on op %s key %s: %v", op.OpID, item.Key, err)
				}
			}
		}
	}
	if !found1MiB {
		t.Fatal("expected at least one 1 MiB value in large-values mode")
	}

	// Specifically verify d2-same-key mode targets the shared key
	cfgD2 := GenConfig{
		Seed:       888,
		Mode:       ModeD2SameKey,
		Lanes:      2,
		Iteration:  1,
		OpsPerLane: 5,
	}
	d2Queues, err := GenerateLaneQueues(cfgD2)
	if err != nil {
		t.Fatalf("generate d2 failed: %v", err)
	}
	k1 := d2Queues[0].Ops[0].Items[0].Key
	k2 := d2Queues[1].Ops[0].Items[0].Key
	if k1 != k2 || k1 != "d2_shared_contested_key" {
		t.Fatalf("d2 mode did not target shared key: lane1=%s, lane2=%s", k1, k2)
	}
}

func TestFormatCommandHex(t *testing.T) {
	// Single PUT
	opPut := Operation{
		OpID: "op1",
		Items: []OpItem{
			{Key: "hello", Val: []byte("world"), IsDel: false},
		},
	}
	linePut := FormatCommandHex(opPut)
	expectedPut := "HPUT op1 68656c6c6f 776f726c64\n"
	if linePut != expectedPut {
		t.Fatalf("FormatCommandHex PUT mismatch:\nexpected: %q\ngot:      %q", expectedPut, linePut)
	}

	// Single DEL
	opDel := Operation{
		OpID: "op2",
		Items: []OpItem{
			{Key: "hello", IsDel: true},
		},
	}
	lineDel := FormatCommandHex(opDel)
	expectedDel := "HDEL op2 68656c6c6f\n"
	if lineDel != expectedDel {
		t.Fatalf("FormatCommandHex DEL mismatch:\nexpected: %q\ngot:      %q", expectedDel, lineDel)
	}

	// Single PUT with empty value -> "-"
	opPutEmpty := Operation{
		OpID: "op3",
		Items: []OpItem{
			{Key: "empty_val", Val: []byte{}, IsDel: false},
		},
	}
	lineEmpty := FormatCommandHex(opPutEmpty)
	expectedEmpty := "HPUT op3 656d7074795f76616c -\n"
	if lineEmpty != expectedEmpty {
		t.Fatalf("FormatCommandHex empty val mismatch:\nexpected: %q\ngot:      %q", expectedEmpty, lineEmpty)
	}

	// Batch
	opBatch := Operation{
		OpID: "batch1",
		Items: []OpItem{
			{Key: "k1", Val: []byte("v1"), IsDel: false},
			{Key: "k2", IsDel: true},
		},
	}
	lineBatch := FormatCommandHex(opBatch)
	expectedBatch := "HBATCH batch1 2\nHPUT 6b31 7631\nHDEL 6b32\n"
	if lineBatch != expectedBatch {
		t.Fatalf("FormatCommandHex batch mismatch:\nexpected: %q\ngot:      %q", expectedBatch, lineBatch)
	}
}

