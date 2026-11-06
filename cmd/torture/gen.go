package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
)

// Mode represents the active workload profile.
type Mode string

const (
	ModeDefault       Mode = "default"
	ModeLargeValues   Mode = "large-values"
	ModeLargeBatches  Mode = "large-batches"
	ModeCompaction    Mode = "compaction"
	ModeRecoveryCrash Mode = "recovery-crash"
	ModeFullDisk      Mode = "full-disk"
	ModeDiskFull      Mode = "disk-full" // alias for full-disk
	ModeD2SameKey     Mode = "d2-same-key"
)

// NormalizeMode validates and normalizes a mode string (handling aliases).
func NormalizeMode(m string) (Mode, error) {
	switch Mode(strings.ToLower(m)) {
	case ModeDefault:
		return ModeDefault, nil
	case ModeLargeValues:
		return ModeLargeValues, nil
	case ModeLargeBatches:
		return ModeLargeBatches, nil
	case ModeCompaction:
		return ModeCompaction, nil
	case ModeRecoveryCrash:
		return ModeRecoveryCrash, nil
	case ModeFullDisk, ModeDiskFull:
		return ModeFullDisk, nil
	case ModeD2SameKey:
		return ModeD2SameKey, nil
	default:
		return "", fmt.Errorf("unknown mode %q (valid: default, large-values, large-batches, compaction, recovery-crash, full-disk, d2-same-key)", m)
	}
}

// GenConfig configures workload generation for an iteration.
type GenConfig struct {
	Seed       int64
	Mode       Mode
	Lanes      int
	Iteration  int
	OpsPerLane int
}

// LaneQueue holds the pre-generated sequence of operations for a single writer lane.
type LaneQueue struct {
	Lane string
	Ops  []Operation
}

// GenerateLaneQueues creates deterministic, reproducible operation queues for each lane.
// The same seed and iteration will produce identical operations across runs.
func GenerateLaneQueues(cfg GenConfig) ([]LaneQueue, error) {
	normMode, err := NormalizeMode(string(cfg.Mode))
	if err != nil {
		return nil, err
	}
	if cfg.Lanes <= 0 {
		return nil, errors.New("lanes must be > 0")
	}
	if cfg.OpsPerLane <= 0 {
		cfg.OpsPerLane = 50 // default ops per lane per iteration
	}

	queues := make([]LaneQueue, cfg.Lanes)

	for l := 0; l < cfg.Lanes; l++ {
		laneName := fmt.Sprintf("w%d", l+1)
		// Derive a deterministic seed per lane and iteration
		laneSeed := cfg.Seed ^ int64((l+1)*7919) ^ int64(cfg.Iteration*104729)
		rng := rand.New(rand.NewSource(laneSeed))

		ops, err := generateOpsForLane(rng, normMode, laneName, l+1, cfg.Iteration, cfg.OpsPerLane)
		if err != nil {
			return nil, fmt.Errorf("lane %s generator: %w", laneName, err)
		}

		queues[l] = LaneQueue{
			Lane: laneName,
			Ops:  ops,
		}
	}

	return queues, nil
}

func generateOpsForLane(rng *rand.Rand, mode Mode, laneName string, laneNum, iter, count int) ([]Operation, error) {
	ops := make([]Operation, 0, count)

	keyPool := make([]string, 0, 100)
	nextKeyID := 1

	for seq := 1; seq <= count; seq++ {
		opID := fmt.Sprintf("%s_iter%d_%d", laneName, iter, seq)

		switch mode {
		case ModeDefault, ModeRecoveryCrash:
			op, err := genDefaultOp(rng, opID, laneName, uint64(seq), &keyPool, &nextKeyID)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)

		case ModeLargeValues:
			op, err := genLargeValueOp(rng, opID, laneName, uint64(seq), &keyPool, &nextKeyID)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)

		case ModeLargeBatches:
			op, err := genLargeBatchOp(rng, opID, laneName, uint64(seq))
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)

		case ModeCompaction:
			op, err := genCompactionOp(rng, opID, laneName, uint64(seq), laneNum)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)

		case ModeFullDisk:
			op, err := genFullDiskOp(rng, opID, laneName, uint64(seq), &keyPool, &nextKeyID)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)

		case ModeD2SameKey:
			op, err := genD2SameKeyOp(rng, opID, laneName, uint64(seq))
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)

		default:
			return nil, fmt.Errorf("unsupported mode %s", mode)
		}
	}

	return ops, nil
}

// -----------------------------------------------------------------------------
// Mode Generators
// -----------------------------------------------------------------------------

// default mode: 70% single HPUT, 15% single HDEL, 15% HBATCH (2-10 ops)
// keys: 16-64 bytes, values: 32-1024 bytes
func genDefaultOp(rng *rand.Rand, opID, laneName string, seq uint64, keyPool *[]string, nextKeyID *int) (Operation, error) {
	roll := rng.Float64()

	if roll < 0.70 {
		// Single PUT
		k := pickOrMakeKey(rng, keyPool, nextKeyID, laneName, 16, 64)
		minLen := MinEnvelopeLen([]byte(k), opID, seq)
		valLen := minLen
		if 1024 > minLen {
			valLen = minLen + rng.Intn(1024-minLen+1)
		}
		v, err := BuildValueEnvelope([]byte(k), opID, seq, valLen, nil)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			OpID:  opID,
			Lane:  laneName,
			Items: []OpItem{{Key: k, Val: v, IsDel: false}},
		}, nil
	} else if roll < 0.85 && len(*keyPool) > 0 {
		// Single DEL
		k := (*keyPool)[rng.Intn(len(*keyPool))]
		return Operation{
			OpID:  opID,
			Lane:  laneName,
			Items: []OpItem{{Key: k, IsDel: true}},
		}, nil
	} else {
		// HBATCH: 2 to 10 ops
		batchSize := 2 + rng.Intn(9)
		items := make([]OpItem, batchSize)
		for i := 0; i < batchSize; i++ {
			if rng.Float64() < 0.80 || len(*keyPool) == 0 {
				k := pickOrMakeKey(rng, keyPool, nextKeyID, laneName, 16, 64)
				subSeq := seq*100 + uint64(i)
				minLen := MinEnvelopeLen([]byte(k), opID, subSeq)
				valLen := minLen
				if 512 > minLen {
					valLen = minLen + rng.Intn(512-minLen+1)
				}
				v, err := BuildValueEnvelope([]byte(k), opID, subSeq, valLen, nil)
				if err != nil {
					return Operation{}, err
				}
				items[i] = OpItem{Key: k, Val: v, IsDel: false}
			} else {
				k := (*keyPool)[rng.Intn(len(*keyPool))]
				items[i] = OpItem{Key: k, IsDel: true}
			}
		}
		return Operation{
			OpID:  opID,
			Lane:  laneName,
			Items: items,
		}, nil
	}
}

// large-values mode: single puts and batches with values up to 1,048,576 bytes
func genLargeValueOp(rng *rand.Rand, opID, laneName string, seq uint64, keyPool *[]string, nextKeyID *int) (Operation, error) {
	const oneMiB = 1048576
	k := pickOrMakeKey(rng, keyPool, nextKeyID, laneName, 16, 64)

	var targetLen int
	roll := rng.Float64()
	if roll < 0.40 {
		targetLen = oneMiB // exactly 1 MiB
	} else if roll < 0.70 {
		targetLen = 512 * 1024 // 512 KiB
	} else {
		minLen := MinEnvelopeLen([]byte(k), opID, seq)
		targetLen = minLen + 64*1024 + rng.Intn(128*1024)
		if targetLen > oneMiB {
			targetLen = oneMiB
		}
	}

	v, err := BuildValueEnvelope([]byte(k), opID, seq, targetLen, nil)
	if err != nil {
		return Operation{}, err
	}

	return Operation{
		OpID:  opID,
		Lane:  laneName,
		Items: []OpItem{{Key: k, Val: v, IsDel: false}},
	}, nil
}

// large-batches mode: batches of 500 to 10,000 ops
func genLargeBatchOp(rng *rand.Rand, opID, laneName string, seq uint64) (Operation, error) {
	batchSize := 500 + rng.Intn(1501)
	items := make([]OpItem, batchSize)

	for i := 0; i < batchSize; i++ {
		k := fmt.Sprintf("%s_b%d_k%d", laneName, seq, i+1)
		subSeq := seq*10000 + uint64(i)
		minLen := MinEnvelopeLen([]byte(k), opID, subSeq)
		valLen := minLen + rng.Intn(16)
		v, err := BuildValueEnvelope([]byte(k), opID, subSeq, valLen, nil)
		if err != nil {
			return Operation{}, err
		}
		items[i] = OpItem{Key: k, Val: v, IsDel: false}
	}

	return Operation{
		OpID:  opID,
		Lane:  laneName,
		Items: items,
	}, nil
}

// compaction mode: high churn on small key space (10-30 keys)
func genCompactionOp(rng *rand.Rand, opID, laneName string, seq uint64, laneNum int) (Operation, error) {
	keyIndex := rng.Intn(20)
	k := fmt.Sprintf("compact_l%d_k%d", laneNum, keyIndex)

	if rng.Float64() < 0.50 {
		minLen := MinEnvelopeLen([]byte(k), opID, seq)
		valLen := minLen + 128 + rng.Intn(1024)
		v, err := BuildValueEnvelope([]byte(k), opID, seq, valLen, nil)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			OpID:  opID,
			Lane:  laneName,
			Items: []OpItem{{Key: k, Val: v, IsDel: false}},
		}, nil
	} else {
		return Operation{
			OpID:  opID,
			Lane:  laneName,
			Items: []OpItem{{Key: k, IsDel: true}},
		}, nil
	}
}

// full-disk mode: rapid writes of medium values to hit --max-bytes cap
func genFullDiskOp(rng *rand.Rand, opID, laneName string, seq uint64, keyPool *[]string, nextKeyID *int) (Operation, error) {
	k := pickOrMakeKey(rng, keyPool, nextKeyID, laneName, 16, 32)
	minLen := MinEnvelopeLen([]byte(k), opID, seq)
	valLen := minLen + 4096 + rng.Intn(16384) // 4KB - 20KB
	v, err := BuildValueEnvelope([]byte(k), opID, seq, valLen, nil)
	if err != nil {
		return Operation{}, err
	}
	return Operation{
		OpID:  opID,
		Lane:  laneName,
		Items: []OpItem{{Key: k, Val: v, IsDel: false}},
	}, nil
}

// d2-same-key mode: multiple lanes target the exact same shared key
func genD2SameKeyOp(rng *rand.Rand, opID, laneName string, seq uint64) (Operation, error) {
	sharedKey := "d2_shared_contested_key"
	minLen := MinEnvelopeLen([]byte(sharedKey), opID, seq)
	valLen := minLen + 64 + rng.Intn(128)
	v, err := BuildValueEnvelope([]byte(sharedKey), opID, seq, valLen, nil)
	if err != nil {
		return Operation{}, err
	}
	return Operation{
		OpID:  opID,
		Lane:  laneName,
		Items: []OpItem{{Key: sharedKey, Val: v, IsDel: false}},
	}, nil
}

func pickOrMakeKey(rng *rand.Rand, keyPool *[]string, nextKeyID *int, laneName string, minLen, maxLen int) string {
	if len(*keyPool) > 0 && rng.Float64() < 0.40 {
		return (*keyPool)[rng.Intn(len(*keyPool))]
	}
	id := *nextKeyID
	*nextKeyID++
	prefix := fmt.Sprintf("%s_k%d_", laneName, id)
	targetLen := minLen
	if maxLen > minLen {
		targetLen = minLen + rng.Intn(maxLen-minLen+1)
	}

	var pad strings.Builder
	for pad.Len()+len(prefix) < targetLen {
		pad.WriteByte(byte('a' + rng.Intn(26)))
	}
	k := prefix + pad.String()
	*keyPool = append(*keyPool, k)
	return k
}

// -----------------------------------------------------------------------------
// Hex Wire Protocol Formatting
// -----------------------------------------------------------------------------

// FormatCommandHex serializes an Operation into the Hex Dialect string for stdin:
// Single PUT:   HPUT <op-id> <hexkey> <hexval>\n
// Single DEL:   HDEL <op-id> <hexkey>\n
// Batch:        HBATCH <op-id> <n>\n followed by n lines of HPUT/HDEL
// Empty value is represented by "-".
func FormatCommandHex(op Operation) string {
	if len(op.Items) == 1 {
		item := op.Items[0]
		keyHex := hex.EncodeToString([]byte(item.Key))
		if item.IsDel {
			return fmt.Sprintf("HDEL %s %s\n", op.OpID, keyHex)
		}
		valHex := "-"
		if len(item.Val) > 0 {
			valHex = hex.EncodeToString(item.Val)
		}
		return fmt.Sprintf("HPUT %s %s %s\n", op.OpID, keyHex, valHex)
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "HBATCH %s %d\n", op.OpID, len(op.Items))
	for _, item := range op.Items {
		keyHex := hex.EncodeToString([]byte(item.Key))
		if item.IsDel {
			fmt.Fprintf(&buf, "HDEL %s\n", keyHex)
		} else {
			valHex := "-"
			if len(item.Val) > 0 {
				valHex = hex.EncodeToString(item.Val)
			}
			fmt.Fprintf(&buf, "HPUT %s %s\n", keyHex, valHex)
		}
	}
	return buf.String()
}
