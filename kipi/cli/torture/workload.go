// cli/torture/workload.go — Workload profiles per mode and operation generation.
//
// Each profile defines:
//   - writers: number of concurrent goroutines sending ops to serve
//   - genOp:   function that generates one random operation for the profile

package main

import (
	"fmt"
	"math/rand"
	"strings"
)

// workloadProfile defines the concurrency and op-generator for a torture mode.
type workloadProfile struct {
	writers int
	genOp   func(rng *rand.Rand, opID string) *serveOp
}

// profileFor returns the workload profile for the given mode name.
func profileFor(mode string, rng *rand.Rand) *workloadProfile {
	switch mode {
	case "large-values":
		return largeValuesProfile()
	case "compaction":
		return compactionProfile()
	case "recovery-crash":
		return defaultProfile()
	case "full-disk":
		return fullDiskProfile()
	default:
		return defaultProfile()
	}
}

// defaultProfile: mixed puts/deletes/gets across 1 000 keys, 2 concurrent writers.
func defaultProfile() *workloadProfile {
	keySpace := makeKeySpace(1000, "default")
	return &workloadProfile{
		writers: 2,
		genOp: func(rng *rand.Rand, opID string) *serveOp {
			key := keySpace[rng.Intn(len(keySpace))]
			r := rng.Intn(10)
			switch {
			case r < 5: // 50% put
				return &serveOp{opID: opID, opType: "put", key: key, value: makeValue(rng, 10, 200)}
			case r < 8: // 30% del
				return &serveOp{opID: opID, opType: "del", key: key}
			default: // 20% get
				return &serveOp{opID: opID, opType: "get", key: key}
			}
		},
	}
}

// largeValuesProfile: writes values close to 1 MB across 100 keys, 2 writers.
func largeValuesProfile() *workloadProfile {
	keySpace := makeKeySpace(100, "large")
	return &workloadProfile{
		writers: 2,
		genOp: func(rng *rand.Rand, opID string) *serveOp {
			key := keySpace[rng.Intn(len(keySpace))]
			val := makeValue(rng, 900*1024, 1024*1024)
			return &serveOp{opID: opID, opType: "put", key: key, value: val}
		},
	}
}

// compactionProfile: high-churn puts + deletes across 5 000 keys to trigger compaction.
func compactionProfile() *workloadProfile {
	keySpace := makeKeySpace(5000, "compact")
	return &workloadProfile{
		writers: 2,
		genOp: func(rng *rand.Rand, opID string) *serveOp {
			key := keySpace[rng.Intn(len(keySpace))]
			if rng.Intn(2) == 0 {
				return &serveOp{opID: opID, opType: "put", key: key, value: makeValue(rng, 50, 500)}
			}
			return &serveOp{opID: opID, opType: "del", key: key}
		},
	}
}

// fullDiskProfile: writes medium values until ErrDiskFull, 1 writer.
func fullDiskProfile() *workloadProfile {
	keySpace := makeKeySpace(100, "fulldisk")
	return &workloadProfile{
		writers: 1,
		genOp: func(rng *rand.Rand, opID string) *serveOp {
			key := keySpace[rng.Intn(len(keySpace))]
			return &serveOp{opID: opID, opType: "put", key: key, value: makeValue(rng, 1000, 5000)}
		},
	}
}

// makeKeySpace returns n distinct keys with a given prefix.
func makeKeySpace(n int, prefix string) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("%s-key-%06d", prefix, i)
	}
	return keys
}

// makeValue generates a random alphanumeric string of length in [minSize, maxSize).
func makeValue(rng *rand.Rand, minSize, maxSize int) string {
	size := minSize
	if maxSize > minSize {
		size = minSize + rng.Intn(maxSize-minSize)
	}
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	var sb strings.Builder
	sb.Grow(size)
	for i := 0; i < size; i++ {
		sb.WriteByte(chars[rng.Intn(len(chars))])
	}
	return sb.String()
}

