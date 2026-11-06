// cmd/torture/main.go — Torture tool: runs serve as a child, sends random ops,
// kills at random, verifies correctness, loops for --iterations.
//
// Usage:
//   torture --iterations 500 --seed 12345
//   torture --iterations 100 --seed 999  --mode large-values
//   torture --iterations 100 --seed 7    --mode compaction
//   torture --iterations 100 --seed 42   --mode recovery-crash
//   torture --iterations 100 --seed 1    --mode full-disk

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

var (
	iterations      = flag.Int("iterations", 500, "number of kill/restart cycles")
	seed            = flag.Int64("seed", 0, "random seed (required)")
	mode            = flag.String("mode", "default", "workload mode: default|large-values|compaction|recovery-crash|full-disk")
	kvdbBin         = flag.String("kvdb", "", "path to kvdb binary (auto-detected if empty)")
	maxBytes        = flag.Int64("max-bytes", 0, "max database size (for full-disk mode)")
	workDir         = flag.String("work-dir", "torture-work", "working directory for torture database")
	verbose         = flag.Bool("v", false, "verbose output")
)

func main() {
	flag.Parse()

	if *seed == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --seed is required")
		flag.Usage()
		os.Exit(1)
	}

	// Find kvdb binary
	bin := *kvdbBin
	if bin == "" {
		bin = findKvdb()
	}
	if bin == "" {
		fmt.Fprintln(os.Stderr, "ERROR: could not find kvdb binary; build with: go build -o . ./cmd/kvdb ./cmd/torture")
		os.Exit(1)
	}

	// Setup work directory
	dbPath := filepath.Join(*workDir, "db.kv")
	if err := os.MkdirAll(*workDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR creating work dir: %v\n", err)
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(*seed))

	// Preload: set up an initial state before the torture loop
	if err := preload(bin, dbPath, rng, *mode); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR in preload: %v\n", err)
		os.Exit(1)
	}

	passed := 0
	failed := 0
	start := time.Now()

	for i := 0; i < *iterations; i++ {
		iterStart := time.Now()
		result, err := runIteration(bin, dbPath, rng, *mode, *maxBytes, *workDir, i)
		iterDur := time.Since(iterStart)

		if err != nil {
			fmt.Fprintf(os.Stderr, "\nERROR in iteration %d: %v\n", i+1, err)
			failed++
			saveFailed(*workDir, dbPath, i+1)
			break // stop on first failure
		}

		if result.ok {
			passed++
			if *verbose {
				fmt.Printf("iter %4d: PASS [%s] %d keys, kill after %v, %s\n",
					i+1, *mode, result.keyCount, result.killAfter, iterDur.Round(time.Millisecond))
			} else {
				fmt.Printf("iter %4d: PASS %d keys kill-after=%v\n", i+1, result.keyCount, result.killAfter.Round(time.Millisecond))
			}
		} else {
			failed++
			fmt.Printf("iter %4d: FAIL — %s\n", i+1, result.failReason)
			saveFailed(*workDir, dbPath, i+1)
			break // stop on first failure
		}
	}

	total := time.Since(start)
	fmt.Printf("\n--- Summary ---\n")
	fmt.Printf("iterations: %d  passed: %d  failed: %d  total: %v\n",
		passed+failed, passed, failed, total.Round(time.Millisecond))

	if failed > 0 {
		fmt.Printf("FAIL — failure bundle saved to %s/failure/\n", *workDir)
		os.Exit(1)
	}
	fmt.Println("PASS — all iterations passed")
}

func findKvdb() string {
	// Look in current directory and parent
	candidates := []string{"kvdb", "./kvdb", "../kvdb"}
	if runtime.GOOS == "windows" {
		candidates = []string{"kvdb.exe", "./kvdb.exe", "../kvdb.exe"}
	}
	for _, c := range candidates {
		if p, err := exec.LookPath(c); err == nil {
			return p
		}
	}
	return ""
}

// preload sets up initial state before the torture loop.
func preload(bin, dbPath string, rng *rand.Rand, mode string) error {
	// If the db already exists, skip preload
	if _, err := os.Stat(dbPath); err == nil {
		return nil
	}

	// Open (creates the file)
	cmd := exec.Command(bin, "open", dbPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("preload open: %w", err)
	}
	return nil
}

// saveFailed copies the database file and oracle to a failure bundle.
func saveFailed(workDir, dbPath string, iteration int) {
	failDir := filepath.Join(workDir, "failure")
	os.MkdirAll(failDir, 0755)

	copyFile(dbPath, filepath.Join(failDir, "db-as-killed.kv"))

	// Write iteration number
	os.WriteFile(filepath.Join(failDir, "report.txt"),
		[]byte(fmt.Sprintf("Failure at iteration %d\nTimestamp: %s\n", iteration, time.Now().Format(time.RFC3339))),
		0644)
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

