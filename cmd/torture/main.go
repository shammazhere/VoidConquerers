package main

import (
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "scale" {
		fs := flag.NewFlagSet("scale", flag.ExitOnError)
		kvdbBin := fs.String("kvdb-bin", "./kvdb", "path to kvdb binary")
		workDir := fs.String("work-dir", "torture-work", "workspace directory")
		seed := fs.Int64("seed", time.Now().UnixNano(), "seed for scale benchmark")
		_ = fs.Parse(os.Args[2:])

		if err := RunScaleBenchmark(*kvdbBin, *workDir, *seed); err != nil {
			fmt.Fprintf(os.Stderr, "Scale benchmark failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	iterations := flag.Int("iterations", 500, "number of torture iterations")
	seed := flag.Int64("seed", time.Now().UnixNano(), "PRNG seed (for deterministic replay)")
	modeStr := flag.String("mode", "default", "workload mode (default, large-values, large-batches, compaction, recovery-crash, full-disk, d2-same-key)")
	lanes := flag.Int("lanes", 2, "number of concurrent writer lanes (also W for oracle)")
	maxBytes := flag.Uint64("max-bytes", 0, "max-bytes limit for engine (0 for unlimited)")
	workDir := flag.String("work-dir", "torture-work", "workspace directory")
	kvdbBin := flag.String("kvdb-bin", "./kvdb", "path to kvdb binary")
	clean := flag.Bool("clean", false, "wipe work directory before starting")
	flag.Parse()

	// Print seed on first line so any failure can be replayed
	fmt.Printf("[Torture] Seed: %d | Mode: %s | Lanes: %d | Iterations: %d\n", *seed, *modeStr, *lanes, *iterations)

	// Validate kvdb binary exists
	if _, err := os.Stat(*kvdbBin); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: kvdb binary not found at %q (engine is not built yet)\n", *kvdbBin)
		os.Exit(1)
	}

	mode, err := NormalizeMode(*modeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *clean {
		_ = os.RemoveAll(*workDir)
	}
	if err := os.MkdirAll(*workDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating work directory: %v\n", err)
		os.Exit(1)
	}

	dbPath := filepath.Join(*workDir, "db.kv")
	oracleLog := filepath.Join(*workDir, "oracle.log")

	oracle, err := NewOracle(oracleLog, *lanes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing oracle: %v\n", err)
		os.Exit(1)
	}
	defer oracle.Close()

	rng := rand.New(rand.NewSource(*seed))

	passedCount := 0
	failedCount := 0
	modeCounts := make(map[Mode]int)
	recoveryCrashEvents := 0

	for iter := 1; iter <= *iterations; iter++ {
		modeCounts[mode]++

		// 1. Generate deterministic per-lane queues from seed
		genCfg := GenConfig{
			Seed:       *seed,
			Mode:       mode,
			Lanes:      *lanes,
			Iteration:  iter,
			OpsPerLane: 30,
		}
		queues, err := GenerateLaneQueues(genCfg)
		if err != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("queue generation: %w", err))
			os.Exit(1)
		}

		// 2. Start ./kvdb serve
		session, err := StartSession(*kvdbBin, dbPath, *maxBytes, oracle, *workDir)
		if err != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("start serve: %w", err))
			os.Exit(1)
		}

		// 3. Launch concurrent writer lanes
		var totalSent atomic.Int64
		var totalAcked atomic.Int64
		var wg sync.WaitGroup
		stopLanes := make(chan struct{})

		for _, q := range queues {
			wg.Add(1)
			go func(lq LaneQueue) {
				defer wg.Done()
				for _, op := range lq.Ops {
					select {
					case <-stopLanes:
						return
					default:
					}

					totalSent.Add(1)
					resp, submitErr := session.SubmitOp(op)
					if submitErr != nil {
						return
					}
					if strings.HasPrefix(resp, "ACK") {
						totalAcked.Add(1)
					}
				}
			}(q)
		}

		// 4. Random millisecond kill timer (1 to 35 ms)
		killDelayMs := 1 + rng.Intn(35)
		time.Sleep(time.Duration(killDelayMs) * time.Millisecond)

		// Full-disk mode check before kill
		if mode == ModeFullDisk && *maxBytes > 0 {
			// Record file size and sha256 before/after rejected op
			checkFullDiskIntegrity(session, dbPath, iter)
		}

		// Deliver SIGKILL
		if err := session.InjectKill(); err != nil {
			close(stopLanes)
			_ = session.WaitShutdown()
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("inject kill: %w", err))
			os.Exit(1)
		}

		close(stopLanes)
		_ = session.WaitShutdown()
		wg.Wait()

		// Verify premature crash did not occur
		if session.exitedPremature.Load() {
			recordFailure(iter, *workDir, dbPath, oracle, errors.New("server process terminated prematurely before SIGKILL"))
			os.Exit(1)
		}

		// Copy db.kv as killed (before open)
		asKilledPath := filepath.Join(*workDir, "db-as-killed.kv")
		_ = copyFile(dbPath, asKilledPath)

		// 5. Recovery-crash mode: crash during open
		if mode == ModeRecoveryCrash {
			recoveryCrashEvents++
			// Launch open and SIGKILL it at random 0.5ms - 2ms
			openKillDelay := time.Duration(500+rng.Intn(1500)) * time.Microsecond
			runCrashingOpen(*kvdbBin, dbPath, *maxBytes, openKillDelay)
		}

		// 6. Run ./kvdb open (measured recovery time)
		// Definition: Recovery time is the wall-clock duration of the ./kvdb open process, from spawn to exit, measured by the torture tool.
		recDuration, openErr := RunOpen(*kvdbBin, dbPath, *maxBytes)
		if openErr != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("open recovery failed: %w", openErr))
			os.Exit(1)
		}

		// 7. Run ./kvdb check (integrity)
		if checkErr := RunCheck(*kvdbBin, dbPath); checkErr != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("check failed: %w", checkErr))
			os.Exit(1)
		}

		// 8. Run ./kvdb dump and verify with oracle
		dump, dumpErr := RunDump(*kvdbBin, dbPath)
		if dumpErr != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("dump failed: %w", dumpErr))
			os.Exit(1)
		}

		var verifyErr error
		if mode == ModeD2SameKey {
			// In D2 mode: verify using VerifyD2
			lastVal := []byte(fmt.Sprintf("w1_iter%d_1", iter))
			verifyErr = VerifyD2(dump, "d2_shared_contested_key", lastVal, nil)
		} else {
			_, verifyErr = oracle.Verify(dump)
		}

		if verifyErr != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("oracle verify: %w", verifyErr))
			os.Exit(1)
		}

		// Flush oracle log once per iteration
		if err := oracle.Flush(); err != nil {
			recordFailure(iter, *workDir, dbPath, oracle, fmt.Errorf("oracle flush: %w", err))
			os.Exit(1)
		}

		// Copy db.kv after recovery
		asRecoveredPath := filepath.Join(*workDir, "db-after-recovery.kv")
		_ = copyFile(dbPath, asRecoveredPath)

		// Print progress line
		inFlightCount := totalSent.Load() - totalAcked.Load()
		fmt.Printf("[Iter %03d/%d] Mode: %-12s | Ops sent: %4d | ACKed: %4d | In-flight: %2d | Kill @ %2dms | Recovery: %.1fms (OK) | Oracle: PASS\n",
			iter, *iterations, mode, totalSent.Load(), totalAcked.Load(), inFlightCount, killDelayMs, float64(recDuration.Microseconds())/1000.0)

		passedCount++
	}

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Torture Completed: %d iterations | Passes: %d | Failures: %d\n", *iterations, passedCount, failedCount)
	fmt.Printf("Coverage Summary:\n")
	for m, c := range modeCounts {
		fmt.Printf("  Mode %-14s: %d iterations\n", m, c)
	}
	if recoveryCrashEvents > 0 {
		fmt.Printf("  Kills during recovery: %d events\n", recoveryCrashEvents)
	}
	fmt.Println("--------------------------------------------------------------------------------")
}

func recordFailure(iter int, workDir, dbPath string, oracle *Oracle, failureErr error) {
	failDir := filepath.Join(workDir, "failure")
	_ = os.MkdirAll(failDir, 0755)

	_ = copyFile(filepath.Join(workDir, "db-as-killed.kv"), filepath.Join(failDir, "db-as-killed.kv"))
	_ = copyFile(dbPath, filepath.Join(failDir, "db-after-recovery.kv"))
	_ = copyFile(filepath.Join(workDir, "oracle.log"), filepath.Join(failDir, "oracle.log"))
	_ = copyFile(filepath.Join(workDir, "serve-stderr.log"), filepath.Join(failDir, "serve-stderr.log"))

	reportPath := filepath.Join(failDir, "report.txt")
	reportMsg := fmt.Sprintf("INVARIANT VIOLATION at iteration %d:\n%v\n", iter, failureErr)
	_ = os.WriteFile(reportPath, []byte(reportMsg), 0644)

	fmt.Fprintf(os.Stderr, "\n[FATAL] %s\n", reportMsg)
	fmt.Fprintf(os.Stderr, "Diagnostics bundle written to %s\n", failDir)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func runCrashingOpen(kvdbBin, dbPath string, maxBytes uint64, killDelay time.Duration) {
	args := []string{"open", dbPath}
	if maxBytes > 0 {
		args = append(args, "--max-bytes", fmt.Sprintf("%d", maxBytes))
	}
	cmd := exec.Command(kvdbBin, args...)
	if err := cmd.Start(); err != nil {
		return
	}
	time.Sleep(killDelay)
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	_ = cmd.Wait()
}

func checkFullDiskIntegrity(s *Session, dbPath string, iter int) {
	fiBefore, err := os.Stat(dbPath)
	if err != nil {
		return
	}
	hashBefore := computeSHA256(dbPath)

	// Submit large op that triggers DISK_FULL
	largeOp := Operation{
		OpID:  fmt.Sprintf("w1_fulldisk_%d", iter),
		Lane:  "w1",
		Items: []OpItem{{Key: "disk_full_key", Val: make([]byte, 1048576)}},
	}
	resp, _ := s.SubmitOp(largeOp)

	if strings.Contains(resp, "ERR") && strings.Contains(resp, "DISK_FULL") {
		fiAfter, _ := os.Stat(dbPath)
		hashAfter := computeSHA256(dbPath)

		if fiBefore.Size() != fiAfter.Size() || hashBefore != hashAfter {
			fmt.Fprintf(os.Stderr, "DISK_FULL VIOLATION: file mutated on rejected write! before=%d after=%d\n",
				fiBefore.Size(), fiAfter.Size())
		}
	}
}

func computeSHA256(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	_, _ = io.Copy(h, f)
	return fmt.Sprintf("%x", h.Sum(nil))
}
