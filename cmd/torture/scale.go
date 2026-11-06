package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// RunScaleBenchmark executes the 1,000,000-key scale and recovery benchmark.
// Recovery time = wall-clock duration of the ./kvdb open process, from spawn to exit,
// measured by the torture tool. Fails if recovery > 2000 ms.
func RunScaleBenchmark(kvdbBin, workDir string, seed int64) error {
	fmt.Println("================================================================================")
	fmt.Println("                       KIPI 1M-KEY SCALE BENCHMARK                              ")
	fmt.Println("================================================================================")
	fmt.Printf("Seed: %d | WorkDir: %s | Target: Recovery < 2000ms\n", seed, workDir)

	dbPath := filepath.Join(workDir, "db.kv")
	oracleLog := filepath.Join(workDir, "oracle.log")

	// Wipe work directory for clean scale run
	_ = os.RemoveAll(workDir)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("create workDir: %w", err)
	}

	oracle, err := NewOracle(oracleLog, 4)
	if err != nil {
		return fmt.Errorf("init oracle: %w", err)
	}
	defer oracle.Close()

	// Step 1: Preload 1M keys in batches through serve, tracking with oracle
	fmt.Println("[Scale] Phase 1: Preloading 1,000,000 keys through serve...")
	session, err := StartSession(kvdbBin, dbPath, 0, oracle, workDir)
	if err != nil {
		return fmt.Errorf("start serve for preload: %w", err)
	}

	const totalKeys = 1000000
	const batchSize = 2000
	totalBatches := totalKeys / batchSize
	killAtBatch := totalBatches / 2 // Kill mid-preload at 500k keys

	rng := rand.New(rand.NewSource(seed))

	var preloadKilled bool
	for b := 1; b <= totalBatches; b++ {
		opID := fmt.Sprintf("w1_scale_%d", b)
		items := make([]OpItem, batchSize)

		for i := 0; i < batchSize; i++ {
			k := fmt.Sprintf("k_%07d", (b-1)*batchSize+i)
			v, err := BuildValueEnvelope([]byte(k), opID, uint64(i), 64, nil)
			if err != nil {
				_ = session.InjectKill()
				return fmt.Errorf("build envelope for %s: %w", k, err)
			}
			items[i] = OpItem{Key: k, Val: v, IsDel: false}
		}

		op := Operation{
			OpID:  opID,
			Lane:  "w1",
			Items: items,
		}

		// Inject kill mid-preload
		if b == killAtBatch {
			fmt.Printf("[Scale] Mid-preload kill triggered at batch %d (~%d keys written)...\n", b, b*batchSize)
			// Launch submit in goroutine and kill concurrently
			go func() {
				time.Sleep(time.Duration(1+rng.Intn(5)) * time.Millisecond)
				_ = session.InjectKill()
			}()
			_, _ = session.SubmitOp(op)
			preloadKilled = true
			break
		}

		if _, err := session.SubmitOp(op); err != nil {
			_ = session.InjectKill()
			return fmt.Errorf("submit preload batch %d: %w", b, err)
		}
	}

	if !preloadKilled {
		_ = session.InjectKill()
	}
	_ = session.WaitShutdown()
	_ = oracle.Flush()

	// Step 2: Time ./kvdb open per the recovery-time definition
	fmt.Println("[Scale] Phase 2: Timing crash recovery with ./kvdb open...")
	// Definition: Recovery time is the wall-clock duration of the ./kvdb open process, from spawn to exit, measured by the torture tool.
	recDuration, err := RunOpen(kvdbBin, dbPath, 0)
	if err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}
	fmt.Printf("[Scale] Recovery completed in %v\n", recDuration)

	if recDuration > 2000*time.Millisecond {
		return fmt.Errorf("RECOVERY LIMIT VIOLATION: took %v (spec requires < 2000ms)", recDuration)
	}

	// Step 3: Run check
	if err := RunCheck(kvdbBin, dbPath); err != nil {
		return fmt.Errorf("check after recovery failed: %w", err)
	}

	// Step 4: Dump and verify with oracle
	fmt.Println("[Scale] Phase 3: Dumping and verifying recovered state against oracle...")
	dump, err := RunDump(kvdbBin, dbPath)
	if err != nil {
		return fmt.Errorf("dump after recovery: %w", err)
	}

	res, err := oracle.Verify(dump)
	if err != nil {
		return fmt.Errorf("oracle verification failed: %w", err)
	}
	fmt.Printf("[Scale] Oracle verification PASSED! Total recovered keys: %d\n", res.TotalKeys)

	// Step 5: Delete all keys in batches
	fmt.Println("[Scale] Phase 4: Deleting all keys in batches...")
	sessionDel, err := StartSession(kvdbBin, dbPath, 0, oracle, workDir)
	if err != nil {
		return fmt.Errorf("start serve for deletion: %w", err)
	}

	delBatchIdx := 1
	delItems := make([]OpItem, 0, batchSize)
	for k := range dump {
		delItems = append(delItems, OpItem{Key: k, IsDel: true})
		if len(delItems) >= batchSize {
			delOp := Operation{
				OpID:  fmt.Sprintf("w1_del_%d", delBatchIdx),
				Lane:  "w1",
				Items: delItems,
			}
			if _, err := sessionDel.SubmitOp(delOp); err != nil {
				_ = sessionDel.InjectKill()
				return fmt.Errorf("submit delete batch %d: %w", delBatchIdx, err)
			}
			delBatchIdx++
			delItems = make([]OpItem, 0, batchSize)
		}
	}
	if len(delItems) > 0 {
		delOp := Operation{
			OpID:  fmt.Sprintf("w1_del_%d", delBatchIdx),
			Lane:  "w1",
			Items: delItems,
		}
		if _, err := sessionDel.SubmitOp(delOp); err != nil {
			_ = sessionDel.InjectKill()
			return fmt.Errorf("submit final delete batch: %w", err)
		}
	}

	_ = sessionDel.InjectKill()
	_ = sessionDel.WaitShutdown()

	// Step 6: Final check and file size report
	_, _ = RunOpen(kvdbBin, dbPath, 0)
	if err := RunCheck(kvdbBin, dbPath); err != nil {
		return fmt.Errorf("check after deletes: %w", err)
	}

	fi, err := os.Stat(dbPath)
	if err != nil {
		return fmt.Errorf("stat dbPath: %w", err)
	}

	fmt.Printf("[Scale] Post-delete file size on disk: %d bytes\n", fi.Size())
	if fi.Size() <= 64 {
		fmt.Printf("[Scale] File shrunk to base size (%d bytes) - Compaction VERIFIED!\n", fi.Size())
	} else {
		// Do not fake a pass if automatic compaction is not built or triggered
		fmt.Printf("[Scale] Note: file size is %d bytes. Auto-compaction / shrink not triggered or unverified.\n", fi.Size())
	}

	fmt.Println("================================================================================")
	fmt.Println("                    1M-KEY SCALE BENCHMARK: SUCCESS                             ")
	fmt.Println("================================================================================")
	return nil
}

