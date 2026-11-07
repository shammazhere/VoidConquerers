package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RunScaleBenchmark is the evidence for F3 and for the two scoring
// adjustments in the PDF (-5 slow recovery, -5 file does not shrink):
//
//  1. write 1,000,000 keys through serve (100 batches of 10,000), all ACKed
//  2. start a 10,000-key overwrite batch and SIGKILL serve while it is being written
//  3. time ./kvdb open (must be < 2000 ms), run check, verify the full dump
//     against the oracle (the killed batch must be all-or-nothing)
//  4. delete every key (no COMPACT command: auto-compaction must do it) and
//     require the file to shrink to the 28-byte minimum (16-byte header +
//     one 12-byte marker), and never to grow past 2x its size before the
//     deletes (it grows until dead bytes exceed live bytes, then compacts)
//
// Recovery time = wall-clock duration of the ./kvdb open process, from spawn
// to exit, as measured by this tool (so it includes process start-up).
func RunScaleBenchmark(kvdbBin, workDir string, seed int64) error {
	fmt.Println("================================================================================")
	fmt.Println("                       KIPI 1M-KEY SCALE BENCHMARK")
	fmt.Println("================================================================================")
	fmt.Printf("Seed: %d | WorkDir: %s | Targets: recovery < 2000 ms, file shrinks after deletes\n", seed, workDir)

	if _, err := os.Stat(kvdbBin); err != nil {
		return fmt.Errorf("kvdb binary not found at %q", kvdbBin)
	}
	dbPath := filepath.Join(workDir, "db.kv")
	_ = os.RemoveAll(workDir)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("create workDir: %w", err)
	}
	oracle, err := NewOracle(filepath.Join(workDir, "oracle.log"), 1)
	if err != nil {
		return fmt.Errorf("init oracle: %w", err)
	}
	defer oracle.Close()
	rng := rand.New(rand.NewSource(seed))

	const totalKeys = 1000000
	const batchSize = 10000
	key := func(i int) string { return fmt.Sprintf("k_%07d", i) }
	batch := func(opID string, from, to int, gen uint64) (Operation, error) {
		items := make([]OpItem, 0, to-from)
		for i := from; i < to; i++ {
			k := key(i)
			v, err := BuildValueEnvelope([]byte(k), opID, gen*10000000+uint64(i), 64, nil)
			if err != nil {
				return Operation{}, err
			}
			items = append(items, OpItem{Key: k, Val: v})
		}
		return Operation{OpID: opID, Lane: "w1", Items: items}, nil
	}

	// Phase 1: 1,000,000 keys, every batch ACKed.
	t0 := time.Now()
	fmt.Println("[Scale] Phase 1: writing 1,000,000 keys through serve (100 batches of 10,000)...")
	s, err := StartSession(kvdbBin, dbPath, 0, oracle, workDir)
	if err != nil {
		return fmt.Errorf("start serve: %w", err)
	}
	for b := 0; b < totalKeys/batchSize; b++ {
		op, err := batch(fmt.Sprintf("w1_load_%d", b), b*batchSize, (b+1)*batchSize, 0)
		if err != nil {
			_ = s.InjectKill()
			return err
		}
		resp, err := s.SubmitOp(op)
		if err != nil || len(resp) < 3 || resp[:3] != "ACK" {
			_ = s.InjectKill()
			return fmt.Errorf("load batch %d: %q %v", b, resp, err)
		}
	}
	_ = s.InjectKill() // everything so far is ACKed; this kill lands while idle
	_ = s.WaitShutdown()
	fi, _ := os.Stat(dbPath)
	fmt.Printf("[Scale]   %d keys ACKed in %.1fs, file %d bytes\n", totalKeys, time.Since(t0).Seconds(), fi.Size())

	// Phase 2: kill serve in the middle of a 10,000-key overwrite batch. A
	// step delay of 3 ms between the engine's I/O steps makes the batch take
	// ~25 ms to write, so the kill lands inside it.
	fmt.Println("[Scale] Phase 2: overwriting keys 0..9,999 in one batch and killing serve mid-write...")
	s, err = StartSession(kvdbBin, dbPath, 0, oracle, workDir, "--step-delay-us", "3000")
	if err != nil {
		return fmt.Errorf("start serve: %w", err)
	}
	probe := Operation{OpID: "w1_probe", Lane: "w1", Items: []OpItem{{Key: "probe", Val: []byte("ready")}}}
	if resp, err := s.SubmitOp(probe); err != nil || resp[:3] != "ACK" { // returns once serve has recovered 1M keys
		_ = s.InjectKill()
		return fmt.Errorf("probe: %q %v", resp, err)
	}
	over, err := batch("w1_overwrite", 0, batchSize, 1)
	if err != nil {
		_ = s.InjectKill()
		return err
	}
	killAfter := time.Duration(3+rng.Intn(22)) * time.Millisecond
	go func() {
		time.Sleep(killAfter)
		_ = s.InjectKill()
	}()
	resp, _ := s.SubmitOp(over)
	_ = s.WaitShutdown()
	ackedBeforeKill := len(resp) >= 3 && resp[:3] == "ACK"
	fmt.Printf("[Scale]   killed %v after sending the batch (batch ACKed before the kill: %v)\n", killAfter, ackedBeforeKill)

	// Phase 3: recovery time, check, full verification.
	fmt.Println("[Scale] Phase 3: timing ./kvdb open, then check and verify every key...")
	recDur, openOut, err := RunOpenInfo(kvdbBin, dbPath, 0)
	if err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}
	fmt.Printf("[Scale]   %s", strings.ReplaceAll(openOut, "\n  ", "\n[Scale]     "))
	fmt.Printf("[Scale]   recovery wall time (process start to exit): %.1f ms (limit 2000 ms)\n", float64(recDur.Microseconds())/1000)
	if recDur > 2000*time.Millisecond {
		return fmt.Errorf("RECOVERY LIMIT VIOLATION: %v (spec requires < 2000 ms)", recDur)
	}
	if err := RunCheck(kvdbBin, dbPath); err != nil {
		return fmt.Errorf("check after recovery: %w", err)
	}
	dump, err := RunDump(kvdbBin, dbPath)
	if err != nil {
		return fmt.Errorf("dump after recovery: %w", err)
	}
	res, err := oracle.Verify(dump)
	if err != nil {
		return fmt.Errorf("oracle verification failed: %w", err)
	}
	batchApplied := len(res.MatchedCandidate) > 0
	if ackedBeforeKill && !batchApplied {
		return errors.New("the overwrite batch was ACKed but is missing after recovery")
	}
	fmt.Printf("[Scale]   oracle PASS: %d keys, every value exact; killed batch applied=%v (all 10,000 or none)\n", res.TotalKeys, batchApplied)
	if res.TotalKeys < totalKeys {
		return fmt.Errorf("expected at least %d keys after recovery, found %d", totalKeys, res.TotalKeys)
	}

	// Phase 4: delete everything; auto-compaction alone must shrink the file.
	fmt.Println("[Scale] Phase 4: deleting every key (no COMPACT sent; auto-compaction must shrink the file)...")
	before, _ := os.Stat(dbPath)
	s, err = StartSession(kvdbBin, dbPath, 0, oracle, workDir)
	if err != nil {
		return fmt.Errorf("start serve: %w", err)
	}
	peak := before.Size()
	keys := make([]string, 0, len(dump))
	for k := range dump {
		keys = append(keys, k)
	}
	for b := 0; b*batchSize < len(keys); b++ {
		end := (b + 1) * batchSize
		if end > len(keys) {
			end = len(keys)
		}
		items := make([]OpItem, 0, end-b*batchSize)
		for _, k := range keys[b*batchSize : end] {
			items = append(items, OpItem{Key: k, IsDel: true})
		}
		if resp, err := s.SubmitOp(Operation{OpID: fmt.Sprintf("w1_del_%d", b), Lane: "w1", Items: items}); err != nil || len(resp) < 3 || resp[:3] != "ACK" {
			_ = s.InjectKill()
			return fmt.Errorf("delete batch %d: %q %v", b, resp, err)
		}
		if fi, err := os.Stat(dbPath); err == nil && fi.Size() > peak {
			peak = fi.Size()
		}
	}
	// Exit cleanly instead of killing: the auto-compaction triggered by the
	// last delete runs right after its ACK, and a kill there would (safely)
	// leave the pre-compaction file. Crashes during compaction are covered by
	// CP11-CP13 and --mode compaction.
	_ = s.CloseInput()
	if err := s.WaitShutdown(); err != nil {
		return fmt.Errorf("serve did not exit after its input closed: %w", err)
	}
	if _, err := RunOpen(kvdbBin, dbPath, 0); err != nil {
		return fmt.Errorf("open after deletes: %w", err)
	}
	if err := RunCheck(kvdbBin, dbPath); err != nil {
		return fmt.Errorf("check after deletes: %w", err)
	}
	final, _ := os.Stat(dbPath)
	fmt.Printf("[Scale]   file size: %d bytes before the deletes, largest during them %d, %d after (no COMPACT sent)\n", before.Size(), peak, final.Size())
	if final.Size() != 28 {
		return fmt.Errorf("SHRINK VIOLATION: file is %d bytes after deleting every key (expected 28: auto-compaction did not run)", final.Size())
	}
	// Until the dead bytes exceed the live bytes, deletes only append
	// tombstones, so the file may grow first; the design bound is 2 x live.
	if peak > 2*before.Size() {
		return fmt.Errorf("SHRINK VIOLATION: file grew to %d bytes while deleting (started at %d, bound 2x)", peak, before.Size())
	}

	fmt.Println("================================================================================")
	fmt.Printf("RESULT: PASS scale: 1,000,000 keys recovered in %.1f ms (< 2000), file shrank by itself %d -> %d bytes after deleting every key\n",
		float64(recDur.Microseconds())/1000, before.Size(), final.Size())
	fmt.Println("================================================================================")
	return nil
}
