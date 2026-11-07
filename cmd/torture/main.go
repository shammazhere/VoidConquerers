package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// modeProfile is the workload and kill timing of one mode. The kill delay
// is drawn from [killMin, killMax] and measured from the moment serve is
// started; opsPerLane is large enough that the writers are still busy when
// the kill lands, so kills hit operations, not idle time.
type modeProfile struct {
	opsPerLane      int
	killMin         time.Duration
	killMax         time.Duration
	serveArgs       []string // extra flags for ./kvdb serve
	defaultMaxBytes uint64   // used when --max-bytes is not given
	stepDelayChance float64  // share of iterations that widen crash windows
	stepDelayMaxUs  int      // upper bound of the random step delay (0 = 1500 us)
}

func profileFor(mode Mode) modeProfile {
	ms := time.Millisecond
	switch mode {
	case ModeLargeValues:
		return modeProfile{opsPerLane: 40, killMin: 20 * ms, killMax: 700 * ms, stepDelayChance: 0.5,
			serveArgs: []string{"--compact-min-bytes", "16777216"}}
	case ModeLargeBatches:
		// Kills must land between the 256 KiB pieces of a big batch, so most
		// iterations widen those gaps.
		return modeProfile{opsPerLane: 40, killMin: 20 * ms, killMax: 500 * ms, stepDelayChance: 0.9, stepDelayMaxUs: 4000,
			serveArgs: []string{"--compact-min-bytes", "8388608"}}
	case ModeCompaction:
		return modeProfile{opsPerLane: 300, killMin: 5 * ms, killMax: 300 * ms, stepDelayChance: 0.5,
			serveArgs: []string{"--compact-min-bytes", "32768"}}
	case ModeRecoveryCrash:
		return modeProfile{opsPerLane: 300, killMin: 5 * ms, killMax: 200 * ms, stepDelayChance: 0.5}
	case ModeFullDisk:
		return modeProfile{opsPerLane: 200, killMin: 5 * ms, killMax: 300 * ms, stepDelayChance: 0.3,
			defaultMaxBytes: 256 << 10, serveArgs: []string{"--compact-min-bytes", "65536"}}
	case ModeD2SameKey:
		return modeProfile{opsPerLane: 200, killMin: 5 * ms, killMax: 150 * ms, stepDelayChance: 0.3}
	default:
		return modeProfile{opsPerLane: 300, killMin: 5 * ms, killMax: 200 * ms, stepDelayChance: 0.5}
	}
}

// coverage is the evidence that kills land during operations.
type coverage struct {
	iterations       int
	killedInFlight   int // an op was outstanding when the kill landed
	zeroAckIters     int
	acked            int64
	durableUnacked   int // in-flight op found committed after recovery (A4)
	droppedInFlight  int // in-flight op found absent after recovery
	tailRepairs      int // open cut a torn/uncommitted tail
	tailBytes        int64
	tmpCleaned       int
	compactionsAcked int64
	diskFull         int64
	ackedAfterFull   int64
	maxFileBytes     int64
	maxRecovery      time.Duration
	sumRecovery      time.Duration
	openKillPhases   map[string]int
}

// phaseName says in words where a killed `kvdb open` had got to.
var phaseName = map[string]string{"before-scan": "before the scan", "during-scan": "during the scan",
	"during-truncate": "while cutting the tail", "completed": "after it finished"}

// commas formats 1234567 as "1,234,567".
func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func msf(d time.Duration) string { return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000) }

const rowHeader = "  Iter    Kill at   Sent  ACKed  Interrupted  Kept  Gone  Repair on restart  Recovery    Keys  Result  Notes"

var cutRe = regexp.MustCompile(`cut (\d+) bytes`)

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
	modeStr := flag.String("mode", "default", "default | large-values | large-batches | compaction | recovery-crash | disk-full | d2-same-key | crash-points | scale")
	lanes := flag.Int("lanes", 2, "number of concurrent writer lanes (also W for oracle)")
	maxBytes := flag.Uint64("max-bytes", 0, "max-bytes limit for engine (0 = unlimited; disk-full mode defaults to 262144)")
	workDir := flag.String("work-dir", "torture-work", "workspace directory")
	kvdbBin := flag.String("kvdb-bin", defaultKvdbBin(), "path to kvdb binary")
	clean := flag.Bool("clean", false, "wipe the whole work directory before starting (the database files are always reset)")
	flag.Parse()

	if strings.EqualFold(*modeStr, "crash-points") {
		if err := RunCrashPoints(*kvdbBin, *workDir); err != nil {
			fmt.Fprintf(os.Stderr, "\n[FATAL] crash-points: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if strings.EqualFold(*modeStr, "scale") {
		if err := RunScaleBenchmark(*kvdbBin, *workDir, *seed); err != nil {
			fmt.Fprintf(os.Stderr, "Scale benchmark failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if _, err := os.Stat(*kvdbBin); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: kvdb binary not found at %q (build with: go build -o . ./cmd/kvdb ./cmd/torture)\n", *kvdbBin)
		os.Exit(1)
	}
	mode, err := NormalizeMode(*modeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	prof := profileFor(mode)
	if *maxBytes == 0 && prof.defaultMaxBytes > 0 {
		*maxBytes = prof.defaultMaxBytes
	}

	if *clean {
		_ = os.RemoveAll(*workDir)
	}
	if err := os.MkdirAll(*workDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating work directory: %v\n", err)
		os.Exit(1)
	}
	// Every run starts from an empty database. The oracle only knows what this
	// run wrote, so a database left by an earlier run would look like data
	// nobody wrote ("phantom keys").
	for _, f := range []string{"db.kv", "db.kv.tmp", "db.kv.lock", "oracle.log", "serve-stderr.log", "db-as-killed.kv", "db-after-recovery.kv"} {
		_ = os.Remove(filepath.Join(*workDir, f))
	}
	_ = os.RemoveAll(filepath.Join(*workDir, "failure"))
	if old, err := filepath.Glob(filepath.Join(*workDir, "db.kv.rejected*")); err == nil {
		for _, f := range old {
			_ = os.Remove(f)
		}
	}

	dbPath := filepath.Join(*workDir, "db.kv")
	oracle, err := NewOracle(filepath.Join(*workDir, "oracle.log"), *lanes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing oracle: %v\n", err)
		os.Exit(1)
	}
	defer oracle.Close()
	if mode == ModeD2SameKey {
		oracle.SetAllowSameKeyInFlight(true)
	}

	fmt.Printf("KIPI torture  |  mode %s  |  seed %d  |  %d iterations  |  %d concurrent writers  |  kill after %v-%v",
		mode, *seed, *iterations, *lanes, prof.killMin, prof.killMax)
	if *maxBytes > 0 {
		fmt.Printf("  |  size cap %s bytes", commas(int64(*maxBytes)))
	}
	fmt.Printf("\nFresh database: %s (files from earlier runs removed)\n", dbPath)
	fmt.Println("Each row: start serve, send writes, kill it (SIGKILL) mid-write, restart (recovery), check every key.")
	fmt.Println("  Interrupted = writes in progress at the kill; after the restart each one must be fully Kept or fully Gone.")
	fmt.Println()
	fmt.Println(rowHeader)

	rng := rand.New(rand.NewSource(*seed))
	cov := coverage{openKillPhases: map[string]int{}}
	var lastOpen time.Duration = 5 * time.Millisecond
	start := time.Now()
	sawDiskFull := false

	for iter := 1; iter <= *iterations; iter++ {
		gens, err := NewLaneGens(GenConfig{Seed: *seed, Mode: mode, Lanes: *lanes, Iteration: iter, OpsPerLane: prof.opsPerLane})
		if err != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("queue generation: %w", err))
			os.Exit(1)
		}

		// Some iterations (half by default) widen the gaps between the engine's
		// I/O steps (write pieces, fsync, marker, fsync) by a random 20-1500 us
		// (up to 4000 us in large-batches). On a
		// fast SSD fsync takes ~0.1 ms, too short for random kills to land
		// between the steps. The order of the steps never changes.
		serveArgs := append([]string{}, prof.serveArgs...)
		stepDelay := 0
		if rng.Float64() < prof.stepDelayChance {
			maxUs := prof.stepDelayMaxUs
			if maxUs == 0 {
				maxUs = 1500
			}
			stepDelay = 20 + rng.Intn(maxUs-19)
			serveArgs = append(serveArgs, "--step-delay-us", strconv.Itoa(stepDelay))
		}
		killDelay := prof.killMin + time.Duration(rng.Int63n(int64(prof.killMax-prof.killMin)+1))

		session, err := StartSession(*kvdbBin, dbPath, *maxBytes, oracle, *workDir, serveArgs...)
		if err != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("start serve: %w", err))
			os.Exit(1)
		}
		started := time.Now()

		var sent, acked, compactions, diskFull atomic.Int64
		var wg sync.WaitGroup
		stopLanes := make(chan struct{})
		for _, g := range gens {
			wg.Add(1)
			go func(g *LaneGen) {
				defer wg.Done()
				for {
					select {
					case <-stopLanes:
						return
					default:
					}
					op, more, genErr := g.Next() // built just before it is sent
					if genErr != nil || !more {
						return
					}
					sent.Add(1)
					resp, submitErr := session.SubmitOp(op)
					if submitErr != nil {
						return
					}
					switch {
					case strings.HasPrefix(resp, "ACK") && op.Compact:
						compactions.Add(1)
					case strings.HasPrefix(resp, "ACK"):
						acked.Add(1)
					case strings.Contains(resp, "DISK_FULL"):
						diskFull.Add(1)
					}
				}
			}(g)
		}

		time.Sleep(time.Until(started.Add(killDelay)))
		if err := session.InjectKill(); err != nil {
			close(stopLanes)
			_ = session.WaitShutdown()
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("inject kill: %w", err))
			os.Exit(1)
		}
		close(stopLanes)
		_ = session.WaitShutdown()
		wg.Wait()
		if session.exitedPremature.Load() {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("serve exited on its own before the kill (see %s/serve-stderr.log)", *workDir))
			os.Exit(1)
		}
		inFlightAtKill := oracle.InFlightCount()

		_ = copyFile(dbPath, filepath.Join(*workDir, "db-as-killed.kv"))

		// Disk-full: the file must never exceed the cap, even mid-kill (E1-E3).
		if *maxBytes > 0 {
			if fi, err := os.Stat(dbPath); err == nil {
				if uint64(fi.Size()) > *maxBytes {
					recordFailure(iter, *workDir, dbPath, fmt.Errorf("file is %d bytes after the kill, above --max-bytes %d", fi.Size(), *maxBytes))
					os.Exit(1)
				}
				if fi.Size() > cov.maxFileBytes {
					cov.maxFileBytes = fi.Size()
				}
			}
		}

		// Recovery-crash: kill ./kvdb open itself 1-3 times, at a random point
		// inside the time a full open takes (measured on the previous
		// iteration), before the real open below (C1, C2, F2).
		openKills := ""
		if mode == ModeRecoveryCrash {
			n := 1 + rng.Intn(3)
			var phases []string
			for k := 0; k < n; k++ {
				window := lastOpen * 13 / 10
				if window < time.Millisecond {
					window = time.Millisecond
				}
				d := time.Duration(rng.Int63n(int64(window)))
				phase := runCrashingOpen(*kvdbBin, dbPath, *maxBytes, d)
				cov.openKillPhases[phase]++
				phases = append(phases, phase)
			}
			for i, ph := range phases {
				phases[i] = phaseName[ph]
			}
			openKills = "restart killed " + strings.Join(phases, ", ")
		}

		recDuration, openOut, openErr := RunOpenInfo(*kvdbBin, dbPath, *maxBytes)
		if openErr != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("open recovery failed: %w", openErr))
			os.Exit(1)
		}
		lastOpen = recDuration
		cov.sumRecovery += recDuration
		if recDuration > cov.maxRecovery {
			cov.maxRecovery = recDuration
		}
		repair := "nothing to fix"
		if m := cutRe.FindStringSubmatch(openOut); m != nil {
			b, _ := strconv.ParseInt(m[1], 10, 64)
			cov.tailRepairs++
			cov.tailBytes += b
			repair = "cut " + commas(b) + " B"
		}
		var notes []string
		if strings.Contains(openOut, "leftover compaction temp") {
			cov.tmpCleaned++
			notes = append(notes, "deleted leftover tmp")
		}

		if checkErr := RunCheck(*kvdbBin, dbPath); checkErr != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("check failed: %w", checkErr))
			os.Exit(1)
		}
		dump, dumpErr := RunDump(*kvdbBin, dbPath)
		if dumpErr != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("dump failed: %w", dumpErr))
			os.Exit(1)
		}

		var verifyErr error
		committedUnacked, dropped := 0, 0
		if mode == ModeD2SameKey {
			lastVal, _ := oracle.GetLastAcked("d2_shared_contested_key")
			inFlightVals := oracle.GetInFlightValues("d2_shared_contested_key")
			verifyErr = VerifyD2(dump, "d2_shared_contested_key", lastVal, inFlightVals)
			if verifyErr == nil {
				if v, ok := dump["d2_shared_contested_key"]; ok {
					oracle.SetCommitted("d2_shared_contested_key", v)
				}
				oracle.ClearInFlight()
			}
		} else {
			var res *VerifyResult
			res, verifyErr = oracle.Verify(dump)
			if res != nil {
				committedUnacked, dropped = len(res.MatchedCandidate), len(res.DroppedOps)
			}
		}
		if verifyErr != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("oracle verify: %w", verifyErr))
			os.Exit(1)
		}
		if err := oracle.Flush(); err != nil {
			recordFailure(iter, *workDir, dbPath, fmt.Errorf("oracle flush: %w", err))
			os.Exit(1)
		}

		// coverage
		cov.iterations++
		if inFlightAtKill > 0 {
			cov.killedInFlight++
		}
		if acked.Load() == 0 {
			cov.zeroAckIters++
		}
		cov.acked += acked.Load()
		cov.durableUnacked += committedUnacked
		cov.droppedInFlight += dropped
		cov.compactionsAcked += compactions.Load()
		if sawDiskFull {
			cov.ackedAfterFull += acked.Load()
		}
		if diskFull.Load() > 0 {
			sawDiskFull = true
			cov.diskFull += diskFull.Load()
		}

		if compactions.Load() > 0 {
			notes = append(notes, fmt.Sprintf("%d compactions", compactions.Load()))
		}
		if diskFull.Load() > 0 {
			notes = append(notes, fmt.Sprintf("%d DISK_FULL", diskFull.Load()))
		}
		if openKills != "" {
			notes = append(notes, openKills)
		}
		if iter > 1 && (iter-1)%50 == 0 {
			fmt.Println(rowHeader)
		}
		gone := inFlightAtKill - committedUnacked
		if gone < 0 {
			gone = 0
		}
		fmt.Printf("%6s %7d ms %6d %6d %12d %5d %5d  %-17s %8s %7d  %-6s  %s\n",
			fmt.Sprintf("%d/%d", iter, *iterations), killDelay.Milliseconds(), sent.Load(), acked.Load(), inFlightAtKill,
			committedUnacked, gone, repair, msf(recDuration), len(dump), "PASS", strings.Join(notes, ", "))
	}

	line := strings.Repeat("=", 86)
	interrupted := cov.durableUnacked + cov.droppedInFlight
	avg := time.Duration(0)
	if cov.iterations > 0 {
		avg = cov.sumRecovery / time.Duration(cov.iterations)
	}
	row := func(label, value string) {
		fmt.Printf("   %s %s %s\n", label, strings.Repeat(".", 50-len(label)), value)
	}
	fmt.Println()
	fmt.Println(line)
	fmt.Printf(" RESULT: PASS   %d of %d iterations, 0 failures          mode %s, seed %d, took %s\n",
		cov.iterations, *iterations, mode, *seed, time.Since(start).Round(100*time.Millisecond))
	fmt.Println(line)
	fmt.Println(" Durability: every acknowledged write must survive")
	row("acknowledged writes checked after restarts", commas(cov.acked)+"   lost: 0")
	row("kills that hit a write in progress", fmt.Sprintf("%d of %d iterations", cov.killedInFlight, cov.iterations))
	fmt.Printf(" Atomicity: what happened to the %d interrupted writes\n", interrupted)
	row("kept in full (committed, ACK not sent yet)", commas(int64(cov.durableUnacked)))
	row("gone in full (never committed)", commas(int64(cov.droppedInFlight)))
	row("half-applied", "0   (never allowed)")
	fmt.Println(" Recovery")
	row("restarts that cut a half-written tail", fmt.Sprintf("%d of %d   (%s bytes cut in total)", cov.tailRepairs, cov.iterations, commas(cov.tailBytes)))
	row("recovery time", fmt.Sprintf("average %s, slowest %s   (limit 2,000 ms)", msf(avg), msf(cov.maxRecovery)))
	if cov.compactionsAcked > 0 || mode == ModeCompaction {
		row("compactions completed", fmt.Sprintf("%s   (leftover temp files deleted by restart: %d)", commas(cov.compactionsAcked), cov.tmpCleaned))
	}
	if *maxBytes > 0 {
		row("DISK_FULL replies", commas(cov.diskFull))
		row("writes accepted after the first DISK_FULL", commas(cov.ackedAfterFull))
		row("largest file seen", fmt.Sprintf("%s of %s bytes allowed", commas(cov.maxFileBytes), commas(int64(*maxBytes))))
	}
	if len(cov.openKillPhases) > 0 {
		total := 0
		var ks []string
		for _, k := range []string{"before-scan", "during-scan", "during-truncate", "completed"} {
			if v := cov.openKillPhases[k]; v > 0 {
				total += v
				ks = append(ks, fmt.Sprintf("%s %d", phaseName[k], v))
			}
		}
		row("times the restart itself was killed", fmt.Sprintf("%d   (%s)", total, strings.Join(ks, ", ")))
	}
	fmt.Println(line)
}

func defaultKvdbBin() string {
	if _, err := os.Stat("./kvdb.exe"); err == nil {
		return "./kvdb.exe"
	}
	return "./kvdb"
}

func recordFailure(iter int, workDir, dbPath string, failureErr error) {
	failDir := filepath.Join(workDir, "failure")
	_ = os.MkdirAll(failDir, 0755)
	_ = copyFile(filepath.Join(workDir, "db-as-killed.kv"), filepath.Join(failDir, "db-as-killed.kv"))
	_ = copyFile(dbPath, filepath.Join(failDir, "db-after-recovery.kv"))
	_ = copyFile(filepath.Join(workDir, "oracle.log"), filepath.Join(failDir, "oracle.log"))
	_ = copyFile(filepath.Join(workDir, "serve-stderr.log"), filepath.Join(failDir, "serve-stderr.log"))
	reportMsg := fmt.Sprintf("INVARIANT VIOLATION at iteration %d:\n%v\n", iter, failureErr)
	_ = os.WriteFile(filepath.Join(failDir, "report.txt"), []byte(reportMsg), 0644)
	fmt.Fprintf(os.Stderr, "\n[FATAL] %s\n", reportMsg)
	fmt.Fprintf(os.Stderr, "Diagnostics bundle written to %s\n", failDir)
	fmt.Println("RESULT: FAIL")
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

// runCrashingOpen starts `./kvdb open --trace`, kills it after killDelay and
// returns the last recovery phase it reported: before-scan, during-scan,
// during-truncate, or completed (open finished before the kill).
func runCrashingOpen(kvdbBin, dbPath string, maxBytes uint64, killDelay time.Duration) string {
	// --step-delay-us widens the gap around the truncate so kills can land there too.
	args := []string{"open", dbPath, "--trace", "--step-delay-us", "1500"}
	if maxBytes > 0 {
		args = append(args, "--max-bytes", fmt.Sprintf("%d", maxBytes))
	}
	cmd := exec.Command(kvdbBin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "error"
	}
	if err := cmd.Start(); err != nil {
		return "error"
	}
	var mu sync.Mutex
	phase := "before-scan"
	done := make(chan struct{})
	go func() {
		defer close(done)
		r := bufio.NewReader(stdout)
		for {
			line, err := r.ReadString('\n')
			mu.Lock()
			switch {
			case strings.HasPrefix(line, "INFO scan-start"):
				phase = "during-scan"
			case strings.HasPrefix(line, "INFO truncate-start"):
				phase = "during-truncate"
			case strings.HasPrefix(line, "opened "):
				phase = "completed"
			}
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	time.Sleep(killDelay)
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	<-done
	_ = cmd.Wait()
	mu.Lock()
	defer mu.Unlock()
	return phase
}
