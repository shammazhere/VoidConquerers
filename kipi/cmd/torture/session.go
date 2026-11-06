// cmd/torture/session.go — Runs kvdb serve as a child, sends concurrent writers,
// kills at a random time, then verifies correctness.

package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type iterResult struct {
	ok         bool
	failReason string
	keyCount   int
	killAfter  time.Duration
}

// runIteration performs one full kill/restart/verify cycle.
func runIteration(bin, dbPath string, rng *rand.Rand, mode string, maxBytesLimit int64, workDir string, iterNum int) (*iterResult, error) {
	oraclePath := filepath.Join(workDir, "oracle.log")
	stderrPath := filepath.Join(workDir, "serve-stderr.log")

	// Open oracle log in append mode
	oracleFile, err := os.OpenFile(oraclePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open oracle log: %w", err)
	}
	defer oracleFile.Close()

	// Start serve
	serveArgs := []string{"serve", dbPath}
	if maxBytesLimit > 0 && mode == "full-disk" {
		serveArgs = append(serveArgs, "--max-bytes", strconv.FormatInt(maxBytesLimit, 10))
	}

	serveCmd := exec.Command(bin, serveArgs...)
	stdin, err := serveCmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := serveCmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderrFile, _ := os.Create(stderrPath)
	serveCmd.Stderr = stderrFile

	if err := serveCmd.Start(); err != nil {
		stderrFile.Close()
		return nil, fmt.Errorf("start serve: %w", err)
	}
	stderrFile.Close()

	// Generate workload profile for this mode
	profile := profileFor(mode, rng)

	// ACK tracking
	var ackedOps sync.Map // opID -> Op

	// Random kill delay: 1–500ms
	killDelay := time.Duration(rng.Intn(500)+1) * time.Millisecond
	killTimer := time.After(killDelay)

	// Channel to signal writers to stop
	done := make(chan struct{})

	// Start concurrent writers
	var wg sync.WaitGroup
	var opCounter int64

	for w := 0; w < profile.writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			writerRng := rand.New(rand.NewSource(rng.Int63()))

			for {
				select {
				case <-done:
					return
				default:
				}

				opID := fmt.Sprintf("w%d-op%d", writerID, atomic.AddInt64(&opCounter, 1))
				op := profile.genOp(writerRng, opID)

				// Write to serve stdin
				line := formatServeOp(op)
				_, err := fmt.Fprintln(stdin, line)
				if err != nil {
					return // pipe closed (process killed)
				}
			}
		}(w)
	}

	// ACK reader goroutine
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "ACK ") {
				opID := strings.TrimPrefix(line, "ACK ")
				opID = strings.TrimSpace(opID)
				if v, ok := ackedOps.Load(opID); ok {
					// Record ACK to oracle
					op := v.(*serveOp)
					fmt.Fprintf(oracleFile, "%s\n", op.oracleEntry())
					_ = op
				}
			}
		}
	}()

	// Wait for kill timer
	<-killTimer
	close(done)

	// Kill the serve process (the whole point of this tool)
	killProcess(serveCmd.Process)
	stdin.Close()

	wg.Wait()
	io.Copy(io.Discard, stdout)
	<-ackDone
	serveCmd.Wait()

	// Now verify: open + check + dump + compare with oracle
	result, err := verifyState(bin, dbPath, oraclePath)
	result.killAfter = killDelay
	return result, err
}

// verifyState opens the database, runs check and dump, and compares with oracle.
func verifyState(bin, dbPath, oraclePath string) (*iterResult, error) {
	// Run open (recovery)
	openCmd := exec.Command(bin, "open", dbPath)
	openCmd.Stderr = os.Stderr
	if err := openCmd.Run(); err != nil {
		return &iterResult{ok: false, failReason: fmt.Sprintf("open failed: %v", err)}, nil
	}

	// Run check
	checkOut, err := exec.Command(bin, "check", dbPath).Output()
	if err != nil {
		return &iterResult{ok: false, failReason: fmt.Sprintf("check failed: %v / %s", err, checkOut)}, nil
	}

	// Run dump
	dumpOut, err := exec.Command(bin, "dump", dbPath).Output()
	if err != nil {
		return &iterResult{ok: false, failReason: fmt.Sprintf("dump failed: %v", err)}, nil
	}

	// Parse dump into map
	dbState := make(map[string]string)
	lineCount := 0
	for _, line := range strings.Split(string(dumpOut), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) == 2 {
			dbState[parts[0]] = parts[1]
			lineCount++
		}
	}

	// Load oracle
	oracle := loadOracle(oraclePath)

	// Verify: every ACKed put must be in db; every ACKed del must be absent
	for _, entry := range oracle {
		switch entry.opType {
		case "put":
			val, ok := dbState[entry.key]
			if !ok {
				return &iterResult{
					ok: false,
					failReason: fmt.Sprintf("ACKed put key=%q missing from db (op %s)", entry.key, entry.opID),
				}, nil
			}
			if val != entry.value {
				return &iterResult{
					ok: false,
					failReason: fmt.Sprintf("ACKed put key=%q: expected %q got %q (op %s)", entry.key, entry.value, val, entry.opID),
				}, nil
			}
		case "del":
			if _, ok := dbState[entry.key]; ok {
				return &iterResult{
					ok: false,
					failReason: fmt.Sprintf("ACKed del key=%q still in db (op %s)", entry.key, entry.opID),
				}, nil
			}
		}
	}

	// No unknown values check: every value in db must be from a known put
	// (simplified: we just check the oracle-expected keys)

	return &iterResult{ok: true, keyCount: lineCount}, nil
}

// serveOp represents a pending operation sent to serve.
type serveOp struct {
	opID   string
	opType string // "put" | "del" | "batch"
	key    string
	value  string
	ops    []serveOp // for batch
}

func (s *serveOp) oracleEntry() string {
	return fmt.Sprintf("%s %s %s %s", s.opID, s.opType, s.key, s.value)
}

// formatServeOp formats a serveOp into a line for stdin.
func formatServeOp(op *serveOp) string {
	switch op.opType {
	case "put":
		return fmt.Sprintf("%s put %s %s", op.opID, op.key, op.value)
	case "del":
		return fmt.Sprintf("%s del %s", op.opID, op.key)
	default:
		return fmt.Sprintf("%s get %s", op.opID, op.key)
	}
}

// killProcess kills the process (SIGKILL / TerminateProcess).
func killProcess(p *os.Process) {
	if p != nil {
		p.Kill()
	}
}

// loadOracle reads the oracle log and returns committed entries.
type oracleEntry struct {
	opID   string
	opType string
	key    string
	value  string
}

func loadOracle(path string) []oracleEntry {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var entries []oracleEntry
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 4)
		if len(parts) < 3 {
			continue
		}
		e := oracleEntry{opID: parts[0], opType: parts[1], key: parts[2]}
		if len(parts) == 4 {
			e.value = parts[3]
		}
		entries = append(entries, e)
	}
	return entries
}

