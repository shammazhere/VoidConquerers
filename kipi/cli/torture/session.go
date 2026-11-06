// cli/torture/session.go — One kill/restart/verify cycle (a single torture iteration).
//
// runIteration:
//   1. Starts kvdb serve as a child process.
//   2. Launches concurrent writer goroutines that fire random ops at serve's stdin.
//   3. Reads ACKs from serve's stdout and records them in an oracle log.
//   4. After a random kill delay (1–500 ms), kills the serve process.
//   5. Runs open + check + dump and verifies the result against the oracle log.

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

// iterResult is the outcome of a single torture iteration.
type iterResult struct {
	ok         bool
	failReason string
	keyCount   int
	killAfter  time.Duration
}

// runIteration performs one full kill → restart → verify cycle.
func runIteration(bin, dbPath string, rng *rand.Rand, mode string, maxBytesLimit int64, workDir string, iterNum int) (*iterResult, error) {
	oraclePath := filepath.Join(workDir, "oracle.log")
	stderrPath := filepath.Join(workDir, "serve-stderr.log")

	// Open oracle log in append mode so previous ACKs are preserved across iterations.
	oracleFile, err := os.OpenFile(oraclePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open oracle log: %w", err)
	}
	defer oracleFile.Close()

	// --- Start serve ---
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

	// --- Concurrent writers ---
	profile := profileFor(mode, rng)
	var ackedOps sync.Map // opID → *serveOp (pending, waiting for ACK)
	var opCounter int64
	done := make(chan struct{})
	killDelay := time.Duration(rng.Intn(500)+1) * time.Millisecond
	killTimer := time.After(killDelay)

	var wg sync.WaitGroup
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
				ackedOps.Store(opID, op)
				line := formatServeOp(op)
				if _, err := fmt.Fprintln(stdin, line); err != nil {
					return // pipe closed (process killed)
				}
			}
		}(w)
	}

	// --- ACK reader ---
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "ACK ") {
				opID := strings.TrimSpace(strings.TrimPrefix(line, "ACK "))
				if v, ok := ackedOps.Load(opID); ok {
					op := v.(*serveOp)
					fmt.Fprintf(oracleFile, "%s\n", op.oracleEntry())
				}
			}
		}
	}()

	// --- Kill ---
	<-killTimer
	close(done)
	killProcess(serveCmd.Process)
	stdin.Close()

	wg.Wait()
	io.Copy(io.Discard, stdout)
	<-ackDone
	serveCmd.Wait()

	// --- Verify ---
	result, err := verifyState(bin, dbPath, oraclePath)
	result.killAfter = killDelay
	return result, err
}

// verifyState opens the database, runs check and dump, and compares with the oracle log.
func verifyState(bin, dbPath, oraclePath string) (*iterResult, error) {
	// Recovery
	if err := exec.Command(bin, "open", dbPath).Run(); err != nil {
		return &iterResult{ok: false, failReason: fmt.Sprintf("open failed: %v", err)}, nil
	}

	// Integrity check
	if out, err := exec.Command(bin, "check", dbPath).Output(); err != nil {
		return &iterResult{ok: false, failReason: fmt.Sprintf("check failed: %v / %s", err, out)}, nil
	}

	// Full dump
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

	// Load oracle and verify
	oracle := loadOracle(oraclePath)
	for _, entry := range oracle {
		switch entry.opType {
		case "put":
			val, ok := dbState[entry.key]
			if !ok {
				return &iterResult{
					ok:         false,
					failReason: fmt.Sprintf("ACKed put key=%q missing from db (op %s)", entry.key, entry.opID),
				}, nil
			}
			if val != entry.value {
				return &iterResult{
					ok:         false,
					failReason: fmt.Sprintf("ACKed put key=%q: expected %q got %q (op %s)", entry.key, entry.value, val, entry.opID),
				}, nil
			}
		case "del":
			if _, ok := dbState[entry.key]; ok {
				return &iterResult{
					ok:         false,
					failReason: fmt.Sprintf("ACKed del key=%q still in db (op %s)", entry.key, entry.opID),
				}, nil
			}
		}
	}

	return &iterResult{ok: true, keyCount: lineCount}, nil
}

// --- Serve op types and formatting ---

// serveOp represents one operation queued for the serve process.
type serveOp struct {
	opID   string
	opType string // "put" | "del" | "get"
	key    string
	value  string
	ops    []serveOp // sub-ops for a batch
}

// oracleEntry formats the op as a line for the oracle log.
func (s *serveOp) oracleEntry() string {
	return fmt.Sprintf("%s %s %s %s", s.opID, s.opType, s.key, s.value)
}

// formatServeOp formats a serveOp into a serve stdin line.
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

// killProcess sends SIGKILL / TerminateProcess to the serve child.
func killProcess(p *os.Process) {
	if p != nil {
		p.Kill()
	}
}

// --- Oracle log ---

// oracleEntry is one record in the persistent oracle log.
type oracleEntry struct {
	opID   string
	opType string
	key    string
	value  string
}

// loadOracle reads all entries from the oracle log file.
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

