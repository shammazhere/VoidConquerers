package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Session manages the execution and lifecycle of a ./kvdb serve child process.
type Session struct {
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	stdinMu         sync.Mutex
	oracle          *Oracle
	workDir         string
	stderrFile      *os.File
	killedByHarness atomic.Bool
	exitedPremature atomic.Bool
	exitErr         error
	waitDone        chan struct{}

	laneMu      sync.Mutex
	laneWaiters map[string]chan string // lane -> response channel ("ACK" or "ERR CODE")
	hvalCh      chan string            // channel for HVAL / VAL / NOTFOUND
	cond        *sync.Cond
}

// StartSession launches ./kvdb serve <dbPath> [--max-bytes N] as a child process.
func StartSession(kvdbBin, dbPath string, maxBytes uint64, oracle *Oracle, workDir string) (*Session, error) {
	if _, err := os.Stat(kvdbBin); os.IsNotExist(err) {
		return nil, fmt.Errorf("kvdb binary not found at %q (engine is not built yet)", kvdbBin)
	}

	args := []string{"serve", dbPath}
	if maxBytes > 0 {
		args = append(args, "--max-bytes", fmt.Sprintf("%d", maxBytes))
	}

	cmd := exec.Command(kvdbBin, args...)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		stdinPipe.Close()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		stdinPipe.Close()
		stdoutPipe.Close()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	stderrPath := filepath.Join(workDir, "serve-stderr.log")
	stderrFile, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		stdinPipe.Close()
		stdoutPipe.Close()
		stderrPipe.Close()
		return nil, fmt.Errorf("open serve-stderr.log: %w", err)
	}

	if err := cmd.Start(); err != nil {
		stdinPipe.Close()
		stdoutPipe.Close()
		stderrPipe.Close()
		stderrFile.Close()
		return nil, fmt.Errorf("start serve process: %w", err)
	}

	s := &Session{
		cmd:         cmd,
		stdin:       stdinPipe,
		oracle:      oracle,
		workDir:     workDir,
		stderrFile:  stderrFile,
		waitDone:    make(chan struct{}),
		laneWaiters: make(map[string]chan string),
		hvalCh:      make(chan string, 100),
	}
	s.cond = sync.NewCond(&s.laneMu)

	// Goroutine 1: Continuous stderr logger
	go func() {
		defer stderrPipe.Close()
		defer stderrFile.Close()
		r := bufio.NewReader(stderrPipe)
		for {
			line, err := r.ReadString('\n')
			if len(line) > 0 {
				stderrFile.WriteString(line)
			}
			if err != nil {
				break
			}
		}
	}()

	// Goroutine 2: Stdout reader line-scanner routing ACK / ERR / HVAL
	go func() {
		defer stdoutPipe.Close()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			s.handleStdoutLine(line)
		}
	}()

	// Goroutine 3: Supervisor goroutine waiting on process exit
	go func() {
		err := cmd.Wait()
		s.exitErr = err
		if !s.killedByHarness.Load() {
			s.exitedPremature.Store(true)
		}
		close(s.waitDone)
		s.laneMu.Lock()
		s.cond.Broadcast()
		s.laneMu.Unlock()
	}()

	return s, nil
}

// handleStdoutLine processes each line from the server's stdout.
func (s *Session) handleStdoutLine(line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}

	respType := parts[0]
	switch respType {
	case "ACK":
		if len(parts) >= 2 {
			opID := parts[1]
			lane := parseLaneFromOpID(opID)
			s.notifyLane(lane, line)
		}
	case "ERR":
		if len(parts) >= 2 {
			opID := parts[1]
			lane := parseLaneFromOpID(opID)
			s.notifyLane(lane, line)
		}
	case "HVAL", "VAL", "NOTFOUND":
		select {
		case s.hvalCh <- line:
		default:
		}
	}
}

func parseLaneFromOpID(opID string) string {
	idx := strings.Index(opID, "_")
	if idx != -1 {
		return opID[:idx]
	}
	return ""
}

func (s *Session) notifyLane(lane, resp string) {
	s.laneMu.Lock()
	defer s.laneMu.Unlock()
	ch, exists := s.laneWaiters[lane]
	if exists {
		select {
		case ch <- resp:
		default:
		}
	}
}

// SubmitOp submits an operation synchronously on behalf of a writer lane:
// 1. Checks oracle CanDispatch; waits if key conflict exists.
// 2. Dispatches to oracle BEFORE writing to stdin.
// 3. Writes formatted Hex dialect command to stdin under stdinMu.
// 4. Waits for matching ACK/ERR response.
func (s *Session) SubmitOp(op Operation) (string, error) {
	// Step 1: Wait until keys are not in flight
	s.laneMu.Lock()
	for !s.oracle.CanDispatch(op) {
		select {
		case <-s.waitDone:
			s.laneMu.Unlock()
			return "", errors.New("server process terminated")
		default:
		}
		if s.killedByHarness.Load() || s.exitedPremature.Load() {
			s.laneMu.Unlock()
			return "", errors.New("server process terminated prematurely")
		}
		s.cond.Wait()
	}

	select {
	case <-s.waitDone:
		s.laneMu.Unlock()
		return "", errors.New("server process terminated")
	default:
	}
	if s.killedByHarness.Load() || s.exitedPremature.Load() {
		s.laneMu.Unlock()
		return "", errors.New("server process terminated prematurely")
	}

	// Step 2: Register with oracle before writing
	if err := s.oracle.Dispatch(op); err != nil {
		s.laneMu.Unlock()
		return "", fmt.Errorf("oracle dispatch: %w", err)
	}

	respCh := make(chan string, 1)
	s.laneWaiters[op.Lane] = respCh
	s.laneMu.Unlock()

	// Step 3: Write to stdin under mutex
	cmdStr := FormatCommandHex(op)
	s.stdinMu.Lock()
	_, writeErr := io.WriteString(s.stdin, cmdStr)
	s.stdinMu.Unlock()

	if writeErr != nil {
		return "", fmt.Errorf("stdin write: %w", writeErr)
	}

	// Step 4: Await response
	select {
	case resp := <-respCh:
		s.laneMu.Lock()
		delete(s.laneWaiters, op.Lane)
		s.cond.Broadcast() // wake other waiting lanes
		s.laneMu.Unlock()

		parts := strings.Fields(resp)
		if len(parts) >= 2 && parts[0] == "ACK" {
			if err := s.oracle.RecordAck(op.OpID); err != nil {
				return resp, fmt.Errorf("oracle record ACK: %w", err)
			}
		} else if len(parts) >= 3 && parts[0] == "ERR" {
			errCode := ServerErrorCode(parts[2])
			if err := s.oracle.RecordErr(op.OpID, errCode); err != nil {
				return resp, fmt.Errorf("oracle record ERR: %w", err)
			}
		}
		return resp, nil

	case <-s.waitDone:
		s.laneMu.Lock()
		delete(s.laneWaiters, op.Lane)
		s.cond.Broadcast()
		s.laneMu.Unlock()
		return "", errors.New("server exited while waiting for ACK")

	case <-time.After(5 * time.Second):
		return "", errors.New("timeout (5s) waiting for server response")
	}
}

// InjectKill delivers SIGKILL to the child process after setting killedByHarness=true.
func (s *Session) InjectKill() error {
	s.killedByHarness.Store(true)
	s.laneMu.Lock()
	s.cond.Broadcast()
	s.laneMu.Unlock()
	if s.cmd.Process != nil {
		return s.cmd.Process.Kill()
	}
	return nil
}

// WaitShutdown waits for the child process to exit with a 5-second hang guard.
func (s *Session) WaitShutdown() error {
	select {
	case <-s.waitDone:
		return nil
	case <-time.After(5 * time.Second):
		if s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
		}
		return errors.New("timeout (5s) waiting for server shutdown")
	}
}

// -----------------------------------------------------------------------------
// CLI Execution Helpers (open, check, dump)
// -----------------------------------------------------------------------------

// RunOpen runs `./kvdb open <path> [--max-bytes N]` and measures its wall-clock duration.
// Recovery time = wall-clock duration of the ./kvdb open process, from spawn to exit.
func RunOpen(kvdbBin, dbPath string, maxBytes uint64) (time.Duration, error) {
	args := []string{"open", dbPath}
	if maxBytes > 0 {
		args = append(args, "--max-bytes", fmt.Sprintf("%d", maxBytes))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // 10s hang guard
	defer cancel()

	cmd := exec.CommandContext(ctx, kvdbBin, args...)

	start := time.Now()
	output, err := cmd.CombinedOutput()
	duration := time.Since(start)

	if ctx.Err() == context.DeadlineExceeded {
		return duration, errors.New("open hang guard timeout (10s) exceeded")
	}
	if err != nil {
		return duration, fmt.Errorf("open failed (exit %v): %s", err, string(output))
	}
	return duration, nil
}

// RunCheck runs `./kvdb check <path>` and asserts the output contains "OK".
// Read-only integrity check; takes no --max-bytes.
func RunCheck(kvdbBin, dbPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, kvdbBin, "check", dbPath)
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		return errors.New("check timeout (2s) exceeded")
	}
	if err != nil {
		return fmt.Errorf("check failed (exit %v): %s", err, string(output))
	}

	outStr := strings.TrimSpace(string(output))
	if !strings.Contains(outStr, "OK") {
		return fmt.Errorf("check output not OK: %s", outStr)
	}
	return nil
}

// RunDump runs `./kvdb dump <path>` and parses its lines via ParseDump.
func RunDump(kvdbBin, dbPath string) (map[string][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, kvdbBin, "dump", dbPath)
	output, err := cmd.Output()

	if ctx.Err() == context.DeadlineExceeded {
		return nil, errors.New("dump timeout (2s) exceeded")
	}
	if err != nil {
		return nil, fmt.Errorf("dump failed: %w", err)
	}

	return ParseDump(strings.NewReader(string(output)))
}

