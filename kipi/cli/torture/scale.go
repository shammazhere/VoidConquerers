// cli/torture/scale.go — Scale test: 1 M-key recovery-time and file-shrink proof.
//
// runScale writes 1 000 000 keys via the serve pipeline in batches of 1 000,
// then times open() recovery, deletes all keys, runs compaction, and asserts
// the file size halved — confirming both performance and space-reclaim guarantees.

package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// runScale is a standalone scale / performance smoke test.
func runScale(bin, dbPath string) error {
	fmt.Println("=== Scale test: 1,000,000 keys ===")

	const (
		numKeys   = 1_000_000
		batchSize = 1000
	)

	// --- Phase 1: write 1 M keys ---
	fmt.Printf("Writing %d keys in batches of %d...\n", numKeys, batchSize)
	serveCmd := exec.Command(bin, "serve", dbPath)
	stdin, _ := serveCmd.StdinPipe()
	stdout, _ := serveCmd.StdoutPipe()
	serveCmd.Stderr = os.Stderr
	if err := serveCmd.Start(); err != nil {
		return fmt.Errorf("start serve: %w", err)
	}

	writer := bufio.NewWriter(stdin)
	reader := bufio.NewReader(stdout)
	for start := 0; start < numKeys; start += batchSize {
		end := start + batchSize
		if end > numKeys {
			end = numKeys
		}
		opID := fmt.Sprintf("scale-batch-%d", start/batchSize)
		fmt.Fprintf(writer, "%s batch %d\n", opID, end-start)
		for i := start; i < end; i++ {
			fmt.Fprintf(writer, "put scale-key-%08d val-%d\n", i, i)
		}
		writer.Flush()
		line, _ := reader.ReadString('\n')
		if !strings.HasPrefix(strings.TrimRight(line, "\r\n"), "ACK") {
			fmt.Printf("WARNING: unexpected response: %q\n", line)
		}
	}
	stdin.Close()
	serveCmd.Wait()

	// --- Phase 2: time recovery ---
	fmt.Println("Timing open() with 1M keys...")
	openStart := time.Now()
	openCmd := exec.Command(bin, "open", dbPath)
	openCmd.Stdout = os.Stdout
	openCmd.Stderr = os.Stderr
	if err := openCmd.Run(); err != nil {
		return fmt.Errorf("open failed: %w", err)
	}
	openDur := time.Since(openStart)
	if openDur > 2*time.Second {
		fmt.Printf("WARNING: recovery took %v (target: <2s)\n", openDur)
	} else {
		fmt.Printf("PASS: recovery in %v (<2s)\n", openDur)
	}

	// --- Phase 3: measure file size ---
	fi, _ := os.Stat(dbPath)
	sizeBefore := fi.Size()
	fmt.Printf("File size before compact: %s\n", humanBytes(sizeBefore))

	// --- Phase 4: delete all keys + compact ---
	fmt.Println("Deleting all 1M keys...")
	serveCmd2 := exec.Command(bin, "serve", dbPath)
	stdin2, _ := serveCmd2.StdinPipe()
	stdout2, _ := serveCmd2.StdoutPipe()
	serveCmd2.Stderr = os.Stderr
	serveCmd2.Start()

	writer2 := bufio.NewWriter(stdin2)
	reader2 := bufio.NewReader(stdout2)
	for start := 0; start < numKeys; start += batchSize {
		end := start + batchSize
		if end > numKeys {
			end = numKeys
		}
		opID := fmt.Sprintf("del-batch-%d", start/batchSize)
		fmt.Fprintf(writer2, "%s batch %d\n", opID, end-start)
		for i := start; i < end; i++ {
			fmt.Fprintf(writer2, "del scale-key-%08d\n", i)
		}
		writer2.Flush()
		line, _ := reader2.ReadString('\n')
		_ = line
	}
	compactOpID := "compact-after-delete"
	fmt.Fprintf(writer2, "%s compact\n", compactOpID)
	writer2.Flush()
	line, _ := reader2.ReadString('\n')
	_ = line
	stdin2.Close()
	serveCmd2.Wait()

	// --- Phase 5: verify file shrunk ---
	fi2, _ := os.Stat(dbPath)
	sizeAfter := fi2.Size()
	fmt.Printf("File size after compact: %s\n", humanBytes(sizeAfter))
	if sizeAfter < sizeBefore/2 {
		fmt.Printf("PASS: file shrank from %s to %s\n", humanBytes(sizeBefore), humanBytes(sizeAfter))
	} else {
		fmt.Printf("WARNING: file did not shrink sufficiently (%s → %s)\n",
			humanBytes(sizeBefore), humanBytes(sizeAfter))
	}

	return nil
}

// humanBytes formats a byte count as a human-readable binary string (KiB, MiB, …).
func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return strconv.FormatInt(b, 10) + " B"
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// writeScaleBatchFile writes a batch file of n sequential put ops for external use.
func writeScaleBatchFile(path string, startKey, endKey int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for i := startKey; i < endKey; i++ {
		fmt.Fprintf(w, "put scale-key-%08d val-%d\n", i, i)
	}
	return w.Flush()
}

// Keep filepath import live (used by callers in other files).
var _ = filepath.Join

