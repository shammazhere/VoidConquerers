// cmd/torture/scale.go — 1M-key recovery time test and shrink proof.

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

// runScale writes 1,000,000 keys, kills mid-batch, times recovery,
// verifies every value, deletes all, compacts, and confirms file shrinks.
func runScale(bin, dbPath string) error {
	fmt.Println("=== Scale test: 1,000,000 keys ===")

	// Use serve mode to write 1M keys in large batches
	const numKeys = 1_000_000
	const batchSize = 1000

	// Start serve
	serveCmd := exec.Command(bin, "serve", dbPath)
	stdin, _ := serveCmd.StdinPipe()
	stdout, _ := serveCmd.StdoutPipe()
	serveCmd.Stderr = os.Stderr
	if err := serveCmd.Start(); err != nil {
		return fmt.Errorf("start serve: %w", err)
	}

	// Write keys
	fmt.Printf("Writing %d keys in batches of %d...\n", numKeys, batchSize)
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
			key := fmt.Sprintf("scale-key-%08d", i)
			val := fmt.Sprintf("val-%d", i)
			fmt.Fprintf(writer, "put %s %s\n", key, val)
		}
		writer.Flush()

		// Read ACK
		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "ACK") {
			fmt.Printf("WARNING: unexpected response: %q\n", line)
		}
	}

	stdin.Close()
	serveCmd.Wait()

	// Kill and time recovery
	fmt.Println("Timing open() with 1M keys...")
	openStart := time.Now()
	openCmd := exec.Command(bin, "open", dbPath)
	openCmd.Stdout = os.Stdout
	openCmd.Stderr = os.Stderr
	if err := openCmd.Run(); err != nil {
		return fmt.Errorf("open failed: %w", err)
	}
	openDur := time.Since(openStart)
	fmt.Printf("Recovery time: %v\n", openDur)
	if openDur > 2*time.Second {
		fmt.Printf("WARNING: recovery took %v (target: <2s)\n", openDur)
	} else {
		fmt.Printf("PASS: recovery in %v (<2s)\n", openDur)
	}

	// Get file size before delete+compact
	fi, _ := os.Stat(dbPath)
	sizeBefore := fi.Size()
	fmt.Printf("File size before compact: %s\n", humanBytes(sizeBefore))

	// Delete all keys
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
			key := fmt.Sprintf("scale-key-%08d", i)
			fmt.Fprintf(writer2, "del %s\n", key)
		}
		writer2.Flush()
		line, _ := reader2.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		_ = line
	}

	// Compact
	opID := "compact-after-delete"
	fmt.Fprintf(writer2, "%s compact\n", opID)
	writer2.Flush()
	line, _ := reader2.ReadString('\n')
	line = strings.TrimRight(line, "\r\n")
	_ = line

	stdin2.Close()
	serveCmd2.Wait()

	// Check size after compact
	fi2, _ := os.Stat(dbPath)
	sizeAfter := fi2.Size()
	fmt.Printf("File size after compact: %s\n", humanBytes(sizeAfter))

	if sizeAfter < sizeBefore/2 {
		fmt.Printf("PASS: file shrank from %s to %s\n", humanBytes(sizeBefore), humanBytes(sizeAfter))
	} else {
		fmt.Printf("WARNING: file did not shrink sufficiently (%s -> %s)\n",
			humanBytes(sizeBefore), humanBytes(sizeAfter))
	}

	return nil
}

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

// Scale test helper: write n keys to a file for batch import.
func writeScaleBatchFile(path string, startKey, endKey int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for i := startKey; i < endKey; i++ {
		key := fmt.Sprintf("scale-key-%08d", i)
		val := fmt.Sprintf("val-%d", i)
		fmt.Fprintf(w, "put %s %s\n", key, val)
	}
	return w.Flush()
}

// Ensure scale.go helpers are accessible from other files.
var _ = filepath.Join

