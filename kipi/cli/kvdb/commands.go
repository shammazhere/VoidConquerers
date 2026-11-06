// cli/kvdb/commands.go — Sub-command handlers: open, put, get, del, batch, dump, check.
//
// Each handler opens the database, executes its operation, and exits.

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"kipi/src/kvdb"
)

// openDB opens (or creates) the database at path and exits on error.
func openDB(path string, opts kvdb.Options) *kvdb.DB {
	start := time.Now()
	db, err := kvdb.Open(path, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)
	fmt.Printf("opened %s: %d keys, recovery in %v\n", path, db.KeyCount(), elapsed)
	return db
}

// runOpen opens or creates the database, prints key count and recovery time.
func runOpen(args []string) {
	flags, pos := parseFlags(args)
	if len(pos) < 1 {
		fmt.Fprintln(os.Stderr, "open requires <path>")
		os.Exit(1)
	}
	path := pos[0]
	maxBytes := parseInt64Flag(flags, "max-bytes", 0)
	db := openDB(path, kvdb.Options{MaxBytes: maxBytes})
	db.Close()
}

// runPut puts a single key-value pair into the database.
func runPut(args []string) {
	flags, pos := parseFlags(args)
	if len(pos) < 3 {
		fmt.Fprintln(os.Stderr, "put requires <path> <key> <value>")
		os.Exit(1)
	}
	path, key, val := pos[0], pos[1], pos[2]
	maxBytes := parseInt64Flag(flags, "max-bytes", 0)
	db, err := kvdb.Open(path, kvdb.Options{MaxBytes: maxBytes})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Put([]byte(key), []byte(val)); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

// runGet retrieves a key's value and prints it, or prints NOTFOUND.
func runGet(args []string) {
	_, pos := parseFlags(args)
	if len(pos) < 2 {
		fmt.Fprintln(os.Stderr, "get requires <path> <key>")
		os.Exit(1)
	}
	path, key := pos[0], pos[1]
	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	val, err := db.Get([]byte(key))
	if err == kvdb.ErrNotFound {
		fmt.Println("NOTFOUND")
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s\n", val)
}

// runDel deletes a key from the database.
func runDel(args []string) {
	flags, pos := parseFlags(args)
	if len(pos) < 2 {
		fmt.Fprintln(os.Stderr, "del requires <path> <key>")
		os.Exit(1)
	}
	path, key := pos[0], pos[1]
	maxBytes := parseInt64Flag(flags, "max-bytes", 0)
	db, err := kvdb.Open(path, kvdb.Options{MaxBytes: maxBytes})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Delete([]byte(key)); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

// runBatch applies operations from a batch file (one per line: "put k v" or "del k").
func runBatch(args []string) {
	flags, pos := parseFlags(args)
	if len(pos) < 2 {
		fmt.Fprintln(os.Stderr, "batch requires <path> <batchfile>")
		os.Exit(1)
	}
	path, batchFile := pos[0], pos[1]
	maxBytes := parseInt64Flag(flags, "max-bytes", 0)

	ops, err := readBatchFile(batchFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR reading batch file: %v\n", err)
		os.Exit(1)
	}

	db, err := kvdb.Open(path, kvdb.Options{MaxBytes: maxBytes})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Apply(ops); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK: applied %d operations\n", len(ops))
}

// readBatchFile parses a batch file: "put k v" or "del k" per line.
func readBatchFile(path string) ([]kvdb.Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ops []kvdb.Op
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		switch strings.ToLower(parts[0]) {
		case "put":
			if len(parts) < 3 {
				return nil, fmt.Errorf("line %d: put requires key and value", lineNum)
			}
			ops = append(ops, kvdb.Op{Type: kvdb.OpPut, Key: []byte(parts[1]), Value: []byte(parts[2])})
		case "del":
			if len(parts) < 2 {
				return nil, fmt.Errorf("line %d: del requires key", lineNum)
			}
			ops = append(ops, kvdb.Op{Type: kvdb.OpDelete, Key: []byte(parts[1])})
		default:
			return nil, fmt.Errorf("line %d: unknown op %q (expected put or del)", lineNum, parts[0])
		}
	}
	return ops, scanner.Err()
}

// runDump prints every key and value sorted by key (tab-separated).
func runDump(args []string) {
	_, pos := parseFlags(args)
	if len(pos) < 1 {
		fmt.Fprintln(os.Stderr, "dump requires <path>")
		os.Exit(1)
	}
	path := pos[0]
	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	w := bufio.NewWriter(os.Stdout)
	err = db.ForEachSorted(func(key, val []byte) error {
		_, err := fmt.Fprintf(w, "%s\t%s\n", key, val)
		return err
	})
	w.Flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

// runCheck verifies file integrity without opening or locking the database.
func runCheck(args []string) {
	_, pos := parseFlags(args)
	if len(pos) < 1 {
		fmt.Fprintln(os.Stderr, "check requires <path>")
		os.Exit(1)
	}
	path := pos[0]
	result, err := kvdb.Check(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(result.Message)
	if !result.OK {
		os.Exit(1)
	}
}

