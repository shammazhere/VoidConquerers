// cli/kvdb/serve.go — Long-running serve pipeline: reads ops from stdin, prints ACK/ERR/VAL.
//
// Protocol (stdin → stdout):
//
//   Input:
//     <op-id> put <key> <value>
//     <op-id> del <key>
//     <op-id> get <key>
//     <op-id> batch <n>          followed by n lines: "put k v" or "del k"
//     <op-id> compact
//     <op-id> dump
//     exit | quit
//
//   Output:
//     ACK <op-id>              — write durable
//     ERR <op-id> <reason>     — operation failed
//     VAL <op-id> <value>      — get result
//     NOTFOUND <op-id>         — key not found
//     KV  <op-id> <key> <val>  — dump row

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"kipi/src/kvdb"
)

// runServe parses flags and starts the serve loop.
func runServe(args []string) {
	flags, pos := parseFlags(args)
	if len(pos) < 1 {
		fmt.Fprintln(os.Stderr, "serve requires <path>")
		os.Exit(1)
	}
	path := pos[0]
	maxBytes := parseInt64Flag(flags, "max-bytes", 0)
	compactMinBytes := parseInt64Flag(flags, "compact-min-bytes", 4*1024*1024)
	stepDelayUs := parseInt64Flag(flags, "step-delay-us", 0)

	db, err := kvdb.Open(path, kvdb.Options{MaxBytes: maxBytes})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	serveLoop(db, reader, writer, compactMinBytes, stepDelayUs)
}

// serveLoop reads and dispatches operations from reader, writing responses to writer.
// It exits cleanly on EOF, pipe close, or "exit"/"quit".
func serveLoop(db *kvdb.DB, reader *bufio.Reader, writer *bufio.Writer, compactMinBytes, stepDelayUs int64) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return // EOF or pipe closed
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		if stepDelayUs > 0 {
			time.Sleep(time.Duration(stepDelayUs) * time.Microsecond)
		}

		lower := strings.ToLower(line)
		if lower == "exit" || lower == "quit" {
			return
		}

		// Parse: "<op-id> <command> [args...]"
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			fmt.Fprintf(writer, "ERR ? malformed line: %q\n", line)
			writer.Flush()
			continue
		}
		opID := parts[0]
		cmd := strings.ToLower(parts[1])
		rest := ""
		if len(parts) == 3 {
			rest = parts[2]
		}

		switch cmd {
		case "put":
			dispatchPut(db, writer, opID, rest)
		case "del":
			dispatchDel(db, writer, opID, rest)
		case "get":
			dispatchGet(db, writer, opID, rest)
		case "batch":
			dispatchBatch(db, reader, writer, opID, rest)
		case "compact":
			dispatchCompact(db, writer, opID)
			_ = compactMinBytes
		case "dump":
			dispatchDump(db, writer, opID)
		default:
			fmt.Fprintf(writer, "ERR %s unknown command %q\n", opID, cmd)
			writer.Flush()
		}
	}
}

func dispatchPut(db *kvdb.DB, w *bufio.Writer, opID, rest string) {
	kv := strings.SplitN(rest, " ", 2)
	if len(kv) < 2 {
		fmt.Fprintf(w, "ERR %s put requires key and value\n", opID)
		w.Flush()
		return
	}
	if err := db.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
		fmt.Fprintf(w, "ERR %s %v\n", opID, err)
	} else {
		fmt.Fprintf(w, "ACK %s\n", opID)
	}
	w.Flush()
}

func dispatchDel(db *kvdb.DB, w *bufio.Writer, opID, rest string) {
	key := strings.TrimSpace(rest)
	if err := db.Delete([]byte(key)); err != nil {
		fmt.Fprintf(w, "ERR %s %v\n", opID, err)
	} else {
		fmt.Fprintf(w, "ACK %s\n", opID)
	}
	w.Flush()
}

func dispatchGet(db *kvdb.DB, w *bufio.Writer, opID, rest string) {
	key := strings.TrimSpace(rest)
	val, err := db.Get([]byte(key))
	if err == kvdb.ErrNotFound {
		fmt.Fprintf(w, "NOTFOUND %s\n", opID)
	} else if err != nil {
		fmt.Fprintf(w, "ERR %s %v\n", opID, err)
	} else {
		fmt.Fprintf(w, "VAL %s %s\n", opID, val)
	}
	w.Flush()
}

func dispatchBatch(db *kvdb.DB, r *bufio.Reader, w *bufio.Writer, opID, rest string) {
	n, err := strconv.Atoi(strings.TrimSpace(rest))
	if err != nil || n < 0 {
		fmt.Fprintf(w, "ERR %s batch requires a count\n", opID)
		w.Flush()
		return
	}
	ops := make([]kvdb.Op, 0, n)
	parseErr := false
	for i := 0; i < n; i++ {
		bline, err := r.ReadString('\n')
		if err != nil {
			parseErr = true
			break
		}
		bline = strings.TrimRight(bline, "\r\n")
		bparts := strings.SplitN(bline, " ", 3)
		switch strings.ToLower(bparts[0]) {
		case "put":
			if len(bparts) < 3 {
				parseErr = true
			} else {
				ops = append(ops, kvdb.Op{Type: kvdb.OpPut, Key: []byte(bparts[1]), Value: []byte(bparts[2])})
			}
		case "del":
			if len(bparts) < 2 {
				parseErr = true
			} else {
				ops = append(ops, kvdb.Op{Type: kvdb.OpDelete, Key: []byte(bparts[1])})
			}
		default:
			parseErr = true
		}
		if parseErr {
			break
		}
	}
	if parseErr {
		fmt.Fprintf(w, "ERR %s batch parse error\n", opID)
		w.Flush()
		return
	}
	if err := db.Apply(ops); err != nil {
		fmt.Fprintf(w, "ERR %s %v\n", opID, err)
	} else {
		fmt.Fprintf(w, "ACK %s\n", opID)
	}
	w.Flush()
}

func dispatchCompact(db *kvdb.DB, w *bufio.Writer, opID string) {
	if err := db.Compact(); err != nil {
		fmt.Fprintf(w, "ERR %s %v\n", opID, err)
	} else {
		fmt.Fprintf(w, "ACK %s\n", opID)
	}
	w.Flush()
}

func dispatchDump(db *kvdb.DB, w *bufio.Writer, opID string) {
	err := db.ForEachSorted(func(key, val []byte) error {
		_, e := fmt.Fprintf(w, "KV %s %s %s\n", opID, key, val)
		return e
	})
	if err != nil {
		fmt.Fprintf(w, "ERR %s dump error: %v\n", opID, err)
	} else {
		fmt.Fprintf(w, "ACK %s\n", opID)
	}
	w.Flush()
}

