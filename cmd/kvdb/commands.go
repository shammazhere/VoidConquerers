package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"kipi/src/kvdb"
)

func runOpen(args []string, out io.Writer) error {
	flags, pos, err := parseArgs(args, "max-bytes")
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: kvdb open <path> [--max-bytes N]")
	}
	maxBytes, err := flagInt64(flags, "max-bytes")
	if err != nil {
		return err
	}

	start := time.Now()
	db, err := kvdb.Open(pos[0], kvdb.Options{MaxBytes: maxBytes})
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	keyCount := db.KeyCount()
	if err := db.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	_, err = fmt.Fprintf(out, "opened %s: %d keys, recovery in %v\n", pos[0], keyCount, elapsed)
	return err
}

func runPut(args []string, out io.Writer) error {
	pos, err := parsePositionals(args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return errors.New("usage: kvdb put <path> <key> <value>")
	}
	db, err := kvdb.Open(pos[0], kvdb.Options{})
	if err != nil {
		return err
	}
	opErr := db.Put([]byte(pos[1]), []byte(pos[2]))
	closeErr := db.Close()
	if opErr != nil {
		return opErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	_, err = fmt.Fprintln(out, "OK")
	return err
}

func runGet(args []string, out io.Writer) error {
	pos, err := parsePositionals(args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errors.New("usage: kvdb get <path> <key>")
	}
	db, err := kvdb.Open(pos[0], kvdb.Options{})
	if err != nil {
		return err
	}
	value, getErr := db.Get([]byte(pos[1]))
	closeErr := db.Close()
	if getErr != nil && !errors.Is(getErr, kvdb.ErrNotFound) {
		return getErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	if errors.Is(getErr, kvdb.ErrNotFound) {
		_, err = fmt.Fprintln(out, "NOTFOUND")
		return err
	}
	_, err = fmt.Fprintf(out, "%s\n", value)
	return err
}

func runDel(args []string, out io.Writer) error {
	pos, err := parsePositionals(args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errors.New("usage: kvdb del <path> <key>")
	}
	db, err := kvdb.Open(pos[0], kvdb.Options{})
	if err != nil {
		return err
	}
	opErr := db.Delete([]byte(pos[1]))
	closeErr := db.Close()
	if opErr != nil {
		return opErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	_, err = fmt.Fprintln(out, "OK")
	return err
}

func runBatch(args []string, out io.Writer) error {
	pos, err := parsePositionals(args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errors.New("usage: kvdb batch <path> <file>")
	}
	ops, err := readBatchFile(pos[1])
	if err != nil {
		return fmt.Errorf("read batch file: %w", err)
	}
	db, err := kvdb.Open(pos[0], kvdb.Options{})
	if err != nil {
		return err
	}
	applyErr := db.Apply(ops)
	closeErr := db.Close()
	if applyErr != nil {
		return applyErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	_, err = fmt.Fprintf(out, "OK: applied %d operations\n", len(ops))
	return err
}

func readBatchFile(path string) ([]kvdb.Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	const maxLineBytes = kvdb.MaxValLen + kvdb.MaxKeyLen + 64
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), maxLineBytes)
	ops := make([]kvdb.Op, 0)
	for lineNum := 1; scanner.Scan(); lineNum++ {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		op, err := parseBatchOperation(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		ops = append(ops, op)
		if len(ops) > kvdb.MaxBatchOps {
			return nil, fmt.Errorf("batch has more than %d operations", kvdb.MaxBatchOps)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read line: %w", err)
	}
	return ops, nil
}

func parseBatchOperation(line string) (kvdb.Op, error) {
	command, rest, ok := takeField(line)
	if !ok {
		return kvdb.Op{}, errors.New("empty operation")
	}
	key, value, hasValue := takeField(rest)
	switch strings.ToLower(command) {
	case "put":
		if !hasValue {
			return kvdb.Op{}, errors.New("put requires a key and value")
		}
		value = strings.TrimLeft(value, " \t")
		if value == "" {
			return kvdb.Op{}, errors.New("put requires a value")
		}
		return kvdb.Op{Type: kvdb.OpPut, Key: []byte(key), Value: []byte(value)}, nil
	case "del":
		if !ok || key == "" || strings.TrimSpace(value) != "" {
			return kvdb.Op{}, errors.New("del requires exactly one key")
		}
		return kvdb.Op{Type: kvdb.OpDelete, Key: []byte(key)}, nil
	default:
		return kvdb.Op{}, fmt.Errorf("unknown operation %q (expected put or del)", command)
	}
}

func takeField(s string) (field, rest string, ok bool) {
	s = strings.TrimLeft(s, " \t")
	if s == "" {
		return "", "", false
	}
	i := strings.IndexAny(s, " \t")
	if i < 0 {
		return s, "", true
	}
	return s[:i], s[i:], true
}

func runDump(args []string, out io.Writer) error {
	pos, err := parsePositionals(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: kvdb dump <path>")
	}
	db, err := kvdb.Open(pos[0], kvdb.Options{})
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(out)
	dumpErr := db.ForEachSorted(func(key, value []byte) error {
		_, err := fmt.Fprintf(writer, "%s\t%s\n", escapeDumpBytes(key), escapeDumpBytes(value))
		return err
	})
	flushErr := writer.Flush()
	closeErr := db.Close()
	if dumpErr != nil {
		return dumpErr
	}
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	return nil
}

func runCheck(args []string, out io.Writer) error {
	pos, err := parsePositionals(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: kvdb check <path>")
	}
	result, err := kvdb.Check(pos[0])
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, result.Message); err != nil {
		return err
	}
	if !result.OK {
		return errors.New("integrity check failed: " + result.Message)
	}
	return nil
}

func escapeDumpBytes(b []byte) string {
	var buf strings.Builder
	buf.Grow(len(b))
	for _, c := range b {
		switch c {
		case '\\':
			buf.WriteString(`\\`)
		case '\t':
			buf.WriteString(`\t`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		default:
			if c >= 0x20 && c <= 0x7E {
				buf.WriteByte(c)
			} else {
				fmt.Fprintf(&buf, `\x%02x`, c)
			}
		}
	}
	return buf.String()
}

