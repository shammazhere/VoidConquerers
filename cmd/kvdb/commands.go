package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kipi/src/kvdb"
)

func runOpen(args []string, out io.Writer) error {
	// --trace takes no value; strip it before the generic flag parser.
	trace := false
	rest := make([]string, 0, len(args))
	for _, a := range args {
		if a == "--trace" {
			trace = true
			continue
		}
		rest = append(rest, a)
	}
	flags, pos, err := parseArgs(rest, "max-bytes", "crash-at", "step-delay-us")
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: kvdb open <path> [--max-bytes N] [--trace] [--crash-at CPn] [--step-delay-us N]")
	}
	maxBytes, err := flagInt64(flags, "max-bytes")
	if err != nil {
		return err
	}
	stepDelayUs, err := flagInt64(flags, "step-delay-us")
	if err != nil {
		return err
	}
	opts := kvdb.Options{
		MaxBytes:  maxBytes,
		CrashAt:   flags["crash-at"],
		StepDelay: time.Duration(stepDelayUs) * time.Microsecond,
	}
	if trace {
		// Phase events, printed as they happen, so a process killed during
		// recovery shows how far it got (used by torture --mode recovery-crash).
		opts.Trace = func(event string) { fmt.Fprintf(out, "INFO %s\n", event) }
	}

	start := time.Now()
	db, err := kvdb.Open(pos[0], opts)
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	keyCount := db.KeyCount()
	rs := db.RecoveryStats()
	if err := db.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	repair := ""
	if rs.TruncatedBytes > 0 {
		repair += fmt.Sprintf("; cut %d bytes of torn/uncommitted tail", rs.TruncatedBytes)
	}
	if rs.MarkerRestored > 0 {
		repair += fmt.Sprintf("; restored the last %d bytes of the final commit marker", rs.MarkerRestored)
	}
	if rs.TmpRemoved {
		repair += "; deleted leftover compaction temp file"
	}
	if rs.Created {
		repair += "; created new database"
	}
	if _, err = fmt.Fprintf(out, "opened %s: %d keys, recovery in %v%s\n", pos[0], keyCount, elapsed, repair); err != nil {
		return err
	}
	if rs.Created {
		return nil
	}
	// The first line above is what scripts parse; the lines below explain it.
	fmt.Fprintf(out, "  scanned  : %d bytes in %v\n", rs.ScannedBytes, rs.Duration.Round(time.Microsecond))
	fmt.Fprintf(out, "  accepted : %d records in %d committed batches (CRC checked, commit marker found)\n", rs.Records, rs.Batches)
	if rs.TruncatedBytes > 0 {
		fmt.Fprintf(out, "  rejected : %d bytes cut (%d complete records without a commit marker, plus any torn bytes)\n", rs.TruncatedBytes, rs.Discarded)
		fmt.Fprintf(out, "  reason   : %s\n", explainStop(rs.StopReason, rs.Discarded))
		if rs.RejectedFile != "" {
			fmt.Fprintf(out, "  saved    : a copy of the cut bytes is in %s (inspection only, never applied; see: kvdb rejected %s)\n", filepath.Base(rs.RejectedFile), pos[0])
		}
	} else {
		fmt.Fprintln(out, "  rejected : nothing (the file ends exactly on a commit marker)")
		if rs.MarkerRestored > 0 {
			fmt.Fprintf(out, "  restored : the last commit marker was cut short by %d bytes; those bytes are constants, so it was rebuilt, verified by its CRC and kept (nothing lost)\n", rs.MarkerRestored)
		}
	}
	_, err = fmt.Fprintf(out, "  file     : %d -> %d bytes\n", rs.ScannedBytes, rs.FileBytes)
	return err
}

// explainStop turns the scanner's stop reason into one plain sentence.
func explainStop(reason string, discarded int) string {
	switch {
	case reason == "end of file" && discarded > 0:
		return "the last write was interrupted before its commit marker reached the disk, so it never happened"
	case strings.HasPrefix(reason, "torn record header"), strings.HasPrefix(reason, "record extends past end of file"):
		return "the process died in the middle of writing a record (" + reason + ")"
	case strings.HasPrefix(reason, "CRC mismatch"), strings.HasPrefix(reason, "invalid record header"):
		return "half-written bytes at the end of the file (" + reason + ")"
	default:
		return reason
	}
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
	value, found, getErr := db.Get([]byte(pos[1]))
	closeErr := db.Close()
	if getErr != nil {
		return getErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	if !found {
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
	puts := 0
	for _, op := range ops {
		if !op.IsDel() {
			puts++
		}
	}
	start := time.Now()
	applyErr := db.Apply(ops)
	elapsed := time.Since(start)
	closeErr := db.Close()
	if applyErr != nil {
		return fmt.Errorf("batch rejected, nothing written (all %d operations or none): %w", len(ops), applyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	_, err = fmt.Fprintf(out, "OK: applied %d operations (%d put, %d del) in %v, durable (2 fsyncs)\n", len(ops), puts, len(ops)-puts, elapsed.Round(time.Microsecond))
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
		return kvdb.Op{Del: false, Type: kvdb.OpPut, Key: []byte(key), Value: []byte(value)}, nil
	case "del":
		if !ok || key == "" || strings.TrimSpace(value) != "" {
			return kvdb.Op{}, errors.New("del requires exactly one key")
		}
		return kvdb.Op{Del: true, Type: kvdb.OpDelete, Key: []byte(key)}, nil
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
	writer := bufio.NewWriterSize(out, 1<<20)
	var line []byte
	dumpErr := db.ForEachSorted(func(key, value []byte) error {
		line = appendEscaped(line[:0], key)
		line = append(line, '\t')
		line = appendEscaped(line, value)
		line = append(line, '\n')
		_, err := writer.Write(line)
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
	start := time.Now()
	report := kvdb.Check(pos[0])
	elapsed := time.Since(start)
	if !report.OK {
		reason := report.Reason
		if reason == "" {
			reason = "corrupted database"
		}
		fmt.Fprintln(out, reason)
		return errors.New("integrity check failed: " + reason)
	}
	if report.TailBytes > 0 {
		fmt.Fprintf(out, "OK (uncommitted tail: %d bytes; will be truncated on open)\n", report.TailBytes)
	} else if report.MarkerFix > 0 {
		fmt.Fprintf(out, "OK (last commit marker is missing its last %d bytes, which are constants; open restores them, nothing is lost)\n", report.MarkerFix)
	} else {
		fmt.Fprintln(out, "OK")
	}
	// The first line is the verdict scripts parse; the indented lines explain it.
	fmt.Fprintf(out, "  checked  : %d bytes in %v (read-only, nothing changed)\n", report.FileBytes, elapsed.Round(time.Microsecond))
	fmt.Fprintf(out, "  committed: %d records in %d batches, %d live keys, %d bytes\n", report.Applied, report.Commits, report.Keys, report.CommittedBytes)
	if report.TailBytes > 0 {
		fmt.Fprintf(out, "  tail     : %d bytes not committed (%d complete records without a marker); reason: %s\n", report.TailBytes, report.Discarded, explainStop(report.TailReason, report.Discarded))
	}
	if report.TmpExists {
		fmt.Fprintln(out, "  tmp      : leftover db.kv.tmp from an interrupted compaction; the next open deletes it")
	}
	return nil
}

// appendEscaped appends b in dump notation: printable ASCII as-is, \\ \t \n \r
// escaped, every other byte as \xHH. It works byte by byte with a lookup
// table (no fmt call per byte), which matters for megabyte binary values.
func appendEscaped(dst, b []byte) []byte {
	const hexdig = "0123456789abcdef"
	for _, c := range b {
		switch {
		case c == '\\':
			dst = append(dst, '\\', '\\')
		case c == '\t':
			dst = append(dst, '\\', 't')
		case c == '\n':
			dst = append(dst, '\\', 'n')
		case c == '\r':
			dst = append(dst, '\\', 'r')
		case c >= 0x20 && c <= 0x7E:
			dst = append(dst, c)
		default:
			dst = append(dst, '\\', 'x', hexdig[c>>4], hexdig[c&15])
		}
	}
	return dst
}

func escapeDumpBytes(b []byte) string { return string(appendEscaped(make([]byte, 0, len(b)), b)) }

// runRejected lists the writes recovery cut off (the uncommitted tail left by
// a crash). They were never acknowledged and are never applied; this shows a
// person what to send again.
func runRejected(args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("%w: usage: kvdb rejected <path>", kvdb.ErrBadRequest)
	}
	files, err := kvdb.RejectedFiles(args[0])
	if err != nil {
		return fmt.Errorf("%w: %v", kvdb.ErrIO, err)
	}
	if len(files) == 0 {
		_, err = fmt.Fprintln(out, "no rejected writes: recovery has not cut anything (or the copies were removed)")
		return err
	}
	fmt.Fprintf(out, "%d rejected tail(s), newest first. These writes were NOT acknowledged and are NOT in the database.\n", len(files))
	fmt.Fprintln(out, "To keep one, write it again (kvdb put / del / batch).")
	for _, f := range files {
		info, err := kvdb.ReadRejected(f)
		if err != nil {
			fmt.Fprintf(out, "\n%s: unreadable (%v)\n", filepath.Base(f), err)
			continue
		}
		fmt.Fprintf(out, "\n%s  saved %s  %d bytes cut at offset %d\n", filepath.Base(f), info.Saved.Format("2006-01-02 15:04:05"), info.Bytes, info.Offset)
		puts, dels := 0, 0
		for i, r := range info.Records {
			if i == 20 {
				fmt.Fprintf(out, "  ... %d more\n", len(info.Records)-20)
				break
			}
			switch r.Kind {
			case "PUT":
				puts++
				fmt.Fprintf(out, "  PUT  %s = %s   (complete, but no commit marker)\n", preview(r.Key), preview(r.Value))
			case "DEL":
				dels++
				fmt.Fprintf(out, "  DEL  %s   (complete, but no commit marker)\n", preview(r.Key))
			case "MARKER":
				fmt.Fprintf(out, "  MARKER for %d records (count did not match)\n", r.Count)
			case "TORN":
				fmt.Fprintf(out, "  TORN %d bytes: a record cut in the middle of being written\n", r.Bytes)
			}
		}
	}
	return nil
}

// preview shows a key or value readably: text as-is (shortened), binary as hex.
func preview(b []byte) string {
	printable := true
	for _, c := range b {
		if c < 32 || c > 126 {
			printable = false
			break
		}
	}
	s := string(b)
	if !printable {
		s = fmt.Sprintf("0x%x", b)
	}
	if len(s) > 60 {
		s = fmt.Sprintf("%s... (%d bytes)", s[:57], len(b))
	}
	return s
}
