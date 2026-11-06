package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"kipi/src/kvdb"
)

const maxServeLineBytes = kvdb.MaxValLen + kvdb.MaxKeyLen + 256

var errLineTooLong = errors.New("input line exceeds the supported size")
var errIncompleteBatch = errors.New("incomplete batch input")

func runServe(args []string, stdin io.Reader, stdout io.Writer) error {
	flags, pos, err := parseArgs(args, "max-bytes", "step-delay-us")
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: kvdb serve <path> [--max-bytes N] [--step-delay-us N]")
	}
	maxBytes, err := flagInt64(flags, "max-bytes")
	if err != nil {
		return err
	}
	stepDelayUs, err := flagInt64(flags, "step-delay-us")
	if err != nil {
		return err
	}
	maxDelayUs := int64((1<<63 - 1) / int64(time.Microsecond))
	if stepDelayUs > maxDelayUs {
		return errors.New("option --step-delay-us is too large")
	}

	db, err := kvdb.Open(pos[0], kvdb.Options{MaxBytes: maxBytes})
	if err != nil {
		return err
	}
	serveErr := serveLoop(db, bufio.NewReaderSize(stdin, 64*1024), bufio.NewWriter(stdout), time.Duration(stepDelayUs)*time.Microsecond)
	closeErr := db.Close()
	if serveErr != nil {
		return serveErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	return nil
}

func serveLoop(db *kvdb.DB, reader *bufio.Reader, writer *bufio.Writer, stepDelay time.Duration) error {
	for {
		line, eof, err := readLine(reader, maxServeLineBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, errLineTooLong) {
				if writeErr := writeReply(writer, "ERR ? %v\n", err); writeErr != nil {
					return fmt.Errorf("write serve response: %w", writeErr)
				}
				if eof {
					return err
				}
				continue
			}
			return fmt.Errorf("read serve input: %w", err)
		}
		if line == "" {
			if eof {
				return nil
			}
			continue
		}
		if stepDelay > 0 {
			time.Sleep(stepDelay)
		}
		if strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit") {
			return nil
		}

		opID, rest, ok := takeField(line)
		if !ok {
			if err := writeReply(writer, "ERR ? malformed line\n"); err != nil {
				return fmt.Errorf("write serve response: %w", err)
			}
			if eof {
				return nil
			}
			continue
		}
		command, rest, ok := takeField(rest)
		if !ok || strings.TrimSpace(opID) == "" {
			if err := writeReply(writer, "ERR ? malformed line\n"); err != nil {
				return fmt.Errorf("write serve response: %w", err)
			}
			if eof {
				return nil
			}
			continue
		}
		args := strings.TrimLeft(rest, " \t")

		var serveErr error
		switch strings.ToLower(command) {
		case "put":
			serveErr = servePut(db, writer, opID, args)
		case "del":
			serveErr = serveDelete(db, writer, opID, args)
		case "get":
			serveErr = serveGet(db, writer, opID, args)
		case "batch":
			serveErr, eof = serveBatch(db, reader, writer, opID, args, eof)
		case "compact":
			if strings.TrimSpace(args) != "" {
				serveErr = writeReply(writer, "ERR %s compact takes no arguments\n", opID)
			} else if err := db.Compact(); err != nil {
				serveErr = writeReply(writer, "ERR %s %v\n", opID, err)
			} else {
				serveErr = writeReply(writer, "ACK %s\n", opID)
			}
		case "dump":
			if strings.TrimSpace(args) != "" {
				serveErr = writeReply(writer, "ERR %s dump takes no arguments\n", opID)
			} else {
				serveErr = serveDump(db, writer, opID)
			}
		default:
			serveErr = writeReply(writer, "ERR %s unknown command %q\n", opID, command)
		}
		if serveErr != nil {
			return fmt.Errorf("serve operation %s: %w", opID, serveErr)
		}
		if eof {
			return nil
		}
	}
}

func servePut(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	key, value, ok := takeField(args)
	if !ok || len(value) == 0 {
		return writeReply(writer, "ERR %s put requires a key and value\n", opID)
	}
	value = strings.TrimLeft(value, " \t")
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		return writeReply(writer, "ERR %s %v\n", opID, err)
	}
	return writeReply(writer, "ACK %s\n", opID)
}

func serveDelete(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	key, rest, ok := takeField(args)
	if !ok || strings.TrimSpace(rest) != "" {
		return writeReply(writer, "ERR %s del requires exactly one key\n", opID)
	}
	if err := db.Delete([]byte(key)); err != nil {
		return writeReply(writer, "ERR %s %v\n", opID, err)
	}
	return writeReply(writer, "ACK %s\n", opID)
}

func serveGet(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	key, rest, ok := takeField(args)
	if !ok || strings.TrimSpace(rest) != "" {
		return writeReply(writer, "ERR %s get requires exactly one key\n", opID)
	}
	value, err := db.Get([]byte(key))
	switch {
	case errors.Is(err, kvdb.ErrNotFound):
		return writeReply(writer, "NOTFOUND %s\n", opID)
	case err != nil:
		return writeReply(writer, "ERR %s %v\n", opID, err)
	default:
		return writeReply(writer, "VAL %s %s\n", opID, value)
	}
}

func serveBatch(db *kvdb.DB, reader *bufio.Reader, writer *bufio.Writer, opID, args string, eof bool) (error, bool) {
	count, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil || count < 0 {
		return writeReply(writer, "ERR %s batch requires a non-negative count\n", opID), eof
	}
	if count > kvdb.MaxBatchOps {
		writeErr := writeReply(writer, "ERR %s batch exceeds the %d operation limit\n", opID, kvdb.MaxBatchOps)
		if writeErr != nil {
			return writeErr, eof
		}
		return fmt.Errorf("batch operation count %d exceeds limit %d", count, kvdb.MaxBatchOps), eof
	}

	ops := make([]kvdb.Op, 0, count)
	var parseErr error
	for i := 0; i < count; i++ {
		line, lineEOF, lineErr := readLine(reader, maxServeLineBytes)
		eof = lineEOF
		if errors.Is(lineErr, io.EOF) {
			parseErr = errIncompleteBatch
			break
		}
		if lineErr != nil {
			if parseErr == nil {
				parseErr = fmt.Errorf("batch line %d: %w", i+1, lineErr)
			}
			if lineEOF {
				break
			}
			continue
		}
		op, opErr := parseBatchOperation(line)
		if opErr != nil {
			if parseErr == nil {
				parseErr = fmt.Errorf("batch line %d: %w", i+1, opErr)
			}
		} else {
			ops = append(ops, op)
		}
		if lineEOF && i != count-1 && parseErr == nil {
			parseErr = errIncompleteBatch
		}
		if lineEOF && i != count-1 {
			break
		}
	}
	if parseErr != nil {
		if err := writeReply(writer, "ERR %s %v\n", opID, parseErr); err != nil {
			return err, eof
		}
		if errors.Is(parseErr, errIncompleteBatch) || eof {
			return parseErr, true
		}
		return nil, false
	}
	if err := db.Apply(ops); err != nil {
		return writeReply(writer, "ERR %s %v\n", opID, err), eof
	}
	return writeReply(writer, "ACK %s\n", opID), eof
}

func serveDump(db *kvdb.DB, writer *bufio.Writer, opID string) error {
	err := db.ForEachSorted(func(key, value []byte) error {
		_, err := fmt.Fprintf(writer, "KV %s %s %s\n", opID, key, value)
		return err
	})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "ACK %s\n", opID); err != nil {
		return err
	}
	return writer.Flush()
}

func writeReply(writer *bufio.Writer, format string, args ...any) error {
	if _, err := fmt.Fprintf(writer, format, args...); err != nil {
		return err
	}
	return writer.Flush()
}

func readLine(reader *bufio.Reader, maxBytes int) (string, bool, error) {
	var line []byte
	tooLong := false
	for {
		part, err := reader.ReadSlice('\n')
		if !tooLong {
			if len(line)+len(part) > maxBytes+2 {
				tooLong = true
			} else {
				line = append(line, part...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return "", false, err
		}
		eof := errors.Is(err, io.EOF)
		if tooLong {
			return "", eof, errLineTooLong
		}
		line = bytesTrimNewline(line)
		if eof && len(line) == 0 {
			return "", true, io.EOF
		}
		if len(line) > maxBytes {
			return "", eof, errLineTooLong
		}
		return string(line), eof, nil
	}
}

func bytesTrimNewline(line []byte) []byte {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}
