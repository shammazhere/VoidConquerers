package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"kipi/src/kvdb"
)

const maxServeLineBytes = 2*kvdb.MaxValLen + 2*kvdb.MaxKeyLen + 1024

var errLineTooLong = errors.New("input line exceeds the supported size")
var errIncompleteBatch = errors.New("incomplete batch input")

// outMu serializes every line written to stdout. Requests from different
// lanes run concurrently, so without it two replies could interleave. The
// ACK for a write is printed from inside the engine's commit callback, i.e.
// while the engine's write lock is held, so ACK order == commit order.
var outMu sync.Mutex

func mapErrorCode(err error) string {
	switch {
	case errors.Is(err, kvdb.ErrPoisoned):
		return "POISONED"
	case errors.Is(err, kvdb.ErrDiskFull):
		return "DISK_FULL"
	case errors.Is(err, kvdb.ErrLocked):
		return "DB_LOCKED"
	case errors.Is(err, kvdb.ErrBadRequest):
		return "BAD_REQUEST"
	case errors.Is(err, kvdb.ErrCorrupt):
		return "CORRUPT"
	case errors.Is(err, kvdb.ErrIO), errors.Is(err, kvdb.ErrClosed):
		return "IO_ERROR"
	default:
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "too large") ||
			strings.Contains(msg, "out of range") ||
			strings.Contains(msg, "malformed") ||
			strings.Contains(msg, "invalid") ||
			strings.Contains(msg, "requires") ||
			strings.Contains(msg, "unknown") ||
			strings.Contains(msg, "hex") {
			return "BAD_REQUEST"
		}
		return "IO_ERROR"
	}
}

func runServe(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags, pos, err := parseArgs(args, "max-bytes", "step-delay-us", "crash-at", "compact-min-bytes")
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: kvdb serve <path> [--max-bytes N] [--step-delay-us N] [--compact-min-bytes N] [--crash-at CPn]")
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
	compactMin, err := flagInt64(flags, "compact-min-bytes")
	if err != nil {
		return err
	}

	db, err := kvdb.Open(pos[0], kvdb.Options{
		MaxBytes:        maxBytes,
		StepDelay:       time.Duration(stepDelayUs) * time.Microsecond,
		CrashAt:         flags["crash-at"],
		CompactMinBytes: compactMin,
	})
	if err != nil {
		return err
	}
	servePath = pos[0]
	stats = &serveStats{start: time.Now(), rejected: map[string]int{}}
	exited, serveErr := serveLoop(db, bufio.NewReaderSize(stdin, 64*1024), bufio.NewWriter(stdout))
	if exited {
		// EXIT typed by a person: show what this session accepted and rejected.
		fmt.Fprint(stderr, stats.report(db))
	}
	closeErr := db.Close()
	if serveErr != nil {
		return serveErr
	}
	if closeErr != nil {
		return fmt.Errorf("close database: %w", closeErr)
	}
	return nil
}

// laneOf returns the lane a request runs on. The part of the op-id before
// the first '_' names the lane ("w1_iter3_7" -> "w1"). Requests in one lane
// run in arrival order; different lanes run concurrently and really contend
// for the engine's write lock. Op-ids without '_' ("1", "op-1") all share
// the default lane, so commands typed by hand always run in order.
func laneOf(opID string) string {
	if i := strings.IndexByte(opID, '_'); i > 0 {
		return opID[:i]
	}
	return ""
}

// lanes runs queued requests: one goroutine per lane.
type lanes struct {
	mu       sync.Mutex
	queues   map[string]chan func() error
	wg       sync.WaitGroup
	errMu    sync.Mutex
	firstErr error
}

func (l *lanes) submit(lane string, job func() error) {
	l.mu.Lock()
	q, ok := l.queues[lane]
	if !ok {
		q = make(chan func() error, 64)
		l.queues[lane] = q
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			for job := range q {
				if err := job(); err != nil {
					l.errMu.Lock()
					if l.firstErr == nil {
						l.firstErr = err
					}
					l.errMu.Unlock()
				}
			}
		}()
	}
	l.mu.Unlock()
	q <- job
}

// finish waits until every queued request has run and returns the first error.
func (l *lanes) finish() error {
	l.mu.Lock()
	for _, q := range l.queues {
		close(q)
	}
	l.mu.Unlock()
	l.wg.Wait()
	return l.firstErr
}

// textCommands are the command words of the text dialect. A line may start
// with the command ("PUT 1 name Ada", as in the README) or with the op-id
// ("1 PUT name Ada"); both are accepted.
var textCommands = map[string]bool{"PUT": true, "DEL": true, "GET": true, "BATCH": true, "COMPACT": true, "DUMP": true, "STATS": true}

const serveHelp = `# kvdb serve: one request per line. Replies: ACK <id> | VAL <id> <value> | NOTFOUND <id> | ERR <id> <CODE>
#   PUT <id> <key> <value...>    DEL <id> <key>    GET <id> <key>
#   BATCH <id> <n>  then n lines: "put key value" or "del key"
#   HPUT/HDEL/HGET/HBATCH <id> ... : same with hex keys and values ("-" = empty value)
#   COMPACT <id>   DUMP <id>   STATS <id> (accepted / rejected / timings)   HELP   EXIT
# The op-id may also come first: "<id> PUT <key> <value>". ACK is printed only after both fsyncs.`

func serveLoop(db *kvdb.DB, reader *bufio.Reader, writer *bufio.Writer) (exited bool, err error) {
	ln := &lanes{queues: make(map[string]chan func() error)}
	readErr := readRequests(db, reader, writer, ln, &exited)
	laneErr := ln.finish()
	if readErr != nil {
		return exited, readErr
	}
	if laneErr != nil {
		return exited, fmt.Errorf("serve: %w", laneErr)
	}
	return exited, nil
}

// readRequests parses requests (a batch includes its n op lines) and hands
// each complete request to its lane. It returns when input ends.
func readRequests(db *kvdb.DB, reader *bufio.Reader, writer *bufio.Writer, ln *lanes, exited *bool) error {
	reply := func(lane, format string, args ...any) {
		ln.submit(lane, func() error { return writeReply(writer, format, args...) })
	}
	for {
		line, eof, err := readLine(reader, maxServeLineBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, errLineTooLong) {
				reply("", "ERR ? BAD_REQUEST %v\n", err)
				if eof {
					return err
				}
				continue
			}
			return fmt.Errorf("read serve input: %w", err)
		}
		if strings.TrimSpace(line) == "" {
			if eof {
				return nil
			}
			continue
		}
		if strings.EqualFold(strings.TrimSpace(line), "exit") || strings.EqualFold(strings.TrimSpace(line), "quit") {
			*exited = true
			return nil
		}
		if strings.EqualFold(strings.TrimSpace(line), "help") {
			reply("", "%s\n", serveHelp)
			if eof {
				return nil
			}
			continue
		}

		first, rest, ok := takeField(line)
		if !ok {
			reply("", "ERR ? BAD_REQUEST malformed line\n")
			if eof {
				return nil
			}
			continue
		}

		var opID, command, args string
		up := strings.ToUpper(first)
		switch {
		case up == "HPUT" || up == "HDEL" || up == "HGET" || up == "HBATCH" || textCommands[up]:
			// Command first: "PUT <id> ..." / "HPUT <id> ..."
			command = up
			opID, rest, ok = takeField(rest)
		default:
			// Op-id first: "<id> PUT ..."
			opID = first
			var cmdToken string
			cmdToken, rest, ok = takeField(rest)
			command = strings.ToUpper(cmdToken)
		}
		if !ok || strings.TrimSpace(opID) == "" {
			reply("", "ERR ? BAD_REQUEST malformed line (expected: PUT <op-id> <key> <value>)\n")
			if eof {
				return nil
			}
			continue
		}
		args = strings.TrimLeft(rest, " \t")
		lane := laneOf(opID)

		switch command {
		case "HPUT":
			ln.submit(lane, func() error { return serveHPUT(db, writer, opID, args) })
		case "HDEL":
			ln.submit(lane, func() error { return serveHDEL(db, writer, opID, args) })
		case "HGET":
			ln.submit(lane, func() error { return serveHGET(db, writer, opID, args) })
		case "PUT":
			ln.submit(lane, func() error { return servePut(db, writer, opID, args) })
		case "DEL":
			ln.submit(lane, func() error { return serveDelete(db, writer, opID, args) })
		case "GET":
			ln.submit(lane, func() error { return serveGet(db, writer, opID, args) })
		case "COMPACT":
			if strings.TrimSpace(args) != "" {
				reply(lane, "ERR %s BAD_REQUEST compact takes no arguments\n", opID)
				break
			}
			ln.submit(lane, func() error {
				if err := db.Compact(); err != nil {
					return writeReply(writer, "ERR %s %s\n", opID, mapErrorCode(err))
				}
				return writeReply(writer, "ACK %s\n", opID)
			})
		case "STATS":
			ln.submit(lane, func() error { return writeReply(writer, "%sSTATS %s\n", stats.report(db), opID) })
		case "DUMP":
			if strings.TrimSpace(args) != "" {
				reply(lane, "ERR %s BAD_REQUEST dump takes no arguments\n", opID)
				break
			}
			ln.submit(lane, func() error { return serveDump(db, writer, opID) })
		case "HBATCH", "BATCH":
			// The n op lines are read here, by the reader, so the request is
			// complete before it is handed to its lane.
			hexMode := command == "HBATCH"
			ops, parseErr, fatal, newEOF := readBatch(reader, args, hexMode)
			eof = newEOF
			if parseErr != nil {
				msg := fmt.Sprintf("ERR %s BAD_REQUEST\n", opID)
				if !hexMode {
					msg = fmt.Sprintf("ERR %s %v\n", opID, parseErr)
				}
				reply(lane, "%s", msg)
				if fatal {
					return parseErr
				}
				break
			}
			ln.submit(lane, func() error { return applyBatch(db, writer, opID, ops) })
		default:
			reply(lane, "ERR %s BAD_REQUEST unknown command %q\n", opID, command)
		}
		if eof {
			return nil
		}
	}
}

func serveHPUT(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	kHex, rest, ok := takeField(args)
	if !ok {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	vHex := strings.TrimSpace(rest)
	if vHex == "" {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	key, err := hex.DecodeString(kHex)
	if err != nil {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	var val []byte
	if vHex != "-" {
		val, err = hex.DecodeString(vHex)
		if err != nil {
			return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
		}
	}
	return applyBatch(db, writer, opID, []kvdb.Op{{Key: key, Value: val}})
}

func serveHDEL(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	kHex, rest, ok := takeField(args)
	if !ok || strings.TrimSpace(rest) != "" {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	key, err := hex.DecodeString(kHex)
	if err != nil {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	return applyBatch(db, writer, opID, []kvdb.Op{{Del: true, Key: key}})
}

func serveHGET(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	kHex, rest, ok := takeField(args)
	if !ok || strings.TrimSpace(rest) != "" {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	key, err := hex.DecodeString(kHex)
	if err != nil {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	val, found, err := db.Get(key)
	switch {
	case err != nil:
		return writeReply(writer, "ERR %s %s\n", opID, mapErrorCode(err))
	case !found:
		return writeReply(writer, "NOTFOUND %s\n", opID)
	case len(val) == 0:
		return writeReply(writer, "HVAL %s -\n", opID)
	default:
		return writeReply(writer, "HVAL %s %s\n", opID, hex.EncodeToString(val))
	}
}

// applyBatch commits ops and prints ACK from inside the engine's commit
// callback: after both fsyncs and the index update, under the write lock.
func applyBatch(db *kvdb.DB, writer *bufio.Writer, opID string, ops []kvdb.Op) error {
	var ackErr error
	start := time.Now()
	err := db.Apply(ops, func() {
		stats.commit(len(ops), time.Since(start))
		ackErr = writeReply(writer, "ACK %s\n", opID)
	})
	if err != nil {
		return writeReply(writer, "ERR %s %s\n", opID, mapErrorCode(err))
	}
	return ackErr
}

// readBatch reads the n op lines of a BATCH/HBATCH request. fatal means the
// input ended inside the batch, so the serve process must stop.
func readBatch(reader *bufio.Reader, args string, hexMode bool) (ops []kvdb.Op, parseErr error, fatal, eof bool) {
	count, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil || count < 1 {
		return nil, errors.New("batch count must be a positive integer"), false, false
	}
	if count > kvdb.MaxBatchOps {
		return nil, fmt.Errorf("batch operation count %d exceeds limit %d", count, kvdb.MaxBatchOps), true, false
	}
	ops = make([]kvdb.Op, 0, count)
	for i := 0; i < count; i++ {
		line, lineEOF, lineErr := readLine(reader, maxServeLineBytes)
		eof = lineEOF
		if errors.Is(lineErr, io.EOF) {
			return nil, errIncompleteBatch, true, true
		}
		if lineErr != nil {
			if parseErr == nil {
				parseErr = fmt.Errorf("batch line %d: %w", i+1, lineErr)
			}
			if lineEOF {
				return nil, parseErr, true, true
			}
			continue
		}
		var op kvdb.Op
		var opErr error
		if hexMode {
			op, opErr = parseHexBatchOperation(line)
		} else {
			op, opErr = parseBatchOperation(line)
		}
		if opErr != nil {
			if parseErr == nil {
				parseErr = fmt.Errorf("batch line %d: %w", i+1, opErr)
			}
		} else {
			ops = append(ops, op)
		}
		if lineEOF && i != count-1 {
			return nil, errIncompleteBatch, true, true
		}
	}
	if parseErr != nil {
		return nil, parseErr, eof, eof
	}
	return ops, nil, false, eof
}

func parseHexBatchOperation(line string) (kvdb.Op, error) {
	cmd, rest, ok := takeField(line)
	if !ok {
		return kvdb.Op{}, errors.New("empty operation")
	}
	switch strings.ToUpper(cmd) {
	case "HPUT":
		kHex, vHex, ok := takeField(rest)
		if !ok {
			return kvdb.Op{}, errors.New("HPUT requires key")
		}
		key, err := hex.DecodeString(kHex)
		if err != nil {
			return kvdb.Op{}, fmt.Errorf("invalid hex key: %w", err)
		}
		vHex = strings.TrimSpace(vHex)
		var val []byte
		if vHex != "" && vHex != "-" {
			val, err = hex.DecodeString(vHex)
			if err != nil {
				return kvdb.Op{}, fmt.Errorf("invalid hex val: %w", err)
			}
		}
		return kvdb.Op{Del: false, Type: kvdb.OpPut, Key: key, Value: val}, nil

	case "HDEL":
		kHex, rest, ok := takeField(rest)
		if !ok || strings.TrimSpace(rest) != "" {
			return kvdb.Op{}, errors.New("HDEL requires exactly one key")
		}
		key, err := hex.DecodeString(kHex)
		if err != nil {
			return kvdb.Op{}, fmt.Errorf("invalid hex key: %w", err)
		}
		return kvdb.Op{Del: true, Type: kvdb.OpDelete, Key: key}, nil

	default:
		return parseBatchOperation(line)
	}
}

func servePut(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	key, value, ok := takeField(args)
	if !ok || len(value) == 0 {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	value = strings.TrimLeft(value, " \t")
	return applyBatch(db, writer, opID, []kvdb.Op{{Key: []byte(key), Value: []byte(value)}})
}

func serveDelete(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	key, rest, ok := takeField(args)
	if !ok || strings.TrimSpace(rest) != "" {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	return applyBatch(db, writer, opID, []kvdb.Op{{Del: true, Key: []byte(key)}})
}

func serveGet(db *kvdb.DB, writer *bufio.Writer, opID, args string) error {
	key, rest, ok := takeField(args)
	if !ok || strings.TrimSpace(rest) != "" {
		return writeReply(writer, "ERR %s BAD_REQUEST\n", opID)
	}
	value, found, err := db.Get([]byte(key))
	switch {
	case err != nil:
		return writeReply(writer, "ERR %s %s\n", opID, mapErrorCode(err))
	case !found:
		return writeReply(writer, "NOTFOUND %s\n", opID)
	default:
		return writeReply(writer, "VAL %s %s\n", opID, value)
	}
}

// serveDump collects the whole dump first (under the engine's read lock) and
// only then takes the output lock. Taking both at once could deadlock with a
// writer that holds the engine lock while printing its ACK.
func serveDump(db *kvdb.DB, writer *bufio.Writer, opID string) error {
	var buf bytes.Buffer
	err := db.ForEachSorted(func(key, value []byte) error {
		_, err := fmt.Fprintf(&buf, "KV %s %s %s\n", opID, escapeDumpBytes(key), escapeDumpBytes(value))
		return err
	})
	if err != nil {
		return writeReply(writer, "ERR %s %s\n", opID, mapErrorCode(err))
	}
	fmt.Fprintf(&buf, "ACK %s\n", opID)
	return writeReply(writer, "%s", buf.String())
}

func writeReply(writer *bufio.Writer, format string, args ...any) error {
	text := fmt.Sprintf(format, args...)
	stats.countReply(text)
	outMu.Lock()
	defer outMu.Unlock()
	if _, err := io.WriteString(writer, text); err != nil {
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
