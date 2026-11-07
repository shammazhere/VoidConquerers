package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"kipi/src/kvdb"
)

// serveStats counts what serve accepted and rejected. It is shown by the
// STATS request and printed to stderr when serve gets EXIT. Counting happens
// on the reply text (every reply goes through writeReply), so a reply cannot
// be sent without being counted.
type serveStats struct {
	mu         sync.Mutex
	start      time.Time
	accepted   int            // ACK replies: durable writes, COMPACT, DUMP
	rejected   map[string]int // ERR replies by code
	found      int            // GET hits
	notFound   int            // GET misses
	commits    int            // successful write commits (PUT, DEL, BATCH)
	opsWritten int            // operations inside those commits
	ackSum     time.Duration  // request start -> ACK, summed over commits
	ackMax     time.Duration
}

var stats = &serveStats{start: time.Now(), rejected: map[string]int{}}

// servePath is the database file, for the file size in the report.
var servePath string

// countReply classifies every line of one reply.
func (s *serveStats) countReply(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case "ACK":
			s.accepted++
		case "VAL", "HVAL":
			s.found++
		case "NOTFOUND":
			s.notFound++
		case "ERR":
			code := "BAD_REQUEST" // parse errors carry a message instead of a code
			if len(f) >= 3 && f[2] == strings.ToUpper(f[2]) && strings.Trim(f[2], "ABCDEFGHIJKLMNOPQRSTUVWXYZ_") == "" {
				code = f[2]
			}
			s.rejected[code]++
		}
	}
}

// commit records one durable write: how many ops and how long from the start
// of the request to the ACK (both fsyncs plus any wait for the write lock).
func (s *serveStats) commit(ops int, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	s.opsWritten += ops
	s.ackSum += d
	if d > s.ackMax {
		s.ackMax = d
	}
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000) }

// report renders the counters as "# " comment lines (like HELP).
func (s *serveStats) report(db *kvdb.DB) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	rejectedTotal := 0
	codes := make([]string, 0, len(s.rejected))
	for c, n := range s.rejected {
		rejectedTotal += n
		codes = append(codes, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(codes)
	why := "none"
	if len(codes) > 0 {
		why = strings.Join(codes, " ")
	}
	avg := time.Duration(0)
	if s.commits > 0 {
		avg = s.ackSum / time.Duration(s.commits)
	}
	size := int64(-1)
	if fi, err := os.Stat(servePath); err == nil {
		size = fi.Size()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# serve stats after %.1f s\n", time.Since(s.start).Seconds())
	fmt.Fprintf(&b, "#   accepted (ACK) : %d   (an ACK is sent only after both fsyncs)\n", s.accepted)
	fmt.Fprintf(&b, "#   rejected (ERR) : %d   reasons: %s\n", rejectedTotal, why)
	fmt.Fprintf(&b, "#   reads          : %d found, %d not found\n", s.found, s.notFound)
	fmt.Fprintf(&b, "#   writes         : %d operations in %d commits; time to ACK avg %s, max %s\n", s.opsWritten, s.commits, ms(avg), ms(s.ackMax))
	fmt.Fprintf(&b, "#   database       : %d keys, file %d bytes\n", db.KeyCount(), size)
	return b.String()
}
