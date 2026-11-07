package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Crash-point mode: deterministic fault injection.
//
// Random kills (the other modes) find problems nobody predicted. This mode
// proves the ones we did predict: for each of the 14 "CPn - CRASH HERE"
// lines in src/kvdb it makes the REAL ./kvdb binary kill itself at exactly
// that line (--crash-at CPn: SIGKILL / TerminateProcess on itself, no
// cleanup), shows what is on disk, runs recovery, and checks the result
// against the answer written in BREAKING.md. For the write path it also
// checks the client's side: before CP7 no ACK may have been printed, at CP7
// it must have been. One extra case (CP7b) writes right after a compaction,
// so a write that goes to the old, replaced file is caught too.
//
// Because every case is deterministic, the planted-bug build fails here on
// every run, in under a second, for all four bugs.
//
//	./torture --mode crash-points

type cpCase struct {
	cp     string
	where  string // the line, in words
	covers string // PDF scenarios this crash point exercises
	setup  func(db string) error
	crash  func(db string) (out string, killed bool, err error)
	expect string
	verify func(db string) (string, bool)
	// ack: "none" = the client must NOT have received an ACK before the kill
	// (the commit was not durable yet, CP1-CP6); "ack" = it must have (CP7).
	ack string
}

func RunCrashPoints(kvdbBin, workDir string) error {
	if _, err := os.Stat(kvdbBin); err != nil {
		return fmt.Errorf("kvdb binary not found at %q (build with: go build -o . ./cmd/kvdb ./cmd/torture)", kvdbBin)
	}
	run := func(stdin string, args ...string) (string, *os.ProcessState, error) {
		cmd := exec.Command(kvdbBin, args...)
		if stdin != "" {
			cmd.Stdin = strings.NewReader(stdin)
		}
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), cmd.ProcessState, err
	}
	killed := func(ps *os.ProcessState, err error) bool {
		if err == nil || ps == nil {
			return false
		}
		if runtime.GOOS == "windows" {
			return ps.ExitCode() == 1 // TerminateProcess(self, 1)
		}
		return !ps.Exited() // terminated by a signal
	}
	serveCrash := func(db, cp, input string) (string, bool, error) {
		out, ps, err := run(input, "serve", db, "--crash-at", cp)
		return out, killed(ps, err), nil
	}
	openCrash := func(db, cp string) (string, bool, error) {
		out, ps, err := run("", "open", db, "--crash-at", cp)
		return out, killed(ps, err), nil
	}
	get := func(db, key string) string {
		out, _, err := run("", "get", db, key)
		if err != nil {
			return "ERROR " + out
		}
		return out
	}
	put := func(db, k, v string) error {
		if out, _, err := run("", "put", db, k, v); err != nil {
			return fmt.Errorf("put: %s", out)
		}
		return nil
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	size := func(p string) int64 {
		fi, err := os.Stat(p)
		if err != nil {
			return -1
		}
		return fi.Size()
	}
	base := func(db string) error { return put(db, "city", "Delhi") }
	expectName := func(want string) func(db string) (string, bool) {
		return func(db string) (string, bool) {
			got, city := get(db, "name"), get(db, "city")
			obs := "name=" + got
			if got == "NOTFOUND" {
				obs = "name absent"
			}
			return obs, got == want && city == "Delhi"
		}
	}
	bigBatch := func() string {
		v := hex.EncodeToString(make([]byte, 200<<10)) // 3 x 200 KiB > one 256 KiB write piece
		var b strings.Builder
		b.WriteString("HBATCH 1 3\n")
		for _, k := range []string{"big1", "big2", "big3"} {
			fmt.Fprintf(&b, "HPUT %s %s\n", hex.EncodeToString([]byte(k)), v)
		}
		return b.String()
	}
	churn := func(db string) error {
		if err := base(db); err != nil {
			return err
		}
		var in strings.Builder
		for i := 0; i < 40; i++ {
			fmt.Fprintf(&in, "%d PUT a version-%d\n", i+1, i)
		}
		in.WriteString("exit\n")
		if out, _, err := run(in.String(), "serve", db); err != nil {
			return fmt.Errorf("churn: %s", out)
		}
		return nil
	}
	compactOK := func(db string) (string, bool) {
		a, city := get(db, "a"), get(db, "city")
		tmp := exists(db + ".tmp")
		obs := fmt.Sprintf("a=%s, city=%s, tmp left=%v", a, city, tmp)
		return obs, a == "version-39" && city == "Delhi" && !tmp
	}
	putName := "1 PUT name Ada\n"

	cases := []cpCase{
		{cp: "CP1", where: "Apply: before any byte is written", covers: "A1", setup: base, ack: "none",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP1", putName) }, expect: "name absent", verify: expectName("NOTFOUND")},
		{cp: "CP2", where: "Apply: between 256 KiB write pieces", covers: "A2 A7 B1", setup: base, ack: "none",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP2", bigBatch()) }, expect: "whole batch absent",
			verify: func(db string) (string, bool) {
				n := 0
				for _, k := range []string{"big1", "big2", "big3"} {
					if get(db, k) != "NOTFOUND" {
						n++
					}
				}
				return fmt.Sprintf("%d of 3 batch keys present", n), n == 0 && get(db, "city") == "Delhi"
			}},
		{cp: "CP3", where: "Apply: records written, before fsync 1", covers: "A3 B2", setup: base, ack: "none",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP3", putName) }, expect: "name absent", verify: expectName("NOTFOUND")},
		{cp: "CP4", where: "Apply: after fsync 1, before the marker", covers: "B2", setup: base, ack: "none",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP4", putName) }, expect: "name absent", verify: expectName("NOTFOUND")},
		{cp: "CP5", where: "Apply: marker written, before fsync 2", covers: "B3", setup: base, ack: "none",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP5", putName) }, expect: "name=Ada", verify: expectName("Ada")},
		{cp: "CP6", where: "Apply: after fsync 2, before index+ACK", covers: "A4", setup: base, ack: "none",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP6", putName) }, expect: "name=Ada, no ACK", verify: expectName("Ada")},
		{cp: "CP7", where: "Apply: after the ACK", covers: "A5", setup: base, ack: "ack",
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP7", putName) }, expect: "name=Ada, ACKed", verify: expectName("Ada")},
		{cp: "CP7b", where: "Apply after a COMPACT: after the ACK", covers: "A5 C4", setup: churn, ack: "ack",
			crash: func(db string) (string, bool, error) {
				return serveCrash(db, "CP7", "1 COMPACT\n2 PUT name Ada\n")
			},
			expect: "name=Ada, a=version-39, ACKed",
			verify: func(db string) (string, bool) {
				n, a := get(db, "name"), get(db, "a")
				return fmt.Sprintf("name=%s, a=%s", n, a), n == "Ada" && a == "version-39" && get(db, "city") == "Delhi"
			}},
		{cp: "CP8", where: "Open: deleting a leftover tmp", covers: "C3 C5",
			setup: func(db string) error {
				if err := base(db); err != nil {
					return err
				}
				return os.WriteFile(db+".tmp", []byte("half-written compaction output"), 0644)
			},
			crash: func(db string) (string, bool, error) { return openCrash(db, "CP8") }, expect: "data intact, tmp gone",
			verify: func(db string) (string, bool) {
				tmp := exists(db + ".tmp")
				return fmt.Sprintf("city=%s, tmp left=%v", get(db, "city"), tmp), get(db, "city") == "Delhi" && !tmp
			}},
		{cp: "CP9", where: "Open: scan done, nothing written yet", covers: "C1 F2",
			setup: func(db string) error {
				if err := base(db); err != nil {
					return err
				}
				_, _, err := serveCrash(db, "CP3", "1 PUT torn never-committed\n") // leaves an uncommitted tail
				return err
			},
			crash: func(db string) (string, bool, error) { return openCrash(db, "CP9") }, expect: "torn absent, tail cut",
			verify: func(db string) (string, bool) {
				t := get(db, "torn")
				return "torn " + map[bool]string{true: "absent", false: "PRESENT"}[t == "NOTFOUND"], t == "NOTFOUND" && get(db, "city") == "Delhi"
			}},
		{cp: "CP10", where: "Open: after ftruncate, before its fsync", covers: "C2 F2",
			setup: func(db string) error {
				if err := base(db); err != nil {
					return err
				}
				_, _, err := serveCrash(db, "CP3", "1 PUT torn never-committed\n")
				return err
			},
			crash: func(db string) (string, bool, error) { return openCrash(db, "CP10") }, expect: "torn absent, tail cut",
			verify: func(db string) (string, bool) {
				t := get(db, "torn")
				return "torn " + map[bool]string{true: "absent", false: "PRESENT"}[t == "NOTFOUND"], t == "NOTFOUND" && get(db, "city") == "Delhi"
			}},
		{cp: "CP11", where: "Compact: halfway through writing tmp", covers: "C3", setup: churn,
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP11", "1 COMPACT\n") }, expect: "data intact, tmp gone", verify: compactOK},
		{cp: "CP12", where: "Compact: tmp fsynced, before rename", covers: "C3", setup: churn,
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP12", "1 COMPACT\n") }, expect: "data intact, tmp gone", verify: compactOK},
		{cp: "CP13", where: "Compact: after the atomic rename", covers: "C4 C5", setup: churn,
			crash: func(db string) (string, bool, error) { return serveCrash(db, "CP13", "1 COMPACT\n") }, expect: "data intact, file shrunk", verify: compactOK},
		{cp: "CP14", where: "Create: tmp has header, before rename", covers: "creation",
			setup: func(db string) error { return nil },
			crash: func(db string) (string, bool, error) { return openCrash(db, "CP14") }, expect: "empty database, no tmp",
			verify: func(db string) (string, bool) {
				out, _, err := run("", "dump", db)
				tmp := exists(db + ".tmp")
				return fmt.Sprintf("keys=%d, tmp left=%v", len(strings.Fields(out)), tmp), err == nil && out == "" && !tmp
			}},
	}

	root := filepath.Join(workDir, "crash-points")
	_ = os.RemoveAll(root)
	if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}

	fmt.Println("[Torture] Mode: crash-points. Deterministic fault injection: the real ./kvdb binary kills itself")
	fmt.Println("          (SIGKILL / TerminateProcess, no cleanup) at each CRASH HERE line in src/kvdb, then we inspect and recover.")
	fmt.Println()
	fmt.Printf("%-5s %-40s %-9s %-44s %-34s %-52s %s\n", "CP", "Process dies at", "Covers", "On disk after the kill (kvdb check)", "Recovery (kvdb open)", "Observed", "")
	fmt.Println(strings.Repeat("-", 198))
	t0 := time.Now()
	failed := 0
	for _, c := range cases {
		dir := filepath.Join(root, strings.ToLower(c.cp))
		_ = os.MkdirAll(dir, 0755)
		db := filepath.Join(dir, "db.kv")
		if err := c.setup(db); err != nil {
			return fmt.Errorf("%s setup: %w", c.cp, err)
		}
		sizeBeforeCrash := size(db)
		crashOut, wasKilled, err := c.crash(db)
		if err != nil {
			return fmt.Errorf("%s: %w", c.cp, err)
		}
		// What the kill left behind, before anything repairs it.
		onDisk := ""
		switch {
		case !exists(db):
			onDisk = "no db.kv yet"
		default:
			chk, _, _ := run("", "check", db)
			onDisk = strings.Replace(firstLine(chk), "; will be truncated on open", "", 1)
		}
		if exists(db + ".tmp") {
			onDisk += " + leftover db.kv.tmp"
		}
		openOut, _, openErr := run("", "open", db)
		openOut = firstLine(openOut)
		recovery := "nothing to repair"
		if openErr != nil {
			recovery = "OPEN FAILED: " + openOut
		} else if i := strings.Index(openOut, "; "); i >= 0 {
			recovery = strings.ReplaceAll(openOut[i+2:], "; ", ", ")
			recovery = strings.Replace(recovery, " of torn/uncommitted tail", " of tail", 1)
			recovery = strings.Replace(recovery, "deleted leftover compaction temp file", "deleted tmp", 1)
		}
		observed, ok := c.verify(db)
		gotAck := false
		for _, line := range strings.Split(crashOut, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "ACK ") {
				gotAck = true
			}
		}
		switch {
		case c.ack == "none" && gotAck:
			ok = false
			observed += ", ACK SENT BEFORE THE COMMIT WAS DURABLE"
		case c.ack == "none":
			observed += ", no ACK"
		case c.ack == "ack" && !gotAck:
			ok = false
			observed += ", ACK MISSING"
		case c.ack == "ack":
			observed += ", ACKed"
		}
		if c.cp == "CP13" {
			ok = ok && size(db) < sizeBeforeCrash
			observed += fmt.Sprintf(", %d->%dB", sizeBeforeCrash, size(db))
		}
		if !wasKilled {
			ok = false
			observed = "process was NOT killed at " + c.cp + " (" + observed + ")"
		}
		if openErr != nil {
			ok = false
		}
		verdict := "PASS"
		if !ok {
			verdict = "FAIL (expected: " + c.expect + ")"
			failed++
		}
		fmt.Printf("%-5s %-40s %-9s %-44s %-34s %-52s %s\n", c.cp, c.where, c.covers, trimTo(onDisk, 44), trimTo(recovery, 34), observed, verdict)
	}
	fmt.Println(strings.Repeat("-", 198))
	if failed > 0 {
		fmt.Println("RESULT: FAIL")
		return fmt.Errorf("%d of %d crash-point cases did not behave as documented", failed, len(cases))
	}
	fmt.Printf("RESULT: PASS %d/%d cases at all 14 crash points behave exactly as BREAKING.md says (%.1fs)\n", len(cases), len(cases), time.Since(t0).Seconds())
	return nil
}

func trimTo(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// firstLine returns the verdict line of a kvdb command; the indented lines
// after it are human-readable details.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
