package kvdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestCrashPoints proves every row of the BREAKING.md crash-point table with
// a REAL process kill at the EXACT line.
//
// For each crash point the parent prepares a database, then re-runs this
// test binary as a child (TestCrashHelper) with Options.CrashAt set. The child
// starts the operation and kills itself (SIGKILL / TerminateProcess) when it
// reaches that line. The parent then opens the file and checks that open()
// succeeds and the state is exactly the one BREAKING.md promises.
func TestCrashPoints(t *testing.T) {
	big := bytes.Repeat([]byte{'v'}, 200<<10) // 3 x 200 KiB > one 256 KiB write piece, so CP2 is reached

	type want struct {
		has    map[string]string // keys that must be present with these values
		absent []string          // keys that must be absent
		noTmp  bool              // db.kv.tmp must be gone after the reopen
	}
	base := map[string]string{"a": "1", "b": "2"}
	cases := []struct {
		cp       string
		scenario string
		want     want
	}{
		{"CP1", "put", want{has: base, absent: []string{"c"}}},
		{"CP2", "bigbatch", want{has: base, absent: []string{"big1", "big2", "big3"}}},
		{"CP3", "put", want{has: base, absent: []string{"c"}}},
		{"CP4", "put", want{has: base, absent: []string{"c"}}},
		{"CP5", "put", want{has: merge(base, "c", "3")}},
		{"CP6", "put", want{has: merge(base, "c", "3")}},
		{"CP7", "put", want{has: merge(base, "c", "3")}},
		{"CP8", "open-with-tmp", want{has: base, noTmp: true}},
		{"CP9", "open-with-torn-tail", want{has: base, absent: []string{"torn"}}},
		{"CP10", "open-with-torn-tail", want{has: base, absent: []string{"torn"}}},
		{"CP11", "compact", want{has: base, noTmp: true}},
		{"CP12", "compact", want{has: base, noTmp: true}},
		{"CP13", "compact", want{has: base, noTmp: true}},
		{"CP14", "create", want{noTmp: true}},
	}
	for _, tc := range cases {
		t.Run(tc.cp, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "db.kv")
			committedEnd := prepare(t, path, tc.scenario, big)

			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
			cmd.Env = append(os.Environ(), "KIPI_CRASH_HELPER=1", "KIPI_DB="+path, "KIPI_CP="+tc.cp, "KIPI_SCENARIO="+tc.scenario)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			if !errors.As(err, &ee) {
				t.Fatalf("child was not killed (err=%v): %s", err, out)
			}
			// Killed: -1 (signal) on Linux/macOS, 1 (TerminateProcess) on Windows.
			// 3 = crash point never reached, 4 = setup error in the child.
			if code := ee.ExitCode(); code == 3 || code == 4 {
				t.Fatalf("child exited %d instead of dying at %s: %s", code, tc.cp, out)
			}

			db, err := Open(path, Options{})
			if err != nil {
				t.Fatalf("open after crash at %s failed: %v", tc.cp, err)
			}
			defer db.Close()
			for k, v := range tc.want.has {
				got, ok, err := db.Get([]byte(k))
				if err != nil || !ok || string(got) != v {
					t.Fatalf("after %s: key %q = %q,%v,%v; want %q", tc.cp, k, got, ok, err, v)
				}
			}
			for _, k := range tc.want.absent {
				if _, ok, _ := db.Get([]byte(k)); ok {
					t.Fatalf("after %s: key %q must be absent", tc.cp, k)
				}
			}
			if tc.want.noTmp {
				if _, err := os.Stat(path + ".tmp"); err == nil {
					t.Fatalf("after %s: db.kv.tmp still exists", tc.cp)
				}
			}
			if committedEnd > 0 && len(tc.want.absent) > 0 && db.fileSize != committedEnd {
				t.Fatalf("after %s: file not truncated back to %d (is %d)", tc.cp, committedEnd, db.fileSize)
			}
			if rep := Check(path); !rep.OK || rep.TailBytes != 0 {
				t.Fatalf("after %s: check: %+v", tc.cp, rep)
			}
		})
	}
}

func merge(m map[string]string, k, v string) map[string]string {
	out := map[string]string{k: v}
	for a, b := range m {
		out[a] = b
	}
	return out
}

// prepare builds the state the child starts from and returns the committed
// end of the file (0 when not meaningful).
func prepare(t *testing.T, path, scenario string, big []byte) int64 {
	t.Helper()
	if scenario == "create" {
		return 0 // the child creates the file
	}
	db := mustOpen(t, path, Options{CompactMinBytes: -1})
	if scenario == "compact" {
		for i := 0; i < 50; i++ { // dead records for compaction to remove
			db.Put([]byte("a"), []byte(fmt.Sprintf("old%d", i)))
		}
	}
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	end := db.fileSize
	db.Close()
	switch scenario {
	case "open-with-tmp":
		os.WriteFile(path+".tmp", []byte("KIPI half-written compaction output"), 0o644)
	case "open-with-torn-tail":
		var torn []byte
		torn = appendRecord(torn, FlagPut|FlagBatch, []byte("torn"), []byte("never committed"))
		f, _ := os.OpenFile(path, os.O_WRONLY, 0)
		f.WriteAt(torn[:len(torn)-3], end) // record cut short, no marker
		f.Close()
	}
	return end
}

// TestCrashHelper is the child process. It is skipped in a normal test run.
func TestCrashHelper(t *testing.T) {
	if os.Getenv("KIPI_CRASH_HELPER") == "" {
		t.Skip("helper process for TestCrashPoints")
	}
	path, cp, scenario := os.Getenv("KIPI_DB"), os.Getenv("KIPI_CP"), os.Getenv("KIPI_SCENARIO")
	opts := Options{CrashAt: cp, CompactMinBytes: -1}
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "helper setup error:", err)
		os.Exit(4)
	}
	db, err := Open(path, opts) // CP8, CP9, CP10, CP14 fire in here
	if err != nil {
		fail(err)
	}
	switch scenario {
	case "put":
		err = db.Apply([]Op{{Key: []byte("c"), Value: []byte("3")}}, func() {})
	case "bigbatch":
		big := bytes.Repeat([]byte{'v'}, 200<<10)
		err = db.Batch([]Op{{Key: []byte("big1"), Value: big}, {Key: []byte("big2"), Value: big}, {Key: []byte("big3"), Value: big}})
	case "compact":
		err = db.Compact()
	}
	if err != nil {
		fail(err)
	}
	fmt.Fprintln(os.Stderr, "crash point", cp, "was never reached")
	os.Exit(3)
}

// TestCorruptionIsNotTruncated: a record that is complete but damaged (bit
// rot, not a kill) in the middle of committed data must make open() refuse,
// not silently cut off the committed records after it.
func TestCorruptionIsNotTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	db.Put([]byte("a"), []byte("first"))
	bOff := db.fileSize
	db.Put([]byte("b"), []byte("second"))
	db.Put([]byte("c"), []byte("third"))
	db.Close()
	size := fileSize(t, path)

	data, _ := os.ReadFile(path)
	data[bOff+RecHdrSize+1] ^= 0xFF // flip a byte inside b's value
	os.WriteFile(path, data, 0o644)

	if _, err := Open(path, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want CORRUPT, got %v", err)
	}
	if fileSize(t, path) != size {
		t.Fatal("open truncated a corrupted file")
	}
	if rep := Check(path); rep.OK {
		t.Fatal("check said OK on a corrupted file")
	}
}
