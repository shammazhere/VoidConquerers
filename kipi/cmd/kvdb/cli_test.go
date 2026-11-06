package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kipi/src/kvdb"
)

func invoke(t *testing.T, args []string, input string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, strings.NewReader(input), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestAllCommandsAndEmptyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.kv")

	code, out, errOut := invoke(t, []string{"open", path}, "")
	if code != 0 || !strings.Contains(out, "0 keys, recovery in ") || errOut != "" {
		t.Fatalf("open: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	code, out, errOut = invoke(t, []string{"get", path, "missing"}, "")
	if code != 0 || out != "NOTFOUND\n" || errOut != "" {
		t.Fatalf("get missing: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	code, out, errOut = invoke(t, []string{"check", path}, "")
	if code != 0 || out != "OK\n" || errOut != "" {
		t.Fatalf("check empty db: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	code, out, errOut = invoke(t, []string{"dump", path}, "")
	if code != 0 || out != "" || errOut != "" {
		t.Fatalf("dump empty db: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	code, out, errOut = invoke(t, []string{"put", path, "z key", "value with spaces"}, "")
	if code != 0 || out != "OK\n" || errOut != "" {
		t.Fatalf("put: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = invoke(t, []string{"put", path, "a", "first"}, "")
	if code != 0 || out != "OK\n" || errOut != "" {
		t.Fatalf("second put: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = invoke(t, []string{"get", path, "z key"}, "")
	if code != 0 || out != "value with spaces\n" || errOut != "" {
		t.Fatalf("get: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = invoke(t, []string{"dump", path}, "")
	if code != 0 || out != "a\tfirst\nz key\tvalue with spaces\n" || errOut != "" {
		t.Fatalf("dump: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	code, out, errOut = invoke(t, []string{"del", path, "z key"}, "")
	if code != 0 || out != "OK\n" || errOut != "" {
		t.Fatalf("del: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	code, out, errOut = invoke(t, []string{"get", path, "z key"}, "")
	if code != 0 || out != "NOTFOUND\n" || errOut != "" {
		t.Fatalf("get after del: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
}

func TestLargeValueRepeatedOpenAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.kv")
	if code, _, stderr := invoke(t, []string{"open", path}, ""); code != 0 {
		t.Fatalf("initial open: code=%d stderr=%s", code, stderr)
	}
	value := strings.Repeat("x y", kvdb.MaxValLen/3)
	if code, out, stderr := invoke(t, []string{"put", path, "large", value}, ""); code != 0 || out != "OK\n" || stderr != "" {
		t.Fatalf("large put: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
	for i := 0; i < 3; i++ {
		if code, _, stderr := invoke(t, []string{"open", path}, ""); code != 0 || stderr != "" {
			t.Fatalf("reopen %d: code=%d stderr=%q", i, code, stderr)
		}
	}
	if code, out, stderr := invoke(t, []string{"get", path, "large"}, ""); code != 0 || out != value+"\n" || stderr != "" {
		t.Fatalf("large get: code=%d value length=%d stderr=%q", code, len(out), stderr)
	}
}

func TestBatchAtomicityAndMaximumSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.kv")
	invoke(t, []string{"open", path}, "")

	invalidFile := filepath.Join(t.TempDir(), "invalid.batch")
	if err := os.WriteFile(invalidFile, []byte("put must-not-apply value\nnot-an-operation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if code, out, stderr := invoke(t, []string{"batch", path, invalidFile}, ""); code == 0 || out != "" || stderr == "" {
		t.Fatalf("invalid batch: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Get([]byte("must-not-apply"))
	if err != kvdb.ErrNotFound {
		db.Close()
		t.Fatalf("invalid batch partially applied: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	batchPath := filepath.Join(t.TempDir(), "large.batch")
	file, err := os.Create(batchPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < kvdb.MaxBatchOps; i++ {
		if _, err := fmt.Fprintf(file, "put key-%05d value-%05d\n", i, i); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if code, out, stderr := invoke(t, []string{"batch", path, batchPath}, ""); code != 0 || out != fmt.Sprintf("OK: applied %d operations\n", kvdb.MaxBatchOps) || stderr != "" {
		t.Fatalf("10k batch: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
	db, err = kvdb.Open(path, kvdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := db.KeyCount(); got != kvdb.MaxBatchOps {
		t.Fatalf("expected %d batch keys, got %d", kvdb.MaxBatchOps, got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServeTortureProtocolAndDurableAcknowledgements(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serve.kv")
	requests := "op-1 put first hello world\n" +
		"op-2 get first\n" +
		"batch-1 batch 2\n" +
		"put second second value\n" +
		"del first\n" +
		"dump-1 dump\n" +
		"op-3 get first\n" +
		"exit\n"
	code, out, stderr := invoke(t, []string{"serve", path}, requests)
	expected := "ACK op-1\n" +
		"VAL op-2 hello world\n" +
		"ACK batch-1\n" +
		"KV dump-1 second second value\n" +
		"ACK dump-1\n" +
		"NOTFOUND op-3\n"
	if code != 0 || out != expected || stderr != "" {
		t.Fatalf("serve: code=%d stdout=%q stderr=%q", code, out, stderr)
	}

	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := db.Get([]byte("second"))
	if err != nil || string(value) != "second value" {
		db.Close()
		t.Fatalf("ACKed batch did not persist: value=%q err=%v", value, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServeRejectsIncompleteBatchWithoutAcknowledging(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incomplete.kv")
	code, out, stderr := invoke(t, []string{"serve", path}, "broken batch 2\nput key value\n")
	if code == 0 || !strings.Contains(out, "ERR broken incomplete batch input\n") || stderr == "" || strings.Contains(out, "ACK broken") {
		t.Fatalf("incomplete batch: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Get([]byte("key"))
	if err != kvdb.ErrNotFound {
		db.Close()
		t.Fatalf("incomplete batch applied a partial write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidArgumentsAndEngineFailures(t *testing.T) {
	if code, _, stderr := invoke(t, []string{"unknown"}, ""); code == 0 || stderr == "" {
		t.Fatalf("unknown command: code=%d stderr=%q", code, stderr)
	}
	if code, _, stderr := invoke(t, []string{"open", filepath.Join(t.TempDir(), "x"), "--max-bytes", "bad"}, ""); code == 0 || stderr == "" {
		t.Fatalf("invalid max bytes: code=%d stderr=%q", code, stderr)
	}

	path := filepath.Join(t.TempDir(), "limited.kv")
	if code, out, stderr := invoke(t, []string{"serve", path, "--max-bytes", "16"}, "full put key value\nexit\n"); code != 0 || out != "ERR full kvdb: max file size reached\n" || stderr != "" {
		t.Fatalf("disk-full serve write: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
	if code, out, stderr := invoke(t, []string{"put", path, strings.Repeat("k", kvdb.MaxKeyLen+1), "value"}, ""); code == 0 || out != "" || stderr == "" {
		t.Fatalf("invalid key put: code=%d stdout=%q stderr=%q", code, out, stderr)
	}

	corruptPath := filepath.Join(t.TempDir(), "corrupt.kv")
	if err := os.WriteFile(corruptPath, []byte("not a KIPI database"), 0600); err != nil {
		t.Fatal(err)
	}
	if code, out, stderr := invoke(t, []string{"check", corruptPath}, ""); code == 0 || out == "" || stderr == "" {
		t.Fatalf("corrupt check: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
}
