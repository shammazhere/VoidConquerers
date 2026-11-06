package tests

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestE2ESmoke(t *testing.T) {
	// Root dir is parent of tests/
	rootDir, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve root dir: %v", err)
	}

	// 1. Build binaries
	cmdBuild := exec.Command("go", "build", "-o", rootDir, "./cmd/kvdb", "./cmd/torture")
	cmdBuild.Dir = rootDir
	if out, err := cmdBuild.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\nOutput: %s", err, string(out))
	}

	kvdbBin := filepath.Join(rootDir, "kvdb")
	tortureBin := filepath.Join(rootDir, "torture")

	// 2. Test CLI basic operations
	tempDB := filepath.Join(t.TempDir(), "smoke.kv")
	out, err := exec.Command(kvdbBin, "open", tempDB).CombinedOutput()
	if err != nil {
		t.Fatalf("kvdb open failed: %v\nOutput: %s", err, string(out))
	}

	out, err = exec.Command(kvdbBin, "put", tempDB, "foo", "bar").CombinedOutput()
	if err != nil {
		t.Fatalf("kvdb put failed: %v\nOutput: %s", err, string(out))
	}

	out, err = exec.Command(kvdbBin, "get", tempDB, "foo").CombinedOutput()
	if err != nil || string(out) != "bar\n" {
		t.Fatalf("kvdb get failed: %v\nOutput: %s", err, string(out))
	}

	out, err = exec.Command(kvdbBin, "check", tempDB).CombinedOutput()
	if err != nil {
		t.Fatalf("kvdb check failed: %v\nOutput: %s", err, string(out))
	}

	// 3. Smoke run torture tool in default mode
	tortureWork := filepath.Join(t.TempDir(), "torture-work")
	cmdTorture := exec.Command(tortureBin, "--clean", "--iterations", "3", "--seed", "42", "--mode", "default", "--work-dir", tortureWork, "--kvdb-bin", kvdbBin)
	cmdTorture.Dir = rootDir
	out, err = cmdTorture.CombinedOutput()
	if err != nil {
		t.Fatalf("torture smoke run failed: %v\nOutput: %s", err, string(out))
	}
}
