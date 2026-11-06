// kvdb_test.go — Unit tests for the KIPI engine.

package kvdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempDB(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "kvdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test.kv")
	return path, func() { os.RemoveAll(dir) }
}

func openDB(t *testing.T, path string, opts Options) *DB {
	t.Helper()
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// TestRoundTrip: basic put/get/delete.
func TestRoundTrip(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	db := openDB(t, path, Options{})
	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
	if err := db.Delete([]byte("hello")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = db.Get([]byte("hello"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	db.Close()
}

// TestTornTailAtEveryByte: truncate the file at every possible byte offset
// and verify that Open always succeeds.
func TestTornTailAtEveryByte(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	// Write some data
	db := openDB(t, path, Options{})
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		if err := db.Put([]byte(key), []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	// Read the file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for truncAt := FileHeaderSize; truncAt <= len(data); truncAt++ {
		truncPath := path + fmt.Sprintf(".trunc%d", truncAt)
		if err := os.WriteFile(truncPath, data[:truncAt], 0644); err != nil {
			t.Fatal(err)
		}
		db2, err := Open(truncPath, Options{})
		if err != nil {
			t.Fatalf("Open at truncation %d: %v", truncAt, err)
		}
		db2.Close()
		os.Remove(truncPath)
		os.Remove(truncPath + ".lock")
	}
}

// TestUncommittedBatch: a batch that is not committed should not appear after recovery.
func TestUncommittedBatch(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	// Write a committed record
	db := openDB(t, path, Options{})
	if err := db.Put([]byte("committed"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Manually append an uncommitted record (no marker)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	torn := appendRecord(nil, typeRecord, []byte("uncommitted"), []byte("should-not-appear"))
	f.Write(torn)
	f.Close()

	// Reopen: should see only 'committed'
	db2 := openDB(t, path, Options{})
	defer db2.Close()

	val, err := db2.Get([]byte("committed"))
	if err != nil || string(val) != "yes" {
		t.Fatalf("committed key missing or wrong: %v %q", err, val)
	}
	_, err = db2.Get([]byte("uncommitted"))
	if err != ErrNotFound {
		t.Fatalf("uncommitted key appeared: %v", err)
	}
}

// TestOrdering: ForEachSorted returns keys in lexicographic order.
func TestOrdering(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	db := openDB(t, path, Options{})
	defer db.Close()

	keys := []string{"zzz", "aaa", "mmm", "bbb"}
	for _, k := range keys {
		if err := db.Put([]byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	db.ForEachSorted(func(key, val []byte) error {
		got = append(got, string(key))
		return nil
	})

	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("not sorted: %v", got)
		}
	}
}

// TestCompaction: after compaction, dead records are removed and live ones accessible.
func TestCompaction(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	db := openDB(t, path, Options{})
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	// Delete half
	for i := 0; i < 50; i++ {
		db.Delete([]byte(fmt.Sprintf("k%d", i)))
	}

	sizeBefore := db.fileSize
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	sizeAfter := db.fileSize
	if sizeAfter >= sizeBefore {
		t.Fatalf("file did not shrink: before=%d after=%d", sizeBefore, sizeAfter)
	}

	// Check surviving keys
	for i := 50; i < 100; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("key k%d missing after compaction: %v", i, err)
		}
		expected := fmt.Sprintf("v%d", i)
		if string(val) != expected {
			t.Fatalf("key k%d: expected %q got %q", i, expected, val)
		}
	}
	db.Close()
}

// TestDiskFull: verify that writes fail cleanly when MaxBytes is reached.
func TestDiskFull(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	// Open with tiny max
	db := openDB(t, path, Options{MaxBytes: FileHeaderSize + 100})
	err := db.Put([]byte("bigkey"), bytes.Repeat([]byte("x"), 200))
	if err != ErrDiskFull {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}
	// DB must still be readable
	_, readErr := db.Get([]byte("bigkey"))
	if readErr != ErrNotFound {
		t.Fatalf("expected ErrNotFound after disk-full, got %v", readErr)
	}
	db.Close()
}

// TestLock: opening same file twice must fail on the second open.
func TestLock(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	db1 := openDB(t, path, Options{})
	defer db1.Close()

	_, err := Open(path, Options{})
	if err == nil {
		t.Fatal("expected lock error on second open, got nil")
	}
}

// TestConcurrency: two goroutines writing different keys simultaneously.
func TestConcurrency(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	db := openDB(t, path, Options{})
	defer db.Close()

	const n = 100
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("w%d-k%d", writerID, i)
				val := fmt.Sprintf("v%d", i)
				if err := db.Put([]byte(key), []byte(val)); err != nil {
					t.Errorf("writer %d: Put %s: %v", writerID, key, err)
				}
			}
		}(w)
	}
	wg.Wait()

	// Verify all keys present
	for w := 0; w < 2; w++ {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("w%d-k%d", w, i)
			val, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("missing key %s: %v", key, err)
			}
			expected := fmt.Sprintf("v%d", i)
			if string(val) != expected {
				t.Fatalf("key %s: expected %q got %q", key, expected, val)
			}
		}
	}
}

// TestConcurrentReaders: get during write sees consistent state.
func TestConcurrentReaders(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	db := openDB(t, path, Options{})
	defer db.Close()

	// Write initial value
	db.Put([]byte("x"), []byte("initial"))

	done := make(chan struct{})
	var readErr error

	// Reader goroutine
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			val, err := db.Get([]byte("x"))
			if err == ErrNotFound {
				continue // key may be briefly missing during batch
			}
			if err != nil {
				readErr = err
				return
			}
			s := string(val)
			if s != "initial" && s != "updated" {
				readErr = fmt.Errorf("torn read: got %q", s)
				return
			}
		}
	}()

	// Writer
	for i := 0; i < 100; i++ {
		db.Put([]byte("x"), []byte("updated"))
		db.Put([]byte("x"), []byte("initial"))
	}
	<-done

	if readErr != nil {
		t.Fatal(readErr)
	}
}

