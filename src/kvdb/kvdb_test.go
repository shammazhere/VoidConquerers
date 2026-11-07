package kvdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mustOpen(t *testing.T, path string, opts Options) *DB {
	t.Helper()
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return db
}

func snapshot(t *testing.T, db *DB) map[string]string {
	t.Helper()
	m := map[string]string{}
	if err := db.ForEachSorted(func(k, v []byte) error { m[string(k)] = string(v); return nil }); err != nil {
		t.Fatalf("ForEachSorted: %v", err)
	}
	return m
}

func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func copyPrefix(t *testing.T, src, dst string, n int64) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data[:n], 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	big := bytes.Repeat([]byte{0xAB}, MaxValLen)
	binKey := []byte{0, 1, 2, 0xff}
	longKey := bytes.Repeat([]byte("k"), MaxKeyLen)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(db.Put([]byte("a"), []byte("1")))
	must(db.Put([]byte("empty"), []byte{}))
	must(db.Put([]byte("big"), big))
	must(db.Put(binKey, []byte("bin")))
	must(db.Put(longKey, []byte("long")))
	must(db.Put([]byte("a"), []byte("2"))) // overwrite
	must(db.Delete([]byte("missing")))     // not an error
	must(db.Batch([]Op{{Key: []byte("b"), Value: []byte("x")}, {Del: true, Key: []byte("empty")}}))

	check := func(db *DB) {
		t.Helper()
		if v, ok, _ := db.Get([]byte("a")); !ok || string(v) != "2" {
			t.Fatalf("a = %q %v", v, ok)
		}
		if _, ok, _ := db.Get([]byte("empty")); ok {
			t.Fatal("empty should be deleted")
		}
		if v, ok, _ := db.Get([]byte("big")); !ok || !bytes.Equal(v, big) {
			t.Fatal("big value mismatch")
		}
		if v, ok, _ := db.Get(binKey); !ok || string(v) != "bin" {
			t.Fatal("binary key mismatch")
		}
		if v, ok, _ := db.Get(longKey); !ok || string(v) != "long" {
			t.Fatal("1024-byte key mismatch")
		}
	}
	check(db)
	db.Close()
	db = mustOpen(t, path, Options{})
	defer db.Close()
	check(db)
	if rep := Check(path); !rep.OK || rep.TailBytes != 0 {
		t.Fatalf("check: %+v", rep)
	}
}

func TestValidation(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.kv"), Options{})
	defer db.Close()
	bad := [][]Op{
		nil,
		{{Key: nil, Value: []byte("x")}},
		{{Key: bytes.Repeat([]byte("k"), MaxKeyLen+1)}},
		{{Key: []byte("k"), Value: make([]byte, MaxValLen+1)}},
		make([]Op, MaxBatchOps+1),
	}
	for i, ops := range bad {
		if err := db.Batch(ops); !errors.Is(err, ErrBadRequest) {
			t.Errorf("case %d: want BAD_REQUEST, got %v", i, err)
		}
	}
}

// TestTornTailEveryOffset builds a committed state, then appends one more
// commit (a single put, a 1 MB put, a batch, a delete). For EVERY possible
// cut point inside the new commit it checks that open() succeeds, the state
// equals the state before the commit, and the file is truncated back to the
// last marker. Cutting at the very end must show the new commit. (A1-A7, B1-B3, C2)
func TestTornTailEveryOffset(t *testing.T) {
	cases := []struct {
		name string
		ops  []Op
	}{
		{"put", []Op{{Key: []byte("k2"), Value: []byte("new-value")}}},
		{"overwrite", []Op{{Key: []byte("k1"), Value: []byte("v1-overwritten")}}},
		{"delete", []Op{{Del: true, Key: []byte("k1")}}},
		{"batch", []Op{
			{Key: []byte("b1"), Value: []byte("one")},
			{Key: []byte("b2"), Value: []byte("two")},
			{Del: true, Key: []byte("k1")},
			{Key: []byte("b1"), Value: []byte("one-again")},
		}},
		{"1MB", []Op{{Key: []byte("big"), Value: bytes.Repeat([]byte{7}, MaxValLen)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "db.kv")
			db := mustOpen(t, path, Options{})
			db.Put([]byte("k1"), []byte("v1"))
			db.Put([]byte("k0"), []byte("v0"))
			before := snapshot(t, db)
			sizeBefore := db.fileSize
			if err := db.Batch(tc.ops); err != nil {
				t.Fatal(err)
			}
			after := snapshot(t, db)
			sizeAfter := db.fileSize
			db.Close()

			step := int64(1)
			if sizeAfter-sizeBefore > 4096 {
				step = 997 // 1 MB case: every byte would take too long; sample densely plus edges below
			}
			cuts := []int64{}
			for c := sizeBefore; c < sizeAfter; c += step {
				cuts = append(cuts, c)
			}
			// always include the interesting edges
			for _, c := range []int64{sizeBefore + 1, sizeBefore + RecHdrSize - 1, sizeBefore + RecHdrSize, sizeAfter - RecHdrSize - 1, sizeAfter - RecHdrSize, sizeAfter - RecHdrSize + 1, sizeAfter - 5, sizeAfter - 4, sizeAfter - 1} {
				if c > sizeBefore && c < sizeAfter {
					cuts = append(cuts, c)
				}
			}
			cut := filepath.Join(dir, "cut.kv")
			for _, c := range cuts {
				copyPrefix(t, path, cut, c)
				os.Remove(cut + ".lock")
				// A cut inside the marker's last 4 bytes (constants) keeps the
				// commit: open() rebuilds the marker. Any earlier cut loses it.
				want, wantSize := before, sizeBefore
				if c >= sizeAfter-4 {
					want, wantSize = after, sizeAfter
				}
				d := mustOpen(t, cut, Options{})
				got := snapshot(t, d)
				fs := d.fileSize
				d.Close()
				if !equalMaps(got, want) {
					t.Fatalf("cut at %d (of %d..%d): state %v, want %v", c, sizeBefore, sizeAfter, got, want)
				}
				if fs != wantSize || fileSize(t, cut) != wantSize {
					t.Fatalf("cut at %d: file is %d bytes after open, want %d", c, fs, wantSize)
				}
				// open again: idempotent (C2)
				d = mustOpen(t, cut, Options{})
				if rs := d.RecoveryStats(); !equalMaps(snapshot(t, d), want) || rs.TruncatedBytes != 0 || rs.MarkerRestored != 0 {
					t.Fatalf("second open after cut %d is not idempotent", c)
				}
				d.Close()
			}
			copyPrefix(t, path, cut, sizeAfter)
			d := mustOpen(t, cut, Options{})
			if !equalMaps(snapshot(t, d), after) {
				t.Fatal("full file does not show the committed batch")
			}
			d.Close()
		})
	}
}

// TestUncommittedBatchDiscarded writes batch records by hand with no marker,
// or with a marker whose count is wrong. (B1, B2)
func TestUncommittedBatchDiscarded(t *testing.T) {
	for _, mode := range []string{"no-marker", "wrong-count", "garbage-after"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db.kv")
			db := mustOpen(t, path, Options{})
			db.Put([]byte("keep"), []byte("yes"))
			end := db.fileSize
			db.Close()

			var buf []byte
			for i := 0; i < 10; i++ {
				buf = appendRecord(buf, FlagPut|FlagBatch, []byte(fmt.Sprintf("b%d", i)), []byte("v"))
			}
			switch mode {
			case "wrong-count":
				buf = append(buf, encodeMarker(9)...)
			case "garbage-after":
				buf = append(buf, encodeMarker(10)...)
				buf = append(buf, bytes.Repeat([]byte{0}, 100)...) // zero-filled tail
			}
			f, _ := os.OpenFile(path, os.O_WRONLY, 0)
			f.WriteAt(buf, end)
			f.Close()

			db = mustOpen(t, path, Options{})
			defer db.Close()
			got := snapshot(t, db)
			if mode == "garbage-after" {
				if len(got) != 11 {
					t.Fatalf("committed batch lost: %v", got)
				}
				return
			}
			if len(got) != 1 || got["keep"] != "yes" {
				t.Fatalf("uncommitted batch visible: %v", got)
			}
			if fileSize(t, path) != end {
				t.Fatalf("not truncated to %d", end)
			}
		})
	}
}

// TestTruncateOneByte is scenario F4: cut bytes off the end by hand.
// The file always ends in a commit marker whose last 4 bytes are constants,
// so cutting 1-4 bytes loses nothing: open() rebuilds the marker, checks its
// CRC and appends the missing bytes. Cutting more (the marker's count or CRC)
// loses the last commit, and only that one.
func TestTruncateOneByte(t *testing.T) {
	for cut := int64(1); cut <= 6; cut++ {
		path := filepath.Join(t.TempDir(), "db.kv")
		db := mustOpen(t, path, Options{})
		db.Put([]byte("a"), []byte("1"))
		db.Put([]byte("b"), []byte("2"))
		db.Close()
		full := fileSize(t, path)
		if err := os.Truncate(path, full-cut); err != nil {
			t.Fatal(err)
		}
		if rep := Check(path); !rep.OK {
			t.Fatalf("cut %d: check: %+v", cut, rep)
		}
		db = mustOpen(t, path, Options{})
		got := snapshot(t, db)
		rs := db.RecoveryStats()
		db.Close()
		if cut <= 4 {
			if !equalMaps(got, map[string]string{"a": "1", "b": "2"}) || rs.MarkerRestored != int(cut) || fileSize(t, path) != full {
				t.Fatalf("cut %d: got %v, restored %d, size %d (want %d)", cut, got, rs.MarkerRestored, fileSize(t, path), full)
			}
		} else if !equalMaps(got, map[string]string{"a": "1"}) {
			t.Fatalf("cut %d: got %v, want only the last commit lost", cut, got)
		}
		// A second open finds a clean file.
		db = mustOpen(t, path, Options{})
		if rs := db.RecoveryStats(); rs.MarkerRestored != 0 || rs.TruncatedBytes != 0 {
			t.Fatalf("cut %d: second open still repairing: %+v", cut, rs)
		}
		db.Close()
	}
}

func TestBadHeaderIsHardError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	os.WriteFile(path, []byte("this is not a kipi database file"), 0o644)
	if _, err := Open(path, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want CORRUPT, got %v", err)
	}
}

// TestBatchOrdering is scenario B5.
func TestBatchOrdering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	db.Batch([]Op{{Key: []byte("x"), Value: []byte("1")}, {Del: true, Key: []byte("x")}})
	db.Batch([]Op{{Del: true, Key: []byte("y")}, {Key: []byte("y"), Value: []byte("2")}})
	db.Batch([]Op{{Key: []byte("z"), Value: []byte("a")}, {Key: []byte("z"), Value: []byte("b")}})
	want := map[string]string{"y": "2", "z": "b"}
	if got := snapshot(t, db); !equalMaps(got, want) {
		t.Fatalf("live: %v", got)
	}
	db.Close()
	db = mustOpen(t, path, Options{})
	defer db.Close()
	if got := snapshot(t, db); !equalMaps(got, want) {
		t.Fatalf("after reopen: %v", got)
	}
	// liveBytes must match a recount
	var sum int64
	for _, e := range db.index {
		sum += int64(e.recSize)
	}
	if sum != db.liveBytes {
		t.Fatalf("liveBytes %d != recount %d", db.liveBytes, sum)
	}
}

// TestCompactionShrinks: many puts then deletes, compaction shrinks the file
// to header + one marker; a leftover db.tmp is removed by the next open (C3, C5).
func TestCompactionShrinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{CompactMinBytes: -1})
	for i := 0; i < 2000; i++ {
		ops := make([]Op, 0, 10)
		for j := 0; j < 10; j++ {
			ops = append(ops, Op{Key: []byte(fmt.Sprintf("k%06d", i*10+j)), Value: bytes.Repeat([]byte("v"), 50)})
		}
		db.Batch(ops)
	}
	db.Put([]byte("survivor"), []byte("still here"))
	for i := 0; i < 2000; i++ {
		ops := make([]Op, 0, 10)
		for j := 0; j < 10; j++ {
			ops = append(ops, Op{Del: true, Key: []byte(fmt.Sprintf("k%06d", i*10+j))})
		}
		db.Batch(ops)
	}
	big := fileSize(t, path)
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	small := fileSize(t, path)
	want := int64(HeaderSize + RecHdrSize + len("survivor") + len("still here") + RecHdrSize)
	if small != want {
		t.Fatalf("after compaction %d bytes, want %d (before %d)", small, want, big)
	}
	// writes after compaction go to the new file
	db.Put([]byte("after"), []byte("compaction"))
	db.Close()

	// leave a stale, half-written temp file as a killed compaction would
	os.WriteFile(path+".tmp", []byte("KIPI partial garbage"), 0o644)
	db = mustOpen(t, path, Options{})
	defer db.Close()
	if !db.RecoveryStats().TmpRemoved {
		t.Fatal("stale tmp not reported removed")
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatal("stale tmp still exists")
	}
	if got := snapshot(t, db); !equalMaps(got, map[string]string{"survivor": "still here", "after": "compaction"}) {
		t.Fatalf("got %v", got)
	}
}

func TestAutoCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{CompactMinBytes: 4096})
	defer db.Close()
	for i := 0; i < 1000; i++ {
		db.Put([]byte("same"), bytes.Repeat([]byte("x"), 100))
	}
	if s := fileSize(t, path); s > 3*4096 {
		t.Fatalf("file grew to %d, auto-compaction did not run", s)
	}
}

// TestDeleteEverythingShrinks: with default options (no explicit Compact),
// deleting every key must shrink the file to the 28-byte minimum, and the
// file must already have shrunk partway through the deletes.
func TestDeleteEverythingShrinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	const n, per = 50000, 5000
	val := bytes.Repeat([]byte("v"), 40)
	for b := 0; b < n/per; b++ {
		ops := make([]Op, 0, per)
		for i := b * per; i < (b+1)*per; i++ {
			ops = append(ops, Op{Key: []byte(fmt.Sprintf("key%07d", i)), Value: val})
		}
		if err := db.Batch(ops); err != nil {
			t.Fatal(err)
		}
	}
	full := fileSize(t, path)
	peak := full
	for b := 0; b < n/per; b++ {
		ops := make([]Op, 0, per)
		for i := b * per; i < (b+1)*per; i++ {
			ops = append(ops, Op{Type: OpDelete, Key: []byte(fmt.Sprintf("key%07d", i))})
		}
		if err := db.Batch(ops); err != nil {
			t.Fatal(err)
		}
		if s := fileSize(t, path); s > peak {
			peak = s
		}
	}
	if s := fileSize(t, path); s != HeaderSize+RecHdrSize {
		t.Fatalf("after deleting all %d keys the file is %d bytes (was %d), want %d", n, s, full, HeaderSize+RecHdrSize)
	}
	if peak > 2*full {
		t.Fatalf("file grew to %d bytes during the deletes (start %d): auto-compaction did not keep up", peak, full)
	}
	db.Close()
	db = mustOpen(t, path, Options{})
	defer db.Close()
	if db.KeyCount() != 0 {
		t.Fatalf("reopen: %d keys, want 0", db.KeyCount())
	}
}

// TestRejectedTailIsSavedNotApplied: an uncommitted tail is cut from the
// database, a copy is kept for inspection, the copy is never applied, a
// repeated recovery of the same tail does not duplicate it, and at most
// KeepRejected copies are kept.
func TestRejectedTailIsSavedNotApplied(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	if err := db.Put([]byte("city"), []byte("Delhi")); err != nil {
		t.Fatal(err)
	}
	db.Close()
	committed := fileSize(t, path)
	tail := appendRecord(nil, FlagPut|FlagBatch, []byte("name"), []byte("Ada"))
	tail = appendRecord(tail, FlagDel|FlagBatch, []byte("city"), nil) // no marker follows
	appendTail := func(b []byte) {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.Write(b)
		f.Close()
	}
	appendTail(tail)
	db = mustOpen(t, path, Options{})
	rs := db.RecoveryStats()
	if rs.RejectedFile == "" || rs.TruncatedBytes != int64(len(tail)) {
		t.Fatalf("stats %+v: want a saved copy of %d bytes", rs, len(tail))
	}
	if _, ok, err := db.Get([]byte("name")); ok || err != nil {
		t.Fatalf("uncommitted PUT was applied: %v %v", ok, err)
	}
	if v, ok, err := db.Get([]byte("city")); !ok || err != nil || string(v) != "Delhi" {
		t.Fatalf("uncommitted DEL was applied: %q %v %v", v, ok, err)
	}
	db.Close()
	if s := fileSize(t, path); s != committed {
		t.Fatalf("database is %d bytes, want %d (tail cut)", s, committed)
	}
	info, err := ReadRejected(rs.RejectedFile)
	if err != nil || len(info.Records) != 2 || info.Records[0].Kind != "PUT" || string(info.Records[0].Value) != "Ada" || info.Records[1].Kind != "DEL" || info.Offset != committed {
		t.Fatalf("rejected copy: %+v %v", info, err)
	}

	// The same tail again (as if recovery had been killed before truncating):
	// no second copy.
	appendTail(tail)
	db = mustOpen(t, path, Options{})
	db.Close()
	if files, _ := RejectedFiles(path); len(files) != 1 {
		t.Fatalf("%d copies after recovering the same tail twice, want 1", len(files))
	}

	// Different tails: only the newest KeepRejected copies stay.
	for i := 0; i < 5; i++ {
		appendTail(appendRecord(nil, FlagPut|FlagBatch, []byte(fmt.Sprintf("k%d", i)), []byte("v")))
		db = mustOpen(t, path, Options{KeepRejected: 3})
		db.Close()
	}
	if files, _ := RejectedFiles(path); len(files) != 3 {
		t.Fatalf("%d copies kept, want 3", len(files))
	}
	// Turned off: nothing saved, tail still cut.
	appendTail(tail)
	db = mustOpen(t, path, Options{KeepRejected: -1})
	if db.RecoveryStats().RejectedFile != "" || fileSize(t, path) != committed {
		t.Fatalf("KeepRejected=-1 saved a copy or did not cut")
	}
	db.Close()
}

// TestDiskFull covers E1, E2, E3, E5 at the engine level.
func TestDiskFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	const max = 8192
	db := mustOpen(t, path, Options{MaxBytes: max, CompactMinBytes: -1})
	val := bytes.Repeat([]byte("v"), 500)
	n := 0
	var err error
	for ; n < 100; n++ {
		if err = db.Put([]byte(fmt.Sprintf("k%03d", n)), val); err != nil {
			break
		}
	}
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("E1: want DISK_FULL, got %v", err)
	}
	sizeAtCap := fileSize(t, path)
	before := snapshot(t, db)
	// E2: a batch that does not fit leaves no trace
	if err := db.Batch([]Op{{Key: []byte("b1"), Value: val}, {Key: []byte("b2"), Value: val}}); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("E2: want DISK_FULL, got %v", err)
	}
	if fileSize(t, path) != sizeAtCap || !equalMaps(snapshot(t, db), before) {
		t.Fatal("E2: rejected batch changed the file")
	}
	// E3: "kill" (close without anything special) and reopen
	db.Close()
	db = mustOpen(t, path, Options{MaxBytes: max, CompactMinBytes: -1})
	if !equalMaps(snapshot(t, db), before) {
		t.Fatal("E3: data changed after reopen")
	}
	// E5: deletes still work at the cap (reserve), then puts work again
	for i := 0; i < n; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatalf("E5: delete %d at cap failed: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("n%03d", i)), val); err != nil {
			t.Fatalf("E5: put %d after deletes failed: %v", i, err)
		}
		if fileSize(t, path) > max {
			t.Fatal("file exceeded max_bytes")
		}
	}
	db.Close()
}

// TestCompactionOverCap is E4: compaction refuses when the compacted file
// would not fit, and nothing is lost.
func TestCompactionOverCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{CompactMinBytes: -1})
	for i := 0; i < 20; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), bytes.Repeat([]byte("v"), 500))
	}
	want := snapshot(t, db)
	db.Close()
	db = mustOpen(t, path, Options{MaxBytes: 1000, CompactMinBytes: -1})
	if err := db.Compact(); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("want DISK_FULL, got %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatal("tmp left behind")
	}
	db.Close()
	db = mustOpen(t, path, Options{})
	defer db.Close()
	if !equalMaps(snapshot(t, db), want) {
		t.Fatal("data lost")
	}
}

func TestSecondOpenIsLocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	if _, err := Open(path, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("want DB_LOCKED, got %v", err)
	}
	db.Close()
	db2 := mustOpen(t, path, Options{})
	db2.Close()
}

// TestConcurrentWriters is D1/D2 without a crash: 4 writers on distinct
// keys plus 4 writers on one shared key; every put is present after reopen
// and the shared key holds a value one writer wrote.
func TestConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := db.Put([]byte(fmt.Sprintf("w%d-%d", w, i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
					t.Error(err)
				}
				db.Put([]byte("shared"), []byte(fmt.Sprintf("writer-%d-%d", w, i)))
			}
		}(w)
	}
	wg.Wait()
	db.Close()
	db = mustOpen(t, path, Options{})
	defer db.Close()
	got := snapshot(t, db)
	if len(got) != 401 {
		t.Fatalf("got %d keys", len(got))
	}
	var w, i int
	if _, err := fmt.Sscanf(got["shared"], "writer-%d-%d", &w, &i); err != nil {
		t.Fatalf("shared holds a value nobody wrote: %q", got["shared"])
	}
}

// TestReaderNeverSeesHalfBatch is D4: a batch sets 50 keys to the same
// version; readers never observe a key older than one they already saw at a
// newer version from a later-committed batch, and every value is well formed.
func TestReaderNeverSeesHalfBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db := mustOpen(t, path, Options{})
	defer db.Close()
	const keys = 50
	write := func(ver int) {
		ops := make([]Op, keys)
		for i := range ops {
			ops[i] = Op{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte(fmt.Sprintf("v%06d", ver))}
		}
		if err := db.Batch(ops); err != nil {
			t.Error(err)
		}
	}
	write(0)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				// A whole-state read under one read lock must be uniform.
				vals := map[string]bool{}
				db.ForEachSorted(func(k, v []byte) error { vals[string(v)] = true; return nil })
				if len(vals) != 1 {
					t.Errorf("reader saw a half-applied batch: %v", vals)
					return
				}
			}
		}()
	}
	for v := 1; v <= 200; v++ {
		write(v)
	}
	close(done)
	wg.Wait()
}
