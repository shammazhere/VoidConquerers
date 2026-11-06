# KIPI engine (`src/kvdb`)

This is the database itself: a single-file key-value store that survives `kill -9` at any instruction. It uses the Go standard library only.

The CLI (`cmd/kvdb`) and the torture tool (`cmd/torture`) are separate parts that call this package. This folder builds and tests on its own.

```
go test ./src/...              # 16 engine tests, including a real self-kill at each of the 14 crash points
go test -race ./src/...        # same, with the race detector
go vet ./src/...
```

Requires Go 1.21 or newer. The module path is `kipi`, and teammates import the engine as `kipi/src/kvdb`.

## Files

| File | Lines | What it does |
|---|---|---|
| `format.go` | 163 | The bytes on disk: 16-byte header, 12-byte record header, commit marker, CRC32C. It checks lengths *before* reading a payload. |
| `db.go` | 210 | The `DB` struct (index, fileSize, liveBytes, RWMutex, poisoned), `Options`, error codes, `Get`, `ForEachSorted`, `Stats`, `Close`. |
| `write.go` | 303 | The only write path, `Apply`, steps 1–12: validate, disk-full guard, pwrite records, fsync, pwrite marker, fsync, index, ACK, auto-compact. It also has the rollback logic. Crash points CP1–CP7. |
| `recovery.go` | 348 | `Open`: lock first, create or clean the temp file, scan, truncate the torn tail. Also the corruption guard (`markerAfter`). Crash points CP8–CP10, CP14. |
| `compact.go` | 145 | Compaction: write live records to `db.kv.tmp`, fsync, rename over `db.kv`, update the index, fsync the directory. Crash points CP11–CP13. |
| `check.go` | 84 | Read-only verification (`kvdb check`). |
| `platform_posix.go` | 51 | Linux/macOS: `flock`, directory fsync, rename where the tmp descriptor becomes the database descriptor. |
| `platform_windows.go` | 79 | Windows: `LockFileEx`, no directory fsync, close both handles, rename, reopen. |
| `crash.go` | 43 | Test only: `Options.CrashAt` makes the process kill itself at an exact crash point. |
| `sabotage_off.go` / `sabotage_on.go` | 25 | `-tags sabotage` builds in 4 planted bugs, to prove the torture tool can fail. A normal build compiles them out. |
| `kvdb_test.go` | 533 | 14 tests: round trip, torn tail at every byte, uncommitted batch, F4, ordering, compaction, disk-full, lock, concurrency, readers. |
| `crash_test.go` | 195 | `TestCrashPoints` (a real self-kill at CP1–CP14, checking the state after reopen) and `TestCorruptionIsNotTruncated`. |

## On-disk format (little-endian)

```
FILE    [HEADER 16 B][record]...[marker][record]...[marker]             append-only
HEADER  "KIPI"(4) | version u16 = 1 | flags u16 = 0 | reserved u64 = 0
RECORD  CRC32C u32 (bytes 4..end) | ValLen u32 | KeyLen u16 | Flags u8 | Pad u8 | Key | Value
        Flags: 0x05 PUT|BATCH_OP   0x06 DEL|BATCH_OP   0x08 COMMIT_MARKER
MARKER  12-byte record: Flags 0x08, KeyLen 0, ValLen = N (records it commits), no payload
```

A commit is N data records followed by one marker whose ValLen equals N. A single put is a batch of one.

## Public API (for the CLI owner)

```go
db, err := kvdb.Open(path, kvdb.Options{MaxBytes: n})   // recovers; never fails on a file a crash produced
err  = db.Put(key, value)          // also db.Delete(key), db.Batch([]kvdb.Op{...})
err  = db.Apply(ops, onCommit)     // serve uses this: onCommit runs AFTER both fsyncs, under the write lock -> print ACK there
v, found, err := db.Get(key)
err  = db.ForEachSorted(func(k, v []byte) error { ... })   // dump, sorted by key bytes, one consistent state
err  = db.Compact()
st  := db.Stats();  rs := db.RecoveryStats()               // keys, recovery time, truncated bytes, tmp removed
rep := kvdb.Check(path)                                     // read-only; rep.OK, rep.Reason, rep.TailBytes
err  = db.Close()
```

**Errors.** Map them with `errors.Is`:

| Error | Meaning |
|---|---|
| `ErrDiskFull` | Nothing was written. |
| `ErrBadRequest` | Bad size, or more than 10,000 ops. |
| `ErrIO` | A write or fsync failed; the database is poisoned. |
| `ErrLocked` | Another process has the file. |
| `ErrCorrupt` | Bad header, or damage in the middle of committed data. |

**Options:**

| Option | Meaning |
|---|---|
| `MaxBytes` | File cap. 0 means unlimited. |
| `CompactMinBytes` | Auto-compaction floor. 0 means 64 MiB; negative turns it off. |
| `Trace` | Phase events. |
| `StepDelay` | Test only: widens crash windows. |
| `CrashAt` / `CrashAfter` | Test only: kill itself at a crash point. |

## Every constraint in the problem statement, and where it is handled

| Requirement (PDF) | Mechanism | Code | Proven by |
|---|---|---|---|
| put / get / delete / batch / open / close (§5) | Single write path `Apply`; values read back with `pread` + CRC | `write.go`, `db.go` | `TestRoundTrip` |
| Keys 1–1,024 B, values 0–1 MB, ≤10,000 ops per batch | `validate` before anything is written | `write.go validate` | `TestValidation` |
| ACK only after the data is durable | pwrite → fsync → marker → fsync → index → `onCommit` | `Apply` steps 5–10 | `TestCrashPoints` CP5–CP7 |
| Torn writes never surface (A1–A7) | Length check before reading, CRC32C, truncate to the last marker | `scan`, `Open` Phase 2 | `TestTornTailEveryOffset`, CP1–CP4 |
| Batch all-or-nothing (B1–B6) | Marker carries the record count; committed only when the count matches | `scan` COMMIT | `TestUncommittedBatchDiscarded`, CP2 |
| Batch order (B5): listed order, last op wins | Index updated in order; replay in file order | `applyToIndex` | `TestBatchOrdering` |
| Crash during recovery (C1, C2) | The scan writes nothing; one truncate + fsync | `Open` | CP9, CP10 |
| Crash-safe compaction (C3–C5) | tmp → fsync → atomic rename → dir fsync; open deletes a leftover tmp only after taking the lock | `compactLocked`, `Open` 0.3 | CP8, CP11–CP13, `TestCompactionShrinks` |
| File shrinks after deletes + compaction | Copy live records only; auto-compact when dead bytes > max(floor, live) | `compactLocked`, `maybeAutoCompact` | `TestCompactionShrinks`, `TestAutoCompaction` |
| Disk full: clean error, still readable (E1–E5) | Whole-write size check first; 1/8 reserve for deletes; compact if that would fit | `spaceGuard` | `TestDiskFull`, `TestCompactionOverCap` |
| 2+ concurrent writers; get never sees half a batch (D1–D4) | RWMutex: writers exclusive for the whole commit, readers shared | `DB.mu` | `TestConcurrentWriters`, `TestReaderNeverSeesHalfBatch` |
| One process at a time; no stale lock after a kill | flock / LockFileEx (released by the OS on death); retried for up to 1 s | `platform_*.go`, `lockWithRetry` | `TestSecondOpenIsLocked` |
| Recovery < 2 s at 1,000,000 keys | One linear scan, 8 MB buffered reader; index holds offsets only | `scan` | Measured: 405 ms at 1M keys (torture scale mode) |
| Single authoritative file | `db.kv` only; `.lock` holds no data; `.tmp` exists only during create or compaction | | |
| Never retry a failed fsync | Poison, then let recovery decide | `Apply`, `rollback` | Code inspection |
| Bonus: bit-rot detection | A complete-but-damaged record followed by a valid marker makes open refuse instead of truncating | `markerAfter` | `TestCorruptionIsNotTruncated` |

## Crash points (test-verified)

Each row is a real self-kill at that line in `TestCrashPoints`, followed by a reopen and a state check.

| CP | Line | On disk at the kill | Next `open()` | Verified result |
|---|---|---|---|---|
| CP1 | `Apply`, before any write | file ends on the last marker | nothing to cut | op absent |
| CP2 | `appendRecords`, after the first 256 KiB piece | part of the batch, no marker | cuts it | whole batch absent |
| CP3 | after the records, before fsync 1 | records, no marker | stages, finds no marker, cuts | absent, file back to the old size |
| CP4 | after fsync 1, before the marker | records durable, no marker | same as CP3 | absent |
| CP5 | after the marker write, before fsync 2 | marker complete in the page cache | commits | **present** |
| CP6 | after fsync 2, before the index update and ACK | committed, not ACKed | commits | present (A4) |
| CP7 | after the ACK | committed and ACKed | commits | present (A5) |
| CP8 | `Open` 0.3, stale tmp not yet deleted | `db.kv` plus a leftover tmp | deletes the tmp | data intact, no tmp |
| CP9 | `Open`, scan done, nothing written | as the last crash left it | same scan again | data intact |
| CP10 | `Open`, after the truncate, before fsync | old length or lastSafe | same lastSafe | data intact, tail gone |
| CP11 | compaction, halfway through the copy | partial tmp, `db.kv` untouched | deletes the tmp | data intact |
| CP12 | compaction, tmp fsynced, before the rename | complete tmp, old `db.kv` | deletes the tmp | data intact |
| CP13 | compaction, after the rename | new compacted `db.kv` | normal scan | data intact, no tmp |
| CP14 | create, tmp header written, before the rename | no `db.kv`, only a tmp | deletes the tmp, creates again | empty database |

## Known weaknesses (say these honestly)

1. **F4 (1 byte cut by hand).** The last marker is torn, so the last commit is dropped even if it was ACKed. No design can recover bytes removed by hand.
2. **A damaged record *header*.** If the flags or lengths themselves are damaged, the record's length is unknown, so it is treated as a torn tail and truncated. Only a damaged *payload* or *CRC* is detected by the corruption guard.
3. **Windows is cross-compiled only, not run.** Run `go test ./src/...` on the demo laptop.
4. **Recovery time grows with log length.** 405 ms is measured with 100-byte values; 1 KB values are unmeasured.
5. **Compaction stops the world and briefly needs 2× disk space.**
6. **Puts may use only 7/8 of `MaxBytes`.** The rest is reserved for deletes.

## For your teammates

- **CLI owner.** Print `ACK <id>` inside the `onCommit` callback of `db.Apply`, never after `Apply` returns. That is what makes ACK order equal commit order.
  - Optional: expose `--crash-at CPn` (sets `Options.CrashAt`) so you can demo any crash point live.
- **Torture owner.**
  - The oracle rule this engine guarantees: after a kill, the database equals all ACKed ops in ACK order, plus **at most one** un-ACKed op, applied whole.
  - `StepDelay` widens crash windows on fast disks.
  - `go build -tags sabotage` plus `KVDB_SABOTAGE=ack-before-fsync|no-truncate|no-marker|compact-old-fd` gives the planted bugs.
- **Docs owner.** The CP numbers above match the `// CPn - CRASH HERE` comments and `db.crashPoint("CPn")` calls in the code. BREAKING.md weakness 2 is now partly fixed by the corruption guard; see weakness 2 above.
