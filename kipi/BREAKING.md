# BREAKING.md — Attack Analysis

## 1. Crash points table

| CP | Location | What is on disk at the crash | What `open()` does |
|----|----------|------------------------------|--------------------|
| CP1 | `write.go Apply`: before `WriteAt` | Nothing new on disk | Scan finds no new record; state unchanged |
| CP2 | `write.go Apply`: disk-full guard fires, returns error | Nothing written | No recovery needed; previous data intact |
| CP3 | `write.go Apply`: after `WriteAt` starts but before it completes | Partial record bytes, no commit marker | Scan sees CRC mismatch or truncated record; torn tail discarded |
| CP4 | `write.go Apply`: after all records written, before commit marker is written | Records on disk, no marker | Scan advances through records but finds no marker; all discarded |
| CP5 | `write.go Apply`: during `file.Sync()` | Records + marker may or may not be durable | If marker is durable, batch is recovered; if not, torn tail is discarded; both are correct |
| CP6 | `write.go Apply`: after `Sync()` returns, before index update | Records + marker fully durable | Batch **will** be recovered on next open; index update is merely a performance optimization |
| CP7 | `write.go Apply`: after index update, before `Apply` returns | Identical to CP6 | Batch recovered; caller may not have received `nil` yet, but data is safe |
| CP8 | `recovery.go Open`: during `os.Remove(tmpPath)` | `.tmp` may or may not be deleted | Next open attempts removal again; idempotent |
| CP9 | `recovery.go Open`: during forward scan | Index partially built | Next open re-scans from byte 16; identical result |
| CP10 | `recovery.go Open`: during `file.Truncate(validEnd)` | File may be partially truncated | Next open: file is ≤ validEnd (truncation is monotone downward); re-scan finds no torn tail or same tail and truncates again |
| CP11 | `compact.go compactLocked`: while writing `.tmp` | `.tmp` is incomplete | `recovery.go` deletes `.tmp` on next open; original file is intact |
| CP12 | `compact.go compactLocked`: during `os.Rename(tmpPath, dbPath)` | Either old or new file exists as `dbPath`; both are complete and CRC-valid | Scanner can read whichever file won the rename; both contain only committed records |
| CP13 | `compact.go compactLocked`: after rename, before dir fsync | New file is `dbPath`; dir entry may be in journal | On POSIX: dir fsync ensures rename is visible after crash. On Windows: NTFS journal guarantees rename durability. No data is lost either way. |
| CP14 | `recovery.go Open`: during lock acquisition | `.lock` file may or may not exist | Next open retries lock; idempotent |

---

## 2. The ordering argument

Write path for a batch:

```
1. WriteAt(records + marker, fileSize)   — single write syscall
2. file.Sync()                           — fsync / FlushFileBuffers
3. Update in-memory index
4. Return nil (ACK to caller)
```

**Why this is safe:** The commit marker is written in the same `WriteAt` call as the records, before `Sync()`. After `Sync()` returns, the OS guarantees all bytes are on the physical device. If the process is killed *after* `Sync()` but before the caller receives `nil`, the commit marker is already durable — recovery will find and replay the batch. If the process is killed *before* `Sync()`, the marker may or may not be on disk. Recovery handles both cases: if the marker is absent (or CRC-invalid due to partial write), the batch is in the torn tail and is discarded. If the marker is present and CRC-valid, the batch is applied. Either outcome satisfies the invariant.

**Compaction rename ordering:**
```
1. Write all live records to path.tmp
2. fsync path.tmp
3. rename(path.tmp, path)       — atomic on POSIX / Windows
4. fsync directory (POSIX only)
5. Reopen path
```

After step 2, `path.tmp` is complete and correct. After step 3, whichever file is at `path` is authoritative and complete. We never have a window where `path` is absent or corrupt.

We do **not** fsync the directory on Windows because NTFS logs metadata operations in its journal, making renames durable without an explicit dir fsync. On Linux/macOS we do fsync the directory because ext4/APFS may not flush directory entries otherwise.

---

## 3. Known weaknesses

1. **O(n) recovery with large log.** Recovery scans the entire file linearly. With 1 M keys and no prior compaction, the log can be large. We hit the 2-second target in testing (batch writes accumulate quickly), but a worst-case scenario (1 M individual puts, each with its own fsync) would produce a very large log. Mitigation: trigger auto-compaction before recovery time grows.

2. **No CRC on file header.** The 16-byte header has no checksum. A single-byte corruption in the magic bytes causes `ErrBadFile` and the database cannot be opened. Mitigation: add a CRC field to the header in a v2 format.

3. **In-flight op values may appear in dump.** Because our oracle-verification in the torture tool only checks ACKed operations, an in-flight put that lands on disk but whose ACK is never received is *allowed* to appear in the dump (the invariant permits it). But a naive external oracle might flag it as "unknown". Our torture tool handles this correctly, but a third-party tool may not.

4. **Windows rename not truly atomic under all conditions.** `os.Rename` on Windows uses `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`. On NTFS this is effectively atomic, but on FAT32 it is not. We document NTFS as the supported filesystem.

5. **Lock file is not cleaned up after crash.** The `.lock` file remains on disk after a crash. This is intentional (we re-acquire the lock on next open), but leaves a stale file. Cleanup happens on `Close()`.

---

## 4. What we tried that failed

- **Separate fsync for each record.** Initial design called `Sync()` after every individual put. This was correct but extremely slow (each put required two disk round-trips). We collapsed all records for a batch into one `WriteAt` + one `Sync()`.
- **Storing the index on disk.** We considered writing a separate index file to speed up recovery. The complexity of keeping index + log in sync across crashes outweighed the benefit. Linear scan at 1 M keys completes in <2 seconds, so we dropped it.
- **Using `O_SYNC` flag.** `O_SYNC` on Linux writes through to disk on every write, which eliminates the need for explicit `Sync()`. However, it is not portable (Windows does not support it) and makes individual small writes very slow. We use explicit `Sync()` instead.

---

## 5. If we had 24 more hours

1. **Persistent index (B-tree or hash table on disk).** Recovery would skip the linear scan entirely, loading the index from a checkpoint file. Recovery would then be O(1) instead of O(n) in the common case.
2. **CRC on file header + format version migration.** Add a 4-byte CRC to the header, and a migration path so that a v1 file can be opened, verified, and upgraded to v2 in place.
3. **More torture modes.** Add a mode that truncates the file by 1 byte at the end (simulating F4), and a mode that kills during `open()` while it is mid-truncation (simulating C2). Currently these scenarios are handled by design but are not specifically exercised by the torture tool.

