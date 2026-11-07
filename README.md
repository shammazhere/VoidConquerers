# KIPI (Kill It and Prove It) Key-Value Database

A single-file, embeddable key-value database in Go (standard library only) that survives being killed (`kill -9`, `TerminateProcess`) at any moment, plus a torture tool that proves it.

What we claim and what we tested:

| Failure | Designed for | How we test it |
|:---|:---|:---|
| Process killed at any instant | yes | `./torture` kills the real `./kvdb serve` with SIGKILL at random moments, and `./torture --mode crash-points` kills it at each of the 14 crash-point lines in the code |
| Process killed during recovery | yes | `./torture --mode recovery-crash` kills `./kvdb open` while it scans and truncates |
| Disk "full" (`--max-bytes`) | yes | `./torture --mode disk-full` |
| Power loss / OS crash | designed for (two fsyncs plus directory fsync) | **not tested**: we cannot cut power in a test. See [BREAKING.md](BREAKING.md) for why `kill -9` alone does not prove it. |

---

## Table of Contents

1. [Quick Start & Build](#1-quick-start--build)
2. [The Invariant & the Design in One Page](#2-the-invariant--the-design-in-one-page)
3. [CLI Reference](#3-cli-reference)
4. [Serve Protocol](#4-serve-protocol)
5. [On-Disk Binary Format](#5-on-disk-binary-format)
6. [Crash Recovery in Plain English](#6-crash-recovery-in-plain-english)
7. [Compaction and the Disk-Full Guard](#7-compaction-and-the-disk-full-guard)
8. [Torture Tool](#8-torture-tool)
9. [Planted-Bug Build (Proof That the Tool Can Fail)](#9-planted-bug-build-proof-that-the-tool-can-fail)

---

## 1. Quick Start & Build

Go 1.21 or newer, no third-party modules, no CGO. Linux, macOS and Windows.

```bash
go build -o . ./cmd/kvdb ./cmd/torture
```

This builds `./kvdb` (the database CLI and server) and `./torture` (the crash tester). On Windows they are `kvdb.exe` and `torture.exe`.

```bash
go test ./...                                   # unit + end-to-end tests (all 4 packages)
./torture --mode crash-points                   # all 14 crash points (15 cases), < 1 s
./torture --iterations 200 --seed 12345         # random kills, ~80 s
```

### What `open` tells you after a kill

```text
$ ./kvdb serve db.kv --crash-at CP4     # type: PUT 1 name Ada   (the process dies after fsync 1, before the marker)
$ ./kvdb open db.kv
opened db.kv: 1 keys, recovery in 1.3ms; cut 19 bytes of torn/uncommitted tail
  scanned  : 68 bytes in 1.301ms
  accepted : 1 records in 1 committed batches (CRC checked, commit marker found)
  rejected : 19 bytes cut (1 complete records without a commit marker, plus any torn bytes)
  reason   : the last write was interrupted before its commit marker reached the disk, so it never happened
  file     : 68 -> 49 bytes
```

### Smoke run

```text
$ ./kvdb open db.kv
opened db.kv: 0 keys, recovery in 1.49ms; created new database
$ ./kvdb put db.kv username alice
OK
$ ./kvdb put db.kv email alice@example.com
OK
$ ./kvdb get db.kv username
alice
$ ./kvdb check db.kv
OK
$ ./kvdb dump db.kv
email	alice@example.com
username	alice
```

---

## 2. The Invariant & the Design in One Page

### The invariant

After a crash at any point, once `open()` has finished recovery:

1. **Durable:** every operation that was acknowledged (`ACK`) before the crash is present.
2. **Atomic:** every operation that was not acknowledged is either fully present or fully absent. A batch is never half applied.
3. **No torn values:** no key holds a partial value, garbage, or another key's value.
4. **Open never fails** on a file this database produced, however it was killed. (It does refuse, with `CORRUPT`, a file whose committed data was damaged by something else; see §6.)

### The design

* **One append-only file** (`db.kv`). Nothing is ever overwritten in place.
* **In-memory hash index:** key → (offset, length) of its latest record. Values stay on disk and are read with `pread`, then their CRC is checked.
* **Every write, even a single `put`, is a batch** followed by a **commit marker** that says "the previous N records are one unit".
* **Two fsyncs per write**, in this order:

```text
write records ──► fsync #1 ──► write marker ──► fsync #2 ──► update index ──► ACK
```

fsync #1 makes sure the records are on disk before the marker can be. Without it, the disk may store the marker before the records, and after a power cut recovery would find a marker that commits garbage. fsync #2 makes the marker durable before we promise anything. The ACK is printed only after fsync #2, while the write lock is still held.

* **Recovery** replays the file from the start and applies records only when their marker arrives. Anything after the last valid marker is uncommitted and is cut off with `ftruncate` + fsync.
* **Compaction** writes the live records into `db.kv.tmp`, fsyncs it, atomically renames it over `db.kv` and fsyncs the directory. A crash at any step leaves either the old file or the new one, never a mix.

[ARCHITECTURE.md](ARCHITECTURE.md) has the full design. [BREAKING.md](BREAKING.md) has every crash point and what happens there.

---

## 3. CLI Reference

```text
kvdb open  <path> [--max-bytes N] [--trace] [--crash-at CPn] [--step-delay-us N]
kvdb put   <path> <key> <value>
kvdb get   <path> <key>
kvdb del   <path> <key>
kvdb batch <path> <file>
kvdb dump  <path>
kvdb check <path>
kvdb rejected <path>
kvdb serve <path> [--max-bytes N] [--compact-min-bytes N] [--step-delay-us N] [--crash-at CPn]
```

Every command opens the database (so it runs recovery), does its work, and closes it. There is no separate `close` command: the lock is released when the process exits, including after `kill -9`.

| Command | What it does | Output |
|:---|:---|:---|
| `open` | Runs recovery and reports what it repaired | `opened db.kv: 2 keys, recovery in 320µs` plus repair notes such as `; cut 8 bytes of torn/uncommitted tail`, `; deleted leftover compaction temp file`, `; created new database`; then indented lines: bytes scanned and time, records **accepted** (committed batches), bytes **rejected** and the **reason**, file size before → after |
| `put` | One-record batch + marker, two fsyncs | `OK` |
| `get` | Index lookup, `pread`, CRC check | the value, or `NOTFOUND` |
| `del` | Tombstone record + marker, two fsyncs | `OK` |
| `batch` | Applies a file of `put <key> <value>` / `del <key>` lines (1 to 10,000) as one atomic batch. **Operations apply in file order** (scenario B5): `put x 1` then `del x` leaves x absent; `del x` then `put x 1` leaves x = 1. Recovery replays them in the same order | `OK: applied 3 operations (2 put, 1 del) in 1.0ms, durable (2 fsyncs)`, or `ERROR: batch rejected, nothing written (all 3 operations or none): <reason>` |
| `dump` | All live keys in ascending byte order, `key<TAB>value` | one line per key |
| `rejected` | Lists the writes that recovery cut off (the uncommitted tail of a crash), from the `db.kv.rejected-*` copies. Never applies them | one block per cut tail: `PUT key = value`, `DEL key`, `TORN n bytes` |
| `check` | Read-only scan of the whole file; never modifies it | `OK`, `OK (uncommitted tail: 8 bytes; will be truncated on open)`, or `CORRUPT: ...` with the byte offset; then indented lines with the time taken, committed records and batches, and why the tail is not committed |
| `serve` | Long-running server on stdin/stdout (used by the torture tool) | see §4 |

Flags used for testing and demos:

| Flag | Meaning |
|:---|:---|
| `--max-bytes N` | File size cap. Writes that would not fit even after compaction get `DISK_FULL`; the last 1/8 is reserved for deletes. |
| `--compact-min-bytes N` | Auto-compact when dead bytes exceed `max(N, live bytes)` (default 1 MiB). An empty database always shrinks to 28 bytes. |
| `--trace` | `open` prints `INFO <event>` lines for each recovery step. |
| `--crash-at CPn` | The process kills itself (SIGKILL / TerminateProcess, no cleanup) when it reaches crash point `CPn`. See [BREAKING.md](BREAKING.md). |
| `--step-delay-us N` | Sleeps N µs between the engine's I/O steps. It never changes their order; it only widens the windows so random kills land inside them. |

---

## 4. Serve Protocol

`./kvdb serve <path>` reads one request per line from stdin and writes one reply per line to stdout. Type `HELP` to see the commands.

### Requests

The op-id may come first or second, so both `PUT 1 name Ada` and `1 PUT name Ada` work.

| Request | Meaning |
|:---|:---|
| `PUT <id> <key> <value...>` | Store (the value is the rest of the line, spaces allowed) |
| `DEL <id> <key>` | Delete |
| `GET <id> <key>` | Read |
| `BATCH <id> <n>` | The next n lines are `put <key> <value>` or `del <key>`; applied atomically |
| `HPUT` / `HDEL` / `HGET` / `HBATCH` | Same, with hex-encoded keys and values (`-` = empty value). Used by the torture tool so any byte can be sent |
| `COMPACT <id>` | Compact now |
| `DUMP <id>` | `KV <id> <key> <value>` for every key, then `ACK <id>` |
| `STATS <id>` | Counters for this session: accepted (ACK), rejected (ERR) by reason, reads found / not found, operations written, average and maximum time to ACK, keys and file size |
| `HELP`, `EXIT` / `QUIT` | Help text; stop (`EXIT` also prints the STATS summary to stderr) |

### Replies

| Reply | Meaning |
|:---|:---|
| `ACK <id>` | The write is durable. It is printed **after fsync #2**, from inside the engine's commit, while the write lock is held. |
| `VAL <id> <value>` / `HVAL <id> <hex>` | `GET` / `HGET` result |
| `NOTFOUND <id>` | Key does not exist |
| `ERR <id> <CODE> <detail>` | `DISK_FULL`, `IO_ERROR`, `BAD_REQUEST`, `DB_LOCKED`, `CORRUPT`. If no op-id can be read: `ERR ? BAD_REQUEST ...` |

### Concurrency (lanes)

The part of the op-id before the first `_` is its **lane**: `w1_17` and `w1_18` are lane `w1`. Requests in one lane run in the order they arrived. Different lanes run concurrently and really compete for the engine's write lock (the torture tool uses lanes `w1`, `w2`, ...). Op-ids without `_` (like `1`, `2`) all share one lane, so commands typed by hand always run in order. Output lines are written under one mutex and never interleave.

```text
$ ./kvdb serve db.kv
PUT 1 name Ada Lovelace
ACK 1
GET 2 name
VAL 2 Ada Lovelace
BATCH 3 2
put x 1
del name
ACK 3
GET 4 name
NOTFOUND 4
```

---

## 5. On-Disk Binary Format

A file is a 16-byte header followed by records. All integers are little-endian.

```text
+-----------+----------+-----+----------+----------------+----------+-----+----------------+
| Header 16 | Record 1 | ... | Record N | Marker (N)     | Record 1 | ... | Marker (M)     |
+-----------+----------+-----+----------+----------------+----------+-----+----------------+
            |<------------ batch 1 ------------------>|<---------- batch 2 ----------->|
```

### File header (16 bytes)

```text
Offset  Size  Field     Value
0       4     Magic     "KIPI" (4b 49 50 49)
4       2     Version   1
6       2     Flags     0
8       8     Reserved  0
```

### Record header (12 bytes)

```text
Offset  Size  Field    Meaning
0       4     CRC32C   Castagnoli CRC over bytes 4..end of the record (header fields + key + value)
4       4     ValLen   PUT: value length (0..1,048,576) | DEL: 0 | MARKER: N = number of records it commits (1..10,000)
8       2     KeyLen   PUT/DEL: 1..1,024 | MARKER: 0
10      1     Flags    0x05 = PUT, 0x06 = DEL, 0x08 = COMMIT MARKER (nothing else is valid)
11      1     Pad      0
```

A data record is the 12-byte header, then the key, then the value: `12 + KeyLen + ValLen` bytes. A marker is just the 12-byte header. Flags are built from bits PUT = 0x01, DEL = 0x02, BATCH_OP = 0x04, MARKER = 0x08; every data record carries BATCH_OP ("belongs to the batch the next marker commits").

### Real hex dump

`./kvdb put db.kv name Ada` on a new database produces exactly this 47-byte file (`od -A x -t x1 db.kv`):

```text
Offset  Bytes                                             Meaning
000000  4b 49 50 49 01 00 00 00 00 00 00 00 00 00 00 00   Header: "KIPI", version 1, flags 0, reserved 0
000010  6b a5 25 4b                                       Record CRC32C = 0x4b25a56b
000014  03 00 00 00                                       ValLen = 3
000018  04 00 05 00                                       KeyLen = 4, Flags = 0x05 (PUT), Pad = 0
00001c  6e 61 6d 65 41 64 61                              Key "name", Value "Ada"
000023  15 0c 00 58 01 00 00 00 00 00 08 00               Marker: CRC32C = 0x58000c15, N = 1, KeyLen 0, Flags 0x08
```

16 + 19 + 12 = 47 bytes. This is enough to write an independent parser.

### Auxiliary files

* **`db.kv.lock`**: held with an exclusive kernel lock (`flock` on POSIX, `LockFileEx` on Windows) for the life of the process. A second process gets `DB_LOCKED`. The kernel releases the lock when the process dies, even after `kill -9`.
* **`db.kv.rejected-<offset>-<crc>`**: a copy of an uncommitted tail that recovery cut, kept for inspection only (see §6). At most 10.
* **`db.kv.tmp`**: exists only while a database is being created or compacted. If one is found at open, it is a leftover from a crash and is deleted; `db.kv` is always the authoritative file.

---

## 6. Crash Recovery in Plain English

`open()` runs these steps. None of them writes anything until the scan has finished, so a crash during recovery is harmless and the next `open()` simply does it again.

1. **Lock** `db.kv.lock`. Fail with `DB_LOCKED` if another process has it.
2. **Create or clean up.** If `db.kv` does not exist, write a header to `db.kv.tmp`, fsync, rename to `db.kv`, fsync the directory. If `db.kv.tmp` is lying around, delete it.
3. **Check the header** (`KIPI`, version 1).
4. **Scan** from byte 16 with an 8 MiB buffered reader. For each record: check the lengths against the limits *before* allocating memory, read it, check the CRC. Data records are **staged**. When a marker arrives and its count N equals the number of staged records, the batch is **committed** into the index and `lastSafe` moves to the end of the marker.
5. **Stop** at the first thing that is not a complete valid record: a short header, a length past the end of the file, a bad CRC, a marker whose count does not match. Everything from `lastSafe` on is the torn or uncommitted tail of the write that was killed.
6. **Truncate** the file to `lastSafe` and fsync it. Staged records without a marker are thrown away.

**One exception, the cut-off marker (F4):** the last 4 bytes of every commit marker are constants (KeyLen 0, Flags 0x08, Pad 0). If the file ends 8 to 11 bytes into a marker (someone cut 1 to 4 bytes off by hand), recovery rebuilds the marker, accepts it only if its CRC and record count are right, and appends the missing bytes. Cutting 1 byte off a database therefore loses nothing.

**Why it is idempotent:** step 6 is the only write to `db.kv`, and it only shortens the file to a prefix that has already been validated. Kill it before, during or after, and the next run finds the same committed prefix.

### Rejected writes are kept aside, never applied

Before step 6 cuts the tail, recovery copies the cut bytes into `db.kv.rejected-<offset>-<crc>` next to the database. `./kvdb rejected db.kv` lists what is in them:

```text
$ ./kvdb rejected db.kv
1 rejected tail(s), newest first. These writes were NOT acknowledged and are NOT in the database.
To keep one, write it again (kvdb put / del / batch).

db.kv.rejected-49-89f1f479  saved 2026-11-07 04:58:06  53 bytes cut at offset 49
  PUT  name = Ada   (complete, but no commit marker)
  PUT  lang = Go   (complete, but no commit marker)
  DEL  city   (complete, but no commit marker)
```

The copy is for a person to inspect; the database never reads it back, because:

* **atomicity:** the batch may be incomplete (3 of 10 records reached the disk), and applying part of it is exactly the half-batch the invariant forbids;
* **no double writes:** the client got no ACK, so a correct client retries. If recovery also applied the old copy, the write would happen twice, possibly on top of newer writes;
* **torn bytes:** the last record may be cut in the middle (`TORN` in the listing).

It cannot weaken any guarantee: it never touches `db.kv`. It is written to `db.kv.rejected.part`, fsynced and renamed, so a crash leaves either no copy or a complete one, and a leftover `.part` is deleted at the next open. The name comes from the tail's offset and CRC, so recovering the same tail twice (recovery killed before it truncated) does not create a duplicate. The newest 10 copies are kept (`Options.KeepRejected`; negative turns it off). If saving fails (for example the disk is full), recovery still cuts the tail and opens normally.


**The corruption guard.** A torn tail is always at the *end* of the file. If a complete record fails its CRC but a valid commit marker follows it, the damage is in the middle of committed data (a bad disk, a stray write). Truncating there would silently delete acknowledged data, so `open` and `check` refuse with `CORRUPT` and leave the file alone:

```text
CORRUPT: record at offset 16 fails its CRC but a valid commit marker follows at offset 35; refusing to truncate committed data
```

---

## 7. Compaction and the Disk-Full Guard

The log grows with every overwrite and delete. Compaction rewrites only the live records. It runs under the write lock, triggered by `COMPACT`, by the disk-full guard, or automatically after a commit when either

* the dead bytes exceed `max(--compact-min-bytes, live bytes)` (default 1 MiB). This keeps the file under about 2 × live data + 1 MiB, which keeps recovery fast, and each compaction copies no more bytes than were written since the last one; or
* the database is empty and the file is larger than 28 bytes. Deleting every key shrinks the file to 28 bytes by itself, with no `COMPACT` command (1,000,000 keys: 85 MB → 28 bytes).

1. Compute the compacted size: `16 + live bytes + 12`. If it is over `--max-bytes`, stop with `DISK_FULL` before touching anything.
2. Create `db.kv.tmp` with `O_EXCL` and write the header.
3. Copy every live record byte for byte, in file-offset order, verifying each CRC on the way.
4. Write one marker for all of them.
5. fsync `db.kv.tmp`.
6. `rename(db.kv.tmp, db.kv)`: atomic. Before it, the old file is the database; after it, the new one is.
7. Point the index at the new offsets. On POSIX the open descriptor of the temp file *is* now `db.kv`, so it is never reopened; on Windows the file is closed, renamed and reopened.
8. fsync the directory (POSIX) so the rename survives a power cut. Windows has no directory fsync; NTFS journals the rename.

**Disk-full guard.** Before a write, the engine checks that `file size + write size` fits under `--max-bytes`. If not, it compacts first if the write would then fit; otherwise it returns `DISK_FULL` without writing a single byte. The last 1/8 of `--max-bytes` is reserved for delete-only batches, so a full database can always be emptied.

---

## 8. Torture Tool

```text
./torture ──starts──► ./kvdb serve db.kv   (real child process)
   │  sends PUT/DEL/BATCH/COMPACT from 2 concurrent lanes
   │  records every ACK in oracle.log  (the oracle only believes ACKs)
   │  SIGKILLs serve at a random moment, mid-operation
   ├── ./kvdb open    → must succeed (recovery)
   ├── ./kvdb check   → must say OK
   └── ./kvdb dump    → must match the oracle exactly
```

### Commands

```bash
./torture --iterations 200 --seed 12345          # default mix
./torture --iterations 100 --mode large-values
./torture --iterations 100 --mode large-batches
./torture --iterations 100 --mode compaction
./torture --iterations 100 --mode recovery-crash
./torture --iterations 100 --mode disk-full      # default cap 256 KiB; override with --max-bytes
./torture --iterations 100 --mode d2-same-key
./torture --mode crash-points                    # the 14 deterministic crash points
./torture --mode scale                           # 1,000,000 keys (also: ./torture scale)
```

Other flags: `--lanes N` (default 2), `--work-dir DIR` (default `torture-work`), `--kvdb-bin PATH`, `--clean` (wipe the whole work directory). Every run starts from a fresh database. The same seed gives the same workload.

| Mode | What it stresses |
|:---|:---|
| `default` | Puts, deletes and small batches from 2 lanes; kills 5 to 200 ms in |
| `large-values` | Values up to 1 MiB, written in 256 KiB pieces, so kills land in the middle of a value |
| `large-batches` | Batches of 1,000 to 10,000 operations, killed mid-batch: all or nothing |
| `compaction` | Low compaction threshold plus explicit `COMPACT`, so kills land in the tmp copy, before and after the rename |
| `recovery-crash` | Kills `./kvdb open` itself while it scans or truncates, then recovers again |
| `disk-full` | A small `--max-bytes` cap with puts, deletes and compactions: `DISK_FULL` must leave no bytes behind, and the file must never exceed the cap |
| `d2-same-key` | Both lanes write one shared key: after recovery it must hold the last ACKed value or one of the in-flight values, never a mix |
| `crash-points` | Kills the real binary at each of the 14 `CRASH HERE` lines (15 cases: CP7 is also run right after a compaction) and checks the exact outcome on disk, after recovery, and whether the client got an ACK (table in [BREAKING.md](BREAKING.md)) |
| `scale` | 1,000,000 keys, a kill in the middle of a 10,000-key batch, recovery must finish under 2 s; then delete every key (no `COMPACT` sent), and the file must shrink to 28 bytes by itself |

### How the oracle decides

The oracle is built only from what serve acknowledged. After each kill:

* every ACKed write must be present with its exact value;
* each operation that was sent but not yet ACKed ("in flight") must be either fully present or fully absent. The oracle tries every combination of the in-flight operations and accepts the dump only if one combination matches exactly. Being present though not ACKed is allowed (the kill landed between fsync #2 and the ACK);
* each value carries an envelope (key, op-id, sequence number, checksum), so a torn value or a value under the wrong key is detected even if its length is right;
* lanes write disjoint keys (except in `d2-same-key`), so the expected state is never ambiguous.

### Output

Each iteration prints one row of a table; the run ends with a summary in plain words:

```text
  Iter    Kill at   Sent  ACKed  Interrupted  Kept  Gone  Repair on restart  Recovery    Keys  Result  Notes
   1/8     193 ms     39     37            2     0     2  cut 958 B           3.2 ms      24  PASS
   2/8     159 ms     18     16            2     1     1  nothing to fix      3.0 ms      48  PASS
...
======================================================================================
 RESULT: PASS   8 of 8 iterations, 0 failures          mode default, seed 42, took 1s
======================================================================================
 Durability: every acknowledged write must survive
   acknowledged writes checked after restarts ........ 778   lost: 0
   kills that hit a write in progress ................ 8 of 8 iterations
 Atomicity: what happened to the 13 interrupted writes
   kept in full (committed, ACK not sent yet) ........ 3
   gone in full (never committed) .................... 10
   half-applied ...................................... 0   (never allowed)
 Recovery
   restarts that cut a half-written tail ............. 4 of 8   (3,144 bytes cut in total)
   recovery time ..................................... average 3.1 ms, slowest 3.8 ms   (limit 2,000 ms)
======================================================================================
```

* **Interrupted**: writes that were sent but not yet acknowledged when the process was killed.
* **Kept / Gone**: after the restart, each interrupted write was found fully present (it had committed; only the ACK was missing) or fully absent. Anything in between fails the run.
* **Repair on restart**: what `kvdb open` cut off the end of the file (the half-written tail of the killed write).
* **Notes**: mode-specific events: compactions, `DISK_FULL` replies, leftover temp files deleted, or where the restart itself was killed (recovery-crash mode).

On a failure the tool prints `RESULT: FAIL`, exits with status 1 and saves `torture-work/failure/`: the database as it was at the kill (`db-as-killed.kv`), after recovery (`db-after-recovery.kv`), the oracle log, serve's stderr and `report.txt` with the exact violation (for example `value mismatch for non-in-flight key ...`). The same `--seed` replays the same workload.

### Our results

| Command | Result |
|:---|:---|
| `./torture --mode crash-points` | 15/15 cases at all 14 crash points PASS in 0.3 s |
| `./torture --iterations 200 --seed 12345` | 200/200 PASS, 17,853 ACKed ops verified, op in flight at 198/200 kills, 108 torn tails cut |
| `--mode large-values --seed 999` (100) | 100/100 PASS, 37 torn tails cut (23.5 MB) |
| `--mode large-batches --seed 5` (100) | 100/100 PASS, batch in flight at 99/100 kills, 31 torn tails cut (13.5 MB) |
| `--mode compaction --seed 7` (100) | 100/100 PASS, 1,419 compactions, 26 leftover tmp files cleaned |
| `--mode recovery-crash --seed 42` (100) | 100/100 PASS; open killed during scan 102×, during truncate 40× |
| `--mode disk-full --seed 3` (100) | 100/100 PASS, 5,343 `DISK_FULL` replies, file never above the 262,144-byte cap |
| `--mode d2-same-key --seed 11` (100) | 100/100 PASS |
| `./torture --mode scale --seed 5` | PASS: 1,000,001 keys recovered in 403–483 ms; file 85,001,250 → 28 bytes after deleting every key, with no `COMPACT` sent |

The seed fixes the workload and the kill times. How far the engine gets before each kill depends on the speed of the disk, so the counts move by a few percent from run to run; the PASS result does not.

---

## 9. Planted-Bug Build (Proof That the Tool Can Fail)

A test that cannot fail proves nothing. The normal build contains no bugs. Building with `-tags sabotage` adds four classic durability bugs, one at a time, selected with `KVDB_SABOTAGE`:

| `KVDB_SABOTAGE=` | The bug | `--mode crash-points` (deterministic) | Random kills (first failing iteration, 3 to 5 runs each) |
|:---|:---|:---|:---|
| `ack-before-fsync` | ACK printed before the data is written | FAIL every run: CP1 to CP6 report "ACK sent before the commit was durable" | default `--seed 12345`: iteration 1 or 2 |
| `no-truncate` | open does not cut the torn tail, so new writes go behind the garbage | FAIL every run: CP10 | default `--seed 12345`: iteration 2 |
| `no-marker` | recovery applies records without waiting for their commit marker | FAIL every run: CP2, CP3, CP4, CP9, CP10 | `--mode large-batches --seed 5`: iteration 1 to 20 |
| `compact-old-fd` | after compaction's rename, writes keep going to the old, deleted file | FAIL every run: CP7b | `--mode compaction --seed 7`: iteration 1 |

```bash
go build -tags sabotage -o kvdb-bad ./cmd/kvdb
KVDB_SABOTAGE=no-marker ./torture --kvdb-bin ./kvdb-bad --mode crash-points               # → RESULT: FAIL, in < 1 s
KVDB_SABOTAGE=ack-before-fsync ./torture --kvdb-bin ./kvdb-bad --iterations 20 --seed 12345 # → RESULT: FAIL
```

The two kinds of testing complement each other: crash points catch every planted bug on every run because they stop at the exact line where the bug matters; random kills find it only when a kill happens to land in the right window, but they can also find bugs nobody thought to put a crash point in front of.

The normal `go build` never contains this code (`sabotage_off.go` sets the switch to a constant empty string).
