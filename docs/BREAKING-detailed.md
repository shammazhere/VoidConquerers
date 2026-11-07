# BREAKING (detailed version, for study; the submitted two-page version is ../BREAKING.md)

Every row of the table below is a line in the code marked `// CPn - CRASH HERE`, and every row can be run. `./torture --mode crash-points` builds nothing new: it starts the real `./kvdb` binary with `--crash-at CPn`, the process kills itself at that exact line (SIGKILL on POSIX, `TerminateProcess` on Windows, no deferred cleanup), and the tool then runs `./kvdb check` and `./kvdb open` and compares what it sees with this table. For the write path it also checks the client's side of the promise: before CP7 no `ACK` may have reached the client, at CP7 it must have.

```bash
go build -o . ./cmd/kvdb ./cmd/torture
./torture --mode crash-points          # 15/15 cases at the 14 crash points, PASS in 0.3 s
./kvdb serve db.kv --crash-at CP4      # try one by hand: type "PUT 1 name Ada", the process dies
./kvdb check db.kv                     # OK (uncommitted tail: 19 bytes; will be truncated on open)
./kvdb open db.kv                      # opened db.kv: 0 keys, ...; cut 19 bytes of torn/uncommitted tail
```

The "Covers" column refers to the requirement IDs in the problem statement.

## 1. Crash points

### Write path (`src/kvdb/write.go`, `Apply`)

The steps of one commit are: space check → write records (in 256 KiB pieces) → **fsync #1** → write marker → **fsync #2** → update index → ACK.

| CP | The process dies... | On disk at that moment | What the next `open()` does | Result | Covers |
|:---|:---|:---|:---|:---|:---|
| CP1 | before any byte of the commit is written | the file ends on the previous marker | nothing to repair | op absent | A1 |
| CP2 | between two 256 KiB pieces of a large batch | a prefix of the batch, possibly ending mid-record | the torn record fails its length or CRC check; the whole uncommitted run is cut (409,632 bytes in our run) | batch absent (0 of 3 keys) | A2 A7 B1 |
| CP3 | after the records are written, before fsync #1 | records in the page cache, no marker | records are staged, EOF arrives without a marker, they are discarded and cut | op absent | A3 B2 |
| CP4 | after fsync #1, before the marker | records durable, no marker | same as CP3 | op absent | B2 |
| CP5 | after the marker is written, before fsync #2 | records durable, marker in the page cache | a process kill does not lose the page cache, so the marker is there and complete: batch committed | op **present**, never ACKed | B3 |
| CP6 | after fsync #2, before the index update and ACK | commit durable | marker found, batch applied | op **present**, never ACKed | A4 |
| CP7 | after the ACK | commit durable | marker found, batch applied | op present, ACKed | A5 |
| CP7b | after the ACK of a write that follows a `COMPACT` | commit durable in the *new* file | marker found, batch applied | op present | A5 C4 |

In CP1 to CP6 the client has no ACK; the tool fails the case if one was printed. CP5 and CP6 are the interesting ones: the client never got an ACK, yet the data is there. The invariant allows this ("fully present or fully absent"), and the torture oracle counts these cases separately as "durable although un-ACKed". If the marker at CP5 were torn (possible only with power loss), its CRC would fail and the batch would be discarded. Either way: all or nothing.

### Recovery (`src/kvdb/recovery.go`, `Open`)

| CP | The process dies... | On disk at that moment | What the next `open()` does | Result | Covers |
|:---|:---|:---|:---|:---|:---|
| CP8 | while deleting a leftover `db.kv.tmp` | `db.kv` intact; the tmp may still exist | deletes the tmp again | all data present, tmp gone | C3 C5 |
| CP9 | after the scan, before anything is written | unchanged: recovery writes nothing until the scan ends | the same scan, the same result, cuts the tail | torn op absent | C1 F2 |
| CP10 | after `ftruncate`, before its fsync | the file is either the old length or already cut | scans to the same `lastSafe`; truncate is a no-op or the same cut | torn op absent | C2 F2 |
| CP14 | while creating a new database: tmp has its header, rename not done | no `db.kv`, a `db.kv.tmp` | "no db.kv" path: delete the tmp, create again | empty database, tmp gone | creation |

### Compaction (`src/kvdb/compact.go`)

| CP | The process dies... | On disk at that moment | What the next `open()` does | Result | Covers |
|:---|:---|:---|:---|:---|:---|
| CP11 | halfway through copying live records into the tmp | `db.kv` untouched; partial, unsynced tmp | `db.kv` is authoritative: delete the tmp | old file, all data present | C3 |
| CP12 | tmp complete and fsynced, before the rename | both files complete | `db.kv` still wins: delete the tmp (nothing new was ACKed meanwhile: we hold the write lock) | old file, all data present | C3 |
| CP13 | right after the atomic rename | `db.kv` is the compacted file, no tmp | normal scan of the new file | new file (1,439 → 72 bytes in our run), all data present | C4 C5 |

**CP7b** exists because one classic compaction bug is invisible at CP11 to CP13: after the rename, the engine keeps writing to the old, already-replaced file. Every write is then silently lost on the next open. Only a write *after* a compaction shows it, so CP7b runs `COMPACT` followed by `PUT name Ada` and kills the process right after that ACK.

At no point are there zero valid copies of the database: before the rename `db.kv` is the old complete file, after it `db.kv` is the new complete file. `rename` is atomic, so there is no third state.

### Output of `./torture --mode crash-points`

```text
CP    Process dies at                          Covers    On disk after the kill (kvdb check)          Recovery (kvdb open)               Observed
CP1   Apply: before any byte is written        A1        OK                                           nothing to repair                  name absent, no ACK                                  PASS
CP2   Apply: between 256 KiB write pieces      A2 A7 B1  OK (uncommitted tail: 409632 bytes)          cut 409632 bytes of tail           0 of 3 batch keys present, no ACK                    PASS
CP3   Apply: records written, before fsync 1   A3 B2     OK (uncommitted tail: 19 bytes)              cut 19 bytes of tail               name absent, no ACK                                  PASS
CP4   Apply: after fsync 1, before the marker  B2        OK (uncommitted tail: 19 bytes)              cut 19 bytes of tail               name absent, no ACK                                  PASS
CP5   Apply: marker written, before fsync 2    B3        OK                                           nothing to repair                  name=Ada, no ACK                                     PASS
CP6   Apply: after fsync 2, before index+ACK   A4        OK                                           nothing to repair                  name=Ada, no ACK                                     PASS
CP7   Apply: after the ACK                     A5        OK                                           nothing to repair                  name=Ada, ACKed                                      PASS
CP7b  Apply after a COMPACT: after the ACK     A5 C4     OK                                           nothing to repair                  name=Ada, a=version-39, ACKed                        PASS
CP8   Open: deleting a leftover tmp            C3 C5     OK + leftover db.kv.tmp                      deleted tmp                        city=Delhi, tmp left=false                           PASS
CP9   Open: scan done, nothing written yet     C1 F2     OK (uncommitted tail: 31 bytes)              cut 31 bytes of tail               torn absent                                          PASS
CP10  Open: after ftruncate, before its fsync  C2 F2     OK                                           nothing to repair                  torn absent                                          PASS
CP11  Compact: halfway through writing tmp     C3        OK + leftover db.kv.tmp                      deleted tmp                        a=version-39, city=Delhi, tmp left=false             PASS
CP12  Compact: tmp fsynced, before rename      C3        OK + leftover db.kv.tmp                      deleted tmp                        a=version-39, city=Delhi, tmp left=false             PASS
CP13  Compact: after the atomic rename         C4 C5     OK                                           nothing to repair                  a=version-39, city=Delhi, tmp left=false, 1439->72B  PASS
CP14  Create: tmp has header, before rename    creation  no db.kv yet + leftover db.kv.tmp            created new database               keys=0, tmp left=false                               PASS
RESULT: PASS 15/15 cases at all 14 crash points behave exactly as BREAKING.md says (0.4s)
```

The same 14 crash points are also checked in-process by the unit test `TestCrashPoints` (`src/kvdb/crash_test.go`).

### Random kills cover the gaps between crash points

The crash points prove the cases we thought of. The random-kill modes look for the ones we did not: `./torture` kills `serve` at a random millisecond (and, in half of the iterations, with `--step-delay-us` so the windows between I/O steps are wide enough to be hit). The summary at the end of each run shows the kills really landed inside operations, for example from `./torture --iterations 200 --seed 12345`:

```text
 Durability: every acknowledged write must survive
   acknowledged writes checked after restarts ........ 17,853   lost: 0
   kills that hit a write in progress ................ 198 of 200 iterations
 Atomicity: what happened to the 337 interrupted writes
   kept in full (committed, ACK not sent yet) ........ 51
   gone in full (never committed) .................... 286
   half-applied ...................................... 0   (never allowed)
 Recovery
   restarts that cut a half-written tail ............. 108 of 200   (72,238 bytes cut in total)
```

---

## 2. The ordering argument

### Why two fsyncs, and why in this order

```text
1. write data records          (pwrite at the end of the file)
2. fsync #1                    records are on the disk
3. write commit marker         (12 bytes, ValLen = number of records)
4. fsync #2                    marker is on the disk
5. update the in-memory index
6. ACK                         printed while the write lock is still held
```

* **ACK after fsync #2.** An ACK is a promise. When it is printed, the marker that commits the operation is already on the disk, so no crash can take it back (A5).
* **Marker after fsync #1.** Recovery treats a valid marker as "the N records before me are complete". That is only true if the records reached the disk before the marker did. A disk is allowed to write the bytes of one fsync interval in any order, so if records and marker were written together and synced once, a power cut could leave the marker on disk and the records not, and the marker would then commit garbage. fsync #1 is the fence that rules this out. (For a plain process kill the page cache keeps everything, so the single-fsync version would pass `kill -9` tests, which is exactly why those tests alone cannot prove power-loss safety.)
* **Index after fsync #2.** A reader can never see a value that is not yet durable. Readers take the same lock, so they see the batch either entirely or not at all (`TestReaderNeverSeesHalfBatch`).
* **ACK inside the commit.** `serve` prints the ACK from a callback that runs inside `Apply`, under the write lock. Two concurrent writers therefore cannot have their ACKs printed in a different order than their commits reached the disk.

### Why recovery can be killed and restarted

Recovery only reads until the scan has finished. Its only write to `db.kv` is `ftruncate(lastSafe)` followed by fsync (just before it, it saves a copy of the cut bytes to a separate `db.kv.rejected-*` file for inspection; that copy is never read back by the engine), and `lastSafe` is the end of a prefix that has just been validated. Killing it anywhere (CP8, CP9, CP10, and 200 random kills in `--mode recovery-crash`) leaves a file whose next scan reaches the same `lastSafe`.

### Why compaction cannot lose data

```text
1. write live records to db.kv.tmp     (O_EXCL; CRC of every record checked while copying)
2. write one marker for all of them
3. fsync db.kv.tmp
4. rename(db.kv.tmp, db.kv)            atomic: old file or new file, never a mix
5. switch the file handle, then point the index at the new offsets
6. fsync the directory                 POSIX only, so the rename survives a power cut
```

`open()` always trusts `db.kv` and deletes any `db.kv.tmp`, so a crash before step 4 means "compaction never happened" and a crash after it means "compaction finished". On POSIX the tmp file's open descriptor becomes the database's descriptor after the rename, so the file is never reopened. Windows cannot rename over a file with open handles, so there we close both, rename and reopen; if that reopen fails the engine poisons itself and refuses further writes rather than guess.

### Why a torn tail is cut but corruption is not

A crash can only damage the **end** of the file, the write that was in progress. So the rule is: a bad record with no valid marker after it is a torn tail, and we cut it. A bad record **with a valid marker after it** is damage in the middle of committed data; cutting there would silently delete acknowledged data, so `open` and `check` refuse with `CORRUPT` and do not touch the file.

### Why the cut tail is kept aside but never replayed

We considered replaying the records found after the last marker instead of cutting them. We rejected it: the batch may be incomplete (that is the half-batch the invariant forbids), the client got no ACK and may already have retried (replaying would write it twice, possibly over newer data), and the last record may be torn. So recovery cuts the tail from `db.kv`, but first saves a copy in `db.kv.rejected-<offset>-<crc>` that `./kvdb rejected db.kv` lists, so a person can see exactly which writes were lost and resend them. The engine never reads that copy, so it cannot affect any guarantee.

---

## 3. Known weaknesses

1. **Power loss is designed for but not tested.** Every test kills the process; the OS and its page cache survive. The two fsyncs and the directory fsync are what make power loss safe, and we argue it above, but no test of ours cuts power. We also rely on the disk honouring fsync; a drive whose write cache lies about it can lose acknowledged writes, and no software can detect that.
2. **A damaged last marker looks like a torn tail.** If something other than a crash damages the final commit marker (a bit flip, or someone cutting the last byte off by hand: scenario F4, `TestTruncateOneByte`), recovery cannot tell that from a torn write and cuts the last batch, even though it was acknowledged. The same applies to damage in a record's *length fields*: the record's end is then unknown, so it is treated as a torn tail. The corruption guard catches a damaged payload or CRC anywhere a valid marker follows. Bit rot is outside the crash model of the problem, but we want to be clear about it.
3. **Recovery is a full scan.** Recovery time grows with the file size. Auto-compaction keeps the file at about 2 × live data + 1 MiB, and 1,000,000 keys recover in 403–486 ms, but a database with gigabytes of live values would take proportionally longer.
4. **Compaction stops writers.** It holds the write lock for its whole duration, so writes wait while the live data is copied.
5. **The header has no checksum.** A damaged header makes `open` refuse with `CORRUPT`. Nothing is lost or truncated, but the file cannot be opened until it is repaired by hand.
6. **The whole index is in memory:** each key plus a few dozen bytes of map overhead. Values stay on disk.
7. **Two fsyncs per commit.** Throughput is bounded by disk latency. Concurrent writers queue on the lock; there is no group commit that merges their batches into one fsync pair.
8. **Windows is built but not torture-tested by us.** The Windows code paths (`LockFileEx`, close-rename-reopen, `TerminateProcess` crash points) compile and are vetted with `GOOS=windows`, but all our runs were on Linux. Windows has no directory fsync; we rely on NTFS journaling the rename.

---

## 4. What we tried that failed

These are real failures we hit while building and integrating, and what we changed.

1. **A single-fsync commit.** The first draft of this document described writing the records and the marker in one write and syncing once. It passes every `kill -9` test, because the page cache survives a process kill. We rejected it because of the power-loss reordering problem in §2 and moved to two fsyncs.
2. **A torture tool that could not fail.** Our first runs passed even with planted bugs (`no-truncate`, `no-marker`) because almost every kill landed *between* operations, where there is nothing to break. We added `--step-delay-us`, which widens the windows between I/O steps without reordering them, and a coverage report. Now the default run has an op in flight at 198 to 200 of 200 kills. The random modes catch all four planted bugs (README §9), but a random kill catches `no-marker` only when it lands between the pieces of a big batch, which took anywhere from 1 to 20 iterations. So we made the crash-point mode also check the client's ACKs and added CP7b: now `--mode crash-points` fails on every planted bug, on every run, in under a second.
3. **The 1M-key kill landed after the ACK.** In the scale test the kill meant to hit the 10,000-key batch arrived after it was already committed. Fixed with the same step delay; the kill now lands mid-batch and the recovered file has a 262,225-byte tail cut.
4. **`serve` reordered commands typed by hand.** Every request ran in its own goroutine, so `PUT 1 ...` then `GET 2 ...` could return the old value. We introduced lanes (the op-id prefix before `_`): in-order within a lane, concurrent across lanes.
5. **Reruns reported phantom keys.** A second `./torture` run reused the old `db.kv` with a fresh oracle and failed on keys from the first run. Every run now starts with a fresh database.
6. **Modes that ran but proved little.** `disk-full` had 99 of 100 iterations with zero ACKs (it only wrote, so it filled up and stayed full); we added deletes and compactions so the database keeps working at the cap. `large-values` took about 15 s per iteration because every write used a new key; we fixed the key space at 8 keys per lane.
7. **The file did not shrink after deleting everything.** A teammate's test put 1,000,000 keys, deleted them all and found 52,793,992 bytes still on disk. Auto-compaction only ran when the dead bytes exceeded `max(64 MiB, live bytes)`; with nothing live, 52 MB of dead bytes stayed under the 64 MiB floor forever. We lowered the floor to 1 MiB and added a second rule: when the database is empty and the file is larger than 28 bytes, compact (an empty compaction copies nothing). Now the same test ends at 28 bytes without any `COMPACT` command, and `./torture --mode scale` and `TestDeleteEverythingShrinks` check it.
8. **Wrong error codes.** A damaged header was reported as `IO_ERROR` instead of `CORRUPT`, and an oversized key as `IO_ERROR` instead of `BAD_REQUEST`. Errors are now matched by type (`errors.Is`), not by text.

---

## 5. If we had 24 more hours

1. **Group commit:** merge concurrent writers' batches into one fsync pair.
2. **Checkpointed index:** write the index next to the log after compaction so recovery reads it instead of scanning.
3. **Header CRC** and a format version upgrade path.
4. **A power-loss test** using a block device that drops unsynced writes (for example a `dm-flakey` device on Linux), to test the claims in §2 that `kill -9` cannot reach.
5. **Run the torture suite on Windows and macOS** in CI.
