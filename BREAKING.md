# BREAKING.md: How We Would Break This

Every row is a `// CPn - CRASH HERE` line in the code. `./torture --mode crash-points` kills the real `./kvdb` at each one (`--crash-at CPn`: SIGKILL, no cleanup), runs `check` and `open`, and compares with this table: 15/15 cases in under 1 s.

## 1. Crash points

Commit order in `Apply` (`src/kvdb/write.go`): space check → pwrite records (256 KiB pieces) → **fsync 1** → pwrite 12-byte marker → **fsync 2** → index → ACK.

| CP | Process dies in… | On disk | Next `open()` → result |
|:--|:--|:--|:--|
| CP1 | `Apply`: before any byte | ends on last marker | nothing to do → op absent |
| CP2 | `Apply`: between 256 KiB pieces | part of the batch, maybe mid-record | length/CRC fails, cut → batch absent |
| CP3 | `Apply`: records written, before fsync 1 | records (page cache), no marker | no marker, cut → op absent |
| CP4 | `Apply`: after fsync 1, before marker | records durable, no marker | no marker, cut → op absent |
| CP5 | `Apply`: marker written, before fsync 2 | records + marker (cache survives kill) | valid marker → op present, no ACK |
| CP6 | `Apply`: after fsync 2, before ACK | commit durable | op present, no ACK (A4) |
| CP7 | `Apply`: after the ACK (CP7b: after a `COMPACT`) | commit durable | op present |
| CP8 | `Open`: deleting leftover tmp | `db.kv` intact, tmp may remain | delete tmp again → all data |
| CP9 | `Open`: scan done, nothing written | unchanged | same scan → torn op absent |
| CP10 | `Open`: after `ftruncate`, before fsync | old length or already cut | same `lastSafe` → torn op absent |
| CP11 | `compact`: halfway through copy | `db.kv` untouched, partial tmp | `db.kv` wins, tmp deleted |
| CP12 | `compact`: tmp fsynced, before rename | both files complete | `db.kv` wins, tmp deleted |
| CP13 | `compact`: right after `rename` | `db.kv` = compacted file | normal scan → all data |
| CP14 | `createFile`: before rename | no `db.kv`, a tmp | delete tmp, create → empty db |

For CP1–CP6 the tool also checks that no `ACK` was printed before the kill; at CP7 one must have been.

## 2. The ordering argument

1. **pwrite records, then `fsync` (1).** The records are on disk before the marker exists. A disk may store the bytes of one fsync interval in any order, so without this fence a power cut could persist the marker but not the records, and the marker would commit garbage.
2. **pwrite marker, then `fsync` (2).** The marker carries the record count and a CRC32C. A marker on disk therefore proves its records are on disk and complete.
3. **Index update, then ACK, under the write lock.** `serve` prints the ACK from a callback inside `Apply`, so ACK order equals commit order and nothing is promised before fsync 2.
4. **Recovery** (`Open`) only reads until its scan ends. Its one write to `db.kv` is `ftruncate(lastSafe)` + `fsync`, to a prefix it has just validated, so a crashed recovery can be repeated (CP8–CP10). Before cutting, it copies the cut bytes to `db.kv.rejected-*` for a person to inspect (`kvdb rejected`); the engine never reads them back, because replaying could apply half a batch or a write the client already retried.
5. **Compaction**: live records + one marker into `db.kv.tmp` (O_EXCL) → `fsync` tmp → `rename(tmp, db.kv)` (atomic) → swap descriptor → `fsync` the **directory**, so the rename itself survives a power cut. `open()` always trusts `db.kv` and deletes any tmp: a crash before the rename means "never happened", after it means "done".
6. **A torn tail is cut, corruption is refused.** A kill only damages the end of the file. A complete record that fails its CRC *with a valid marker after it* is damage inside committed data, so `open` refuses with `CORRUPT` instead of truncating.
7. **A marker cut by up to 4 bytes is restored.** Its last 4 bytes (KeyLen 0, Flags 0x08, Pad 0) are constants, so `open` rebuilds it, checks its CRC and count, and appends the missing bytes. Cutting 1 byte off by hand (F4) loses nothing.

## 3. Known weaknesses

1. **Power loss is designed for but not tested.** All tests kill the process, and the page cache survives that. We also trust the disk to honour fsync.
2. **Cutting 5 or more bytes, or a bit flip in the last marker,** looks like a torn tail, so the last (acknowledged) commit is cut. Damage anywhere a valid marker follows is caught.
3. **Recovery is a full scan**, O(file size). Auto-compaction keeps the file under about 2 × live + 1 MiB; 1,000,000 keys recover in about 0.45 s, but gigabytes of live values would miss 2 s.
4. **Compaction holds the write lock** for the whole copy, and every commit costs two fsyncs with no group commit: writers queue.
5. **Windows** builds and vets, but our torture runs were on Linux. Windows has no directory fsync; we rely on NTFS journaling the rename.

## 4. What we tried that failed

1. **A single-fsync commit** (records and marker in one write). It passes every `kill -9` test because the page cache survives. Rejected for the power-loss reordering in §2.1.
2. **A torture tool that could not fail.** Planted bugs (`no-truncate`, `no-marker`) passed because kills landed between operations. Fix: `--step-delay-us` widens the gaps between I/O steps without reordering them, plus a coverage report. All four planted bugs are now caught (`-tags sabotage`, README §9).
3. **`serve` reordered hand-typed commands** (one goroutine per request). Fix: lanes by op-id prefix.
4. **The file did not shrink after deleting 1M keys** (52 MB left): auto-compaction waited for 64 MiB of dead space. Fix: a 1 MiB floor, and an empty database always compacts to 28 bytes.
5. **F4 lost an acknowledged write** when one byte was cut off by hand. Fix: §2.7. Also: reruns reported phantom keys (old `db.kv`, new oracle); every run now starts fresh.
6. **Our oracle had a race.** `--mode d2-same-key --seed 11` failed at iteration 38. The file showed the engine was right (`w1_iter38_57` committed before `w2_iter38_62`), but each lane recorded its own ACK after waking up, so the oracle saw them in the opposite order. Fix: ACKs are recorded by the one goroutine reading `serve`'s stdout, which is commit order.

## 5. If we had 24 more hours

1. A power-loss test on a block device that drops unsynced writes (`dm-flakey`).
2. Group commit: one fsync pair for several writers' batches.
3. A checkpointed index after compaction, so recovery reads it instead of scanning.
