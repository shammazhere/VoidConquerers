# DESIGN-EXTENSION.md: Per-Key Expiry (TTL)

## Question

How would you add per-key expiry (time-to-live) to this database without breaking any crash guarantee?

## Short answer

Store an absolute expiry time **inside the record**, covered by the CRC and committed by the same marker as the value. Expiry then needs no new kind of write and no new crash point: an expiring PUT is just a PUT, and recovery, batches and compaction already handle PUTs. The hard part is not the format; it is the four subtle problems at the end.

---

## 1. Format change

Today's record header (12 bytes): `CRC32C u32 | ValLen u32 | KeyLen u16 | Flags u8 | Pad u8`, then key, then value.

Add one flag bit and an optional 8-byte field:

```text
Flags bit 0x10 = HAS_EXPIRY   (valid combination: 0x15 = PUT | BATCH_OP | HAS_EXPIRY)

Record with expiry:
Offset  Size  Field
0       4     CRC32C      over bytes 4..end, so it now also covers ExpiresAt
4       4     ValLen
8       2     KeyLen
10      1     Flags       0x15
11      1     Pad         0
12      8     ExpiresAt   int64 LE, Unix time in milliseconds (absolute, not a duration)
20      K     Key
20+K    V     Value
```

* Records without the flag are byte-for-byte unchanged, so old records stay valid.
* The file header version becomes **2**. A v1 binary refuses a v2 file with `CORRUPT` (the header check already does this) instead of misreading `ExpiresAt` as key bytes. A v2 binary reads v1 files unchanged.
* A torn `ExpiresAt` fails the CRC exactly like a torn value, so the torn-tail rules need no change.

## 2. API and serve

```go
db.Apply([]kvdb.Op{{Type: kvdb.OpPut, Key: k, Value: v, ExpiresAt: t}})   // ExpiresAt 0 = never
```

`serve` gains `PUTX <op-id> <ttl-ms> <key> <value...>`. The engine converts the TTL to an absolute time **once per batch**, under the write lock, before writing (see problem 2).

## 3. Index, reads, recovery, compaction

* **Index:** `entry` gets `expiresAt int64`.
* **Get / dump:** a key whose `expiresAt` is in the past is reported as `NOTFOUND` / left out, even though it is still in the index and on disk.
* **Recovery:** no change in the commit rule. A committed expiring PUT is applied to the index like any PUT, *including when it has already expired*: it replaces the older version of the key, and reads then hide it. (Skipping it would resurrect the older version; see problem 1.)
* **Compaction:** when copying live records, drop the ones that have expired. Because compaction only ever copies the *latest* version of each key, dropping an expired one cannot bring back an older one.
* **Space accounting:** an expired record still counts in `liveBytes` until compaction drops it. The disk-full guard therefore overestimates usage, which is the safe direction; its "would it fit after compaction?" check can subtract the expired bytes to be exact.

---

## 4. The subtle problems

1. **Resurrection.** `PUT k v1` (no expiry), later `PUT k v2` with a 1-second TTL. After 1 second, `k` must be absent. If recovery or compaction simply *skipped* expired records, the index would still point at `v1`, and an old value would come back from the dead after a crash. The rule: an expired record still replaces older versions; it is only hidden from reads, and physically removed only by compaction, which has already discarded the older versions.

2. **Batch atomicity across the expiry moment.** A batch that sets `a` and `b` with the same TTL must expire as one unit, or a reader could see `a` but not `b`, which breaks the "never half a batch" guarantee for readers. So the engine computes `ExpiresAt` once per batch, not per record, and every read compares against one clock reading.

3. **The wall clock is not trustworthy.** Expiry must be stored as wall-clock time so it survives a restart, but NTP can move the clock backwards (a key that already disappeared reappears) or forwards (keys disappear early). Mitigations: record the highest time ever seen in the header area during compaction and treat "now" as `max(clock, highest seen)` so time never moves backwards for the database; and document that expiry precision is no better than the clock.

4. **Expired is not erased.** An expired value stays in the file until compaction rewrites it, and the old file may survive in free disk blocks after that. If expiry is used for sensitive data, the guarantee must be stated as "unreadable through the API", not "deleted from disk". Compaction can also be triggered when the expired bytes alone exceed the auto-compaction threshold.

## 5. How we would test it

* Add the expiry flag to the torture generator; the oracle gets the ACK time and the TTL of each op, and accepts a key only if `ack_time + ttl` puts it on the correct side of the time of the dump (with a tolerance for the clock reading inside the engine).
* A crash-point case: an expiring batch killed at CP3 to CP6 behaves exactly like a normal batch.
* The resurrection test: `PUT k v1`, `PUT k v2 ttl=1ms`, kill, wait, `open`, `get k` must be `NOTFOUND`, also after `COMPACT`.
