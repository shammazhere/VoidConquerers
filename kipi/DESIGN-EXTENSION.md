# DESIGN-EXTENSION.md — Expiry Question

## Question

How would you add per-key TTL (time-to-live) expiry to this database?

---

## Format changes

### Record header extension

Add a 4-byte field `expiresAt` (uint32 Unix timestamp, seconds) to the record header, making it 17 bytes instead of 13:

```
Offset  Size  Field
0       1     Type (1=put, 2=delete, 3=marker)
1       4     keyLen  (uint32 LE)
5       4     valLen  (uint32 LE)
9       4     expiresAt (uint32 LE, 0 = no expiry)
13      4     CRC32C (now covers expiresAt too)
17      keyLen  key bytes
17+kL   valLen  value bytes
```

The `expiresAt` field is included in the CRC32C computation so that a corrupt expiry is detected on recovery.

### In-memory index extension

Add `expiresAt uint32` to `indexEntry`. On `Get`, check `time.Now().Unix() > int64(e.expiresAt)` before returning the value; return `ErrNotFound` if expired.

### New op type for `Put` with TTL

```go
db.PutWithTTL(key, value []byte, ttl time.Duration) error
```

This encodes `expiresAt = uint32(time.Now().Add(ttl).Unix())` in the record. A TTL of 0 means no expiry.

---

## Recovery changes

During the forward scan in `recovery.go`, when a committed record has `expiresAt > 0`:
- Check if `expiresAt <= uint32(time.Now().Unix())`.
- If expired: **do not add to the index**. The record is treated as if it were a delete tombstone.
- If not expired: add to index with `expiresAt` populated.

This means recovery is TTL-aware: expired keys do not appear in the database after restart, even if they were committed before the crash.

---

## Compaction changes

During compaction, skip records where `e.expiresAt > 0 && uint32(time.Now().Unix()) >= e.expiresAt`. This reclaims space for expired keys without an explicit delete.

---

## At least one subtle problem

**Clock skew and monotonicity.** TTL expiry depends on wall-clock time. If the system clock is adjusted backward (NTP correction, leap second), a key that "expired" may become "alive" again on the next `Get` call. Worse, if the system clock is adjusted forward by a large amount, keys that should still be live are silently expired.

Mitigation options:
1. Use a monotonic clock for in-process TTL checks (but monotonic clocks do not survive restart — the absolute expiry timestamp must still be a wall-clock time).
2. Add a "skew tolerance" window (e.g., treat a key as expired only if `now > expiresAt + 5 seconds`).
3. Document that KIPI does not guarantee TTL precision beyond ±the NTP adjustment window.

A second subtle problem: **expired keys are still on disk**. A client that calls `Get` and receives `ErrNotFound` may assume the key was never written, but a hex dump of the file would show the value. If the value is sensitive, compaction must be run explicitly to purge expired records from disk. This is not unique to KIPI (Redis has the same property in its RDB snapshots), but it must be documented.

