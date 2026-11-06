# KIPI — Kill It and Prove It

A crash-safe, embedded key-value database written in Go. Standard library only. No dependencies.

---

## Build

```bash
go build -o . ./cmd/kvdb ./cmd/torture
```

On Windows the binaries are `kvdb.exe` and `torture.exe`. Run as `.\kvdb.exe` and `.\torture.exe`.

Requires Go 1.21 or later. Build works offline on a clean machine with only the Go toolchain.

---

## Commands

Replace `./kvdb` with `.\kvdb.exe` on Windows.

```bash
# Open or create the database, run recovery, print key count and recovery time
./kvdb open  <path> [--max-bytes N]

# Store a value under a key (durable on return)
./kvdb put   <path> <key> <value>

# Return the current value, or NOTFOUND
./kvdb get   <path> <key>

# Remove a key (durable on return)
./kvdb del   <path> <key>

# Apply a batch from a file atomically (one op per line: "put k v" or "del k")
./kvdb batch <path> <batchfile>

# Print every key and value, sorted by key (tab-separated)
./kvdb dump  <path>

# Verify file integrity without locking; prints OK or a reason
./kvdb check <path>

# Long-running mode: reads ops from stdin, prints ACK after each durable op
./kvdb serve <path> [--max-bytes N] [--step-delay-us N]
```

### serve protocol

Each line sent to stdin is: `<op-id> <command> [args]`

| Command | Syntax | Response |
|---------|--------|----------|
| put | `<id> put <key> <value>` | `ACK <id>` |
| del | `<id> del <key>` | `ACK <id>` |
| get | `<id> get <key>` | `VAL <id> <value>` or `NOTFOUND <id>` |
| batch | `<id> batch <n>` then n lines of `put k v` / `del k` | `ACK <id>` |
| compact | `<id> compact` | `ACK <id>` |
| dump | `<id> dump` | lines of `KV <id> <key> <value>`, then `ACK <id>` |

`ACK` is printed to stdout **only after both fsyncs complete** — it is a durability promise.
Protocol errors are returned as `ERR <op-id> <reason>`; startup and I/O diagnostics go to stderr.
Serve opens the database once and processes requests serially. Values may contain spaces; keys
in the line protocol are single tokens.

The batch-file form accepts one `put key value` or `del key` per line. Values may contain
spaces. The complete file is validated before its operations are passed to the engine's
atomic `Apply` API; batches are limited to 10,000 operations.

## CLI examples

```text
kvdb open demo.kv
opened demo.kv: 0 keys, recovery in 1.2ms

kvdb put demo.kv greeting "hello world"
OK

kvdb get demo.kv greeting
hello world

kvdb get demo.kv absent
NOTFOUND

kvdb dump demo.kv
greeting    hello world

kvdb check demo.kv
OK
```

`del` reports `OK` after the engine confirms its durable delete; the current engine treats
deleting an absent key as a successful no-op. `open --max-bytes` configures only that open
handle, while the database is closed when the command exits. Use `serve --max-bytes N` to
enforce a limit for the lifetime of a serve process.

## Tests

Run the CLI integration tests and engine tests with:

```bash
go test ./...
```

---

## Torture tool

```bash
./torture --iterations 500 --seed 12345
./torture --iterations 100 --seed 999  --mode large-values
./torture --iterations 100 --seed 7    --mode compaction
./torture --iterations 100 --seed 42   --mode recovery-crash
./torture --iterations 100 --seed 1    --mode full-disk
```

Flags: `--iterations N`, `--seed N` (required), `--mode <mode>`, `--max-bytes N`, `--work-dir <dir>`, `-v` (verbose).

---

## On-disk format

One authoritative file at `<path>`. Two transient files: `<path>.lock` (held while open) and `<path>.tmp` (only during compaction).

### File header (16 bytes)

```
Offset  Size  Field
0       4     Magic: "KIPI"
4       4     Version: 1 (uint32 LE)
8       8     Reserved (zeros)
```

### Record (13-byte header + payload)

```
Offset  Size  Field
0       1     Type: 1=put record, 2=delete tombstone, 3=commit marker
1       4     keyLen  (uint32 LE)
5       4     valLen  (uint32 LE, 0 for deletes and markers)
9       4     CRC32C of (type||keyLen||valLen||key||val) (uint32 LE)
13      keyLen  key bytes
13+kL   valLen  value bytes
```

### Commit marker (special record)

Type=3, keyLen=8, valLen=0, key=batchID (uint64 LE). CRC32C covers the whole record.

### Batch structure

```
[record 1] [record 2] ... [record N] [commit marker]
```

A batch is **committed** only when its commit marker is fully written and fsynced.

### Hex example

A single `put "hi" "yo"` followed by its commit marker:

```
Offset  Bytes                          Meaning
0000    4b 49 50 49                    Magic "KIPI"
0004    01 00 00 00                    Version 1
0008    00 00 00 00 00 00 00 00        Reserved
0010    01                             Type=put
0011    02 00 00 00                    keyLen=2
0015    02 00 00 00                    valLen=2
0019    XX XX XX XX                    CRC32C
001d    68 69                          key="hi"
001f    79 6f                          val="yo"
0021    03                             Type=marker
0022    08 00 00 00                    keyLen=8
0026    00 00 00 00                    valLen=0
002a    YY YY YY YY                    CRC32C
002e    00 00 00 00 00 00 00 00        batchID=0
```

---

## Recovery algorithm (plain English)

1. **Lock** — acquire an exclusive file lock (`LockFileEx` on Windows, `flock` on POSIX). If the lock fails, another process has the database open.
2. **Header** — if the file is empty, write the 16-byte KIPI header and fsync. Otherwise verify the magic and version.
3. **Clean tmp** — delete `<path>.tmp` if it exists (leftover from an aborted compaction; the old file is authoritative).
4. **Scan forward** from byte 16. For each record:
   - Read the 13-byte header.
   - Validate type, keyLen, valLen (reject values > 1 MB, keys > 1 KB).
   - Check that the record fits within the file.
   - Verify CRC32C. If wrong, stop scanning — everything after is the torn tail.
   - If type=put or type=delete: add to the pending batch buffer.
   - If type=marker: commit the pending buffer to the in-memory index; advance `validEnd`.
5. **Truncate** — if `fileSize > validEnd`, truncate to `validEnd` and fsync. This removes the torn tail.
6. **Ready** — the in-memory index is complete. All live keys point to their byte offsets in the file.

Recovery is **idempotent**: if killed during step 4 or 5, the next open repeats the scan from the beginning and reaches the same result.
