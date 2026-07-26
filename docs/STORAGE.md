# Storage Layer — Normative Specification

This document describes the on-disk storage format used by mini-kafka's partition log.
It is derived from `docs/PROTOCOL.md` Section 3 and the implementation in `internal/storage/`.

---

## 1. Overview

Each partition is backed by a **Log**: an ordered, append-only collection of **Segments**.
At any time exactly one segment is **active** (receiving writes); the rest are **sealed** (read-only).

```
partition-0/
├── 00000000000000000000.log      ← sealed segment
├── 00000000000000000000.index
├── 00000000000000016384.log      ← sealed segment
├── 00000000000000016384.index
├── 00000000000000032768.log      ← active segment (receives appends)
└── 00000000000000032768.index
```

---

## 2. Record Format

All multi-byte fields are **big-endian**. A single record on disk:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        length (int32)                         |  bytes 0-3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                       offset (int64)                          |  bytes 4-11
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                     timestamp (int64)                         |  bytes 12-19
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       crc32 (uint32)                          |  bytes 20-23
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   attributes  |                                               |
|    (int8)     |           ... variable payload ...            |  bytes 24+
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Field Details

| Field        | Type   | Size    | Description                                              |
|-------------|--------|---------|----------------------------------------------------------|
| `length`     | int32  | 4 bytes | Byte count of everything **after** this field             |
| `offset`     | int64  | 8 bytes | Absolute offset within the partition (monotonically increasing) |
| `timestamp`  | int64  | 8 bytes | Unix milliseconds at append time                          |
| `crc32`      | uint32 | 4 bytes | CRC32-Castagnoli over `attributes` through end of `value` |
| `attributes` | int8   | 1 byte  | Bit 0: tombstone flag; bits 1-7: reserved                 |
| `keyLength`  | int32  | 4 bytes | Length of `key`; **-1 = null key** (no key bytes follow)  |
| `key`        | bytes  | variable| `keyLength` bytes (omitted when keyLength == -1)          |
| `valueLength`| int32  | 4 bytes | Length of `value`; **-1 = null value** (no value bytes follow) |
| `value`      | bytes  | variable| `valueLength` bytes (omitted when valueLength == -1)      |

### CRC32 Rules

- Algorithm: **CRC32-Castagnoli** (`hash/crc32` with `crc32.Castagnoli` polynomial)
- Covered bytes: from `attributes` byte through the last byte of `value`
- On read: CRC is recomputed and compared; mismatch → `ErrCorruptRecord`
- Maximum record size: `max.message.bytes` config (default 1 MB); exceeding → `ErrRecordTooLarge`

### Tombstone Support

When `attributes` bit 0 is set, the record is a **tombstone** — it marks a key for deletion.
The key and value may both be null (keyLength=-1, valueLength=-1).

### Minimum Record Size

A record with null key and null value occupies:
`4 (length) + 8 (offset) + 8 (timestamp) + 4 (crc32) + 1 (attributes) + 4 (keyLength=-1) + 4 (valueLength=-1) = 33 bytes`

---

## 3. Segment File Naming

Each segment consists of two files sharing a common base offset prefix:

```
{baseOffset zero-padded to 20 digits}.log
{baseOffset zero-padded to 20 digits}.index
```

Examples:
```
00000000000000000000.log       baseOffset = 0
00000000000000000000.index
00000000000000016384.log       baseOffset = 16384
00000000000000016384.index
00000000000000032768.log       baseOffset = 32768
00000000000000032768.index
```

The 20-digit zero-padding ensures lexicographic sort equals numeric sort.
File discovery: scan directory for `*.log` files, parse the 20-digit stem as int64.

---

## 4. Sparse Index Format

The index maps **relative offsets** to **byte positions** in the companion `.log` file.
Each entry is a fixed-width **8 bytes**, big-endian:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    relativeOffset (uint32)                    |  bytes 0-3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      position (uint32)                        |  bytes 4-7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field            | Type   | Description                                           |
|-----------------|--------|-------------------------------------------------------|
| `relativeOffset` | uint32 | `absoluteOffset - segment.BaseOffset`                  |
| `position`       | uint32 | Byte offset within the `.log` file where the record starts |

### Sparse Indexing Rules

- **Not every record is indexed.** An entry is written every `index.interval.bytes` (default 4096) bytes of log data.
- The **first record** of a segment is always indexed (ensures a lookup starting point).
- Lookup uses **binary search** (`sort.Search`) on `relativeOffset`:
  - Exact match → return position with `found=true`
  - No exact match → return position of the **nearest lower entry** with `found=false`
  - Sequential scan from that position finds the target record

### mmap Access

- The index file is **preallocated** to `index.max.bytes` (default 10 MB) at open time.
- The entire file is **memory-mapped** (`mmap`) for zero-copy reads.
- `Append` writes directly into the mmap region.
- On segment close, the file is **truncated** to the actual logical size (`entries * 8`).

### Capacity

- Maximum entries per segment: `index.max.bytes / 8` = 1,310,720 entries (at default 10 MB)
- When the index is full, `Append` returns `ErrIndexFull`, triggering segment rotation.

---

## 5. Segment Rotation

A new segment is opened (the active segment is **sealed**) when any of these conditions is met:

| Condition | Config Key | Default | Description |
|-----------|-----------|---------|-------------|
| Size limit | `segment.bytes` | 128 MB | Active `.log` file size >= threshold |
| Time limit | `segment.ms` | 7 days | Active segment age >= threshold |
| Index full | `index.max.bytes` | 10 MB | No room for another index entry |

### Rotation Sequence

1. `Flush()` the outgoing segment (fsync buffered data)
2. New segment's `baseOffset = outgoingSegment.NextOffset`
3. Create new `{baseOffset}.log` and `{baseOffset}.index` files
4. Update `Log.active` pointer
5. Reset flush counters

### Time-Based Rotation

Time-based rotation only fires when the segment has **at least one record**.
An empty active segment is never rotated (it would just be replaced by another empty one).

---

## 6. Recovery Process

On broker startup, each partition's Log is recovered:

```
1. Scan partition directory for {20-digit}.log files
2. Sort by base offset ascending
3. Reopen all segments
4. For the ACTIVE segment (highest base offset):
   a. Flush any buffered writer
   b. Discard existing index, create fresh one
   c. Read .log from beginning, record by record:
      - Decode record, verify CRC32
      - On success: update index, advance NextOffset
      - On CRC failure or EOF: stop scan
   d. Truncate .log to last valid byte position
   e. Reposition write cursor at new EOF
5. Set LEO = active segment.NextOffset
6. Start background retention goroutine
```

### Key Properties

- **Partial write safety:** If the broker crashed mid-write, the trailing incomplete record is detected by CRC mismatch and discarded.
- **Index rebuild:** The index is always rebuilt from scratch during recovery, guaranteeing consistency.
- **Sealed segments** are not scanned end-to-end; their `NextOffset` is set by the upper-level Log from the next segment's `BaseOffset`.

---

## 7. Concurrency Model

| Operation | Lock Type | Scope |
|-----------|----------|-------|
| `Log.Append` | `Log.mu` write lock | Entire append (may trigger rotation) |
| `Log.Read` | `Log.mu` read lock | Segment lookup + read |
| `Log.ReadFrom` | `Log.mu` read lock | Cross-segment sequential scan |
| `Segment.Append` | `Segment.mu` write lock | Single record write |
| `Segment.Read` | `Segment.mu` read lock | Index lookup + scan |
| `Index.Append` | `Index.mu` write lock | Mmap write |
| `Index.Lookup` | `Index.mu` read lock | Binary search on mmap data |

The upper-level Log serializes appends per partition (single writer goroutine in the broker layer).
Reads use separate file handles to avoid contention with the shared write cursor.

---

## 8. Flush and Durability

| Config | Default | Behavior |
|--------|---------|----------|
| `flush.ms` | 1000 ms | Background goroutine fsyncs active segment at this interval |
| `flush.messages` | 0 (OS decides) | Fsync after every N records appended |

- `Flush()` calls `writer.Flush()` (buffered → OS) then `logFile.Sync()` (OS → disk).
- `FlushWriter()` only flushes the buffered writer (no fsync) — used to make unflushed data visible to reads.

---

## 9. Retention

A background goroutine periodically applies retention policies:

### Time-Based Retention
- A sealed segment is deleted when `time.Now() - segment.CreatedAt >= retention.ms`
- Default: 7 days
- `-1` disables time-based retention

### Byte-Based Retention
- Oldest sealed segments are deleted until total size <= `retention.bytes`
- Default: `-1` (unlimited)
- The active segment is never deleted

---

## 10. Configuration Reference

| Config Key | Type | Default | Description |
|-----------|------|---------|-------------|
| `segment.bytes` | int64 | 134217728 (128 MB) | Max `.log` file size before rotation |
| `segment.ms` | int64 | 604800000 (7 days) | Max active segment age before rotation |
| `index.interval.bytes` | int64 | 4096 | Log bytes between index entries |
| `index.max.bytes` | int64 | 10485760 (10 MB) | Max `.index` file size |
| `retention.ms` | int64 | 604800000 (7 days) | Sealed segment max age (-1 = unlimited) |
| `retention.bytes` | int64 | -1 | Total log max size (-1 = unlimited) |
| `flush.ms` | int64 | 1000 | Fsync interval for active segment |
| `flush.messages` | int64 | 0 | Fsync every N records (0 = OS decides) |
| `max.message.bytes` | int64 | 1048576 (1 MB) | Max single record size |
