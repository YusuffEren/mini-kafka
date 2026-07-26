# mini-kafka Protocol Specification

This document is the **normative** reference for the mini-kafka binary wire protocol,
on-disk record format and protocol error codes. It is derived from the implementation
in `internal/protocol/` (codec, framing, request/response schemas), the record layout
in `internal/storage/record.go` and the dispatch error codes in
`internal/server/handler.go`.

The mini-kafka binary protocol operates over standard TCP connections. All multi-byte
numerical fields are encoded in **big-endian** (network byte order) using the
`encoding/binary` package from the Go standard library.

---

## 1. Frame Formatı

A frame is a length-prefixed sequence of fields encoded with the primitive codec
functions defined in Section 2. The leading `size` field is an `int32` giving the
byte count of **everything after the size field**; the receiver bounds the remaining
reads by `size` and rejects frames whose declared size is negative or exceeds
`MaxFrameSize` (`100 * 1024 * 1024` = 100 MB, see `frame.go`).

### 1.1 Request Frame

Fields are read/written by `ReadRequestFrame` / `RequestFrame.Write` in the exact
order below:

| # | Field         | Type   | Size | Description                                                       |
|---|---------------|--------|------|-------------------------------------------------------------------|
| 1 | size          | int32  | 4    | Byte count of fields 2..6 (the frame body)                        |
| 2 | apiKey        | int16  | 2    | API identifier (0=Produce, 1=Fetch, 12=ApiVersions)               |
| 3 | apiVersion    | int16  | 2    | API version requested by the client (currently always 1)         |
| 4 | correlationID | int32  | 4    | Client-supplied request identifier, mirrored in the response     |
| 5 | clientID      | string | var  | Length-prefixed (int16 + UTF-8) client name; -1 length = null      |
| 6 | payload       | bytes  | var  | API-specific request body (int32 length + raw bytes; -1 = null)   |

Total wire bytes = 4 (size) + size.

### 1.2 Response Frame

Fields are read/written by `ReadResponseFrame` / `ResponseFrame.Write` in the exact
order below:

| # | Field         | Type   | Size | Description                                                       |
|---|---------------|--------|------|-------------------------------------------------------------------|
| 1 | size          | int32  | 4    | Byte count of fields 2..4 (the frame body)                       |
| 2 | correlationID | int32  | 4    | Matches correlationID from the request being answered            |
| 3 | errorCode     | int16  | 2    | 0 = Success, non-zero = protocol error (see Section 5)           |
| 4 | payload       | bytes  | var  | API-specific response body (int32 length + raw bytes; -1 = null) |

Total wire bytes = 4 (size) + size.

### 1.3 Frame Validation

- `size < 0` or `size > MaxFrameSize` → `ErrFrameTooLarge`.
- The decoded body must consume **exactly** `size` bytes; any leftover byte means
  `ErrFrameSizeMismatch`.

---

## 2. Primitif Tipler

The primitive wire types implemented in `codec.go`. All multi-byte integers are
big-endian.

| Type      | Encoding                                                                  |
|-----------|---------------------------------------------------------------------------|
| `int8`    | 1 byte, signed                                                            |
| `int16`   | 2 bytes, signed big-endian                                                |
| `int32`   | 4 bytes, signed big-endian                                                |
| `int64`   | 8 bytes, signed big-endian                                                |
| `uint32`  | 4 bytes, unsigned big-endian                                              |
| `string`  | `int16` length + UTF-8 bytes; length `-1` means null string               |
| `bytes`   | `int32` length + raw bytes; length `-1` means null bytes                  |
| `array<T>`| `int32` element count + sequential elements of type T; count `-1` = null  |
| `bool`    | `int8`, `0` = false, `1` = true                                           |

A null sentinel (`-1` length / count) read in a non-nullable context yields
`ErrNullValue`.

---

## 3. Kayıt Formatı

A single immutable entry in a partition log, as implemented in
`internal/storage/record.go`. The layout is big-endian:

| # | Field       | Type  | Size | Description                                                        |
|---|-------------|-------|------|--------------------------------------------------------------------|
| 1 | length      | int32 | 4    | Byte count of fields 2..9 (everything after this field)           |
| 2 | offset      | int64 | 8    | Absolute offset within the partition                               |
| 3 | timestamp   | int64 | 8    | Unix milliseconds                                                  |
| 4 | crc32       | uint32| 4    | CRC32C (Castagnoli) over fields 5..9 (attributes..value)           |
| 5 | attributes  | int8  | 1    | Bit 0 = tombstone; bits 1-7 reserved (must be 0)                   |
| 6 | keyLength   | int32 | 4    | `-1` = null key (no key bytes follow); otherwise key byte count   |
| 7 | key         | bytes | var  | `keyLength` bytes (omitted when `keyLength == -1`)                |
| 8 | valueLength | int32 | 4    | `-1` = null value (no value bytes follow); otherwise value count |
| 9 | value       | bytes | var  | `valueLength` bytes (omitted when `valueLength == -1`)           |

### 3.1 CRC Kapsamı

The CRC32 checksum is computed using `crc32.Castagnoli` (CRC32C) over the
**CRC-covered region**, which spans `attributes` (field 5) through the end of
`value` (field 9). The `length`, `offset`, `timestamp` and `crc32` fields
themselves are **not** covered by the checksum.

### 3.2 Bounds

- `MaxRecordSize` = `1048576` (1 MiB), measured as the total encoded size
  including the leading `length` field. Records exceeding this are rejected with
  `ErrRecordTooLarge` on both encode and decode.
- A negative `length`, or a negative `keyLength`/`valueLength` other than the `-1`
  null sentinel, yields `ErrCorruptRecord`.
- A stored CRC that does not match the computed CRC yields `ErrCorruptRecord`.

---

## 4. API'ler

Each API is identified by its `apiKey` (Section 1.1, field 2). Request and response
payloads are encoded with the primitives from Section 2. All multi-byte integers are
big-endian.

### 4.1 Produce (ApiKey 0)

#### Request
- `Acks`: `int16` (0 = fire-and-forget, 1 = leader ack, -1 = all ISR)
- `TimeoutMs`: `int32`
- `Topics`: `array<ProduceRequestTopic>`
  - `Name`: `string`
  - `Partitions`: `array<ProduceRequestPartition>`
    - `PartitionID`: `int32`
    - `RecordSet`: `bytes` (sequential Section 3 records)

#### Response
- `Topics`: `array<ProduceResponseTopic>`
  - `Name`: `string`
  - `Partitions`: `array<ProduceResponsePartition>`
    - `PartitionID`: `int32`
    - `ErrorCode`: `int16`
    - `BaseOffset`: `int64`
    - `LogAppendTime`: `int64`

---

### 4.2 Fetch (ApiKey 1)

#### Request
- `MaxWaitMs`: `int32` (long-polling maximum wait duration)
- `MinBytes`: `int32`
- `MaxBytes`: `int32`
- `Topics`: `array<FetchRequestTopic>`
  - `Name`: `string`
  - `Partitions`: `array<FetchRequestPartition>`
    - `PartitionID`: `int32`
    - `FetchOffset`: `int64`
    - `MaxBytes`: `int32`

#### Response
- `Topics`: `array<FetchResponseTopic>`
  - `Name`: `string`
  - `Partitions`: `array<FetchResponsePartition>`
    - `PartitionID`: `int32`
    - `ErrorCode`: `int16`
    - `HighWatermark`: `int64`
    - `LogStartOffset`: `int64`
    - `RecordSet`: `bytes`

---

### 4.3 ApiVersions (ApiKey 12)

#### Request
Empty payload.

#### Response
- `ApiKeys`: `array<ApiVersion>`
  - `ApiKey`: `int16`
  - `MinVersion`: `int16`
  - `MaxVersion`: `int16`

---

## 5. Hata Kodları

Protocol error codes returned in the response frame's `errorCode` field (Section
1.2, field 3) and defined in `internal/server/handler.go`. `0` always means success.

| Code | Constant                       | Meaning                                                |
|------|--------------------------------|--------------------------------------------------------|
| 0    | `ErrNone`                      | Success                                                |
| 1    | `ErrUnknown`                   | Unexpected server error / handler panic                |
| 2    | `ErrOffsetOutOfRange`           | Requested offset is outside the valid range            |
| 3    | `ErrCorruptMessage`            | Message failed CRC validation                          |
| 4    | `ErrUnknownTopicOrPartition`   | Topic or partition does not exist                      |
| 5    | `ErrNotLeaderForPartition`     | Broker is not the leader for the partition             |
| 6    | `ErrRequestTimedOut`           | Request timed out                                      |
| 7    | `ErrMessageTooLarge`           | Message size exceeds the configured maximum            |
| 8    | `ErrNotEnoughReplicas`         | Not enough in-sync replicas to satisfy `acks`          |
| 9    | `ErrUnknownMemberID`           | Consumer group member ID is unknown                    |
| 10   | `ErrRebalanceInProgress`       | Consumer group is rebalancing                           |
| 11   | `ErrIllegalGeneration`         | Generation ID is invalid                                |
| 12   | `ErrInvalidGroupID`            | Consumer group ID is invalid                            |
| 13   | `ErrUnsupportedVersion`       | API key has no registered handler / version unsupported |
| 14   | `ErrTopicAlreadyExists`        | Topic already exists                                    |
| 15   | `ErrInvalidPartitionCount`     | Partition count is invalid                              |
| 17   | `ErrInvalidTopicException`     | Topic name failed validation (illegal chars, escape)  |

Code `16` is reserved and currently unused. When the dispatch layer (`Mux.Dispatch`)
finds no handler for an `apiKey`, it returns `ErrUnsupportedVersion` (13); a panicking
handler or a handler returning an error yields `ErrUnknown` (1).