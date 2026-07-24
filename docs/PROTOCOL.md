# mini-kafka Protocol Specification

## 1. Overview
The mini-kafka binary protocol operates over standard TCP connections. All multi-byte numerical fields are encoded in **big-endian** (network byte order).

## 2. Frame Structure

### Request Frame
```
┌───────────────┬────────┬──────────────────────────────────────────┐
│ Field         │ Type   │ Description                              │
├───────────────┼────────┼──────────────────────────────────────────┤
│ size          │ int32  │ Byte count of frame after size field     │
│ apiKey        │ int16  │ API identifier                           │
│ apiVersion    │ int16  │ Currently version 1                      │
│ correlationID │ int32  │ Client-supplied request identifier       │
│ clientID      │ string │ Length-prefixed UTF-8 client name        │
│ payload       │ bytes  │ API-specific request body                │
└───────────────┴────────┴──────────────────────────────────────────┘
```

### Response Frame
```
┌───────────────┬────────┬──────────────────────────────────────────┐
│ Field         │ Type   │ Description                              │
├───────────────┼────────┼──────────────────────────────────────────┤
│ size          │ int32  │ Byte count of frame after size field     │
│ correlationID │ int32  │ Matches correlationID from request       │
│ errorCode     │ int16  │ 0 = Success, non-zero = Protocol error   │
│ payload       │ bytes  │ API-specific response body               │
└───────────────┴────────┴──────────────────────────────────────────┘
```

## 3. Primitive Types

| Type | Encoding |
|---|---|
| `int8`, `int16`, `int32`, `int64` | Signed big-endian integer |
| `uint32` | Unsigned big-endian integer |
| `string` | `int16` length + UTF-8 bytes (-1 length = null string) |
| `bytes` | `int32` length + raw byte payload (-1 length = null bytes) |
| `array<T>` | `int32` element count + sequential elements (-1 = null array) |

## 4. API Keys & Schemas

### Produce (ApiKey 0)
#### Request
- `Acks`: `int16` (0 = fire-and-forget, 1 = leader ack, -1 = all ISR)
- `TimeoutMs`: `int32`
- `Topics`: `array<ProduceRequestTopic>`
  - `Name`: `string`
  - `Partitions`: `array<ProduceRequestPartition>`
    - `PartitionID`: `int32`
    - `RecordSet`: `bytes` (sequential 4.1 format records)

#### Response
- `Topics`: `array<ProduceResponseTopic>`
  - `Name`: `string`
  - `Partitions`: `array<ProduceResponsePartition>`
    - `PartitionID`: `int32`
    - `ErrorCode`: `int16`
    - `BaseOffset`: `int64`
    - `LogAppendTime`: `int64`

---

### Fetch (ApiKey 1)
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

### ApiVersions (ApiKey 12)
#### Request
Empty payload.

#### Response
- `ApiKeys`: `array<ApiVersion>`
  - `ApiKey`: `int16`
  - `MinVersion`: `int16`
  - `MaxVersion`: `int16`
