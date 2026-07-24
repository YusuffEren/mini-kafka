# Changelog

All notable changes to this project will be documented in this file.
Format based on [Conventional Commits](https://www.conventionalcommits.org/).

## [Unreleased]

### Project Skeleton
- `chore: initialize project skeleton`
  - Go module init (`github.com/yusuf/mini-kafka`)
  - Directory structure: `cmd/`, `internal/`, `pkg/`, `test/`, `benchmark/`, `config/`
  - Makefile with build, test, lint, bench, cover, run targets
  - `.gitignore`, `.golangci.yml` linter configuration
  - `config/broker.yaml` with full spec-driven configuration

### Phase 1 — Storage Layer
- `feat(storage): complete storage layer (record, index, segment, log)`
  - Record encode/decode with CRC32-Castagnoli checksum validation
  - Sparse offset index with mmap-backed binary search (`internal/storage/index.go`)
  - Segment management: `.log` + `.index` file pairs with 20-digit base offset naming
  - Partition Log: segment collection with append, read, truncate operations
  - Segment rotation on size (`segment.bytes`) and time (`segment.ms`) limits
  - Crash recovery: CRC-based scan, truncation of corrupt/partial records, index rebuild
  - Background retention: time-based (`retention.ms`) and byte-based (`retention.bytes`) cleanup
  - Background flushing: periodic fsync via `flush.ms`

### Phase 2 & 3 — Protocol, Server, Topic, Partition
- `feat(protocol): complete protocol & server layer (codec, frame, produce/fetch, long-poll)`
  - Primitive codec: big-endian int8/16/32/64, string, bytes, array encode/decode
  - TCP frame layer: 12-byte request header + response header with correlation ID
  - Produce (apiKey 0) and Fetch (apiKey 1) request/response schemas
  - ApiVersions (apiKey 12) endpoint
  - TCP server with accept-loop, connection-per-goroutine, graceful shutdown
  - API dispatch mux with `UnsupportedVersion` fallback
  - Long-poll fetch: blocks until `minBytes` available or `maxWaitMs` expires
  - Producer client with batching (`linger.ms`) and automatic reconnect
  - Consumer client with offset-based fetch
  - CLI binaries: `cmd/broker`, `cmd/producer`, `cmd/consumer`
  - Multi-topic, multi-partition support with Murmur2 partitioner (Kafka-compatible)
  - Metadata persistence to `meta/topics.json` (atomic write via `.tmp` + rename)
  - Metadata (apiKey 2) and CreateTopics (apiKey 3) API handlers
  - Auto-create topic support (`auto.create.topics.enable`)

### Phase 4 — Consumer Group & Offset Management
- `feat(coordinator): complete Phase 4 - group coordinator, assignors, offset store and group consumer client`
  - Group Coordinator state machine: Empty → PreparingRebalance → CompletingRebalance → Stable
  - JoinGroup, SyncGroup, Heartbeat, LeaveGroup API handlers (apiKeys 4-7)
  - Partition assignors: Range and RoundRobin strategies
  - Offset store: in-memory cache with JSON file persistence (`meta/offsets.json`)
  - OffsetCommit (apiKey 8) and OffsetFetch (apiKey 9) handlers
  - ListOffsets (apiKey 10) handler
  - GroupConsumer client with automatic rebalance, heartbeat goroutine, auto-commit
  - Rebalance callbacks: `OnPartitionsRevoked`, `OnPartitionsAssigned`

### Phase 5 & 6 — Replication, Benchmark, Docs
- `feat(benchmark): complete Phase 5 & 6 - replication, benchmark harness, docs and README`
  - ISR Tracker: per-replica LEO tracking, lag-based in-sync evaluation
  - High Watermark: `min(ISR LEOs)`, checkpoint persistence to `replication-offset-checkpoint`
  - Purgatory: `acks=all` requests wait for HW advancement
  - Leader Epoch: `EpochManager` for split-brain prevention, epoch validation
  - ReplicaFetch (apiKey 11) request/response
  - Benchmark harness with JSON output, latency histogram (p50/p95/p99)
  - Benchmark scenarios: single producer, message size, acks, consumer throughput
  - Full documentation: `docs/PROTOCOL.md`, `docs/PHASE_1.md` through `docs/PHASE_5.md`, `docs/BENCHMARK.md`
  - README with architecture diagram, quickstart, feature matrix

### Quality Gate
- `chore: add kontrol.sh quality gate, gofmt, and accumulated fixes`
  - `kontrol.sh` / `kontrol.ps1` quality gate scripts
  - gofmt formatting pass
  - Accumulated bug fixes and polish
