# mini-kafka 🚀

Zero-dependency, high-performance Apache Kafka protocol-compatible distributed streaming platform written in pure Go.

```
                  +-----------------------------------+
                  |             mini-kafka            |
                  +-----------------+-----------------+
                                    |
            +-----------------------+-----------------------+
            |                       |                       |
            v                       v                       v
  +------------------+    +------------------+    +------------------+
  |  Storage Engine  |    | Protocol Transport|   | Group Coordinator|
  |  Log / Segment   |    | Frame / Codec    |    | Rebalance / ISR  |
  |  Memory Map Index|    | TCP Server / Mux |    | Assignors        |
  +------------------+    +------------------+    +------------------+
```

## Features ✨

- **Custom Storage Engine**: Append-only log files with binary sparse `.index` indexing, automatic 128MB log segment rolling, and configurable retention.
- **Kafka Protocol Compatibility**: Binary request/response frame codec implementation supporting `Produce` (apiKey 0), `Fetch` (apiKey 1), `Metadata` (apiKey 2), `CreateTopics` (apiKey 3), `JoinGroup` (apiKey 4), `SyncGroup` (apiKey 5), `Heartbeat` (apiKey 6), `LeaveGroup` (apiKey 7), `OffsetCommit` (apiKey 8), `OffsetFetch` (apiKey 9), `ListOffsets` (apiKey 10), and `ReplicaFetch` (apiKey 11).
- **Partition Router**: Key-based Murmur2 hash partitioner for uniform record distribution.
- **Consumer Group Coordinator**: State machine managing rebalancing (`Range` and `RoundRobin` assignment strategies), session timeouts, auto-commits, and committed offset persistence.
- **Replication & ISR**: Leader-follower replica tracking with High Watermark (HW) consistency bounds, `replication-offset-checkpoint` persistence, and Leader Epoch split-brain prevention.

---

## Quickstart ⚡

```bash
# 1. Build binaries
make build

# 2. Run single broker
./bin/broker -config config/broker.yaml

# 3. Produce messages using CLI
./bin/producer -brokers 127.0.0.1:9092 -topic my-topic -key "user1" -value "hello mini-kafka"

# 4. Consume messages using CLI
./bin/consumer -brokers 127.0.0.1:9092 -topic my-topic -group my-group
```

---

## Supported Features vs Apache Kafka 📊

| Feature | Apache Kafka | mini-kafka | Notes |
|---|---|---|---|
| Binary Log Storage | ✅ | ✅ | Custom `.log` & `.index` implementation |
| Murmur2 Partitioner | ✅ | ✅ | Bitwise identical to Kafka Java client |
| Group Coordinator | ✅ | ✅ | Range & RoundRobin assignors supported |
| Replication & ISR | ✅ | ✅ | High Watermark (HW) & Leader Epoch |
| Zero-Copy (`sendfile`)| ✅ | ❌ | User-space buffer reading in Go |
| Record Compression | ✅ (LZ4/Snappy) | ❌ | Raw uncompressed record batches |

---

## Benchmarks 📈

See detailed methodology, throughput numbers, and JVM vs Go latency comparisons in [BENCHMARK.md](docs/BENCHMARK.md).

---

## Architecture & Learnings 🧠

Read complete technical deep dives in the `docs/` folder:
- [Phase 1: Storage Layer](docs/PHASE_1.md)
- [Phase 2: Protocol Transport](docs/PHASE_2.md)
- [Phase 3: Topic & Partition Management](docs/PHASE_3.md)
- [Phase 4: Consumer Groups & Offsets](docs/PHASE_4.md)
- [Phase 5: Replication & ISR](docs/PHASE_5.md)
