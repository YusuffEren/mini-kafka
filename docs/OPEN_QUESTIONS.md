# Open Questions & Design Decisions

This document records design decisions made during development where the spec
was ambiguous or silent. Each entry follows the format mandated by
`MINI_KAFKA_SPEC.md` Section 1.5 (Belirsizlik Politikası):

> 1. Pick the simplest solution.
> 2. Document it here: question, chosen solution, alternatives, rationale.
> 3. Leave a `// DECISION:` comment in the code.

---

## 1. Why No Consensus Protocol for Controller Election?

**Question:** How does the cluster elect a controller broker to manage leader
assignments and failover?

**Chosen Solution:** Static configuration. The controller is the broker with
the lowest `nodeID` in `config/broker.yaml`. No dynamic election.

**Alternatives:**
- Raft-based controller election (like KRaft in Kafka 3.x+)
- ZooKeeper-based coordination
- Gossip protocol for leader discovery

**Rationale:** A full consensus protocol (Raft, Paxos) is a project in itself.
The spec explicitly scopes out ZooKeeper/KRaft. Static assignment is the
simplest solution that still demonstrates leader-follower replication. For an
educational project, the interesting part is ISR and High Watermark, not
consensus. Dynamic election can be added as a bonus phase.

**Code Reference:** `internal/replication/epoch.go` — `EpochManager` uses
in-memory state, no inter-broker coordination.

---

## 2. Why a Custom Binary Protocol Instead of Kafka Wire Protocol?

**Question:** Should mini-kafka implement the Kafka binary protocol for
compatibility with real Kafka clients?

**Chosen Solution:** Custom binary protocol over raw TCP. Frame structure is
similar to Kafka's but not compatible.

**Alternatives:**
- Implement Kafka's actual wire protocol (apiKey-compatible)
- Use HTTP/REST (like Kafka REST Proxy)
- Use gRPC/protobuf

**Rationale:** The spec explicitly states Kafka wire protocol compatibility is
out of scope ("kendi protokolümüz olacak"). A custom protocol is more
educational — every byte is a deliberate design choice. HTTP adds overhead
irrelevant to the learning goals. gRPC would hide the serialization details
we want to understand. The custom protocol mirrors Kafka's structure
(length-prefixed frames, big-endian, correlation IDs) so concepts transfer.

**Code Reference:** `internal/protocol/codec.go`, `internal/protocol/frame.go`

---

## 3. Why JSON File for Offset Store Instead of `__consumer_offsets` Topic?

**Question:** Where and how should consumer group offsets be persisted?

**Chosen Solution:** JSON file at `meta/offsets.json`. In-memory map cache
with atomic file writes (`.tmp` + rename).

**Alternatives:**
- Internal `__consumer_offsets` topic with 50 partitions (Kafka's approach)
- Embedded key-value store (BoltDB, LevelDB)
- In-memory only (no persistence)

**Rationale:** The spec mentions `__consumer_offsets` as an internal topic, but
implementing a full compacted log topic for offsets adds significant complexity
(separate log compaction, coordinator partitioning, key-based deduplication).
A JSON file is the simplest durable solution. It survives broker restarts,
is human-readable for debugging, and the atomic rename pattern prevents
corruption. For an educational project, the interesting part is the consumer
group state machine, not the offset storage format.

**Code Reference:** `internal/coordinator/offsets.go` — `OffsetStore.saveLocked()`

---

## 4. Why mmap for Index Instead of Standard File I/O?

**Question:** How should the sparse index be read for offset lookups?

**Chosen Solution:** Memory-mapped file (`mmap`) for the entire index region.
The file is preallocated to `index.max.bytes` at open time and mapped into
the process address space.

**Alternatives:**
- Standard `os.File.Read` with `Seek` for each lookup
- `bufio.Reader` with caching
- `pread` syscall (positional read without seek)

**Rationale:** The index is accessed via binary search, which requires random
access to arbitrary entries. `mmap` provides zero-copy reads directly from the
page cache — the OS handles paging. Standard file I/O would require explicit
`Seek` + `Read` syscalls for each binary search probe. `mmap` also allows
the `Append` path to write directly into the mapped region without additional
copy. The tradeoff is platform-specific syscall wrappers (`unix.Mmap`), but
`golang.org/x/sys` is an allowed dependency.

**Code Reference:** `internal/storage/index.go` — `mmapIndex()`, `Index.mmapData`

---

## 5. Why Partition-Bound Single Writer Goroutine?

**Question:** How should concurrent produce requests to the same partition be
handled?

**Chosen Solution:** Each partition has a dedicated writer goroutine with a
buffered channel. All append requests are serialized through this channel.

**Alternatives:**
- `sync.Mutex` on each partition (simple locking)
- Lock-free append with atomic operations
- Per-request goroutines with mutex contention

**Rationale:** Mutex-based locking causes contention under high write load —
every produce request fights for the same lock. A single writer goroutine
eliminates contention entirely: requests are enqueued to a channel and
processed sequentially. This also naturally enables batching (multiple
requests buffered in the channel can be written in one I/O operation). The
channel acts as a backpressure mechanism. This pattern matches Kafka's
partition-level write ordering guarantee.

**Code Reference:** `internal/broker/partition.go` — `appendCh chan appendRequest`

---

## 6. Why Pull-Based Follower Replication Instead of Push?

**Question:** How do follower replicas receive data from the leader?

**Chosen Solution:** Followers continuously poll the leader via
`ReplicaFetch` requests (pull model). The leader serves these like regular
fetches but tracks each replica's LEO.

**Alternatives:**
- Leader pushes new records to followers (push model)
- Shared write-ahead log (WAL) replicated via consensus
- Asynchronous message queue between brokers

**Rationale:** Kafka uses pull-based replication, and for good reason:
- The follower controls its own pace (natural backpressure).
- The leader doesn't need to manage connections to followers.
- A slow follower doesn't block the leader.
- Failed fetches are naturally retried on the next poll.

Push-based replication would require the leader to track follower
connections, handle slow followers, and implement retry logic — all
unnecessary complexity. The pull model also reuses the existing Fetch
handler infrastructure.

**Code Reference:** `internal/replication/replication.go` — `ReplicaFetch` handler

---

## 7. Why Murmur2 for Partition Assignment?

**Question:** How should messages without a key be assigned to partitions?

**Chosen Solution:** Keyed messages use `Murmur2(key) % numPartitions`
(Kafka-compatible). Keyless messages use round-robin.

**Alternatives:**
- CRC32-based hashing (older Kafka versions)
- FNV-1a hash
- Consistent hashing
- Random assignment

**Rationale:** Kafka uses Murmur2 for key-based partitioning (since 0.8+).
Using the same hash function means that if a user migrates from mini-kafka
to real Kafka with the same key and partition count, messages land on the
same partitions. This is an educational project — matching Kafka's behavior
where possible makes the learning more transferable. Round-robin for keyless
messages is Kafka's default behavior.

**Code Reference:** `internal/broker/topic.go` — `Murmur2()` function

---

## 8. Why Leader Epoch Instead of Fencing Tokens?

**Question:** How does the system prevent split-brain writes when a former
leader rejoins after a network partition?

**Chosen Solution:** Monotonically increasing `LeaderEpoch` per partition.
Each leader change increments the epoch. Producers and followers must present
the current epoch; stale epochs are rejected with `NotLeaderForPartition`.

**Alternatives:**
- Fencing tokens (STONITH-style)
- Vector clocks
- Last-write-wins with timestamps
- No protection (accept potential divergence)

**Rationale:** Leader Epoch is Kafka's actual mechanism (introduced in KIP-279).
It's simple, effective, and well-understood. When a former leader rejoins, it
discovers its epoch is stale, truncates its log to the new leader's High
Watermark, and becomes a follower. This prevents the "log divergence" problem
where two brokers think they're leader and accept conflicting writes. Vector
clocks are overkill for a single-leader model. Fencing tokens require external
infrastructure.

**Code Reference:** `internal/replication/epoch.go` — `EpochManager`

---

## 9. Why Big-Endian Serialization?

**Question:** What byte order should the binary protocol and storage format use?

**Chosen Solution:** Big-endian (network byte order) for all multi-byte fields.

**Alternatives:**
- Little-endian (native x86/ARM order)
- Variable-length encoding (like protobuf varint)
- Self-describing format (TLV — type-length-value)

**Rationale:** Kafka uses big-endian. Network protocols traditionally use
big-endian (it's called "network byte order" for a reason). Using the same
byte order as Kafka makes the format comparable and the educational
translation easier. Little-endian would be slightly faster on x86 (no byte
swap needed) but the performance difference is negligible for an educational
project. Variable-length encoding adds complexity to both encode and decode
paths.

**Code Reference:** All `binary.BigEndian` calls in `internal/protocol/codec.go` and `internal/storage/record.go`
