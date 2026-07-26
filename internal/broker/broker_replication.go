package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/replication"
	"github.com/YusuffEren/mini-kafka/internal/storage"
)

// errProduceTimeout is returned when an acks=all produce exceeds TimeoutMs.
var errProduceTimeout = errors.New("produce: acks=all timed out")

// clusterReplicaIDs returns broker IDs that form the static replica set.
// When the cluster list is empty, the local broker is the sole replica.
func clusterReplicaIDs(cfg *config.Config) []int32 {
	if cfg == nil {
		return []int32{1}
	}
	if len(cfg.Cluster.Brokers) == 0 {
		return []int32{int32(cfg.Broker.ID)}
	}
	ids := make([]int32, 0, len(cfg.Cluster.Brokers))
	seen := make(map[int32]struct{}, len(cfg.Cluster.Brokers))
	for _, br := range cfg.Cluster.Brokers {
		id := int32(br.ID)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return []int32{int32(cfg.Broker.ID)}
	}
	return ids
}

func (b *Broker) minInsyncReplicas() int {
	n := b.config.Replication.MinInsyncReplicas
	if n <= 0 {
		return 1
	}
	return n
}

func (b *Broker) lagTimeMaxMs() int64 {
	ms := b.config.Replication.ReplicaLagTimeMaxMs
	if ms <= 0 {
		return 30000
	}
	return ms
}

// staticPartitionLeader returns the config-derived leader for a partition.
// DECISION: leader = replicaIDs[partition % len(replicaIDs)] (no consensus).
func (b *Broker) staticPartitionLeader(partition int32) int32 {
	if len(b.replicaIDs) == 0 {
		return b.localID
	}
	idx := int(partition) % len(b.replicaIDs)
	if idx < 0 {
		idx = -idx
	}
	return b.replicaIDs[idx]
}

// partitionLeader returns the effective leader: EpochManager if tracked, else static.
func (b *Broker) partitionLeader(topic string, partition int32) int32 {
	_, leaderID, err := b.epochMgr.GetEpoch(topic, partition)
	if err != nil {
		return b.staticPartitionLeader(partition)
	}
	return leaderID
}

// isLeader reports whether this broker is the current leader for the partition.
// Uses EpochManager as the source of truth (split-brain fence).
func (b *Broker) isLeader(topic string, partition int32) bool {
	_, leaderID, err := b.epochMgr.GetEpoch(topic, partition)
	if err != nil {
		// Untracked: seed epoch from static assignment.
		static := b.staticPartitionLeader(partition)
		b.epochMgr.IncrementEpoch(topic, partition, static)
		return static == b.localID
	}
	return leaderID == b.localID
}

// initTopicReplication creates ISR trackers and leader epochs for every partition.
func (b *Broker) initTopicReplication(topic string, numPartitions int32) {
	for p := int32(0); p < numPartitions; p++ {
		if _, _, err := b.epochMgr.GetEpoch(topic, p); err != nil {
			leader := b.staticPartitionLeader(p)
			b.epochMgr.IncrementEpoch(topic, p, leader)
		}
		_ = b.getOrCreateISRTracker(topic, p)
	}
}

func (b *Broker) getISRTracker(topic string, partition int32) *replication.ISRTracker {
	b.isrMu.RLock()
	defer b.isrMu.RUnlock()
	if m, ok := b.isrTrackers[topic]; ok {
		return m[partition]
	}
	return nil
}

func (b *Broker) getOrCreateISRTracker(topic string, partition int32) *replication.ISRTracker {
	b.isrMu.Lock()
	defer b.isrMu.Unlock()

	if m, ok := b.isrTrackers[topic]; ok {
		if t, ok := m[partition]; ok {
			return t
		}
	} else {
		b.isrTrackers[topic] = make(map[int32]*replication.ISRTracker)
	}

	leader := b.partitionLeader(topic, partition)
	tracker := replication.NewISRTracker(
		b.config.Broker.DataDir,
		topic,
		partition,
		leader,
		b.replicaIDs,
		b.lagTimeMaxMs(),
	)
	b.isrTrackers[topic][partition] = tracker
	return tracker
}

func (b *Broker) updateLeaderLEO(topic string, partition int32, leaderLEO int64) int64 {
	tracker := b.getOrCreateISRTracker(topic, partition)
	return tracker.UpdateLEO(b.localID, leaderLEO, leaderLEO)
}

func (b *Broker) updateReplicaLEO(topic string, partition int32, replicaID int32, leo, leaderLEO int64) int64 {
	tracker := b.getOrCreateISRTracker(topic, partition)
	return tracker.UpdateLEO(replicaID, leo, leaderLEO)
}

// waitForAcksAll blocks until HW reaches requiredHW or the shared deadline
// elapses. The deadline is computed once at the start of handleProduce and
// shared across every partition in the request, matching Kafka's semantics:
// TimeoutMs is a single budget for the whole produce, not a per-partition
// budget that restarts on each iteration.
func (b *Broker) waitForAcksAll(topic string, partition int32, requiredHW int64, deadline time.Time) error {
	if requiredHW <= 0 {
		return nil
	}

	// Fast path: already replicated.
	if tracker := b.getOrCreateISRTracker(topic, partition); tracker.HighWatermark() >= requiredHW {
		return nil
	}

	respCh := make(chan error, 1)
	id := b.purgatory.Watch(topic, partition, requiredHW, respCh)

	// Re-check after Watch to close the race with a concurrent ReplicaFetch.
	if tracker := b.getOrCreateISRTracker(topic, partition); tracker.HighWatermark() >= requiredHW {
		b.purgatory.CheckAndComplete(topic, partition, tracker.HighWatermark())
	}

	// If the shared deadline has already passed, do not wait at all.
	remaining := time.Until(deadline)
	if remaining <= 0 {
		b.purgatory.Cancel(id)
		return errProduceTimeout
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case err := <-respCh:
		return err
	case <-timer.C:
		b.purgatory.Cancel(id)
		return errProduceTimeout
	}
}

func (b *Broker) clusterBrokerMetadata() []protocol.BrokerMetadata {
	if len(b.config.Cluster.Brokers) == 0 {
		return []protocol.BrokerMetadata{{
			NodeID: b.localID,
			Host:   b.config.Broker.Host,
			Port:   int32(b.config.Broker.Port),
		}}
	}
	out := make([]protocol.BrokerMetadata, 0, len(b.config.Cluster.Brokers))
	for _, br := range b.config.Cluster.Brokers {
		out = append(out, protocol.BrokerMetadata{
			NodeID: int32(br.ID),
			Host:   br.Host,
			Port:   int32(br.Port),
		})
	}
	return out
}

func (b *Broker) brokerAddr(id int32) (string, bool) {
	if len(b.config.Cluster.Brokers) == 0 {
		if id == b.localID {
			return fmt.Sprintf("%s:%d", b.config.Broker.Host, b.config.Broker.Port), true
		}
		return "", false
	}
	for _, br := range b.config.Cluster.Brokers {
		if int32(br.ID) == id {
			return fmt.Sprintf("%s:%d", br.Host, br.Port), true
		}
	}
	return "", false
}

// startFollowerFetchers launches one goroutine per remote broker that periodically
// issues ReplicaFetch for partitions where that broker is the leader and we are a replica.
func (b *Broker) startFollowerFetchers() {
	ctx, cancel := context.WithCancel(context.Background())
	b.fetchCancel = cancel

	for _, id := range b.replicaIDs {
		if id == b.localID {
			continue
		}
		leaderID := id
		b.fetchWg.Add(1)
		go func() {
			defer b.fetchWg.Done()
			b.followerFetchLoop(ctx, leaderID)
		}()
	}
}

func (b *Broker) stopFollowerFetchers() {
	if b.fetchCancel != nil {
		b.fetchCancel()
		b.fetchCancel = nil
	}
	b.fetchWg.Wait()
}

func (b *Broker) followerFetchLoop(ctx context.Context, leaderID int32) {
	interval := time.Duration(b.config.Replication.ReplicaFetchWaitMaxMs) * time.Millisecond
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// fetchOffsets tracks next fetch offset per topic-partition.
	offsets := make(map[string]map[int32]int64)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.followerFetchOnce(ctx, leaderID, offsets)
		}
	}
}

func (b *Broker) followerFetchOnce(ctx context.Context, leaderID int32, offsets map[string]map[int32]int64) {
	addr, ok := b.brokerAddr(leaderID)
	if !ok {
		return
	}

	type partRef struct {
		topic     string
		partition int32
	}
	var targets []partRef

	b.mu.RLock()
	for name, t := range b.topics {
		t.mu.RLock()
		for pID := range t.Partitions {
			// Fetch only partitions whose effective leader is the remote broker.
			if b.partitionLeader(name, pID) != leaderID {
				continue
			}
			targets = append(targets, partRef{topic: name, partition: pID})
		}
		t.mu.RUnlock()
	}
	b.mu.RUnlock()

	if len(targets) == 0 {
		return
	}

	maxWait := int32(b.config.Replication.ReplicaFetchWaitMaxMs)
	if maxWait <= 0 {
		maxWait = 500
	}

	for _, tgt := range targets {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if offsets[tgt.topic] == nil {
			offsets[tgt.topic] = make(map[int32]int64)
		}
		fetchOff := offsets[tgt.topic][tgt.partition]

		// Prefer local LEO if the partition already has data (restart).
		b.mu.RLock()
		t := b.topics[tgt.topic]
		b.mu.RUnlock()
		if t != nil {
			if p := t.GetPartition(tgt.partition); p != nil {
				if leo := p.LogEndOffset(); leo > fetchOff {
					fetchOff = leo
				}
			}
		}

		records, hw, err := b.sendReplicaFetch(addr, tgt.topic, tgt.partition, fetchOff, maxWait)
		if err != nil {
			continue
		}

		if len(records) > 0 {
			b.mu.RLock()
			t := b.topics[tgt.topic]
			b.mu.RUnlock()
			if t == nil {
				continue
			}
			p := t.GetPartition(tgt.partition)
			if p == nil {
				continue
			}
			if _, err := p.AppendBatch(records); err != nil {
				continue
			}
			offsets[tgt.topic][tgt.partition] = p.LogEndOffset()
		} else {
			offsets[tgt.topic][tgt.partition] = fetchOff
		}

		b.mu.RLock()
		t = b.topics[tgt.topic]
		b.mu.RUnlock()
		if t != nil {
			if p := t.GetPartition(tgt.partition); p != nil {
				// Follower adopts leader HW (capped at local LEO).
				localLEO := p.LogEndOffset()
				if hw > localLEO {
					hw = localLEO
				}
				p.SetHighWatermark(hw)
			}
		}
	}
}

func (b *Broker) sendReplicaFetch(addr, topic string, partition int32, fetchOffset int64, maxWaitMs int32) ([]*storage.Record, int64, error) {
	payload, err := replication.EncodeReplicaFetchRequest(&replication.ReplicaFetchRequest{
		ReplicaID:   b.localID,
		MaxWaitMs:   maxWaitMs,
		Topic:       topic,
		Partition:   partition,
		FetchOffset: fetchOffset,
	})
	if err != nil {
		return nil, 0, err
	}

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	frame := &protocol.RequestFrame{
		ApiKey:        apiKeyReplicaFetch,
		ApiVersion:    1,
		CorrelationID: 1,
		ClientID:      fmt.Sprintf("replica-%d", b.localID),
		Payload:       payload,
	}
	if _, err := frame.Write(conn); err != nil {
		return nil, 0, err
	}

	resp, err := protocol.ReadResponseFrame(conn)
	if err != nil {
		return nil, 0, err
	}
	if resp.ErrorCode != 0 {
		return nil, 0, fmt.Errorf("replica fetch error code %d", resp.ErrorCode)
	}

	errCode, hw, records, err := replication.DecodeReplicaFetchResponse(resp.Payload)
	if err != nil {
		return nil, 0, err
	}
	if errCode != 0 {
		return nil, 0, fmt.Errorf("replica fetch body error code %d", errCode)
	}
	return records, hw, nil
}

// PromoteLeader transfers leadership for a partition to newLeaderID and bumps the epoch.
// Used by controller/failover paths.
func (b *Broker) PromoteLeader(topic string, partition int32, newLeaderID int32) int32 {
	epoch := b.epochMgr.IncrementEpoch(topic, partition, newLeaderID)
	// Recreate ISR tracker with the new leader so leader-only InSync rules apply.
	b.isrMu.Lock()
	if b.isrTrackers[topic] == nil {
		b.isrTrackers[topic] = make(map[int32]*replication.ISRTracker)
	}
	b.isrTrackers[topic][partition] = replication.NewISRTracker(
		b.config.Broker.DataDir,
		topic,
		partition,
		newLeaderID,
		b.replicaIDs,
		b.lagTimeMaxMs(),
	)
	b.isrMu.Unlock()
	return epoch
}
