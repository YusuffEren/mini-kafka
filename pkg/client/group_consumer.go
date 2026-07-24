package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/coordinator"
	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/storage"
)

// TopicPartition identifies a specific partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// GroupConsumerConfig holds parameters for the GroupConsumer client.
type GroupConsumerConfig struct {
	ClientID            string
	SessionTimeoutMs    int32
	HeartbeatIntervalMs int32
	RebalanceTimeoutMs  int32
	AutoCommit          bool
	AutoCommitInterval  time.Duration
	AutoOffsetReset     string // "earliest" | "latest" | "none"
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	AssignorStrategy    string // "range" | "roundrobin"

	// Rebalance callbacks
	OnPartitionsRevoked  func(revoked []TopicPartition)
	OnPartitionsAssigned func(assigned []TopicPartition)
}

// DefaultGroupConsumerConfig returns sensible defaults for GroupConsumer.
func DefaultGroupConsumerConfig() GroupConsumerConfig {
	return GroupConsumerConfig{
		ClientID:            "mini-kafka-group-consumer",
		SessionTimeoutMs:    45000,
		HeartbeatIntervalMs: 3000,
		RebalanceTimeoutMs:  60000,
		AutoCommit:          true,
		AutoCommitInterval:  5 * time.Second,
		AutoOffsetReset:     "earliest",
		MaxWaitMs:           500,
		MinBytes:            1,
		MaxBytes:            1024 * 1024,
		AssignorStrategy:    "range",
	}
}

// GroupConsumer manages high-level consumer group partition consumption and
// rebalancing. It handles JoinGroup/SyncGroup flows, heartbeats, offset
// management, and partition assignment automatically.
type GroupConsumer struct {
	addrs   []string
	groupID string
	topics  []string
	config  GroupConsumerConfig

	mu                 sync.Mutex
	conn               net.Conn
	closed             bool
	nextID             int32
	memberID           string
	generationID       int32
	leaderID           string
	assignedPartitions []TopicPartition
	partitionOffsets   map[TopicPartition]int64

	stopCh   chan struct{}
	rejoinCh chan struct{}
	wg       sync.WaitGroup
}

// NewGroupConsumer constructs a GroupConsumer and immediately joins the group.
func NewGroupConsumer(addrs []string, groupID string, topics []string, cfg GroupConsumerConfig) (*GroupConsumer, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("group_consumer: at least one broker address required")
	}
	if groupID == "" {
		return nil, fmt.Errorf("group_consumer: groupID required")
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("group_consumer: at least one topic required")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "mini-kafka-group-consumer"
	}
	if cfg.SessionTimeoutMs <= 0 {
		cfg.SessionTimeoutMs = 45000
	}
	if cfg.HeartbeatIntervalMs <= 0 {
		cfg.HeartbeatIntervalMs = 3000
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1024 * 1024
	}
	if cfg.AutoCommitInterval <= 0 {
		cfg.AutoCommitInterval = 5 * time.Second
	}
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "earliest"
	}
	if cfg.AssignorStrategy == "" {
		cfg.AssignorStrategy = "range"
	}

	gc := &GroupConsumer{
		addrs:            addrs,
		groupID:          groupID,
		topics:           topics,
		config:           cfg,
		partitionOffsets: make(map[TopicPartition]int64),
		stopCh:           make(chan struct{}),
		rejoinCh:         make(chan struct{}, 1),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := gc.joinAndSync(ctx); err != nil {
		return nil, fmt.Errorf("group_consumer: initial join: %w", err)
	}

	gc.wg.Add(1)
	go gc.heartbeatLoop()

	if cfg.AutoCommit {
		gc.wg.Add(1)
		go gc.autoCommitLoop()
	}

	return gc, nil
}

func (gc *GroupConsumer) closeConn() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	if gc.conn != nil {
		_ = gc.conn.Close()
		gc.conn = nil
	}
}

func (gc *GroupConsumer) dialNewConn() (net.Conn, error) {
	var lastErr error
	for _, addr := range gc.addrs {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("group_consumer connect failed: %w", lastErr)
}

func (gc *GroupConsumer) sendRequestOnConn(conn net.Conn, apiKey int16, payload []byte) (*protocol.ResponseFrame, error) {
	corrID := atomic.AddInt32(&gc.nextID, 1)
	frame := &protocol.RequestFrame{
		ApiKey:        apiKey,
		ApiVersion:    1,
		CorrelationID: corrID,
		ClientID:      gc.config.ClientID,
		Payload:       payload,
	}

	if _, err := frame.Write(conn); err != nil {
		return nil, fmt.Errorf("group_consumer: write frame: %w", err)
	}

	respFrame, err := protocol.ReadResponseFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("group_consumer: read response: %w", err)
	}

	return respFrame, nil
}

func (gc *GroupConsumer) sendRequest(apiKey int16, payload []byte) (*protocol.ResponseFrame, error) {
	conn, err := gc.dialNewConn()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	return gc.sendRequestOnConn(conn, apiKey, payload)
}

func (gc *GroupConsumer) joinAndSync(ctx context.Context) error {
	// Step 1: JoinGroup
	joinReq := &protocol.JoinGroupRequest{
		GroupID:          gc.groupID,
		SessionTimeoutMs: gc.config.SessionTimeoutMs,
		MemberID:         gc.memberID, // empty on first join
		ProtocolType:     "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{
				Name:     gc.config.AssignorStrategy,
				Metadata: nil,
			},
		},
	}

	var payload bytes.Buffer
	if err := joinReq.Encode(&payload); err != nil {
		return fmt.Errorf("encode join: %w", err)
	}

	resp, err := gc.sendRequest(4, payload.Bytes()) // apiKeyJoinGroup
	if err != nil {
		return fmt.Errorf("join request: %w", err)
	}

	if resp.ErrorCode != 0 {
		return fmt.Errorf("join frame error code %d", resp.ErrorCode)
	}

	var joinResp protocol.JoinGroupResponse
	if err := joinResp.Decode(bytes.NewReader(resp.Payload)); err != nil {
		return fmt.Errorf("decode join response: %w", err)
	}

	if joinResp.ErrorCode != 0 {
		return fmt.Errorf("join error code %d", joinResp.ErrorCode)
	}

	gc.mu.Lock()
	gc.memberID = joinResp.MemberID
	gc.generationID = joinResp.GenerationID
	gc.leaderID = joinResp.LeaderID
	gc.mu.Unlock()

	// Step 2: SyncGroup — leader assigns, followers send empty
	syncReq := &protocol.SyncGroupRequest{
		GroupID:      gc.groupID,
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
	}

	if joinResp.MemberID == joinResp.LeaderID {
		// I am the leader: compute assignments
		topicPartitions := make(map[string]int32)
		if err := gc.fetchTopicMetadata(topicPartitions); err != nil {
			return fmt.Errorf("leader fetch metadata: %w", err)
		}

		memberIDs := make([]string, len(joinResp.Members))
		for i, m := range joinResp.Members {
			memberIDs[i] = m.MemberID
		}

		assignor, err := coordinator.GetAssignor(gc.config.AssignorStrategy)
		if err != nil {
			return fmt.Errorf("get assignor: %w", err)
		}
		assignments := assignor.Assign(memberIDs, topicPartitions)

		syncAssignments := make([]protocol.SyncGroupAssignment, 0, len(assignments))
		for mID, ma := range assignments {
			encoded, err := coordinator.EncodeAssignment(ma)
			if err != nil {
				return fmt.Errorf("encode assignment for %s: %w", mID, err)
			}
			syncAssignments = append(syncAssignments, protocol.SyncGroupAssignment{
				MemberID:   mID,
				Assignment: encoded,
			})
		}
		syncReq.Assignments = syncAssignments
	}

	var syncPayload bytes.Buffer
	if err := syncReq.Encode(&syncPayload); err != nil {
		return fmt.Errorf("encode sync: %w", err)
	}

	syncResp, err := gc.sendRequest(5, syncPayload.Bytes()) // apiKeySyncGroup
	if err != nil {
		return fmt.Errorf("sync request: %w", err)
	}

	if syncResp.ErrorCode != 0 {
		return fmt.Errorf("sync frame error code %d", syncResp.ErrorCode)
	}

	var syncResponse protocol.SyncGroupResponse
	if err := syncResponse.Decode(bytes.NewReader(syncResp.Payload)); err != nil {
		return fmt.Errorf("decode sync response: %w", err)
	}

	if syncResponse.ErrorCode != 0 {
		return fmt.Errorf("sync error code %d", syncResponse.ErrorCode)
	}

	assignment, err := coordinator.DecodeAssignment(syncResponse.Assignment)
	if err != nil {
		return fmt.Errorf("decode assignment: %w", err)
	}

	// Build assigned partitions list
	oldAssigned := gc.assignedPartitions
	var newAssigned []TopicPartition
	for _, ta := range assignment.Topics {
		for _, pID := range ta.Partitions {
			newAssigned = append(newAssigned, TopicPartition{Topic: ta.Topic, Partition: pID})
		}
	}

	// Invoke callbacks
	if gc.config.OnPartitionsRevoked != nil && len(oldAssigned) > 0 {
		gc.config.OnPartitionsRevoked(oldAssigned)
	}

	gc.mu.Lock()
	gc.assignedPartitions = newAssigned
	gc.mu.Unlock()

	// Fetch committed offsets for new assignments
	if err := gc.fetchInitialOffsets(ctx); err != nil {
		return fmt.Errorf("fetch initial offsets: %w", err)
	}

	if gc.config.OnPartitionsAssigned != nil && len(newAssigned) > 0 {
		gc.config.OnPartitionsAssigned(newAssigned)
	}

	return nil
}

func (gc *GroupConsumer) fetchTopicMetadata(topicPartitions map[string]int32) error {
	metaReq := &protocol.MetadataRequest{
		Topics: gc.topics,
	}

	var payload bytes.Buffer
	if err := metaReq.Encode(&payload); err != nil {
		return err
	}

	resp, err := gc.sendRequest(2, payload.Bytes()) // apiKeyMetadata
	if err != nil {
		return err
	}

	var metaResp protocol.MetadataResponse
	if err := metaResp.Decode(bytes.NewReader(resp.Payload)); err != nil {
		return err
	}

	for _, t := range metaResp.Topics {
		if t.ErrorCode == 0 {
			topicPartitions[t.Name] = int32(len(t.Partitions))
		}
	}
	return nil
}

func (gc *GroupConsumer) fetchInitialOffsets(ctx context.Context) error {
	gc.mu.Lock()
	assigned := append([]TopicPartition(nil), gc.assignedPartitions...)
	gc.mu.Unlock()

	if len(assigned) == 0 {
		return nil
	}

	// Group by topic for OffsetFetch request
	topicMap := make(map[string][]int32)
	for _, tp := range assigned {
		topicMap[tp.Topic] = append(topicMap[tp.Topic], tp.Partition)
	}

	var topics []protocol.OffsetFetchRequestTopic
	for tName, parts := range topicMap {
		topics = append(topics, protocol.OffsetFetchRequestTopic{
			Name:       tName,
			Partitions: parts,
		})
	}

	fetchReq := &protocol.OffsetFetchRequest{
		GroupID: gc.groupID,
		Topics:  topics,
	}

	var payload bytes.Buffer
	if err := fetchReq.Encode(&payload); err != nil {
		return err
	}

	resp, err := gc.sendRequest(9, payload.Bytes()) // apiKeyOffsetFetch
	if err != nil {
		return err
	}

	var fetchResp protocol.OffsetFetchResponse
	if err := fetchResp.Decode(bytes.NewReader(resp.Payload)); err != nil {
		return err
	}

	gc.mu.Lock()
	for _, t := range fetchResp.Topics {
		for _, p := range t.Partitions {
			tp := TopicPartition{Topic: t.Name, Partition: p.PartitionID}
			if p.Offset >= 0 {
				gc.partitionOffsets[tp] = p.Offset
			} else {
				// No committed offset — use auto.offset.reset
				gc.partitionOffsets[tp] = -1 // mark for resolution
			}
		}
	}
	gc.mu.Unlock()

	// Resolve unset offsets via ListOffsets
	return gc.resolveUnsetOffsets(ctx)
}

func (gc *GroupConsumer) resolveUnsetOffsets(ctx context.Context) error {
	gc.mu.Lock()
	var needResolve []TopicPartition
	for tp, off := range gc.partitionOffsets {
		if off < 0 {
			needResolve = append(needResolve, tp)
		}
	}
	gc.mu.Unlock()

	if len(needResolve) == 0 {
		return nil
	}

	var timestamp int64
	switch gc.config.AutoOffsetReset {
	case "earliest":
		timestamp = -2
	case "latest":
		timestamp = -1
	case "none":
		return fmt.Errorf("group_consumer: no committed offset and auto.offset.reset=none")
	default:
		timestamp = -2 // default earliest
	}

	topicMap := make(map[string][]int32)
	for _, tp := range needResolve {
		topicMap[tp.Topic] = append(topicMap[tp.Topic], tp.Partition)
	}

	var listTopics []protocol.ListOffsetsRequestTopic
	for tName, parts := range topicMap {
		var partitions []protocol.ListOffsetsRequestPartition
		for _, pID := range parts {
			partitions = append(partitions, protocol.ListOffsetsRequestPartition{
				PartitionID: pID,
				Timestamp:   timestamp,
			})
		}
		listTopics = append(listTopics, protocol.ListOffsetsRequestTopic{
			Name:       tName,
			Partitions: partitions,
		})
	}

	listReq := &protocol.ListOffsetsRequest{
		Topics: listTopics,
	}

	var payload bytes.Buffer
	if err := listReq.Encode(&payload); err != nil {
		return err
	}

	resp, err := gc.sendRequest(10, payload.Bytes()) // apiKeyListOffsets
	if err != nil {
		return err
	}

	var listResp protocol.ListOffsetsResponse
	if err := listResp.Decode(bytes.NewReader(resp.Payload)); err != nil {
		return err
	}

	gc.mu.Lock()
	for _, t := range listResp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode == 0 {
				tp := TopicPartition{Topic: t.Name, Partition: p.PartitionID}
				gc.partitionOffsets[tp] = p.Offset
			}
		}
	}
	gc.mu.Unlock()

	return nil
}

func (gc *GroupConsumer) heartbeatLoop() {
	defer gc.wg.Done()
	ticker := time.NewTicker(time.Duration(gc.config.HeartbeatIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-gc.stopCh:
			return
		case <-ticker.C:
			gc.mu.Lock()
			memberID := gc.memberID
			generationID := gc.generationID
			gc.mu.Unlock()

			if memberID == "" {
				continue
			}

			hbReq := &protocol.HeartbeatRequest{
				GroupID:      gc.groupID,
				GenerationID: generationID,
				MemberID:     memberID,
			}

			var payload bytes.Buffer
			if err := hbReq.Encode(&payload); err != nil {
				continue
			}

			resp, err := gc.sendRequest(6, payload.Bytes()) // apiKeyHeartbeat
			if err != nil {
				continue
			}

			var hbResp protocol.HeartbeatResponse
			if err := hbResp.Decode(bytes.NewReader(resp.Payload)); err != nil {
				continue
			}

			// Check if rebalance needed
			if hbResp.ErrorCode == 10 || hbResp.ErrorCode == 11 || hbResp.ErrorCode == 9 {
				// ErrRebalanceInProgress, ErrIllegalGeneration, ErrUnknownMemberID
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					_ = gc.joinAndSync(ctx)
					cancel()
				}()
			}
		}
	}
}

func (gc *GroupConsumer) autoCommitLoop() {
	defer gc.wg.Done()
	ticker := time.NewTicker(gc.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gc.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = gc.Commit(ctx)
			cancel()
		}
	}
}

// Poll fetches messages from assigned partitions.
func (gc *GroupConsumer) Poll(ctx context.Context, timeout time.Duration) ([]Message, error) {
	gc.mu.Lock()
	if gc.closed {
		gc.mu.Unlock()
		return nil, fmt.Errorf("group_consumer closed")
	}
	assigned := append([]TopicPartition(nil), gc.assignedPartitions...)
	offsets := make(map[TopicPartition]int64, len(gc.partitionOffsets))
	for tp, off := range gc.partitionOffsets {
		offsets[tp] = off
	}
	gc.mu.Unlock()

	if len(assigned) == 0 {
		// No partitions assigned, sleep a bit
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		case <-gc.stopCh:
			return nil, fmt.Errorf("group_consumer closed")
		}
	}

	// Build multi-partition FetchRequest
	topicMap := make(map[string][]protocol.FetchRequestPartition)
	for _, tp := range assigned {
		off, ok := offsets[tp]
		if !ok || off < 0 {
			off = 0
		}
		topicMap[tp.Topic] = append(topicMap[tp.Topic], protocol.FetchRequestPartition{
			PartitionID: tp.Partition,
			FetchOffset: off,
			MaxBytes:    gc.config.MaxBytes,
		})
	}

	var fetchTopics []protocol.FetchRequestTopic
	for tName, parts := range topicMap {
		fetchTopics = append(fetchTopics, protocol.FetchRequestTopic{
			Name:       tName,
			Partitions: parts,
		})
	}

	fetchReq := &protocol.FetchRequest{
		MaxWaitMs: gc.config.MaxWaitMs,
		MinBytes:  gc.config.MinBytes,
		MaxBytes:  gc.config.MaxBytes,
		Topics:    fetchTopics,
	}

	var payload bytes.Buffer
	if err := fetchReq.Encode(&payload); err != nil {
		return nil, fmt.Errorf("group_consumer: fetch encode: %w", err)
	}

	resp, err := gc.sendRequest(1, payload.Bytes()) // apiKeyFetch
	if err != nil {
		return nil, fmt.Errorf("group_consumer: fetch request: %w", err)
	}

	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("group_consumer: broker error code %d", resp.ErrorCode)
	}

	var fetchResp protocol.FetchResponse
	if err := fetchResp.Decode(bytes.NewReader(resp.Payload)); err != nil {
		return nil, fmt.Errorf("group_consumer: fetch response decode: %w", err)
	}

	var messages []Message
	for _, t := range fetchResp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				continue
			}

			tp := TopicPartition{Topic: t.Name, Partition: p.PartitionID}
			buf := bytes.NewReader(p.RecordSet)
			for buf.Len() > 0 {
				rec, _, err := storage.DecodeRecord(buf)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					break
				}
				messages = append(messages, Message{
					Key:       rec.Key,
					Value:     rec.Value,
					Offset:    rec.Offset,
					Timestamp: rec.Timestamp,
				})
				// Advance offset for next fetch
				gc.mu.Lock()
				gc.partitionOffsets[tp] = rec.Offset + 1
				gc.mu.Unlock()
			}
		}
	}

	return messages, nil
}

// Commit sends OffsetCommit for all currently tracked partition offsets.
func (gc *GroupConsumer) Commit(ctx context.Context) error {
	gc.mu.Lock()
	if gc.closed {
		gc.mu.Unlock()
		return fmt.Errorf("group_consumer closed")
	}

	topicMap := make(map[string][]protocol.OffsetCommitRequestPartition)
	for tp, off := range gc.partitionOffsets {
		if off >= 0 {
			topicMap[tp.Topic] = append(topicMap[tp.Topic], protocol.OffsetCommitRequestPartition{
				PartitionID: tp.Partition,
				Offset:      off,
			})
		}
	}
	generationID := gc.generationID
	memberID := gc.memberID
	gc.mu.Unlock()

	if len(topicMap) == 0 {
		return nil
	}

	var topics []protocol.OffsetCommitRequestTopic
	for tName, parts := range topicMap {
		topics = append(topics, protocol.OffsetCommitRequestTopic{
			Name:       tName,
			Partitions: parts,
		})
	}

	commitReq := &protocol.OffsetCommitRequest{
		GroupID:      gc.groupID,
		GenerationID: generationID,
		MemberID:     memberID,
		Topics:       topics,
	}

	var payload bytes.Buffer
	if err := commitReq.Encode(&payload); err != nil {
		return fmt.Errorf("group_consumer: commit encode: %w", err)
	}

	resp, err := gc.sendRequest(8, payload.Bytes()) // apiKeyOffsetCommit
	if err != nil {
		return fmt.Errorf("group_consumer: commit request: %w", err)
	}

	if resp.ErrorCode != 0 {
		return fmt.Errorf("group_consumer: commit error code %d", resp.ErrorCode)
	}

	return nil
}

// CommitOffset commits a specific offset for a single TopicPartition.
func (gc *GroupConsumer) CommitOffset(ctx context.Context, tp TopicPartition, offset int64) error {
	gc.mu.Lock()
	generationID := gc.generationID
	memberID := gc.memberID
	gc.partitionOffsets[tp] = offset
	gc.mu.Unlock()

	commitReq := &protocol.OffsetCommitRequest{
		GroupID:      gc.groupID,
		GenerationID: generationID,
		MemberID:     memberID,
		Topics: []protocol.OffsetCommitRequestTopic{
			{
				Name: tp.Topic,
				Partitions: []protocol.OffsetCommitRequestPartition{
					{
						PartitionID: tp.Partition,
						Offset:      offset,
					},
				},
			},
		},
	}

	var payload bytes.Buffer
	if err := commitReq.Encode(&payload); err != nil {
		return fmt.Errorf("group_consumer: commit encode: %w", err)
	}

	resp, err := gc.sendRequest(8, payload.Bytes())
	if err != nil {
		return fmt.Errorf("group_consumer: commit request: %w", err)
	}

	if resp.ErrorCode != 0 {
		return fmt.Errorf("group_consumer: commit error code %d", resp.ErrorCode)
	}

	return nil
}

// Close sends LeaveGroup and terminates background goroutines.
func (gc *GroupConsumer) Close() error {
	gc.mu.Lock()
	if gc.closed {
		gc.mu.Unlock()
		return nil
	}
	gc.closed = true
	memberID := gc.memberID
	gc.mu.Unlock()

	close(gc.stopCh)

	// Invoke revoked callback
	if gc.config.OnPartitionsRevoked != nil && len(gc.assignedPartitions) > 0 {
		gc.config.OnPartitionsRevoked(gc.assignedPartitions)
	}

	// Send LeaveGroup
	if memberID != "" {
		leaveReq := &protocol.LeaveGroupRequest{
			GroupID:  gc.groupID,
			MemberID: memberID,
		}

		var payload bytes.Buffer
		if err := leaveReq.Encode(&payload); err == nil {
			_, _ = gc.sendRequest(7, payload.Bytes()) // apiKeyLeaveGroup
		}
	}

	gc.wg.Wait()
	gc.closeConn()

	return nil
}
