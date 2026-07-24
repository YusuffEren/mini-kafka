package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/yusuf/mini-kafka/internal/config"
	"github.com/yusuf/mini-kafka/internal/coordinator"
	"github.com/yusuf/mini-kafka/internal/protocol"
	"github.com/yusuf/mini-kafka/internal/server"
	"github.com/yusuf/mini-kafka/internal/storage"
)

const (
	apiKeyProduce      int16 = 0
	apiKeyFetch        int16 = 1
	apiKeyMetadata     int16 = 2
	apiKeyCreateTopics int16 = 3
	apiKeyJoinGroup    int16 = 4
	apiKeySyncGroup    int16 = 5
	apiKeyHeartbeat    int16 = 6
	apiKeyLeaveGroup   int16 = 7
	apiKeyOffsetCommit int16 = 8
	apiKeyOffsetFetch  int16 = 9
	apiKeyListOffsets  int16 = 10
	apiKeyApiVersions  int16 = 12
)

// Broker is the top-level mini-kafka broker. It manages cluster configuration,
// topic/partition routing, metadata state, group coordination, and the TCP server.
type Broker struct {
	config      *config.Config
	server      *server.Server
	mux         *server.Mux
	metaManager *MetadataManager
	coordinator *coordinator.GroupCoordinator

	mu        sync.RWMutex
	topics    map[string]*Topic
	listeners map[string]map[int32][]chan struct{} // topic -> partition -> listeners
}

// New constructs a Broker from cfg.
func New(cfg *config.Config) (*Broker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("broker: nil config")
	}

	metaMgr, err := NewMetadataManager(cfg.Broker.DataDir)
	if err != nil {
		return nil, fmt.Errorf("broker: metadata manager: %w", err)
	}

	offsetStore := coordinator.NewOffsetStore(cfg.Broker.DataDir)
	gc := coordinator.NewGroupCoordinator(offsetStore)

	b := &Broker{
		config:      cfg,
		metaManager: metaMgr,
		coordinator: gc,
		topics:      make(map[string]*Topic),
		listeners:   make(map[string]map[int32][]chan struct{}),
	}

	// Load existing persisted topics
	for _, meta := range metaMgr.ListTopics() {
		t, err := NewTopic(cfg.Broker.DataDir, meta.Name, meta.NumPartitions, storage.Config{})
		if err != nil {
			return nil, fmt.Errorf("broker: load topic %s: %w", meta.Name, err)
		}
		b.topics[meta.Name] = t
	}

	mux := server.NewMux()
	mux.Handle(apiKeyApiVersions, handleApiVersions)
	mux.Handle(apiKeyProduce, b.handleProduce)
	mux.Handle(apiKeyFetch, b.handleFetch)
	mux.Handle(apiKeyMetadata, b.handleMetadata)
	mux.Handle(apiKeyCreateTopics, b.handleCreateTopics)
	mux.Handle(apiKeyJoinGroup, b.handleJoinGroup)
	mux.Handle(apiKeySyncGroup, b.handleSyncGroup)
	mux.Handle(apiKeyHeartbeat, b.handleHeartbeat)
	mux.Handle(apiKeyLeaveGroup, b.handleLeaveGroup)
	mux.Handle(apiKeyOffsetCommit, b.handleOffsetCommit)
	mux.Handle(apiKeyOffsetFetch, b.handleOffsetFetch)
	mux.Handle(apiKeyListOffsets, b.handleListOffsets)
	b.mux = mux

	addr := fmt.Sprintf("%s:%d", cfg.Broker.Host, cfg.Broker.Port)
	srv := server.NewServer(addr, mux)
	b.server = srv

	return b, nil
}

// Addr returns the network address the broker's TCP server is listening on.
func (b *Broker) Addr() net.Addr {
	if b.server == nil {
		return nil
	}
	return b.server.Addr()
}

// Start begins accepting TCP connections.
func (b *Broker) Start() error {
	return b.server.Start()
}

// Shutdown gracefully stops the broker, group coordinator, and closes all partition storage logs.
func (b *Broker) Shutdown(ctx context.Context) error {
	if b.coordinator != nil {
		b.coordinator.Close()
	}

	err := b.server.Shutdown(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, t := range b.topics {
		if tErr := t.Close(); tErr != nil && err == nil {
			err = tErr
		}
	}
	return err
}

func (b *Broker) getTopic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

func (b *Broker) getOrCreateTopic(name string, requestedPartitions int32) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, exists := b.topics[name]; exists {
		return t, nil
	}

	numPartitions := requestedPartitions
	if numPartitions <= 0 {
		if !b.config.Topic.AutoCreate {
			return nil, fmt.Errorf("topic %s not found and auto creation disabled", name)
		}
		numPartitions = int32(b.config.Topic.DefaultPartitions)
		if numPartitions <= 0 {
			numPartitions = 1
		}
	}

	rf := int16(b.config.Topic.DefaultReplicationFactor)
	if rf <= 0 {
		rf = 1
	}

	meta, _, err := b.metaManager.CreateTopic(name, numPartitions, rf)
	if err != nil {
		return nil, err
	}

	t, err := NewTopic(b.config.Broker.DataDir, name, meta.NumPartitions, storage.Config{})
	if err != nil {
		return nil, err
	}

	b.topics[name] = t
	return t, nil
}

func (b *Broker) registerListener(topic string, partitionID int32) chan struct{} {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.listeners[topic] == nil {
		b.listeners[topic] = make(map[int32][]chan struct{})
	}
	b.listeners[topic][partitionID] = append(b.listeners[topic][partitionID], ch)
	return ch
}

func (b *Broker) notifyAppended(topic string, partitionID int32) {
	b.mu.Lock()
	var list []chan struct{}
	if pMap, ok := b.listeners[topic]; ok {
		list = pMap[partitionID]
		delete(pMap, partitionID)
	}
	b.mu.Unlock()

	for _, ch := range list {
		close(ch)
	}
}

// handleApiVersions handles ApiVersions requests.
func handleApiVersions(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	resp := &protocol.ApiVersionsResponse{
		ApiKeys: []protocol.ApiVersion{
			{ApiKey: apiKeyProduce, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyFetch, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyMetadata, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyCreateTopics, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyJoinGroup, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeySyncGroup, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyHeartbeat, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyLeaveGroup, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyOffsetCommit, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyOffsetFetch, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyApiVersions, MinVersion: 0, MaxVersion: 0},
		},
	}

	var body bytes.Buffer
	if err := resp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleProduce handles Produce requests (apiKey 0).
func (b *Broker) handleProduce(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var produceReq protocol.ProduceRequest
	if err := produceReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	produceResp := &protocol.ProduceResponse{
		Topics: make([]protocol.ProduceResponseTopic, len(produceReq.Topics)),
	}

	for i, tReq := range produceReq.Topics {
		topicResp := protocol.ProduceResponseTopic{
			Name:       tReq.Name,
			Partitions: make([]protocol.ProduceResponsePartition, len(tReq.Partitions)),
		}

		tObj, err := b.getOrCreateTopic(tReq.Name, 0)
		for j, pReq := range tReq.Partitions {
			partResp := protocol.ProduceResponsePartition{
				PartitionID: pReq.PartitionID,
			}

			if err != nil || tObj == nil {
				partResp.ErrorCode = server.ErrUnknownTopicOrPartition
				topicResp.Partitions[j] = partResp
				continue
			}

			pObj := tObj.GetPartition(pReq.PartitionID)
			if pObj == nil {
				partResp.ErrorCode = server.ErrUnknownTopicOrPartition
				topicResp.Partitions[j] = partResp
				continue
			}

			buf := bytes.NewReader(pReq.RecordSet)
			var records []*storage.Record
			for buf.Len() > 0 {
				rec, _, decodeErr := storage.DecodeRecord(buf)
				if decodeErr != nil {
					if errors.Is(decodeErr, io.EOF) {
						break
					}
					partResp.ErrorCode = server.ErrCorruptMessage
					break
				}
				records = append(records, rec)
			}

			if partResp.ErrorCode == 0 && len(records) > 0 {
				baseOffset, appendErr := pObj.AppendBatch(records)
				if appendErr != nil {
					partResp.ErrorCode = server.ErrUnknown
				} else {
					partResp.ErrorCode = server.ErrNone
					partResp.BaseOffset = baseOffset
					partResp.LogAppendTime = time.Now().UnixMilli()
					b.notifyAppended(tReq.Name, pReq.PartitionID)
				}
			}
			topicResp.Partitions[j] = partResp
		}
		produceResp.Topics[i] = topicResp
	}

	var body bytes.Buffer
	if err := produceResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleFetch handles Fetch requests (apiKey 1) with long-polling per partition.
func (b *Broker) handleFetch(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var fetchReq protocol.FetchRequest
	if err := fetchReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	// Long-polling: wait if first partition fetchOffset >= LEO and maxWaitMs > 0
	if fetchReq.MaxWaitMs > 0 && len(fetchReq.Topics) > 0 && len(fetchReq.Topics[0].Partitions) > 0 {
		topicName := fetchReq.Topics[0].Name
		partID := fetchReq.Topics[0].Partitions[0].PartitionID
		targetOffset := fetchReq.Topics[0].Partitions[0].FetchOffset
		deadline := time.Now().Add(time.Duration(fetchReq.MaxWaitMs) * time.Millisecond)

		for {
			tObj := b.getTopic(topicName)
			if tObj != nil {
				pObj := tObj.GetPartition(partID)
				if pObj != nil && pObj.LogEndOffset() > targetOffset {
					break
				}
			}

			now := time.Now()
			if !now.Before(deadline) {
				break
			}
			ch := b.registerListener(topicName, partID)
			tObj = b.getTopic(topicName)
			if tObj != nil {
				pObj := tObj.GetPartition(partID)
				if pObj != nil && pObj.LogEndOffset() > targetOffset {
					break
				}
			}
			select {
			case <-ch:
			case <-time.After(deadline.Sub(now)):
			}
		}
	}

	fetchResp := &protocol.FetchResponse{
		Topics: make([]protocol.FetchResponseTopic, len(fetchReq.Topics)),
	}

	for i, tReq := range fetchReq.Topics {
		topicResp := protocol.FetchResponseTopic{
			Name:       tReq.Name,
			Partitions: make([]protocol.FetchResponsePartition, len(tReq.Partitions)),
		}

		tObj := b.getTopic(tReq.Name)
		for j, pReq := range tReq.Partitions {
			partResp := protocol.FetchResponsePartition{
				PartitionID: pReq.PartitionID,
			}

			if tObj == nil {
				partResp.ErrorCode = server.ErrUnknownTopicOrPartition
				topicResp.Partitions[j] = partResp
				continue
			}

			pObj := tObj.GetPartition(pReq.PartitionID)
			if pObj == nil {
				partResp.ErrorCode = server.ErrUnknownTopicOrPartition
				topicResp.Partitions[j] = partResp
				continue
			}

			records, err := pObj.ReadFrom(pReq.FetchOffset, pReq.MaxBytes)
			if err != nil && !errors.Is(err, io.EOF) {
				partResp.ErrorCode = server.ErrUnknown
			} else {
				partResp.ErrorCode = server.ErrNone
				partResp.HighWatermark = pObj.HighWatermark()
				partResp.LogStartOffset = pObj.LogStartOffset()

				var recordBuf bytes.Buffer
				for _, r := range records {
					if _, err := r.Encode(&recordBuf); err != nil {
						partResp.ErrorCode = server.ErrUnknown
						break
					}
				}
				if partResp.ErrorCode == server.ErrNone {
					partResp.RecordSet = recordBuf.Bytes()
				}
			}
			topicResp.Partitions[j] = partResp
		}
		fetchResp.Topics[i] = topicResp
	}

	var body bytes.Buffer
	if err := fetchResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleMetadata handles Metadata requests (apiKey 2).
func (b *Broker) handleMetadata(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var metaReq protocol.MetadataRequest
	if err := metaReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	brokerMeta := protocol.BrokerMetadata{
		NodeID: int32(b.config.Broker.ID),
		Host:   b.config.Broker.Host,
		Port:   int32(b.config.Broker.Port),
	}

	var targetTopics []string
	if metaReq.Topics == nil {
		for _, tm := range b.metaManager.ListTopics() {
			targetTopics = append(targetTopics, tm.Name)
		}
	} else {
		targetTopics = metaReq.Topics
	}

	topicMetas := make([]protocol.TopicMetadata, len(targetTopics))
	for i, topicName := range targetTopics {
		tm, exists := b.metaManager.GetTopic(topicName)
		if !exists && b.config.Topic.AutoCreate {
			tObj, err := b.getOrCreateTopic(topicName, 0)
			if err == nil && tObj != nil {
				tm, exists = b.metaManager.GetTopic(topicName)
			}
		}

		if !exists {
			topicMetas[i] = protocol.TopicMetadata{
				Name:      topicName,
				ErrorCode: server.ErrUnknownTopicOrPartition,
			}
			continue
		}

		parts := make([]protocol.PartitionMetadata, tm.NumPartitions)
		for pID := int32(0); pID < tm.NumPartitions; pID++ {
			parts[pID] = protocol.PartitionMetadata{
				PartitionID: pID,
				Leader:      int32(b.config.Broker.ID),
				Replicas:    []int32{int32(b.config.Broker.ID)},
				ISR:         []int32{int32(b.config.Broker.ID)},
			}
		}

		topicMetas[i] = protocol.TopicMetadata{
			Name:       topicName,
			ErrorCode:  server.ErrNone,
			Partitions: parts,
		}
	}

	resp := &protocol.MetadataResponse{
		Brokers: []protocol.BrokerMetadata{brokerMeta},
		Topics:  topicMetas,
	}

	var body bytes.Buffer
	if err := resp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleCreateTopics handles CreateTopics requests (apiKey 3).
func (b *Broker) handleCreateTopics(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var createReq protocol.CreateTopicsRequest
	if err := createReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	respTopics := make([]protocol.CreateTopicResponseTopic, len(createReq.Topics))
	for i, tReq := range createReq.Topics {
		if tReq.NumPartitions <= 0 {
			respTopics[i] = protocol.CreateTopicResponseTopic{
				Name:      tReq.Name,
				ErrorCode: server.ErrInvalidPartitionCount,
			}
			continue
		}

		_, created, err := b.metaManager.CreateTopic(tReq.Name, tReq.NumPartitions, tReq.ReplicationFactor)
		if err != nil {
			respTopics[i] = protocol.CreateTopicResponseTopic{
				Name:      tReq.Name,
				ErrorCode: server.ErrUnknown,
			}
			continue
		}

		if !created {
			respTopics[i] = protocol.CreateTopicResponseTopic{
				Name:      tReq.Name,
				ErrorCode: server.ErrTopicAlreadyExists,
			}
			continue
		}

		b.mu.Lock()
		tObj, err := NewTopic(b.config.Broker.DataDir, tReq.Name, tReq.NumPartitions, storage.Config{})
		if err != nil {
			b.mu.Unlock()
			respTopics[i] = protocol.CreateTopicResponseTopic{
				Name:      tReq.Name,
				ErrorCode: server.ErrUnknown,
			}
			continue
		}
		b.topics[tReq.Name] = tObj
		b.mu.Unlock()

		respTopics[i] = protocol.CreateTopicResponseTopic{
			Name:      tReq.Name,
			ErrorCode: server.ErrNone,
		}
	}

	resp := &protocol.CreateTopicsResponse{
		Topics: respTopics,
	}

	var body bytes.Buffer
	if err := resp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleJoinGroup handles JoinGroup requests (apiKey 4).
func (b *Broker) handleJoinGroup(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var joinReq protocol.JoinGroupRequest
	if err := joinReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	joinResp, err := b.coordinator.JoinGroup(context.Background(), &joinReq, req.ClientID)
	if err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	var body bytes.Buffer
	if err := joinResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}


// handleSyncGroup handles SyncGroup requests (apiKey 5).
func (b *Broker) handleSyncGroup(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var syncReq protocol.SyncGroupRequest
	if err := syncReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	syncResp, err := b.coordinator.SyncGroup(context.Background(), &syncReq)
	if err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	var body bytes.Buffer
	if err := syncResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleHeartbeat handles Heartbeat requests (apiKey 6).
func (b *Broker) handleHeartbeat(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var hbReq protocol.HeartbeatRequest
	if err := hbReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	hbResp := b.coordinator.Heartbeat(&hbReq)

	var body bytes.Buffer
	if err := hbResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleLeaveGroup handles LeaveGroup requests (apiKey 7).
func (b *Broker) handleLeaveGroup(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var leaveReq protocol.LeaveGroupRequest
	if err := leaveReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	leaveResp := b.coordinator.LeaveGroup(&leaveReq)

	var body bytes.Buffer
	if err := leaveResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleOffsetCommit handles OffsetCommit requests (apiKey 8).
func (b *Broker) handleOffsetCommit(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var commitReq protocol.OffsetCommitRequest
	if err := commitReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	respTopics := make([]protocol.OffsetCommitResponseTopic, len(commitReq.Topics))
	for i, t := range commitReq.Topics {
		respParts := make([]protocol.OffsetCommitResponsePartition, len(t.Partitions))
		for j, p := range t.Partitions {
			b.coordinator.CommitOffset(commitReq.GroupID, t.Name, p.PartitionID, p.Offset)
			respParts[j] = protocol.OffsetCommitResponsePartition{
				PartitionID: p.PartitionID,
				ErrorCode:   server.ErrNone,
			}
		}
		respTopics[i] = protocol.OffsetCommitResponseTopic{
			Name:       t.Name,
			Partitions: respParts,
		}
	}

	commitResp := &protocol.OffsetCommitResponse{
		Topics: respTopics,
	}

	var body bytes.Buffer
	if err := commitResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleOffsetFetch handles OffsetFetch requests (apiKey 9).
func (b *Broker) handleOffsetFetch(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var fetchReq protocol.OffsetFetchRequest
	if err := fetchReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	respTopics := make([]protocol.OffsetFetchResponseTopic, len(fetchReq.Topics))
	for i, t := range fetchReq.Topics {
		respParts := make([]protocol.OffsetFetchResponsePartition, len(t.Partitions))
		for j, pID := range t.Partitions {
			off := b.coordinator.FetchOffset(fetchReq.GroupID, t.Name, pID)
			respParts[j] = protocol.OffsetFetchResponsePartition{
				PartitionID: pID,
				Offset:      off,
				ErrorCode:   server.ErrNone,
			}
		}
		respTopics[i] = protocol.OffsetFetchResponseTopic{
			Name:       t.Name,
			Partitions: respParts,
		}
	}

	fetchResp := &protocol.OffsetFetchResponse{
		Topics: respTopics,
	}

	var body bytes.Buffer
	if err := fetchResp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}

// handleListOffsets handles ListOffsets requests (apiKey 10).
// timestamp: -1 = latest (LEO), -2 = earliest (log start offset).
func (b *Broker) handleListOffsets(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	var listReq protocol.ListOffsetsRequest
	if err := listReq.Decode(bytes.NewReader(req.Payload)); err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	respTopics := make([]protocol.ListOffsetsResponseTopic, len(listReq.Topics))
	for i, t := range listReq.Topics {
		topic := b.getTopic(t.Name)
		respParts := make([]protocol.ListOffsetsResponsePartition, len(t.Partitions))
		for j, p := range t.Partitions {
			if topic == nil {
				respParts[j] = protocol.ListOffsetsResponsePartition{
					PartitionID: p.PartitionID,
					ErrorCode:   server.ErrUnknownTopicOrPartition,
					Offset:      -1,
				}
				continue
			}

			part, ok := topic.Partitions[p.PartitionID]
			if !ok {
				respParts[j] = protocol.ListOffsetsResponsePartition{
					PartitionID: p.PartitionID,
					ErrorCode:   server.ErrUnknownTopicOrPartition,
					Offset:      -1,
				}
				continue
			}

			var offset int64
			switch p.Timestamp {
			case -1: // Latest
				offset = part.LogEndOffset()
			case -2: // Earliest
				offset = part.LogStartOffset()
			default:
				// For simplicity, return latest for any other timestamp
				offset = part.LogEndOffset()
			}

			respParts[j] = protocol.ListOffsetsResponsePartition{
				PartitionID: p.PartitionID,
				ErrorCode:   server.ErrNone,
				Offset:      offset,
			}
		}
		respTopics[i] = protocol.ListOffsetsResponseTopic{
			Name:       t.Name,
			Partitions: respParts,
		}
	}

	resp := &protocol.ListOffsetsResponse{
		Topics: respTopics,
	}

	var body bytes.Buffer
	if err := resp.Encode(&body); err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
}
