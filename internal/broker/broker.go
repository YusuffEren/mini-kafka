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

	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/internal/coordinator"
	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/replication"
	"github.com/YusuffEren/mini-kafka/internal/server"
	"github.com/YusuffEren/mini-kafka/internal/storage"
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
	apiKeyReplicaFetch int16 = 11
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

	// Replication state
	purgatory   *replication.Purgatory
	epochMgr    *replication.EpochManager
	localID     int32
	replicaIDs  []int32
	isrMu       sync.RWMutex
	isrTrackers map[string]map[int32]*replication.ISRTracker // topic -> partition -> tracker

	fetchCancel context.CancelFunc
	fetchWg     sync.WaitGroup

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

	offsetStore := coordinator.NewOffsetStore(cfg.Broker.DataDir, cfg.Group.OffsetsTopicPartitions)
	if err := offsetStore.Start(); err != nil {
		return nil, fmt.Errorf("broker: offset store: %w", err)
	}
	gc := coordinator.NewGroupCoordinator(offsetStore)

	localID := int32(cfg.Broker.ID)
	replicaIDs := clusterReplicaIDs(cfg)

	b := &Broker{
		config:      cfg,
		metaManager: metaMgr,
		coordinator: gc,
		purgatory:   replication.NewPurgatory(),
		epochMgr:    replication.NewEpochManager(),
		localID:     localID,
		replicaIDs:  replicaIDs,
		isrTrackers: make(map[string]map[int32]*replication.ISRTracker),
		topics:      make(map[string]*Topic),
		listeners:   make(map[string]map[int32][]chan struct{}),
	}

	// Load existing persisted topics and wire replication state.
	logCfg := storageConfigFrom(cfg)
	for _, meta := range metaMgr.ListTopics() {
		if err := validateTopicName(cfg.Broker.DataDir, meta.Name); err != nil {
			return nil, fmt.Errorf("broker: load topic %s: %w", meta.Name, err)
		}
		t, err := NewTopic(cfg.Broker.DataDir, meta.Name, meta.NumPartitions, logCfg)
		if err != nil {
			return nil, fmt.Errorf("broker: load topic %s: %w", meta.Name, err)
		}
		b.topics[meta.Name] = t
		b.initTopicReplication(meta.Name, meta.NumPartitions)
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
	mux.Handle(apiKeyReplicaFetch, b.handleReplicaFetch)
	b.mux = mux

	addr := fmt.Sprintf("%s:%d", cfg.Broker.Host, cfg.Broker.Port)
	srv := server.NewServer(addr, mux, cfg.Broker.MaxConnections)
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

// Start begins accepting TCP connections and starts follower fetch loops.
func (b *Broker) Start() error {
	b.startFollowerFetchers()
	return b.server.Start()
}

// Shutdown gracefully stops the broker, group coordinator, and closes all partition storage logs.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.stopFollowerFetchers()

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

// storageConfigFrom maps broker log settings onto storage.Config so YAML
// segment/retention knobs actually reach the storage layer.
func storageConfigFrom(cfg *config.Config) storage.Config {
	if cfg == nil {
		return storage.Config{}
	}
	return storage.Config{
		SegmentBytes:       cfg.Log.SegmentBytes,
		SegmentMs:          cfg.Log.SegmentMs,
		IndexIntervalBytes: cfg.Log.IndexIntervalBytes,
		IndexMaxBytes:      cfg.Log.IndexMaxBytes,
		RetentionMs:        cfg.Log.RetentionMs,
		RetentionBytes:     cfg.Log.RetentionBytes,
		FlushMessages:      cfg.Log.FlushMessages,
		FlushMs:            cfg.Log.FlushMs,
	}
}

func (b *Broker) storageConfig() storage.Config {
	return storageConfigFrom(b.config)
}

func (b *Broker) getOrCreateTopic(name string, requestedPartitions int32) (*Topic, error) {
	if err := validateTopicName(b.config.Broker.DataDir, name); err != nil {
		return nil, err
	}

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

	t, err := NewTopic(b.config.Broker.DataDir, name, meta.NumPartitions, b.storageConfig())
	if err != nil {
		return nil, err
	}

	b.topics[name] = t
	b.initTopicReplication(name, meta.NumPartitions)
	return t, nil
}

func (b *Broker) registerListener(topic string, partitionID int32) chan struct{} {
	ch := make(chan struct{}, 10)
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
			{ApiKey: apiKeyListOffsets, MinVersion: 1, MaxVersion: 1},
			{ApiKey: apiKeyReplicaFetch, MinVersion: 1, MaxVersion: 1},
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
				if errors.Is(err, ErrInvalidTopicName) {
					partResp.ErrorCode = server.ErrInvalidTopicException
				} else {
					partResp.ErrorCode = server.ErrUnknownTopicOrPartition
				}
				topicResp.Partitions[j] = partResp
				continue
			}

			pObj := tObj.GetPartition(pReq.PartitionID)
			if pObj == nil {
				partResp.ErrorCode = server.ErrUnknownTopicOrPartition
				topicResp.Partitions[j] = partResp
				continue
			}

			// Epoch / leadership fence: reject produce if we are not the leader.
			if !b.isLeader(tReq.Name, pReq.PartitionID) {
				partResp.ErrorCode = server.ErrNotLeaderForPartition
				topicResp.Partitions[j] = partResp
				continue
			}

			// acks=all requires enough in-sync replicas before accepting.
			if produceReq.Acks == -1 {
				tracker := b.getOrCreateISRTracker(tReq.Name, pReq.PartitionID)
				if len(tracker.GetISR()) < b.minInsyncReplicas() {
					partResp.ErrorCode = server.ErrNotEnoughReplicas
					topicResp.Partitions[j] = partResp
					continue
				}
			}

			// Reject oversized record sets before decoding to avoid wasting
			// work on payloads that exceed the configured max message bytes.
			// MaxMessageBytes <= 0 means unlimited (no limit enforced).
			if b.config.Log.MaxMessageBytes > 0 && int64(len(pReq.RecordSet)) > b.config.Log.MaxMessageBytes {
				partResp.ErrorCode = server.ErrMessageTooLarge
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
					leaderLEO := pObj.LogEndOffset()
					hw := b.updateLeaderLEO(tReq.Name, pReq.PartitionID, leaderLEO)
					pObj.SetHighWatermark(hw)

					partResp.ErrorCode = server.ErrNone
					partResp.BaseOffset = baseOffset
					partResp.LogAppendTime = time.Now().UnixMilli()
					b.notifyAppended(tReq.Name, pReq.PartitionID)

					// acks=all: wait in purgatory until HW reaches the new LEO.
					if produceReq.Acks == -1 {
						if waitErr := b.waitForAcksAll(tReq.Name, pReq.PartitionID, leaderLEO, produceReq.TimeoutMs); waitErr != nil {
							if errors.Is(waitErr, errProduceTimeout) {
								partResp.ErrorCode = server.ErrRequestTimedOut
							} else {
								partResp.ErrorCode = server.ErrNotEnoughReplicas
							}
						}
					}
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

			// Consumers may only observe records below the high watermark.
			// Returning LEO-visible uncommitted data would break the
			// replication visibility invariant.
			hw := pObj.HighWatermark()
			partResp.ErrorCode = server.ErrNone
			partResp.HighWatermark = hw
			partResp.LogStartOffset = pObj.LogStartOffset()

			if pReq.FetchOffset >= hw {
				topicResp.Partitions[j] = partResp
				continue
			}

			records, err := pObj.ReadFrom(pReq.FetchOffset, pReq.MaxBytes)
			if err != nil && !errors.Is(err, io.EOF) {
				partResp.ErrorCode = server.ErrUnknown
			} else {
				var recordBuf bytes.Buffer
				for _, r := range records {
					if r.Offset >= hw {
						break
					}
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

	brokersMeta := b.clusterBrokerMetadata()

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
			leader := b.partitionLeader(topicName, pID)
			replicas := append([]int32(nil), b.replicaIDs...)
			isr := replicas
			if tracker := b.getISRTracker(topicName, pID); tracker != nil {
				isr = tracker.GetISR()
			}
			parts[pID] = protocol.PartitionMetadata{
				PartitionID: pID,
				Leader:      leader,
				Replicas:    replicas,
				ISR:         isr,
			}
		}

		topicMetas[i] = protocol.TopicMetadata{
			Name:       topicName,
			ErrorCode:  server.ErrNone,
			Partitions: parts,
		}
	}

	resp := &protocol.MetadataResponse{
		Brokers: brokersMeta,
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
		if err := validateTopicName(b.config.Broker.DataDir, tReq.Name); err != nil {
			respTopics[i] = protocol.CreateTopicResponseTopic{
				Name:      tReq.Name,
				ErrorCode: server.ErrInvalidTopicException,
			}
			continue
		}

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
		tObj, err := NewTopic(b.config.Broker.DataDir, tReq.Name, tReq.NumPartitions, b.storageConfig())
		if err != nil {
			b.mu.Unlock()
			code := server.ErrUnknown
			if errors.Is(err, ErrInvalidTopicName) {
				code = server.ErrInvalidTopicException
			}
			respTopics[i] = protocol.CreateTopicResponseTopic{
				Name:      tReq.Name,
				ErrorCode: code,
			}
			continue
		}
		b.topics[tReq.Name] = tObj
		b.initTopicReplication(tReq.Name, tReq.NumPartitions)
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

// handleReplicaFetch handles ReplicaFetch requests (apiKey 11).
func (b *Broker) handleReplicaFetch(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
	replicaReq, err := replication.DecodeReplicaFetchRequest(req.Payload)
	if err != nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	b.mu.RLock()
	t, ok := b.topics[replicaReq.Topic]
	b.mu.RUnlock()
	if !ok {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknownTopicOrPartition}, nil
	}

	p := t.GetPartition(replicaReq.Partition)
	if p == nil {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknownTopicOrPartition}, nil
	}

	// Only the leader serves replica fetches.
	if !b.isLeader(replicaReq.Topic, replicaReq.Partition) {
		return &protocol.ResponseFrame{ErrorCode: server.ErrNotLeaderForPartition}, nil
	}

	maxBytes := b.config.Replication.ReplicaFetchMaxBytes
	if maxBytes <= 0 {
		maxBytes = 1024 * 1024
	}
	records, err := p.ReadFrom(replicaReq.FetchOffset, maxBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return &protocol.ResponseFrame{ErrorCode: server.ErrUnknown}, nil
	}

	leaderLEO := p.LogEndOffset()
	followerLEO := replicaReq.FetchOffset
	if len(records) > 0 {
		followerLEO = records[len(records)-1].Offset + 1
	}

	// Keep leader LEO current, then advance follower LEO and recompute HW.
	_ = b.updateLeaderLEO(replicaReq.Topic, replicaReq.Partition, leaderLEO)
	hw := b.updateReplicaLEO(replicaReq.Topic, replicaReq.Partition, replicaReq.ReplicaID, followerLEO, leaderLEO)
	p.SetHighWatermark(hw)
	b.purgatory.CheckAndComplete(replicaReq.Topic, replicaReq.Partition, hw)

	payload, err := replication.EncodeReplicaFetchResponse(0, hw, records)
	if err != nil {
		return nil, err
	}
	return &protocol.ResponseFrame{ErrorCode: 0, Payload: payload}, nil
}
