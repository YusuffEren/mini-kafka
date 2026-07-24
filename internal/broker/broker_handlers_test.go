package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/replication"
	"github.com/YusuffEren/mini-kafka/internal/server"
	"github.com/YusuffEren/mini-kafka/internal/storage"
)

// testConfigAutoCreateDisabled returns a config with auto-create turned off.
func testConfigAutoCreateDisabled(t *testing.T) *config.Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.Topic.AutoCreate = false
	return cfg
}

func encodeRequest(t *testing.T, body interface{ Encode(io.Writer) error }) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := body.Encode(&buf); err != nil {
		t.Fatalf("encode request body: %v", err)
	}
	return buf.Bytes()
}

func TestBroker_Metadata_tum_topicleri_listeler(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// Create two topics via the broker's metadata manager.
	if _, _, err := b.metaManager.CreateTopic("orders", 3, 1); err != nil {
		t.Fatalf("CreateTopic orders error: %v", err)
	}
	if _, _, err := b.metaManager.CreateTopic("payments", 1, 1); err != nil {
		t.Fatalf("CreateTopic payments error: %v", err)
	}

	// Request metadata for all topics (nil Topics array).
	reqBody := &protocol.MetadataRequest{Topics: nil}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyMetadata, ApiVersion: 1, CorrelationID: 1, ClientID: "meta", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleMetadata(reqFrame)
	if err != nil {
		t.Fatalf("handleMetadata error: %v", err)
	}
	if respFrame.ErrorCode != 0 {
		t.Fatalf("response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}

	var resp protocol.MetadataResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("MetadataResponse.Decode error: %v", err)
	}
	if len(resp.Brokers) != 1 {
		t.Fatalf("len(Brokers) = %d, want 1", len(resp.Brokers))
	}
	if len(resp.Topics) != 2 {
		t.Fatalf("len(Topics) = %d, want 2", len(resp.Topics))
	}
	for _, tm := range resp.Topics {
		if tm.ErrorCode != server.ErrNone {
			t.Errorf("topic %s ErrorCode = %d, want 0", tm.Name, tm.ErrorCode)
		}
	}
	ordersFound := false
	for _, tm := range resp.Topics {
		if tm.Name == "orders" && len(tm.Partitions) == 3 {
			ordersFound = true
		}
	}
	if !ordersFound {
		t.Errorf("expected orders topic with 3 partitions")
	}
}

func TestBroker_Metadata_bilinmeyen_topic_auto_create_kapali_hata_doner(t *testing.T) {
	cfg := testConfigAutoCreateDisabled(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	reqBody := &protocol.MetadataRequest{Topics: []string{"missing-topic"}}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyMetadata, ApiVersion: 1, CorrelationID: 2, ClientID: "meta", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleMetadata(reqFrame)
	if err != nil {
		t.Fatalf("handleMetadata error: %v", err)
	}
	if respFrame.ErrorCode != 0 {
		t.Fatalf("response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}

	var resp protocol.MetadataResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("MetadataResponse.Decode error: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("len(Topics) = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Fatalf("topic ErrorCode = %d, want %d (UnknownTopicOrPartition)", resp.Topics[0].ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}

func TestBroker_Metadata_bilinmeyen_topic_auto_create_acik_olusturur(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	reqBody := &protocol.MetadataRequest{Topics: []string{"auto-topic"}}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyMetadata, ApiVersion: 1, CorrelationID: 3, ClientID: "meta", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleMetadata(reqFrame)
	if err != nil {
		t.Fatalf("handleMetadata error: %v", err)
	}

	var resp protocol.MetadataResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("MetadataResponse.Decode error: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("len(Topics) = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != server.ErrNone {
		t.Fatalf("topic ErrorCode = %d, want 0", resp.Topics[0].ErrorCode)
	}
	if len(resp.Topics[0].Partitions) != cfg.Topic.DefaultPartitions {
		t.Fatalf("partition count = %d, want %d", len(resp.Topics[0].Partitions), cfg.Topic.DefaultPartitions)
	}
	if b.getTopic("auto-topic") == nil {
		t.Errorf("auto-created topic was not registered in broker")
	}
}

func TestBroker_CreateTopics_yeni_topic_olusturur(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	reqBody := &protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicsRequestTopic{
			{Name: "created-topic", NumPartitions: 2, ReplicationFactor: 1},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyCreateTopics, ApiVersion: 1, CorrelationID: 4, ClientID: "create", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleCreateTopics(reqFrame)
	if err != nil {
		t.Fatalf("handleCreateTopics error: %v", err)
	}
	if respFrame.ErrorCode != 0 {
		t.Fatalf("response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}

	var resp protocol.CreateTopicsResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("CreateTopicsResponse.Decode error: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("len(Topics) = %d, want 1", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != server.ErrNone {
		t.Fatalf("topic ErrorCode = %d, want 0", resp.Topics[0].ErrorCode)
	}
	if b.getTopic("created-topic") == nil {
		t.Errorf("created topic was not registered in broker")
	}
}

func TestBroker_CreateTopics_gecersiz_bolum_sayisi_hata_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	reqBody := &protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicsRequestTopic{
			{Name: "bad-topic", NumPartitions: 0, ReplicationFactor: 1},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyCreateTopics, ApiVersion: 1, CorrelationID: 5, ClientID: "create", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleCreateTopics(reqFrame)
	if err != nil {
		t.Fatalf("handleCreateTopics error: %v", err)
	}

	var resp protocol.CreateTopicsResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("CreateTopicsResponse.Decode error: %v", err)
	}
	if resp.Topics[0].ErrorCode != server.ErrInvalidPartitionCount {
		t.Fatalf("topic ErrorCode = %d, want %d (InvalidPartitionCount)", resp.Topics[0].ErrorCode, server.ErrInvalidPartitionCount)
	}
}

func TestBroker_CreateTopics_zaten_var_olan_topic_hata_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// Pre-create the topic through the metadata manager.
	if _, _, err := b.metaManager.CreateTopic("existing-topic", 2, 1); err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	reqBody := &protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicsRequestTopic{
			{Name: "existing-topic", NumPartitions: 2, ReplicationFactor: 1},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyCreateTopics, ApiVersion: 1, CorrelationID: 6, ClientID: "create", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleCreateTopics(reqFrame)
	if err != nil {
		t.Fatalf("handleCreateTopics error: %v", err)
	}

	var resp protocol.CreateTopicsResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("CreateTopicsResponse.Decode error: %v", err)
	}
	if resp.Topics[0].ErrorCode != server.ErrTopicAlreadyExists {
		t.Fatalf("topic ErrorCode = %d, want %d (TopicAlreadyExists)", resp.Topics[0].ErrorCode, server.ErrTopicAlreadyExists)
	}
}

func TestBroker_OffsetCommit_ve_OffsetFetch(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// Commit offsets for a group/topic/partition.
	commitBody := &protocol.OffsetCommitRequest{
		GroupID: "group-1",
		Topics: []protocol.OffsetCommitRequestTopic{
			{
				Name: "topic-1",
				Partitions: []protocol.OffsetCommitRequestPartition{
					{PartitionID: 0, Offset: 42, Metadata: "meta"},
					{PartitionID: 1, Offset: 7, Metadata: "meta"},
				},
			},
		},
	}
	commitFrame := &protocol.RequestFrame{ApiKey: apiKeyOffsetCommit, ApiVersion: 1, CorrelationID: 7, ClientID: "consumer", Payload: encodeRequest(t, commitBody)}
	commitRespFrame, err := b.handleOffsetCommit(commitFrame)
	if err != nil {
		t.Fatalf("handleOffsetCommit error: %v", err)
	}
	if commitRespFrame.ErrorCode != 0 {
		t.Fatalf("OffsetCommit response ErrorCode = %d, want 0", commitRespFrame.ErrorCode)
	}
	var commitResp protocol.OffsetCommitResponse
	if err := commitResp.Decode(bytes.NewReader(commitRespFrame.Payload)); err != nil {
		t.Fatalf("OffsetCommitResponse.Decode error: %v", err)
	}
	if len(commitResp.Topics) != 1 || len(commitResp.Topics[0].Partitions) != 2 {
		t.Fatalf("unexpected OffsetCommit response shape")
	}
	for _, p := range commitResp.Topics[0].Partitions {
		if p.ErrorCode != server.ErrNone {
			t.Errorf("partition %d ErrorCode = %d, want 0", p.PartitionID, p.ErrorCode)
		}
	}

	// Fetch back the committed offsets.
	fetchBody := &protocol.OffsetFetchRequest{
		GroupID: "group-1",
		Topics: []protocol.OffsetFetchRequestTopic{
			{Name: "topic-1", Partitions: []int32{0, 1}},
		},
	}
	fetchFrame := &protocol.RequestFrame{ApiKey: apiKeyOffsetFetch, ApiVersion: 1, CorrelationID: 8, ClientID: "consumer", Payload: encodeRequest(t, fetchBody)}
	fetchRespFrame, err := b.handleOffsetFetch(fetchFrame)
	if err != nil {
		t.Fatalf("handleOffsetFetch error: %v", err)
	}
	if fetchRespFrame.ErrorCode != 0 {
		t.Fatalf("OffsetFetch response ErrorCode = %d, want 0", fetchRespFrame.ErrorCode)
	}
	var fetchResp protocol.OffsetFetchResponse
	if err := fetchResp.Decode(bytes.NewReader(fetchRespFrame.Payload)); err != nil {
		t.Fatalf("OffsetFetchResponse.Decode error: %v", err)
	}
	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 2 {
		t.Fatalf("unexpected OffsetFetch response shape")
	}
	got := make(map[int32]int64)
	for _, p := range fetchResp.Topics[0].Partitions {
		got[p.PartitionID] = p.Offset
	}
	if got[0] != 42 {
		t.Errorf("offset for partition 0 = %d, want 42", got[0])
	}
	if got[1] != 7 {
		t.Errorf("offset for partition 1 = %d, want 7", got[1])
	}
}

func TestBroker_ListOffsets_earliest_latest_default_timestamp(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// Create topic with one partition and append two records.
	_, _, _ = b.metaManager.CreateTopic("offset-topic", 1, 1)
	topic, err := NewTopic(cfg.Broker.DataDir, "offset-topic", 1, storage.Config{})
	if err != nil {
		t.Fatalf("NewTopic error: %v", err)
	}
	b.mu.Lock()
	b.topics["offset-topic"] = topic
	b.mu.Unlock()

	recs := []*storage.Record{
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2")},
	}
	if _, err := topic.GetPartition(0).AppendBatch(recs); err != nil {
		t.Fatalf("AppendBatch error: %v", err)
	}

	reqBody := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsRequestTopic{
			{
				Name: "offset-topic",
				Partitions: []protocol.ListOffsetsRequestPartition{
					{PartitionID: 0, Timestamp: -2}, // earliest
					{PartitionID: 0, Timestamp: -1}, // latest
					{PartitionID: 0, Timestamp: 0},  // default -> latest
				},
			},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyListOffsets, ApiVersion: 1, CorrelationID: 9, ClientID: "offsets", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleListOffsets(reqFrame)
	if err != nil {
		t.Fatalf("handleListOffsets error: %v", err)
	}
	if respFrame.ErrorCode != 0 {
		t.Fatalf("response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}

	var resp protocol.ListOffsetsResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("ListOffsetsResponse.Decode error: %v", err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 3 {
		t.Fatalf("unexpected response shape")
	}

	parts := resp.Topics[0].Partitions
	if parts[0].Offset != 0 {
		t.Errorf("earliest offset = %d, want 0", parts[0].Offset)
	}
	if parts[1].Offset != 2 {
		t.Errorf("latest offset = %d, want 2", parts[1].Offset)
	}
	if parts[2].Offset != 2 {
		t.Errorf("default timestamp offset = %d, want 2", parts[2].Offset)
	}
	for _, p := range parts {
		if p.ErrorCode != server.ErrNone {
			t.Errorf("partition %d ErrorCode = %d, want 0", p.PartitionID, p.ErrorCode)
		}
	}
}

func TestBroker_ListOffsets_bilinmeyen_topic_ve_partition_hata_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// Create a topic with one partition so that an invalid partition ID is detectable.
	_, _, _ = b.metaManager.CreateTopic("known-topic", 1, 1)
	topic, err := NewTopic(cfg.Broker.DataDir, "known-topic", 1, storage.Config{})
	if err != nil {
		t.Fatalf("NewTopic error: %v", err)
	}
	b.mu.Lock()
	b.topics["known-topic"] = topic
	b.mu.Unlock()

	reqBody := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsRequestTopic{
			{Name: "unknown-topic", Partitions: []protocol.ListOffsetsRequestPartition{{PartitionID: 0, Timestamp: -1}}},
			{Name: "known-topic", Partitions: []protocol.ListOffsetsRequestPartition{{PartitionID: 99, Timestamp: -1}}},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyListOffsets, ApiVersion: 1, CorrelationID: 10, ClientID: "offsets", Payload: encodeRequest(t, reqBody)}
	respFrame, err := b.handleListOffsets(reqFrame)
	if err != nil {
		t.Fatalf("handleListOffsets error: %v", err)
	}

	var resp protocol.ListOffsetsResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("ListOffsetsResponse.Decode error: %v", err)
	}
	if len(resp.Topics) != 2 {
		t.Fatalf("len(Topics) = %d, want 2", len(resp.Topics))
	}
	if resp.Topics[0].Partitions[0].ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Errorf("unknown topic ErrorCode = %d, want %d", resp.Topics[0].Partitions[0].ErrorCode, server.ErrUnknownTopicOrPartition)
	}
	if resp.Topics[1].Partitions[0].ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Errorf("invalid partition ErrorCode = %d, want %d", resp.Topics[1].Partitions[0].ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}

func TestBroker_ReplicaFetch_kayitlari_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	_, _, _ = b.metaManager.CreateTopic("replica-topic", 1, 1)
	topic, err := NewTopic(cfg.Broker.DataDir, "replica-topic", 1, storage.Config{})
	if err != nil {
		t.Fatalf("NewTopic error: %v", err)
	}
	b.mu.Lock()
	b.topics["replica-topic"] = topic
	b.mu.Unlock()

	recs := []*storage.Record{
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("rk"), Value: []byte("rv")},
	}
	if _, err := topic.GetPartition(0).AppendBatch(recs); err != nil {
		t.Fatalf("AppendBatch error: %v", err)
	}

	payload, err := replication.EncodeReplicaFetchRequest(&replication.ReplicaFetchRequest{
		ReplicaID:   2,
		MaxWaitMs:   0,
		Topic:       "replica-topic",
		Partition:   0,
		FetchOffset: 0,
	})
	if err != nil {
		t.Fatalf("EncodeReplicaFetchRequest error: %v", err)
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyReplicaFetch, ApiVersion: 1, CorrelationID: 11, ClientID: "replica", Payload: payload}
	respFrame, err := b.handleReplicaFetch(reqFrame)
	if err != nil {
		t.Fatalf("handleReplicaFetch error: %v", err)
	}
	if respFrame.ErrorCode != 0 {
		t.Fatalf("response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}
	if len(respFrame.Payload) < 10 {
		t.Fatalf("payload too short: %d bytes", len(respFrame.Payload))
	}
	gotErrCode := int16(binary.BigEndian.Uint16(respFrame.Payload[0:2]))
	gotHW := int64(binary.BigEndian.Uint64(respFrame.Payload[2:10]))
	if gotErrCode != 0 {
		t.Fatalf("replica response error code = %d, want 0", gotErrCode)
	}
	if gotHW != 1 {
		t.Fatalf("high watermark = %d, want 1", gotHW)
	}
	// Beyond the header, there should be at least one encoded record.
	if len(respFrame.Payload) <= 10 {
		t.Errorf("expected records after header, got %d bytes total", len(respFrame.Payload))
	}
}

func TestBroker_ReplicaFetch_bilinmeyen_topic_hata_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	payload, err := replication.EncodeReplicaFetchRequest(&replication.ReplicaFetchRequest{
		ReplicaID:   2,
		MaxWaitMs:   0,
		Topic:       "missing-topic",
		Partition:   0,
		FetchOffset: 0,
	})
	if err != nil {
		t.Fatalf("EncodeReplicaFetchRequest error: %v", err)
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyReplicaFetch, ApiVersion: 1, CorrelationID: 12, ClientID: "replica", Payload: payload}
	respFrame, err := b.handleReplicaFetch(reqFrame)
	if err != nil {
		t.Fatalf("handleReplicaFetch error: %v", err)
	}
	if respFrame.ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Fatalf("ErrorCode = %d, want %d (UnknownTopicOrPartition)", respFrame.ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}

func TestBroker_ReplicaFetch_bilinmeyen_partition_hata_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	_, _, _ = b.metaManager.CreateTopic("replica-topic2", 1, 1)
	topic, err := NewTopic(cfg.Broker.DataDir, "replica-topic2", 1, storage.Config{})
	if err != nil {
		t.Fatalf("NewTopic error: %v", err)
	}
	b.mu.Lock()
	b.topics["replica-topic2"] = topic
	b.mu.Unlock()

	payload, err := replication.EncodeReplicaFetchRequest(&replication.ReplicaFetchRequest{
		ReplicaID:   2,
		MaxWaitMs:   0,
		Topic:       "replica-topic2",
		Partition:   99,
		FetchOffset: 0,
	})
	if err != nil {
		t.Fatalf("EncodeReplicaFetchRequest error: %v", err)
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyReplicaFetch, ApiVersion: 1, CorrelationID: 13, ClientID: "replica", Payload: payload}
	respFrame, err := b.handleReplicaFetch(reqFrame)
	if err != nil {
		t.Fatalf("handleReplicaFetch error: %v", err)
	}
	if respFrame.ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Fatalf("ErrorCode = %d, want %d (UnknownTopicOrPartition)", respFrame.ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}

func TestBroker_Addr_nil_server(t *testing.T) {
	b := &Broker{server: nil}
	if b.Addr() != nil {
		t.Errorf("Addr() with nil server = %v, want nil", b.Addr())
	}
}

func TestBroker_getOrCreateTopic_auto_create(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// First call should auto-create.
	t1, err := b.getOrCreateTopic("auto-1", 0)
	if err != nil {
		t.Fatalf("getOrCreateTopic first call error: %v", err)
	}
	if t1 == nil {
		t.Fatal("getOrCreateTopic returned nil topic")
	}
	// Second call should return the existing topic.
	t2, err := b.getOrCreateTopic("auto-1", 0)
	if err != nil {
		t.Fatalf("getOrCreateTopic second call error: %v", err)
	}
	if t2 != t1 {
		t.Errorf("getOrCreateTopic returned different topic on second call")
	}

	// Explicit partition count should be respected.
	t3, err := b.getOrCreateTopic("auto-3", 5)
	if err != nil {
		t.Fatalf("getOrCreateTopic explicit partitions error: %v", err)
	}
	if int32(len(t3.Partitions)) != 5 {
		t.Errorf("explicit partition count = %d, want 5", len(t3.Partitions))
	}
}

func TestBroker_getOrCreateTopic_auto_create_kapali_hata_doner(t *testing.T) {
	cfg := testConfigAutoCreateDisabled(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	_, err = b.getOrCreateTopic("missing", 0)
	if err == nil {
		t.Fatal("getOrCreateTopic succeeded with auto-create disabled, want error")
	}
}

func TestBroker_JoinGroup_SyncGroup_Heartbeat_LeaveGroup(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// 1. JoinGroup: a new member joins a group.
	joinBody := &protocol.JoinGroupRequest{
		GroupID:          "group-a",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{Name: "range", Metadata: []byte("metadata")},
		},
	}
	joinFrame := &protocol.RequestFrame{ApiKey: apiKeyJoinGroup, ApiVersion: 1, CorrelationID: 14, ClientID: "client-a", Payload: encodeRequest(t, joinBody)}
	joinRespFrame, err := b.handleJoinGroup(joinFrame)
	if err != nil {
		t.Fatalf("handleJoinGroup error: %v", err)
	}
	if joinRespFrame.ErrorCode != 0 {
		t.Fatalf("JoinGroup response ErrorCode = %d, want 0", joinRespFrame.ErrorCode)
	}
	var joinResp protocol.JoinGroupResponse
	if err := joinResp.Decode(bytes.NewReader(joinRespFrame.Payload)); err != nil {
		t.Fatalf("JoinGroupResponse.Decode error: %v", err)
	}
	if joinResp.ErrorCode != server.ErrNone {
		t.Fatalf("JoinGroup body ErrorCode = %d, want 0", joinResp.ErrorCode)
	}
	if joinResp.MemberID == "" {
		t.Fatal("JoinGroup returned empty MemberID")
	}
	if joinResp.LeaderID != joinResp.MemberID {
		t.Fatalf("LeaderID = %s, want %s", joinResp.LeaderID, joinResp.MemberID)
	}
	memberID := joinResp.MemberID
	generationID := joinResp.GenerationID

	// 2. SyncGroup as leader with assignment.
	assignBody := &protocol.SyncGroupRequest{
		GroupID:      "group-a",
		GenerationID: generationID,
		MemberID:     memberID,
		Assignments: []protocol.SyncGroupAssignment{
			{MemberID: memberID, Assignment: []byte("assign")},
		},
	}
	assignFrame := &protocol.RequestFrame{ApiKey: apiKeySyncGroup, ApiVersion: 1, CorrelationID: 15, ClientID: "client-a", Payload: encodeRequest(t, assignBody)}
	syncRespFrame, err := b.handleSyncGroup(assignFrame)
	if err != nil {
		t.Fatalf("handleSyncGroup error: %v", err)
	}
	if syncRespFrame.ErrorCode != 0 {
		t.Fatalf("SyncGroup response ErrorCode = %d, want 0", syncRespFrame.ErrorCode)
	}
	var syncResp protocol.SyncGroupResponse
	if err := syncResp.Decode(bytes.NewReader(syncRespFrame.Payload)); err != nil {
		t.Fatalf("SyncGroupResponse.Decode error: %v", err)
	}
	if syncResp.ErrorCode != server.ErrNone {
		t.Fatalf("SyncGroup body ErrorCode = %d, want 0", syncResp.ErrorCode)
	}
	if string(syncResp.Assignment) != "assign" {
		t.Errorf("SyncGroup assignment = %q, want %q", syncResp.Assignment, "assign")
	}

	// 3. Heartbeat in stable state.
	hbBody := &protocol.HeartbeatRequest{GroupID: "group-a", GenerationID: generationID, MemberID: memberID}
	hbFrame := &protocol.RequestFrame{ApiKey: apiKeyHeartbeat, ApiVersion: 1, CorrelationID: 16, ClientID: "client-a", Payload: encodeRequest(t, hbBody)}
	hbRespFrame, err := b.handleHeartbeat(hbFrame)
	if err != nil {
		t.Fatalf("handleHeartbeat error: %v", err)
	}
	var hbResp protocol.HeartbeatResponse
	if err := hbResp.Decode(bytes.NewReader(hbRespFrame.Payload)); err != nil {
		t.Fatalf("HeartbeatResponse.Decode error: %v", err)
	}
	if hbResp.ErrorCode != server.ErrNone {
		t.Fatalf("Heartbeat ErrorCode = %d, want 0", hbResp.ErrorCode)
	}

	// 4. LeaveGroup.
	leaveBody := &protocol.LeaveGroupRequest{GroupID: "group-a", MemberID: memberID}
	leaveFrame := &protocol.RequestFrame{ApiKey: apiKeyLeaveGroup, ApiVersion: 1, CorrelationID: 17, ClientID: "client-a", Payload: encodeRequest(t, leaveBody)}
	leaveRespFrame, err := b.handleLeaveGroup(leaveFrame)
	if err != nil {
		t.Fatalf("handleLeaveGroup error: %v", err)
	}
	var leaveResp protocol.LeaveGroupResponse
	if err := leaveResp.Decode(bytes.NewReader(leaveRespFrame.Payload)); err != nil {
		t.Fatalf("LeaveGroupResponse.Decode error: %v", err)
	}
	if leaveResp.ErrorCode != server.ErrNone {
		t.Fatalf("LeaveGroup ErrorCode = %d, want 0", leaveResp.ErrorCode)
	}
}

func TestBroker_group_handlers_bilinmeyen_grup_hata_doner(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// SyncGroup for unknown group.
	syncBody := &protocol.SyncGroupRequest{GroupID: "unknown-group", GenerationID: 1, MemberID: "m-1"}
	syncFrame := &protocol.RequestFrame{ApiKey: apiKeySyncGroup, ApiVersion: 1, CorrelationID: 18, ClientID: "c", Payload: encodeRequest(t, syncBody)}
	syncRespFrame, err := b.handleSyncGroup(syncFrame)
	if err != nil {
		t.Fatalf("handleSyncGroup error: %v", err)
	}
	var syncResp protocol.SyncGroupResponse
	if err := syncResp.Decode(bytes.NewReader(syncRespFrame.Payload)); err != nil {
		t.Fatalf("SyncGroupResponse.Decode error: %v", err)
	}
	if syncResp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("SyncGroup unknown group ErrorCode = %d, want %d", syncResp.ErrorCode, server.ErrUnknownMemberID)
	}

	// Heartbeat for unknown group.
	hbBody := &protocol.HeartbeatRequest{GroupID: "unknown-group", GenerationID: 1, MemberID: "m-1"}
	hbFrame := &protocol.RequestFrame{ApiKey: apiKeyHeartbeat, ApiVersion: 1, CorrelationID: 19, ClientID: "c", Payload: encodeRequest(t, hbBody)}
	hbRespFrame, err := b.handleHeartbeat(hbFrame)
	if err != nil {
		t.Fatalf("handleHeartbeat error: %v", err)
	}
	var hbResp protocol.HeartbeatResponse
	if err := hbResp.Decode(bytes.NewReader(hbRespFrame.Payload)); err != nil {
		t.Fatalf("HeartbeatResponse.Decode error: %v", err)
	}
	if hbResp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("Heartbeat unknown group ErrorCode = %d, want %d", hbResp.ErrorCode, server.ErrUnknownMemberID)
	}

	// LeaveGroup for unknown group.
	leaveBody := &protocol.LeaveGroupRequest{GroupID: "unknown-group", MemberID: "m-1"}
	leaveFrame := &protocol.RequestFrame{ApiKey: apiKeyLeaveGroup, ApiVersion: 1, CorrelationID: 20, ClientID: "c", Payload: encodeRequest(t, leaveBody)}
	leaveRespFrame, err := b.handleLeaveGroup(leaveFrame)
	if err != nil {
		t.Fatalf("handleLeaveGroup error: %v", err)
	}
	var leaveResp protocol.LeaveGroupResponse
	if err := leaveResp.Decode(bytes.NewReader(leaveRespFrame.Payload)); err != nil {
		t.Fatalf("LeaveGroupResponse.Decode error: %v", err)
	}
	if leaveResp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("LeaveGroup unknown group ErrorCode = %d, want %d", leaveResp.ErrorCode, server.ErrUnknownMemberID)
	}
}

func TestBroker_Shutdown_topikleri_kapatir(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	// Create a topic directly and register it.
	_, _, _ = b.metaManager.CreateTopic("shutdown-topic", 1, 1)
	topic, err := NewTopic(cfg.Broker.DataDir, "shutdown-topic", 1, storage.Config{})
	if err != nil {
		t.Fatalf("NewTopic error: %v", err)
	}
	b.mu.Lock()
	b.topics["shutdown-topic"] = topic
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
	// After shutdown, calling Close on a partition should return nil/closed without panic.
	_ = topic.Close()
}

func TestBroker_Produce_bilinmeyen_topic_auto_create_kapali(t *testing.T) {
	cfg := testConfigAutoCreateDisabled(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	produceBody := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceRequestTopic{
			{
				Name: "missing-produce-topic",
				Partitions: []protocol.ProduceRequestPartition{
					{PartitionID: 0, RecordSet: []byte{}},
				},
			},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyProduce, ApiVersion: 1, CorrelationID: 21, ClientID: "producer", Payload: encodeRequest(t, produceBody)}
	respFrame, err := b.handleProduce(reqFrame)
	if err != nil {
		t.Fatalf("handleProduce error: %v", err)
	}
	if respFrame.ErrorCode != 0 {
		t.Fatalf("response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}
	var resp protocol.ProduceResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("ProduceResponse.Decode error: %v", err)
	}
	if resp.Topics[0].Partitions[0].ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Fatalf("partition ErrorCode = %d, want %d", resp.Topics[0].Partitions[0].ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}

func TestBroker_Produce_gecersiz_partition(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	// Auto-create topic.
	_, _ = b.getOrCreateTopic("produce-topic", 1)

	produceBody := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceRequestTopic{
			{
				Name: "produce-topic",
				Partitions: []protocol.ProduceRequestPartition{
					{PartitionID: 99, RecordSet: []byte{}},
				},
			},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyProduce, ApiVersion: 1, CorrelationID: 22, ClientID: "producer", Payload: encodeRequest(t, produceBody)}
	respFrame, err := b.handleProduce(reqFrame)
	if err != nil {
		t.Fatalf("handleProduce error: %v", err)
	}
	var resp protocol.ProduceResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("ProduceResponse.Decode error: %v", err)
	}
	if resp.Topics[0].Partitions[0].ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Fatalf("partition ErrorCode = %d, want %d", resp.Topics[0].Partitions[0].ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}

func TestBroker_Fetch_bilinmeyen_topic(t *testing.T) {
	cfg := testConfigAutoCreateDisabled(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	fetchBody := &protocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  1,
		MaxBytes:  1024 * 1024,
		Topics: []protocol.FetchRequestTopic{
			{Name: "missing-fetch-topic", Partitions: []protocol.FetchRequestPartition{{PartitionID: 0, FetchOffset: 0, MaxBytes: 1024 * 1024}}},
		},
	}
	reqFrame := &protocol.RequestFrame{ApiKey: apiKeyFetch, ApiVersion: 1, CorrelationID: 23, ClientID: "consumer", Payload: encodeRequest(t, fetchBody)}
	respFrame, err := b.handleFetch(reqFrame)
	if err != nil {
		t.Fatalf("handleFetch error: %v", err)
	}
	var resp protocol.FetchResponse
	if err := resp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("FetchResponse.Decode error: %v", err)
	}
	if resp.Topics[0].Partitions[0].ErrorCode != server.ErrUnknownTopicOrPartition {
		t.Fatalf("partition ErrorCode = %d, want %d", resp.Topics[0].Partitions[0].ErrorCode, server.ErrUnknownTopicOrPartition)
	}
}
