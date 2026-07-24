package broker

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/storage"
)

func TestBroker_Produce_and_Fetch_Integration(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// 1. Prepare record batch
	rec1 := &storage.Record{
		Offset:    0,
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("key-1"),
		Value:     []byte("value-1"),
	}
	rec2 := &storage.Record{
		Offset:    0,
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("key-2"),
		Value:     []byte("value-2"),
	}

	var recBuf bytes.Buffer
	if _, err := rec1.Encode(&recBuf); err != nil {
		t.Fatalf("rec1.Encode: %v", err)
	}
	if _, err := rec2.Encode(&recBuf); err != nil {
		t.Fatalf("rec2.Encode: %v", err)
	}

	produceBody := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceRequestTopic{
			{
				Name: "test-topic",
				Partitions: []protocol.ProduceRequestPartition{
					{
						PartitionID: 0,
						RecordSet:   recBuf.Bytes(),
					},
				},
			},
		},
	}

	var produceReqPayload bytes.Buffer
	if err := produceBody.Encode(&produceReqPayload); err != nil {
		t.Fatalf("produceBody.Encode: %v", err)
	}

	reqFrame := &protocol.RequestFrame{
		ApiKey:        apiKeyProduce,
		ApiVersion:    1,
		CorrelationID: 101,
		ClientID:      "test-producer",
		Payload:       produceReqPayload.Bytes(),
	}

	sendRequest(t, conn, reqFrame)
	respFrame := readResponse(t, conn)

	if respFrame.ErrorCode != 0 {
		t.Fatalf("Produce Response ErrorCode = %d, want 0", respFrame.ErrorCode)
	}

	var produceResp protocol.ProduceResponse
	if err := produceResp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		t.Fatalf("produceResp.Decode: %v", err)
	}

	if len(produceResp.Topics) != 1 || len(produceResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected produceResp topics/partitions count")
	}

	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Fatalf("partition produce ErrorCode = %d, want 0", partResp.ErrorCode)
	}
	if partResp.BaseOffset != 0 {
		t.Fatalf("BaseOffset = %d, want 0", partResp.BaseOffset)
	}

	// 2. Fetch records
	fetchBody := &protocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  1,
		MaxBytes:  1024 * 1024,
		Topics: []protocol.FetchRequestTopic{
			{
				Name: "test-topic",
				Partitions: []protocol.FetchRequestPartition{
					{
						PartitionID: 0,
						FetchOffset: 0,
						MaxBytes:    1024 * 1024,
					},
				},
			},
		},
	}

	var fetchReqPayload bytes.Buffer
	if err := fetchBody.Encode(&fetchReqPayload); err != nil {
		t.Fatalf("fetchBody.Encode: %v", err)
	}

	fetchReqFrame := &protocol.RequestFrame{
		ApiKey:        apiKeyFetch,
		ApiVersion:    1,
		CorrelationID: 102,
		ClientID:      "test-consumer",
		Payload:       fetchReqPayload.Bytes(),
	}

	sendRequest(t, conn, fetchReqFrame)
	fetchRespFrame := readResponse(t, conn)

	if fetchRespFrame.ErrorCode != 0 {
		t.Fatalf("Fetch Response ErrorCode = %d, want 0", fetchRespFrame.ErrorCode)
	}

	var fetchResp protocol.FetchResponse
	if err := fetchResp.Decode(bytes.NewReader(fetchRespFrame.Payload)); err != nil {
		t.Fatalf("fetchResp.Decode: %v", err)
	}

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Fatalf("Fetch partition ErrorCode = %d, want 0", fetchPartResp.ErrorCode)
	}

	// Decode records from returned recordSet
	rsBuf := bytes.NewReader(fetchPartResp.RecordSet)
	r1, _, err := storage.DecodeRecord(rsBuf)
	if err != nil {
		t.Fatalf("DecodeRecord r1: %v", err)
	}
	r2, _, err := storage.DecodeRecord(rsBuf)
	if err != nil {
		t.Fatalf("DecodeRecord r2: %v", err)
	}

	if string(r1.Key) != "key-1" || string(r1.Value) != "value-1" {
		t.Errorf("r1 mismatch: key=%s value=%s", string(r1.Key), string(r1.Value))
	}
	if string(r2.Key) != "key-2" || string(r2.Value) != "value-2" {
		t.Errorf("r2 mismatch: key=%s value=%s", string(r2.Key), string(r2.Value))
	}
}

func TestBroker_LongPolling_Integration(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	consumerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial consumer error: %v", err)
	}
	defer func() { _ = consumerConn.Close() }()

	producerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial producer error: %v", err)
	}
	defer func() { _ = producerConn.Close() }()

	// Consumer sends Fetch with FetchOffset=0, MaxWaitMs=1000 on an empty log
	fetchBody := &protocol.FetchRequest{
		MaxWaitMs: 1000,
		MinBytes:  1,
		MaxBytes:  1024 * 1024,
		Topics: []protocol.FetchRequestTopic{
			{
				Name: "test-topic",
				Partitions: []protocol.FetchRequestPartition{
					{
						PartitionID: 0,
						FetchOffset: 0,
						MaxBytes:    1024 * 1024,
					},
				},
			},
		},
	}

	var fetchReqPayload bytes.Buffer
	_ = fetchBody.Encode(&fetchReqPayload)
	fetchReqFrame := &protocol.RequestFrame{
		ApiKey:        apiKeyFetch,
		ApiVersion:    1,
		CorrelationID: 201,
		ClientID:      "lp-consumer",
		Payload:       fetchReqPayload.Bytes(),
	}

	startTime := time.Now()

	// Producer appends after 150ms
	go func() {
		time.Sleep(150 * time.Millisecond)

		rec := &storage.Record{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("lp-key"),
			Value:     []byte("lp-value"),
		}
		var recBuf bytes.Buffer
		_, _ = rec.Encode(&recBuf)

		produceBody := &protocol.ProduceRequest{
			Acks:      1,
			TimeoutMs: 1000,
			Topics: []protocol.ProduceRequestTopic{
				{
					Name: "test-topic",
					Partitions: []protocol.ProduceRequestPartition{
						{PartitionID: 0, RecordSet: recBuf.Bytes()},
					},
				},
			},
		}

		var produceReqPayload bytes.Buffer
		_ = produceBody.Encode(&produceReqPayload)
		reqFrame := &protocol.RequestFrame{
			ApiKey:        apiKeyProduce,
			ApiVersion:    1,
			CorrelationID: 202,
			ClientID:      "lp-producer",
			Payload:       produceReqPayload.Bytes(),
		}
		sendRequest(t, producerConn, reqFrame)
		_ = readResponse(t, producerConn)
	}()

	sendRequest(t, consumerConn, fetchReqFrame)
	fetchRespFrame := readResponse(t, consumerConn)
	elapsed := time.Since(startTime)

	if elapsed < 100*time.Millisecond {
		t.Errorf("long-poll returned too fast: %v", elapsed)
	}
	if elapsed > 900*time.Millisecond {
		t.Errorf("long-poll timed out instead of being woken up: %v", elapsed)
	}

	var fetchResp protocol.FetchResponse
	if err := fetchResp.Decode(bytes.NewReader(fetchRespFrame.Payload)); err != nil {
		t.Fatalf("fetchResp.Decode: %v", err)
	}

	rsBuf := bytes.NewReader(fetchResp.Topics[0].Partitions[0].RecordSet)
	r, _, err := storage.DecodeRecord(rsBuf)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if string(r.Key) != "lp-key" || string(r.Value) != "lp-value" {
		t.Errorf("record mismatch: key=%s value=%s", string(r.Key), string(r.Value))
	}
}
