// Package client provides high-level Producer and Consumer client implementations
// for communicating with mini-kafka brokers over TCP.
package client

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yusuf/mini-kafka/internal/protocol"
	"github.com/yusuf/mini-kafka/internal/storage"
)

// Message represents a single key-value record published or consumed.
type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64
	Timestamp int64
}

// ProducerConfig holds parameters for the Producer client.
type ProducerConfig struct {
	ClientID  string
	Acks      int16
	TimeoutMs int32
	LingerMs  int32
	BatchSize int
}

// DefaultProducerConfig returns sensible default producer parameters.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		ClientID:  "mini-kafka-producer",
		Acks:      1,
		TimeoutMs: 30000,
		LingerMs:  0,
		BatchSize: 16384,
	}
}

// Producer manages publishing record batches to mini-kafka brokers.
type Producer struct {
	addrs  []string
	config ProducerConfig

	mu     sync.Mutex
	conn   net.Conn
	closed bool
	nextID int32
}

// NewProducer constructs a Producer targeting the provided broker addresses.
func NewProducer(addrs []string, cfg ProducerConfig) (*Producer, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("producer: at least one broker address required")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "mini-kafka-producer"
	}
	if cfg.Acks == 0 {
		cfg.Acks = 1
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 30000
	}

	return &Producer{
		addrs:  addrs,
		config: cfg,
	}, nil
}

func (p *Producer) getConn() (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("producer closed")
	}
	if p.conn != nil {
		return p.conn, nil
	}

	var lastErr error
	for _, addr := range p.addrs {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			p.conn = conn
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("producer connect failed: %w", lastErr)
}

func (p *Producer) closeConn() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

// Send publishes a single message and returns its assigned offset.
func (p *Producer) Send(ctx context.Context, topic string, partition int32, key, value []byte) (int64, error) {
	offsets, err := p.SendBatch(ctx, topic, partition, []Message{{Key: key, Value: value}})
	if err != nil {
		return -1, err
	}
	if len(offsets) == 0 {
		return -1, fmt.Errorf("producer: empty offset response")
	}
	return offsets[0], nil
}

// SendBatch publishes a slice of messages to a single topic-partition.
func (p *Producer) SendBatch(ctx context.Context, topic string, partition int32, msgs []Message) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	if p.config.LingerMs > 0 {
		time.Sleep(time.Duration(p.config.LingerMs) * time.Millisecond)
	}

	var recBuf bytes.Buffer
	now := time.Now().UnixMilli()
	for _, msg := range msgs {
		ts := msg.Timestamp
		if ts <= 0 {
			ts = now
		}
		rec := &storage.Record{
			Timestamp: ts,
			Key:       msg.Key,
			Value:     msg.Value,
		}
		if _, err := rec.Encode(&recBuf); err != nil {
			return nil, fmt.Errorf("producer: record encode: %w", err)
		}
	}

	produceReq := &protocol.ProduceRequest{
		Acks:      p.config.Acks,
		TimeoutMs: p.config.TimeoutMs,
		Topics: []protocol.ProduceRequestTopic{
			{
				Name: topic,
				Partitions: []protocol.ProduceRequestPartition{
					{
						PartitionID: partition,
						RecordSet:   recBuf.Bytes(),
					},
				},
			},
		},
	}

	var payload bytes.Buffer
	if err := produceReq.Encode(&payload); err != nil {
		return nil, fmt.Errorf("producer: produce request encode: %w", err)
	}

	corrID := atomic.AddInt32(&p.nextID, 1)
	frame := &protocol.RequestFrame{
		ApiKey:        0, // Produce
		ApiVersion:    1,
		CorrelationID: corrID,
		ClientID:      p.config.ClientID,
		Payload:       payload.Bytes(),
	}

	conn, err := p.getConn()
	if err != nil {
		return nil, err
	}

	if _, err := frame.Write(conn); err != nil {
		p.closeConn()
		return nil, fmt.Errorf("producer: write frame: %w", err)
	}

	respFrame, err := protocol.ReadResponseFrame(conn)
	if err != nil {
		p.closeConn()
		return nil, fmt.Errorf("producer: read response: %w", err)
	}

	if respFrame.ErrorCode != 0 {
		return nil, fmt.Errorf("producer: broker returned error code %d", respFrame.ErrorCode)
	}

	var produceResp protocol.ProduceResponse
	if err := produceResp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		return nil, fmt.Errorf("producer: produce response decode: %w", err)
	}

	if len(produceResp.Topics) == 0 || len(produceResp.Topics[0].Partitions) == 0 {
		return nil, fmt.Errorf("producer: malformed response from broker")
	}

	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		return nil, fmt.Errorf("producer: partition error code %d", partResp.ErrorCode)
	}

	offsets := make([]int64, len(msgs))
	for i := range msgs {
		offsets[i] = partResp.BaseOffset + int64(i)
	}
	return offsets, nil
}

// Close terminates active connections and releases resources.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.conn != nil {
		err := p.conn.Close()
		p.conn = nil
		return err
	}
	return nil
}
