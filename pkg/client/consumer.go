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

	"github.com/yusuf/mini-kafka/internal/protocol"
	"github.com/yusuf/mini-kafka/internal/storage"
)

// ConsumerConfig holds parameters for the Consumer client.
type ConsumerConfig struct {
	ClientID  string
	MaxWaitMs int32
	MinBytes  int32
	MaxBytes  int32
}

// DefaultConsumerConfig returns sensible default consumer parameters.
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		ClientID:  "mini-kafka-consumer",
		MaxWaitMs: 500,
		MinBytes:  1,
		MaxBytes:  1024 * 1024,
	}
}

// Consumer manages fetching records from mini-kafka brokers.
type Consumer struct {
	addrs  []string
	config ConsumerConfig

	mu     sync.Mutex
	conn   net.Conn
	closed bool
	nextID int32
}

// NewConsumer constructs a Consumer targeting the provided broker addresses.
func NewConsumer(addrs []string, cfg ConsumerConfig) (*Consumer, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("consumer: at least one broker address required")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "mini-kafka-consumer"
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1024 * 1024
	}

	return &Consumer{
		addrs:  addrs,
		config: cfg,
	}, nil
}

func (c *Consumer) getConn() (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("consumer closed")
	}
	if c.conn != nil {
		return c.conn, nil
	}

	var lastErr error
	for _, addr := range c.addrs {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			c.conn = conn
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("consumer connect failed: %w", lastErr)
}

func (c *Consumer) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// Fetch requests records from a single topic-partition starting at fetchOffset.
func (c *Consumer) Fetch(ctx context.Context, topic string, partition int32, fetchOffset int64) ([]Message, error) {
	fetchReq := &protocol.FetchRequest{
		MaxWaitMs: c.config.MaxWaitMs,
		MinBytes:  c.config.MinBytes,
		MaxBytes:  c.config.MaxBytes,
		Topics: []protocol.FetchRequestTopic{
			{
				Name: topic,
				Partitions: []protocol.FetchRequestPartition{
					{
						PartitionID: partition,
						FetchOffset: fetchOffset,
						MaxBytes:    c.config.MaxBytes,
					},
				},
			},
		},
	}

	var payload bytes.Buffer
	if err := fetchReq.Encode(&payload); err != nil {
		return nil, fmt.Errorf("consumer: fetch request encode: %w", err)
	}

	corrID := atomic.AddInt32(&c.nextID, 1)
	frame := &protocol.RequestFrame{
		ApiKey:        1, // Fetch
		ApiVersion:    1,
		CorrelationID: corrID,
		ClientID:      c.config.ClientID,
		Payload:       payload.Bytes(),
	}

	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}

	if _, err := frame.Write(conn); err != nil {
		c.closeConn()
		return nil, fmt.Errorf("consumer: write frame: %w", err)
	}

	respFrame, err := protocol.ReadResponseFrame(conn)
	if err != nil {
		c.closeConn()
		return nil, fmt.Errorf("consumer: read response: %w", err)
	}

	if respFrame.ErrorCode != 0 {
		return nil, fmt.Errorf("consumer: broker error code %d", respFrame.ErrorCode)
	}

	var fetchResp protocol.FetchResponse
	if err := fetchResp.Decode(bytes.NewReader(respFrame.Payload)); err != nil {
		return nil, fmt.Errorf("consumer: fetch response decode: %w", err)
	}

	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		return nil, fmt.Errorf("consumer: empty response from broker")
	}

	partResp := fetchResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		return nil, fmt.Errorf("consumer: partition error code %d", partResp.ErrorCode)
	}

	var messages []Message
	buf := bytes.NewReader(partResp.RecordSet)
	for buf.Len() > 0 {
		rec, _, err := storage.DecodeRecord(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("consumer: record decode error: %w", err)
		}
		messages = append(messages, Message{
			Key:       rec.Key,
			Value:     rec.Value,
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
		})
	}

	return messages, nil
}

// Close terminates active connections and releases resources.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}
