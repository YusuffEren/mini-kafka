package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/storage"
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
// Consumer is safe for concurrent use by multiple goroutines; requests are
// serialised over a single connection.
type Consumer struct {
	addrs  []string
	config ConsumerConfig

	mu     sync.Mutex
	conn   net.Conn
	br     *bufio.Reader
	bw     *bufio.Writer
	closed bool
	nextID int32

	// reqMu serialises full request/response round-trips over the shared
	// connection. See Producer.reqMu for rationale.
	reqMu sync.Mutex
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
			// Wrap the fresh connection in buffered reader/writer. The old
			// bufio wrappers (if any) referenced the previous, now-closed
			// connection and must never be reused — any buffered bytes they
			// hold belong to a dead stream. Recreate them on every new conn.
			c.br = bufio.NewReaderSize(conn, 64*1024)
			c.bw = bufio.NewWriterSize(conn, 64*1024)
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
		// Drop references to the bufio wrappers tied to the closed conn so
		// that a subsequent getConn recreates them over the new connection
		// instead of reading stale buffered bytes from the old one.
		c.br = nil
		c.bw = nil
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

	// Serialise the full round-trip so concurrent Fetch callers cannot
	// interleave request frames or read each other's responses.
	c.reqMu.Lock()
	defer c.reqMu.Unlock()

	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}
	_ = conn // br/bw are the buffered views over conn; conn itself is not used directly.

	if _, err := frame.Write(c.bw); err != nil {
		c.closeConn()
		return nil, fmt.Errorf("consumer: write frame: %w", err)
	}
	// Flush after every request: bufio.Writer buffers writes locally and will
	// not push them to the conn until the buffer fills or Flush is called.
	// Skipping the flush leaves the request stuck in the buffer and the broker
	// never sees it, deadlocking the round-trip.
	if err := c.bw.Flush(); err != nil {
		c.closeConn()
		return nil, fmt.Errorf("consumer: flush frame: %w", err)
	}

	respFrame, err := protocol.ReadResponseFrame(c.br)
	if err != nil {
		c.closeConn()
		return nil, fmt.Errorf("consumer: read response: %w", err)
	}

	if respFrame.CorrelationID != corrID {
		// The response stream is out of sync with our request stream; the
		// connection is now unusable for any in-flight or future requests.
		c.closeConn()
		return nil, fmt.Errorf("consumer: correlation id mismatch: got %d, want %d", respFrame.CorrelationID, corrID)
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
		c.br = nil
		c.bw = nil
		return err
	}
	return nil
}
