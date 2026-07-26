// Package client provides high-level Producer and Consumer client implementations
// for communicating with mini-kafka brokers over TCP.
package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/storage"
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

// topicPartition identifies a single partition within a topic for batching.
type topicPartition struct {
	topic     string
	partition int32
}

// batchResult is delivered to a waiting Send caller after its record is flushed.
type batchResult struct {
	offset int64
	err    error
}

// pendingBatch accumulates records for one topic-partition until flush.
type pendingBatch struct {
	records []*storage.Record
	bytes   int
	waiters []chan batchResult
	timer   *time.Timer
}

// batcher accumulates records per topic-partition and flushes them as a single
// Produce request when BatchSize is reached or LingerMs elapses after the first
// record in the batch.
type batcher struct {
	mu      sync.Mutex
	pending map[topicPartition]*pendingBatch
}

// Producer manages publishing record batches to mini-kafka brokers.
// Producer is safe for concurrent use by multiple goroutines; requests are
// serialised over a single connection.
//
// When LingerMs > 0, Send accumulates records via an internal batcher and
// blocks until the batch is flushed (by size or linger timer). LingerMs == 0
// keeps the synchronous per-call Produce path.
type Producer struct {
	addrs  []string
	config ProducerConfig

	mu   sync.Mutex
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
	// closed rejects new Send/SendBatch calls. It is set at the start of Close
	// so no further records are enqueued while pending batches drain.
	closed bool
	// finalized is set at the end of Close after the connection is torn down.
	// getConn may still dial/use the connection while closed&&!finalized so
	// that flushAllPending can complete Produce round-trips during Close.
	finalized bool
	nextID    int32

	// reqMu serialises full request/response round-trips over the shared
	// connection. It is held for the entire write+read sequence of a single
	// request so that concurrent goroutines cannot interleave frames or read
	// each other's responses. It is intentionally separate from mu, which
	// only guards the connection/closed state.
	reqMu sync.Mutex

	// batch is used only when config.LingerMs > 0.
	batch batcher

	// batchFill tracks average batch fill ratio (bytes/BatchSize) across flushes
	// when LingerMs > 0. Safe for concurrent flushBatch callers via batchFillMu.
	batchFillMu  sync.Mutex
	batchFillSum float64
	batchFillN   int
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
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 16384
	}

	return &Producer{
		addrs:  addrs,
		config: cfg,
		batch: batcher{
			pending: make(map[topicPartition]*pendingBatch),
		},
	}, nil
}

func (p *Producer) getConn() (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		return p.conn, nil
	}
	// Reject dials only after Close has finished draining. While
	// closed && !finalized, Close may still need to Produce pending batches.
	if p.finalized {
		return nil, fmt.Errorf("producer closed")
	}

	var lastErr error
	for _, addr := range p.addrs {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			p.conn = conn
			// Wrap the fresh connection in buffered reader/writer. The old
			// bufio wrappers (if any) referenced the previous, now-closed
			// connection and must never be reused — any buffered bytes they
			// hold belong to a dead stream. Recreate them on every new conn.
			p.br = bufio.NewReaderSize(conn, 64*1024)
			p.bw = bufio.NewWriterSize(conn, 64*1024)
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
		// Drop references to the bufio wrappers tied to the closed conn so
		// that a subsequent getConn recreates them over the new connection
		// instead of reading stale buffered bytes from the old one.
		p.br = nil
		p.bw = nil
	}
}

func (p *Producer) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Send publishes a single message and returns its assigned offset.
//
// When LingerMs > 0 the message is enqueued into the per-partition batch and
// the call blocks until the batch is flushed (size threshold or linger timer).
// When LingerMs == 0 the message is sent synchronously in its own Produce.
func (p *Producer) Send(ctx context.Context, topic string, partition int32, key, value []byte) (int64, error) {
	if p.config.LingerMs > 0 {
		return p.sendBatched(ctx, topic, partition, key, value)
	}
	offsets, err := p.SendBatch(ctx, topic, partition, []Message{{Key: key, Value: value}})
	if err != nil {
		return -1, err
	}
	if len(offsets) == 0 {
		return -1, fmt.Errorf("producer: empty offset response")
	}
	return offsets[0], nil
}

// sendBatched enqueues one record into the batcher and blocks until flush.
func (p *Producer) sendBatched(ctx context.Context, topic string, partition int32, key, value []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if p.isClosed() {
		return -1, fmt.Errorf("producer closed")
	}

	now := time.Now().UnixMilli()
	rec := &storage.Record{
		Timestamp: now,
		Key:       key,
		Value:     value,
	}
	recBytes := rec.EncodedSize()

	// Buffered so the flusher never blocks on a cancelled/abandoned waiter.
	waiter := make(chan batchResult, 1)
	tp := topicPartition{topic: topic, partition: partition}

	var toFlush *pendingBatch

	p.batch.mu.Lock()
	if p.isClosed() {
		p.batch.mu.Unlock()
		return -1, fmt.Errorf("producer closed")
	}

	pb := p.batch.pending[tp]
	if pb == nil {
		pb = &pendingBatch{}
		p.batch.pending[tp] = pb
		linger := time.Duration(p.config.LingerMs) * time.Millisecond
		// Capture pb identity so a late timer after size-flush is a no-op.
		pb.timer = time.AfterFunc(linger, func() {
			p.lingerFire(tp, pb)
		})
	}

	pb.records = append(pb.records, rec)
	pb.bytes += recBytes
	pb.waiters = append(pb.waiters, waiter)

	if pb.bytes >= p.config.BatchSize {
		// Detach under lock; I/O happens outside.
		delete(p.batch.pending, tp)
		if pb.timer != nil {
			pb.timer.Stop()
			pb.timer = nil
		}
		toFlush = pb
	}
	p.batch.mu.Unlock()

	if toFlush != nil {
		p.flushBatch(tp, toFlush)
	}

	select {
	case res := <-waiter:
		if res.err != nil {
			return -1, res.err
		}
		return res.offset, nil
	case <-ctx.Done():
		// Record stays in its batch (or is already in-flight). The buffered
		// waiter absorbs the eventual result so the flusher never blocks.
		return -1, ctx.Err()
	}
}

// lingerFire is the timer callback for a pending batch. It flushes only if
// this exact batch is still the one registered for tp (timer/size race).
func (p *Producer) lingerFire(tp topicPartition, pb *pendingBatch) {
	p.batch.mu.Lock()
	cur, ok := p.batch.pending[tp]
	if !ok || cur != pb {
		// Already flushed by size, Close, or a previous timer.
		p.batch.mu.Unlock()
		return
	}
	delete(p.batch.pending, tp)
	pb.timer = nil
	p.batch.mu.Unlock()

	p.flushBatch(tp, pb)
}

// flushBatch sends one Produce for the detached batch and fans out results.
// Must NOT hold batch.mu — network I/O runs unlocked.
func (p *Producer) flushBatch(tp topicPartition, pb *pendingBatch) {
	if pb == nil || len(pb.records) == 0 {
		return
	}

	// Record fill ratio: accumulated batch bytes / configured BatchSize.
	// len(batch) in the harness sense is the byte length of the pending batch.
	if p.config.BatchSize > 0 {
		ratio := float64(pb.bytes) / float64(p.config.BatchSize)
		p.batchFillMu.Lock()
		p.batchFillSum += ratio
		p.batchFillN++
		p.batchFillMu.Unlock()
	}

	offsets, err := p.produceRecords(tp.topic, tp.partition, pb.records)
	if err != nil {
		for _, w := range pb.waiters {
			w <- batchResult{offset: -1, err: err}
		}
		return
	}
	for i, w := range pb.waiters {
		if i >= len(offsets) {
			w <- batchResult{offset: -1, err: fmt.Errorf("producer: missing offset for record %d", i)}
			continue
		}
		w <- batchResult{offset: offsets[i], err: nil}
	}
}

// flushAllPending detaches every pending batch and flushes them. Used by Close.
func (p *Producer) flushAllPending() {
	p.batch.mu.Lock()
	batches := make(map[topicPartition]*pendingBatch, len(p.batch.pending))
	for tp, pb := range p.batch.pending {
		batches[tp] = pb
		if pb.timer != nil {
			pb.timer.Stop()
			pb.timer = nil
		}
	}
	p.batch.pending = make(map[topicPartition]*pendingBatch)
	p.batch.mu.Unlock()

	for tp, pb := range batches {
		p.flushBatch(tp, pb)
	}
}

// SendBatch publishes a slice of messages to a single topic-partition
// synchronously (one Produce round-trip). It does not use the linger batcher;
// callers that want linger accumulation should use Send.
func (p *Producer) SendBatch(ctx context.Context, topic string, partition int32, msgs []Message) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.isClosed() {
		return nil, fmt.Errorf("producer closed")
	}

	now := time.Now().UnixMilli()
	records := make([]*storage.Record, len(msgs))
	for i, msg := range msgs {
		ts := msg.Timestamp
		if ts <= 0 {
			ts = now
		}
		records[i] = &storage.Record{
			Timestamp: ts,
			Key:       msg.Key,
			Value:     msg.Value,
		}
	}
	return p.produceRecords(topic, partition, records)
}

// produceRecords encodes records and performs a single Produce round-trip.
// Concurrent callers are serialised by reqMu.
func (p *Producer) produceRecords(topic string, partition int32, records []*storage.Record) ([]int64, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var recBuf bytes.Buffer
	for _, rec := range records {
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

	// Serialise the full round-trip so concurrent Send/SendBatch callers
	// cannot interleave request frames or read each other's responses.
	p.reqMu.Lock()
	defer p.reqMu.Unlock()

	conn, err := p.getConn()
	if err != nil {
		return nil, err
	}
	_ = conn // br/bw are the buffered views over conn; conn itself is not used directly.

	if _, err := frame.Write(p.bw); err != nil {
		p.closeConn()
		return nil, fmt.Errorf("producer: write frame: %w", err)
	}
	// Flush after every request: bufio.Writer buffers writes locally and will
	// not push them to the conn until the buffer fills or Flush is called.
	// Skipping the flush leaves the request stuck in the buffer and the broker
	// never sees it, deadlocking the round-trip.
	if err := p.bw.Flush(); err != nil {
		p.closeConn()
		return nil, fmt.Errorf("producer: flush frame: %w", err)
	}

	respFrame, err := protocol.ReadResponseFrame(p.br)
	if err != nil {
		p.closeConn()
		return nil, fmt.Errorf("producer: read response: %w", err)
	}

	if respFrame.CorrelationID != corrID {
		// The response stream is out of sync with our request stream; the
		// connection is now unusable for any in-flight or future requests.
		p.closeConn()
		return nil, fmt.Errorf("producer: correlation id mismatch: got %d, want %d", respFrame.CorrelationID, corrID)
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

	offsets := make([]int64, len(records))
	for i := range records {
		offsets[i] = partResp.BaseOffset + int64(i)
	}
	return offsets, nil
}

// AvgBatchFillRatio returns the mean batch fill ratio (batch bytes / BatchSize)
// observed across all flushes since the Producer was created. Returns 0 when
// no batched flush has occurred (LingerMs == 0 or no Send calls).
func (p *Producer) AvgBatchFillRatio() float64 {
	p.batchFillMu.Lock()
	defer p.batchFillMu.Unlock()
	if p.batchFillN == 0 {
		return 0
	}
	return p.batchFillSum / float64(p.batchFillN)
}

// Close flushes any pending linger batches, terminates active connections, and
// releases resources. Waiting Send callers are released with either a successful
// offset or an error. Close is safe to call multiple times.
func (p *Producer) Close() error {
	p.mu.Lock()
	if p.finalized {
		p.mu.Unlock()
		return nil
	}
	// Reject new enqueues/sends, but keep finalized=false so getConn can still
	// dial or reuse the connection while we drain pending batches.
	alreadyClosing := p.closed
	p.closed = true
	p.mu.Unlock()

	if !alreadyClosing && p.config.LingerMs > 0 {
		p.flushAllPending()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finalized {
		return nil
	}
	p.finalized = true
	if p.conn != nil {
		err := p.conn.Close()
		p.conn = nil
		p.br = nil
		p.bw = nil
		return err
	}
	return nil
}
