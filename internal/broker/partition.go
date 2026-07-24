package broker

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/yusuf/mini-kafka/internal/storage"
)

type appendResponse struct {
	baseOffset int64
	err        error
}

type appendRequest struct {
	records []*storage.Record
	respCh  chan appendResponse
}

// Partition represents a single topic partition backed by a storage Log.
// It uses a single writer goroutine to process append requests sequentially,
// eliminating lock contention during record appends.
type Partition struct {
	Topic         string
	ID            int32
	log           *storage.Log
	highWatermark int64
	appendCh      chan appendRequest
	closeCh       chan struct{}
	wg            sync.WaitGroup
	mu            sync.RWMutex
}

// NewPartition initializes a Partition instance for a topic and partition ID.
func NewPartition(baseDir string, topic string, partitionID int32, cfg storage.Config) (*Partition, error) {
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("%d", partitionID))
	log, err := storage.NewLog(dir, cfg)
	if err != nil {
		return nil, fmt.Errorf("partition %s-%d: %w", topic, partitionID, err)
	}

	p := &Partition{
		Topic:         topic,
		ID:            partitionID,
		log:           log,
		highWatermark: log.HighestOffset(),
		appendCh:      make(chan appendRequest, 256),
		closeCh:       make(chan struct{}),
	}

	p.wg.Add(1)
	go p.writerLoop()

	return p, nil
}

func (p *Partition) writerLoop() {
	defer p.wg.Done()
	for {
		select {
		case req := <-p.appendCh:
			baseOffset, err := p.log.AppendBatch(req.records)
			if err == nil {
				p.mu.Lock()
				// Default single-broker HW behavior: HW equals LEO
				p.highWatermark = p.log.HighestOffset()
				p.mu.Unlock()
			}
			req.respCh <- appendResponse{baseOffset: baseOffset, err: err}
		case <-p.closeCh:
			// Drain remaining append requests
			for {
				select {
				case req := <-p.appendCh:
					baseOffset, err := p.log.AppendBatch(req.records)
					if err == nil {
						p.mu.Lock()
						p.highWatermark = p.log.HighestOffset()
						p.mu.Unlock()
					}
					req.respCh <- appendResponse{baseOffset: baseOffset, err: err}
				default:
					return
				}
			}
		}
	}
}

// AppendBatch enqueues a batch of records to the partition writer loop.
func (p *Partition) AppendBatch(records []*storage.Record) (int64, error) {
	respCh := make(chan appendResponse, 1)
	req := appendRequest{
		records: records,
		respCh:  respCh,
	}

	select {
	case p.appendCh <- req:
		res := <-respCh
		return res.baseOffset, res.err
	case <-p.closeCh:
		return -1, fmt.Errorf("partition %s-%d is closed", p.Topic, p.ID)
	}
}

// ReadFrom reads records starting from offset.
func (p *Partition) ReadFrom(offset int64, maxBytes int32) ([]*storage.Record, error) {
	return p.log.ReadFrom(offset, maxBytes)
}

// HighWatermark returns current high watermark.
func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.highWatermark
}

// SetHighWatermark updates the high watermark for this partition (used in replication).
func (p *Partition) SetHighWatermark(hw int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.highWatermark = hw
}

// LogStartOffset returns lowest offset in partition.
func (p *Partition) LogStartOffset() int64 {
	return p.log.LowestOffset()
}

// LogEndOffset returns highest offset (LEO).
func (p *Partition) LogEndOffset() int64 {
	return p.log.HighestOffset()
}

// Close gracefully stops writer loop and closes log.
func (p *Partition) Close() error {
	select {
	case <-p.closeCh:
		return nil
	default:
		close(p.closeCh)
	}
	p.wg.Wait()
	return p.log.Close()
}
