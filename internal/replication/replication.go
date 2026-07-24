package replication

import (
	"bytes"

	"github.com/yusuf/mini-kafka/internal/protocol"
	"github.com/yusuf/mini-kafka/internal/storage"
)

// ProduceRequestItem represents an in-flight acks=all Produce request awaiting HW progress.
type ProduceRequestItem struct {
	ID         int64
	Topic      string
	Partition  int32
	RequiredHW int64
	RespCh     chan error
}

// Purgatory manages pending acks=all requests awaiting replication completion.
type Purgatory struct {
	items  map[int64]*ProduceRequestItem
	nextID int64
}

// NewPurgatory initializes a Purgatory instance.
func NewPurgatory() *Purgatory {
	return &Purgatory{
		items: make(map[int64]*ProduceRequestItem),
	}
}

// Watch enqueues a request waiting for target HW offset.
func (p *Purgatory) Watch(topic string, partition int32, requiredHW int64, respCh chan error) int64 {
	p.nextID++
	id := p.nextID
	p.items[id] = &ProduceRequestItem{
		ID:         id,
		Topic:      topic,
		Partition:  partition,
		RequiredHW: requiredHW,
		RespCh:     respCh,
	}
	return id
}

// CheckAndComplete releases requests whose required HW condition has been met.
func (p *Purgatory) CheckAndComplete(topic string, partition int32, currentHW int64) {
	for id, item := range p.items {
		if item.Topic == topic && item.Partition == partition && currentHW >= item.RequiredHW {
			select {
			case item.RespCh <- nil:
			default:
			}
			delete(p.items, id)
		}
	}
}

// FollowerFetcher continuously fetches records from partition leaders.
type FollowerFetcher struct {
	brokerID int32
}

// NewFollowerFetcher constructs a FollowerFetcher for brokerID.
func NewFollowerFetcher(brokerID int32) *FollowerFetcher {
	return &FollowerFetcher{brokerID: brokerID}
}

// ReplicaFetchRequest represents a fetch request issued by a follower.
type ReplicaFetchRequest struct {
	ReplicaID   int32
	MaxWaitMs   int32
	Topic       string
	Partition   int32
	FetchOffset int64
}

// ReplicaFetchResponse represents a leader's response to ReplicaFetch.
type ReplicaFetchResponse struct {
	Topic         string
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	Records       []*storage.Record
}

// EncodeReplicaFetchRequest encodes a follower fetch request into binary format.
func EncodeReplicaFetchRequest(req *ReplicaFetchRequest) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := protocol.PutInt32(&buf, req.ReplicaID); err != nil {
		return nil, err
	}
	if _, err := protocol.PutInt32(&buf, req.MaxWaitMs); err != nil {
		return nil, err
	}
	if _, err := protocol.PutString(&buf, req.Topic); err != nil {
		return nil, err
	}
	if _, err := protocol.PutInt32(&buf, req.Partition); err != nil {
		return nil, err
	}
	if _, err := protocol.PutInt64(&buf, req.FetchOffset); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeReplicaFetchRequest decodes a binary follower fetch request.
func DecodeReplicaFetchRequest(data []byte) (*ReplicaFetchRequest, error) {
	rd := bytes.NewReader(data)
	repID, err := protocol.Int32(rd)
	if err != nil {
		return nil, err
	}
	maxWait, err := protocol.Int32(rd)
	if err != nil {
		return nil, err
	}
	topic, err := protocol.String(rd)
	if err != nil {
		return nil, err
	}
	part, err := protocol.Int32(rd)
	if err != nil {
		return nil, err
	}
	off, err := protocol.Int64(rd)
	if err != nil {
		return nil, err
	}
	return &ReplicaFetchRequest{
		ReplicaID:   repID,
		MaxWaitMs:   maxWait,
		Topic:       topic,
		Partition:   part,
		FetchOffset: off,
	}, nil
}
