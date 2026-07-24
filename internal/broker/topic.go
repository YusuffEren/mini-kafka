package broker

import (
	"sync"
	"sync/atomic"

	"github.com/yusuf/mini-kafka/internal/storage"
)

// Topic manages a set of partitions for a given topic name.
type Topic struct {
	Name              string
	Partitions        map[int32]*Partition
	roundRobinCounter uint32
	mu                sync.RWMutex
}

// NewTopic creates a Topic with the specified number of partitions.
func NewTopic(baseDir string, name string, numPartitions int32, cfg storage.Config) (*Topic, error) {
	partitions := make(map[int32]*Partition, numPartitions)
	for i := int32(0); i < numPartitions; i++ {
		p, err := NewPartition(baseDir, name, i, cfg)
		if err != nil {
			// Close already opened partitions on error
			for j := int32(0); j < i; j++ {
				_ = partitions[j].Close()
			}
			return nil, err
		}
		partitions[i] = p
	}

	return &Topic{
		Name:       name,
		Partitions: partitions,
	}, nil
}

// PartitionFor returns the target partition ID for a message key.
// If key is non-empty, Kafka's Murmur2 hash is used to select partition: murmur2(key) % numPartitions.
// If key is empty or nil, round-robin selection is used.
func (t *Topic) PartitionFor(key []byte) int32 {
	t.mu.RLock()
	numPartitions := int32(len(t.Partitions))
	t.mu.RUnlock()

	if numPartitions <= 0 {
		return 0
	}

	if len(key) > 0 {
		hash := Murmur2(key)
		return int32(hash % uint32(numPartitions))
	}

	val := atomic.AddUint32(&t.roundRobinCounter, 1) - 1
	return int32(val % uint32(numPartitions))
}

// GetPartition returns partition by ID, or nil if not found.
func (t *Topic) GetPartition(partitionID int32) *Partition {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Partitions[partitionID]
}

// Close closes all partitions under this topic.
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var firstErr error
	for _, p := range t.Partitions {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Murmur2 computes the Kafka-compatible MurmurHash2 for a byte array.
// Uses seed 0x973afb51 and applies a positive bitmask (0x7fffffff) as Kafka does.
func Murmur2(data []byte) uint32 {
	length := uint32(len(data))
	seed := uint32(0x973afb51)
	m := uint32(0x5bd1e995)
	r := uint32(24)

	h := seed ^ length
	length4 := length / 4

	for i := uint32(0); i < length4; i++ {
		i4 := i * 4
		k := uint32(data[i4]) | (uint32(data[i4+1]) << 8) | (uint32(data[i4+2]) << 16) | (uint32(data[i4+3]) << 24)
		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= uint32(data[(length&^3)+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length&^3)+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length&^3])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h & 0x7fffffff
}
