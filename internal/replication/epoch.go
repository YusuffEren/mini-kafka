package replication

import (
	"fmt"
	"sync"
)

// PartitionEpoch tracks leader epoch for split-brain prevention.
type PartitionEpoch struct {
	Topic     string
	Partition int32
	Epoch     int32
	LeaderID  int32
}

// EpochManager tracks and validates Leader Epoch across topics and partitions.
type EpochManager struct {
	mu     sync.RWMutex
	epochs map[string]map[int32]*PartitionEpoch
}

// NewEpochManager initializes an EpochManager.
func NewEpochManager() *EpochManager {
	return &EpochManager{
		epochs: make(map[string]map[int32]*PartitionEpoch),
	}
}

// IncrementEpoch increments and updates the leader epoch for a partition.
func (em *EpochManager) IncrementEpoch(topic string, partition int32, newLeaderID int32) int32 {
	em.mu.Lock()
	defer em.mu.Unlock()

	tEpochs, exists := em.epochs[topic]
	if !exists {
		tEpochs = make(map[int32]*PartitionEpoch)
		em.epochs[topic] = tEpochs
	}

	pe, exists := tEpochs[partition]
	if !exists {
		pe = &PartitionEpoch{
			Topic:     topic,
			Partition: partition,
			Epoch:     1,
			LeaderID:  newLeaderID,
		}
		tEpochs[partition] = pe
		return 1
	}

	pe.Epoch++
	pe.LeaderID = newLeaderID
	return pe.Epoch
}

// GetEpoch returns the current leader epoch and leader ID.
func (em *EpochManager) GetEpoch(topic string, partition int32) (int32, int32, error) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	tEpochs, exists := em.epochs[topic]
	if !exists {
		return 0, -1, fmt.Errorf("epoch: unknown topic %s", topic)
	}
	pe, exists := tEpochs[partition]
	if !exists {
		return 0, -1, fmt.Errorf("epoch: unknown partition %s-%d", topic, partition)
	}
	return pe.Epoch, pe.LeaderID, nil
}

// ValidateEpoch checks if the provided epoch matches the active leader epoch.
func (em *EpochManager) ValidateEpoch(topic string, partition int32, epoch int32) bool {
	currentEpoch, _, err := em.GetEpoch(topic, partition)
	if err != nil {
		return true // Default to valid if untracked
	}
	return epoch >= currentEpoch
}
