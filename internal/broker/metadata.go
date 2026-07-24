package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TopicMeta holds persisted metadata for a topic.
type TopicMeta struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
}

// metadataState represents the disk layout for metadata.
type metadataState struct {
	Topics map[string]TopicMeta `json:"topics"`
}

// MetadataManager handles reading and writing cluster/topic metadata to disk.
type MetadataManager struct {
	metaFile string
	topics   map[string]TopicMeta
	mu       sync.RWMutex
}

// NewMetadataManager creates a MetadataManager backed by dataDir/meta/topics.json.
func NewMetadataManager(dataDir string) (*MetadataManager, error) {
	metaDir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("metadata: mkdir meta: %w", err)
	}

	mm := &MetadataManager{
		metaFile: filepath.Join(metaDir, "topics.json"),
		topics:   make(map[string]TopicMeta),
	}

	if err := mm.load(); err != nil {
		return nil, err
	}

	return mm, nil
}

func (mm *MetadataManager) load() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	data, err := os.ReadFile(mm.metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("metadata: read file: %w", err)
	}

	var state metadataState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("metadata: unmarshal: %w", err)
	}

	if state.Topics != nil {
		mm.topics = state.Topics
	}
	return nil
}

func (mm *MetadataManager) saveLocked() error {
	state := metadataState{Topics: mm.topics}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("metadata: marshal: %w", err)
	}

	tmpFile := mm.metaFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("metadata: write tmp: %w", err)
	}

	if err := os.Rename(tmpFile, mm.metaFile); err != nil {
		return fmt.Errorf("metadata: rename: %w", err)
	}
	return nil
}

// CreateTopic stores a topic metadata entry if it doesn't already exist.
func (mm *MetadataManager) CreateTopic(name string, numPartitions int32, replicationFactor int16) (TopicMeta, bool, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if meta, exists := mm.topics[name]; exists {
		return meta, false, nil
	}

	meta := TopicMeta{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	mm.topics[name] = meta
	if err := mm.saveLocked(); err != nil {
		delete(mm.topics, name)
		return TopicMeta{}, false, err
	}

	return meta, true, nil
}

// GetTopic returns metadata for topic name.
func (mm *MetadataManager) GetTopic(name string) (TopicMeta, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	meta, exists := mm.topics[name]
	return meta, exists
}

// ListTopics returns a slice of all topic metadata.
func (mm *MetadataManager) ListTopics() []TopicMeta {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	list := make([]TopicMeta, 0, len(mm.topics))
	for _, t := range mm.topics {
		list = append(list, t)
	}
	return list
}
