package coordinator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type offsetKey struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type offsetRecord struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OffsetStore manages committed offsets per consumer group, topic, and partition.
type OffsetStore struct {
	mu      sync.RWMutex
	offsets map[offsetKey]int64
	dataDir string
}

// NewOffsetStore initializes an OffsetStore with optional persistence in dataDir.
func NewOffsetStore(dataDir ...string) *OffsetStore {
	store := &OffsetStore{
		offsets: make(map[offsetKey]int64),
	}
	if len(dataDir) > 0 && dataDir[0] != "" {
		store.dataDir = dataDir[0]
		_ = store.load()
	}
	return store
}

func (s *OffsetStore) load() error {
	if s.dataDir == "" {
		return nil
	}
	filePath := filepath.Join(s.dataDir, "meta", "offsets.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var records []offsetRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range records {
		key := offsetKey{Group: rec.Group, Topic: rec.Topic, Partition: rec.Partition}
		s.offsets[key] = rec.Offset
	}
	return nil
}

func (s *OffsetStore) saveLocked() {
	if s.dataDir == "" {
		return
	}
	metaDir := filepath.Join(s.dataDir, "meta")
	_ = os.MkdirAll(metaDir, 0755)
	filePath := filepath.Join(metaDir, "offsets.json")

	records := make([]offsetRecord, 0, len(s.offsets))
	for k, off := range s.offsets {
		records = append(records, offsetRecord{
			Group:     k.Group,
			Topic:     k.Topic,
			Partition: k.Partition,
			Offset:    off,
		})
	}

	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return
	}
	tmpFile := filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err == nil {
		_ = os.Rename(tmpFile, filePath)
	}
}

// Commit stores a committed offset for group, topic, partition.
func (s *OffsetStore) Commit(group, topic string, partition int32, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := offsetKey{Group: group, Topic: topic, Partition: partition}
	s.offsets[key] = offset
	s.saveLocked()
}

// Fetch retrieves the committed offset for group, topic, partition. Returns -1 if not committed.
func (s *OffsetStore) Fetch(group, topic string, partition int32) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := offsetKey{Group: group, Topic: topic, Partition: partition}
	if off, exists := s.offsets[key]; exists {
		return off
	}
	return -1
}
