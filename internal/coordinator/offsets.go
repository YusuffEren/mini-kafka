package coordinator

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/storage"
)

// OffsetStore manages committed consumer-group offsets backed by an internal
// __consumer_offsets log (storage.Log partitions) plus an in-memory cache.
type OffsetStore struct {
	mu       sync.RWMutex
	cache    map[string]int64 // "groupID:topic:partition" → offset
	dataDir  string
	numParts int
	logs     []*storage.Log
	started  bool
}

// NewOffsetStore creates an OffsetStore. Call Start before Commit/Fetch against
// the durable log. numPartitions defaults to 50 when <= 0.
func NewOffsetStore(dataDir string, numPartitions int) *OffsetStore {
	if numPartitions <= 0 {
		numPartitions = 50
	}
	return &OffsetStore{
		cache:    make(map[string]int64),
		dataDir:  dataDir,
		numParts: numPartitions,
	}
}

// Start opens (or creates) __consumer_offsets partition logs and rebuilds the
// in-memory cache by replaying every record (last-write-wins).
func (s *OffsetStore) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	baseDir := filepath.Join(s.dataDir, "__consumer_offsets")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("offset store: create dir: %w", err)
	}

	cfg := storage.Config{
		SegmentBytes:       10 * 1024 * 1024, // 10 MB
		IndexIntervalBytes: 4096,
		IndexMaxBytes:      1 * 1024 * 1024, // 1 MB
		RetentionMs:        -1,              // never delete
		RetentionBytes:     -1,              // unlimited
		FlushMs:            1000,            // flush every second
	}

	s.logs = make([]*storage.Log, s.numParts)
	for i := 0; i < s.numParts; i++ {
		dir := filepath.Join(baseDir, fmt.Sprintf("%d", i))
		log, err := storage.NewLog(dir, cfg)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = s.logs[j].Close()
			}
			s.logs = nil
			return fmt.Errorf("offset store: open log %d: %w", i, err)
		}
		s.logs[i] = log

		// Replay records into cache (last-write-wins).
		offset := int64(0)
		for {
			records, err := log.ReadFrom(offset, 1<<30) // 1GB batch
			if err != nil || len(records) == 0 {
				break
			}
			for _, rec := range records {
				if len(rec.Key) == 0 || len(rec.Value) < 8 {
					continue
				}
				cacheKey := string(rec.Key)
				val := int64(binary.BigEndian.Uint64(rec.Value))
				s.cache[cacheKey] = val
			}
			offset = records[len(records)-1].Offset + 1
		}
	}

	s.started = true
	return nil
}

// Commit stores a committed offset for group, topic, partition. The in-memory
// cache is updated first; on log append failure the cache is rolled back.
func (s *OffsetStore) Commit(groupID, topic string, partition int32, offset int64) {
	key := fmt.Sprintf("%s:%s:%d", groupID, topic, partition)

	h := fnv.New32a()
	_, _ = h.Write([]byte(groupID))
	partIdx := int(h.Sum32() % uint32(s.numParts))

	valBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valBytes, uint64(offset))

	s.mu.Lock()
	oldVal, hadOld := s.cache[key]
	s.cache[key] = offset
	started := s.started
	var log *storage.Log
	if started && partIdx >= 0 && partIdx < len(s.logs) {
		log = s.logs[partIdx]
	}
	s.mu.Unlock()

	if log == nil {
		// Not started: cache-only (tests / early use). Durable path requires Start.
		return
	}

	rec := &storage.Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte(key),
		Value:     valBytes,
	}
	if _, err := log.Append(rec); err != nil {
		s.mu.Lock()
		if hadOld {
			s.cache[key] = oldVal
		} else {
			delete(s.cache, key)
		}
		s.mu.Unlock()
	}
}

// Fetch returns the committed offset for group/topic/partition, or -1 if none.
func (s *OffsetStore) Fetch(groupID, topic string, partition int32) int64 {
	key := fmt.Sprintf("%s:%s:%d", groupID, topic, partition)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if off, ok := s.cache[key]; ok {
		return off
	}
	return -1
}

// Close closes every partition log. It is safe to call more than once.
func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	var firstErr error
	for i, log := range s.logs {
		if log == nil {
			continue
		}
		if err := log.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("offset store: close log %d: %w", i, err)
		}
	}
	s.logs = nil
	s.started = false
	return firstErr
}
