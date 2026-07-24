package replication

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ReplicaState tracks the sync state and LEO of a replica for a single partition.
type ReplicaState struct {
	BrokerID         int32
	LogEndOffset     int64
	LastCaughtUpTime time.Time
	InSync           bool
}

// ISRTracker manages the in-sync replica set and High Watermark (HW) calculation.
type ISRTracker struct {
	mu                  sync.RWMutex
	replicas            map[int32]*ReplicaState
	leaderID            int32
	replicaLagTimeMaxMs int64
	highWatermark       int64
	dataDir             string
	topic               string
	partition           int32
}

// NewISRTracker initializes an ISRTracker for a partition.
func NewISRTracker(dataDir, topic string, partition int32, leaderID int32, replicaIDs []int32, lagTimeMaxMs int64) *ISRTracker {
	if lagTimeMaxMs <= 0 {
		lagTimeMaxMs = 30000
	}

	replicas := make(map[int32]*ReplicaState, len(replicaIDs))
	now := time.Now()

	for _, id := range replicaIDs {
		replicas[id] = &ReplicaState{
			BrokerID:         id,
			LogEndOffset:     0,
			LastCaughtUpTime: now,
			InSync:           true,
		}
	}

	tracker := &ISRTracker{
		replicas:            replicas,
		leaderID:            leaderID,
		replicaLagTimeMaxMs: lagTimeMaxMs,
		dataDir:             dataDir,
		topic:               topic,
		partition:           partition,
	}

	tracker.loadCheckpoint()
	return tracker
}

// UpdateLEO updates a replica's LEO and caught-up timestamp, re-evaluates ISR, and returns updated HW.
func (t *ISRTracker) UpdateLEO(brokerID int32, leo int64, leaderLEO int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	rep, exists := t.replicas[brokerID]
	if !exists {
		rep = &ReplicaState{
			BrokerID:         brokerID,
			LogEndOffset:     leo,
			LastCaughtUpTime: now,
			InSync:           true,
		}
		t.replicas[brokerID] = rep
	} else {
		rep.LogEndOffset = leo
		if leo >= leaderLEO {
			rep.LastCaughtUpTime = now
		}
	}

	t.updateISRLocked(now)
	return t.calculateHWLocked()
}

func (t *ISRTracker) updateISRLocked(now time.Time) {
	maxLag := time.Duration(t.replicaLagTimeMaxMs) * time.Millisecond
	for _, rep := range t.replicas {
		if rep.BrokerID == t.leaderID {
			rep.InSync = true
			continue
		}
		if now.Sub(rep.LastCaughtUpTime) > maxLag {
			rep.InSync = false
		} else {
			rep.InSync = true
		}
	}
}

func (t *ISRTracker) calculateHWLocked() int64 {
	var minLEO int64 = -1
	for _, rep := range t.replicas {
		if rep.InSync {
			if minLEO == -1 || rep.LogEndOffset < minLEO {
				minLEO = rep.LogEndOffset
			}
		}
	}
	if minLEO >= 0 {
		t.highWatermark = minLEO
	}
	return t.highWatermark
}

// GetISR returns the slice of broker IDs currently in the ISR.
func (t *ISRTracker) GetISR() []int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var isr []int32
	for _, rep := range t.replicas {
		if rep.InSync {
			isr = append(isr, rep.BrokerID)
		}
	}
	return isr
}

// HighWatermark returns the current High Watermark.
func (t *ISRTracker) HighWatermark() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.highWatermark
}

func (t *ISRTracker) checkpointPath() string {
	if t.dataDir == "" {
		return ""
	}
	return filepath.Join(t.dataDir, "replication-offset-checkpoint")
}

func (t *ISRTracker) loadCheckpoint() {
	path := t.checkpointPath()
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	lines := strings.Split(string(data), "\n")
	prefix := t.topic + " " + strconv.Itoa(int(t.partition)) + " "
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if hw, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
					t.highWatermark = hw
				}
			}
		}
	}
}

// CheckpointHW persists current HW to `replication-offset-checkpoint`.
func (t *ISRTracker) CheckpointHW() error {
	path := t.checkpointPath()
	if path == "" {
		return nil
	}

	t.mu.RLock()
	hw := t.highWatermark
	t.mu.RUnlock()

	line := t.topic + " " + strconv.Itoa(int(t.partition)) + " " + strconv.FormatInt(hw, 10) + "\n"

	t.mu.Lock()
	defer t.mu.Unlock()

	var existing []string
	if data, err := os.ReadFile(path); err == nil {
		lines := strings.Split(string(data), "\n")
		prefix := t.topic + " " + strconv.Itoa(int(t.partition)) + " "
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l != "" && !strings.HasPrefix(l, prefix) {
				existing = append(existing, l)
			}
		}
	}

	existing = append(existing, strings.TrimSpace(line))
	content := strings.Join(existing, "\n") + "\n"

	tmpFile := path + ".tmp"
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, path)
}
