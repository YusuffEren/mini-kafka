package replication_test

import (
	"sync"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/replication"
)

func TestISRTracker_HW_Calculation(t *testing.T) {
	dir := t.TempDir()
	tracker := replication.NewISRTracker(dir, "test-topic", 0, 1, []int32{1, 2, 3}, 30000)

	// Leader LEO = 100, Follower 2 LEO = 80, Follower 3 LEO = 50
	tracker.UpdateLEO(1, 100, 100)
	tracker.UpdateLEO(2, 80, 100)
	hw := tracker.UpdateLEO(3, 50, 100)

	if hw != 50 {
		t.Fatalf("expected HW=50, got %d", hw)
	}

	// Follower 3 catches up to 90
	hw = tracker.UpdateLEO(3, 90, 100)
	if hw != 80 {
		t.Fatalf("expected HW=80, got %d", hw)
	}

	// Checkpoint HW to disk
	if err := tracker.CheckpointHW(); err != nil {
		t.Fatalf("failed to checkpoint HW: %v", err)
	}

	// Re-load checkpoint
	tracker2 := replication.NewISRTracker(dir, "test-topic", 0, 1, []int32{1, 2, 3}, 30000)
	if tracker2.HighWatermark() != 80 {
		t.Fatalf("expected reloaded HW=80, got %d", tracker2.HighWatermark())
	}
}

// TestISRTracker_UpdateLEO_MonotonicConcurrent reproduces the acks=all @ N
// producers race: many goroutines observe increasing LEOs after append but
// call UpdateLEO out of order. Stale lower LEOs must not regress HW.
func TestISRTracker_UpdateLEO_MonotonicConcurrent(t *testing.T) {
	dir := t.TempDir()
	// Single-broker ISR (min_insync_replicas=1 benchmark shape).
	tracker := replication.NewISRTracker(dir, "bench-topic", 0, 1, []int32{1}, 30000)

	const n = 500
	var wg sync.WaitGroup
	wg.Add(n)
	for i := int64(1); i <= n; i++ {
		go func(leo int64) {
			defer wg.Done()
			// Deliberately pass leaderLEO == leo the way handleProduce does.
			_ = tracker.UpdateLEO(1, leo, leo)
		}(i)
	}
	wg.Wait()

	if hw := tracker.HighWatermark(); hw != n {
		t.Fatalf("expected HW=%d after concurrent monotonic updates, got %d", n, hw)
	}
}

func TestISRTracker_LagTimeout(t *testing.T) {
	dir := t.TempDir()
	// Short lag timeout 100ms for test
	tracker := replication.NewISRTracker(dir, "test-topic", 0, 1, []int32{1, 2}, 100)

	tracker.UpdateLEO(1, 100, 100)
	tracker.UpdateLEO(2, 100, 100)

	isr := tracker.GetISR()
	if len(isr) != 2 {
		t.Fatalf("expected 2 in-sync replicas, got %d", len(isr))
	}

	time.Sleep(150 * time.Millisecond)

	// Only leader updates
	tracker.UpdateLEO(1, 150, 150)
	isr = tracker.GetISR()
	if len(isr) != 1 || isr[0] != 1 {
		t.Fatalf("expected only leader in ISR after lag timeout, got %v", isr)
	}
}

func TestPurgatory(t *testing.T) {
	purgatory := replication.NewPurgatory()
	respCh := make(chan error, 1)

	purgatory.Watch("topic1", 0, 100, respCh)

	// Current HW 80 — not complete
	purgatory.CheckAndComplete("topic1", 0, 80)
	select {
	case <-respCh:
		t.Fatal("expected no response at HW=80")
	default:
	}

	// HW advances to 100 — completes
	purgatory.CheckAndComplete("topic1", 0, 100)
	select {
	case err := <-respCh:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	default:
		t.Fatal("expected response channel signal at HW=100")
	}
}

func TestEpochManager(t *testing.T) {
	em := replication.NewEpochManager()

	ep := em.IncrementEpoch("topic1", 0, 1)
	if ep != 1 {
		t.Fatalf("expected initial epoch=1, got %d", ep)
	}

	ep2 := em.IncrementEpoch("topic1", 0, 2)
	if ep2 != 2 {
		t.Fatalf("expected incremented epoch=2, got %d", ep2)
	}

	if !em.ValidateEpoch("topic1", 0, 2) {
		t.Fatal("epoch 2 should be valid")
	}

	if em.ValidateEpoch("topic1", 0, 1) {
		t.Fatal("old epoch 1 should be invalid")
	}
}
