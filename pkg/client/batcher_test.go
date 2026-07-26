package client

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestBatcherFlushesOnSize verifies that when BatchSize is small and LingerMs
// is large, the batch is flushed as soon as the encoded byte threshold is
// reached — without waiting for the linger timer.
func TestBatcherFlushesOnSize(t *testing.T) {
	b, addr := startTestBroker(t)
	defer func() { _ = b.Shutdown(context.Background()) }()

	// Tiny batch size so a handful of small messages trip the threshold.
	// Large linger so a timer-based flush would be obviously slow.
	cfg := DefaultProducerConfig()
	cfg.LingerMs = 5000
	cfg.BatchSize = 64
	cfg.TimeoutMs = 5000

	producer, err := NewProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = producer.Close() }()

	const n = 8
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := producer.Send(ctx, "batch-size", 0,
				[]byte(fmt.Sprintf("k%d", i)),
				[]byte(fmt.Sprintf("value-%04d", i)),
			)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	elapsed := time.Since(start)

	for err := range errs {
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	// Must complete well under the 5s linger — size flush, not timer.
	if elapsed >= 2*time.Second {
		t.Fatalf("flush took %v; expected size-based flush well under linger (5s)", elapsed)
	}
	t.Logf("size flush completed in %v", elapsed)
}

// TestBatcherFlushesOnLinger verifies that with a large BatchSize a single
// Send is delivered after approximately LingerMs.
func TestBatcherFlushesOnLinger(t *testing.T) {
	b, addr := startTestBroker(t)
	defer func() { _ = b.Shutdown(context.Background()) }()

	const lingerMs = 50
	cfg := DefaultProducerConfig()
	cfg.LingerMs = lingerMs
	cfg.BatchSize = 1 << 20 // 1 MiB — one small record will not trip size
	cfg.TimeoutMs = 5000

	producer, err := NewProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	off, err := producer.Send(ctx, "batch-linger", 0, []byte("k"), []byte("v"))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if off != 0 {
		t.Fatalf("offset = %d, want 0", off)
	}

	// Allow generous slack for scheduling; lower bound avoids an immediate send.
	if elapsed < 30*time.Millisecond {
		t.Fatalf("flush took %v; expected to wait ~%dms linger", elapsed, lingerMs)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("flush took %v; expected around %dms linger, not seconds", elapsed, lingerMs)
	}
	t.Logf("linger flush completed in %v (linger=%dms)", elapsed, lingerMs)
}

// TestBatcherOffsetsCorrect sends 100 concurrent messages through the batcher
// and verifies each caller receives its own offset, the set is exactly [0,100),
// and broker contents match the unique values.
func TestBatcherOffsetsCorrect(t *testing.T) {
	b, addr := startTestBroker(t)
	defer func() { _ = b.Shutdown(context.Background()) }()

	cfg := DefaultProducerConfig()
	cfg.LingerMs = 50
	cfg.BatchSize = 4096
	cfg.TimeoutMs = 10000

	producer, err := NewProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = producer.Close() }()

	const n = 100
	type result struct {
		id  int
		off int64
		err error
	}
	results := make(chan result, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			val := fmt.Sprintf("unique-value-%03d", id)
			off, err := producer.Send(ctx, "batch-offsets", 0,
				[]byte(fmt.Sprintf("key-%03d", id)),
				[]byte(val),
			)
			results <- result{id: id, off: off, err: err}
		}(i)
	}
	wg.Wait()
	close(results)

	// Map offset -> caller id and collect offset set.
	offSet := make(map[int64]struct{}, n)
	idByOffset := make(map[int64]int, n)
	for r := range results {
		if r.err != nil {
			t.Fatalf("Send id=%d: %v", r.id, r.err)
		}
		if _, dup := offSet[r.off]; dup {
			t.Fatalf("duplicate offset %d (id=%d and id=%d)", r.off, idByOffset[r.off], r.id)
		}
		offSet[r.off] = struct{}{}
		idByOffset[r.off] = r.id
	}

	if len(offSet) != n {
		t.Fatalf("offset set size = %d, want %d", len(offSet), n)
	}
	for i := int64(0); i < int64(n); i++ {
		if _, ok := offSet[i]; !ok {
			t.Fatalf("missing offset %d in [0, %d)", i, n)
		}
	}

	// Verify broker contents: each offset holds the unique value for that caller.
	consumer, err := NewConsumer([]string{addr}, DefaultConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	msgs, err := consumer.Fetch(context.Background(), "batch-offsets", 0, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	// May need multiple fetches if MaxBytes limits; accumulate.
	for len(msgs) < n {
		more, err := consumer.Fetch(context.Background(), "batch-offsets", 0, int64(len(msgs)))
		if err != nil {
			t.Fatalf("Fetch continue: %v", err)
		}
		if len(more) == 0 {
			break
		}
		msgs = append(msgs, more...)
	}
	if len(msgs) != n {
		t.Fatalf("broker message count = %d, want %d", len(msgs), n)
	}

	for _, m := range msgs {
		id, ok := idByOffset[m.Offset]
		if !ok {
			t.Fatalf("unexpected broker offset %d", m.Offset)
		}
		want := fmt.Sprintf("unique-value-%03d", id)
		if string(m.Value) != want {
			t.Errorf("offset %d: value=%q, want %q (caller id=%d)", m.Offset, m.Value, want, id)
		}
	}
}

// TestBatcherCloseReleasesWaiters ensures Close flushes or fails pending Send
// waiters so none block forever, and no goroutines are leaked.
func TestBatcherCloseReleasesWaiters(t *testing.T) {
	before := runtime.NumGoroutine()

	b, addr := startTestBroker(t)
	defer func() {
		_ = b.Shutdown(context.Background())
		// Grace period for goroutine cleanup (mirrors stress assertNoGoroutineLeak).
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if runtime.NumGoroutine() <= before+2 {
				return
			}
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
		}
		after := runtime.NumGoroutine()
		if after > before+2 {
			t.Fatalf("goroutine leak: before=%d after=%d", before, after)
		}
	}()

	cfg := DefaultProducerConfig()
	cfg.LingerMs = 5000 // long linger so Sends are still waiting when we Close
	cfg.BatchSize = 1 << 20
	cfg.TimeoutMs = 5000

	producer, err := NewProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	const n = 20
	var wg sync.WaitGroup
	done := make(chan struct{}, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// No short timeout: we want Close to be what unblocks us.
			_, _ = producer.Send(context.Background(), "batch-close", 0,
				[]byte(fmt.Sprintf("k%d", i)),
				[]byte(fmt.Sprintf("v%d", i)),
			)
			done <- struct{}{}
		}(i)
	}

	// Give Senders time to enqueue into the pending batch.
	time.Sleep(50 * time.Millisecond)

	if err := producer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// All waiters must be released promptly.
	released := 0
	timeout := time.After(2 * time.Second)
	for released < n {
		select {
		case <-done:
			released++
		case <-timeout:
			t.Fatalf("only %d/%d waiters released after Close", released, n)
		}
	}
	wg.Wait()
}
