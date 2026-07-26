//go:build stress

// Package stress contains long-running and concurrent stress tests that expose
// W1 concurrency bugs. They are excluded from the normal test run because they
// intentionally fail on the current implementation.
//
// Run these tests explicitly:
//
//	go test -race -tags=stress ./test/stress/... -count=1
//	go test -tags=stress ./internal/broker/... -run TestListenerNoLeak -count=1
package stress_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/internal/storage"
	"github.com/YusuffEren/mini-kafka/pkg/client"
)

// startStressBroker starts a broker on a random ephemeral port and waits for it
// to be listening. The caller must shut down the returned broker.
func startStressBroker(t *testing.T) (*broker.Broker, string) {
	t.Helper()

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			ID:               1,
			Host:             "127.0.0.1",
			Port:             0,
			DataDir:          t.TempDir(),
			MaxConnections:   1024,
			RequestTimeoutMs: 30000,
		},
		Topic: config.TopicConfig{
			AutoCreate:        true,
			DefaultPartitions: 1,
		},
	}

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}

	startErr := make(chan error, 1)
	go func() { startErr <- b.Start() }()

	var addr string
	for i := 0; i < 100; i++ {
		if a := b.Addr(); a != nil {
			addr = a.String()
			break
		}
		select {
		case err := <-startErr:
			t.Fatalf("broker.Start: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("broker did not start listening within timeout")
	}

	return b, addr
}

// assertNoGoroutineLeak waits up to 500ms for runtime.NumGoroutine() to return
// within ±2 of the baseline recorded before the broker was started.
func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		now := runtime.NumGoroutine()
		if now <= before+2 {
			return
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	after := runtime.NumGoroutine()
	t.Fatalf("goroutine leak detected: before=%d after=%d, want within ±2", before, after)
}

// TestGoroutineLeak starts and shuts down a broker, then checks that the
// goroutine count returns to its baseline. It is intentionally both a standalone
// test and a reusable helper (assertNoGoroutineLeak) for the other stress tests.
func TestGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	b, _ := startStressBroker(t)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
		assertNoGoroutineLeak(t, before)
	}()

	// Exercise the broker briefly so any background goroutines that should be
	// cleaned up have a chance to start.
	time.Sleep(50 * time.Millisecond)
}

// TestConcurrentReadWrite exercises storage.Log with concurrent appends and
// reads. When run with -race, it surfaces the T-10 data race in Segment.FlushWriter
// which uses an RLock while mutating the shared buffered writer.
func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.NewLog(dir, storage.Config{
		SegmentBytes:       1024 * 1024,
		IndexIntervalBytes: 4096,
		IndexMaxBytes:      10 * 1024 * 1024,
		FlushMessages:      0,
		FlushMs:            0,
		RetentionMs:        -1,
		RetentionBytes:     -1,
	})
	if err != nil {
		t.Fatalf("storage.NewLog: %v", err)
	}
	defer func() { _ = log.Close() }()

	const writers = 8
	const readers = 8
	const duration = 2 * time.Second

	stop := make(chan struct{})
	var once sync.Once
	stopAfter := time.AfterFunc(duration, func() { once.Do(func() { close(stop) }) })
	defer stopAfter.Stop()

	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec := &storage.Record{
					Key:   []byte(fmt.Sprintf("writer-%d", id)),
					Value: []byte(fmt.Sprintf("value-%d", id)),
				}
				_, _ = log.Append(rec)
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				leo := log.HighestOffset()
				if leo > 0 {
					_, _ = log.Read(leo - 1)
					_, _ = log.ReadFrom(0, 1024*1024)
				}
			}
		}(i)
	}

	wg.Wait()

	// The test is intentionally not asserting offset values here: its job is to
	// be run with -race and expose the T-10 data race in FlushWriter.
}

// TestProducerConcurrentSend uses a single client.Producer from 8 goroutines
// each sending 200 messages. The broker must assign each produced message a
// unique offset in the range [0, 1600). This test catches the T-15 producer
// connection concurrency bug.
func TestProducerConcurrentSend(t *testing.T) {
	before := runtime.NumGoroutine()

	b, addr := startStressBroker(t)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
		assertNoGoroutineLeak(t, before)
	}()

	cfg := client.DefaultProducerConfig()
	cfg.Acks = 1
	cfg.TimeoutMs = 5000
	producer, err := client.NewProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("client.NewProducer: %v", err)
	}
	defer func() { _ = producer.Close() }()

	const goroutines = 8
	const messages = 200
	wantTotal := goroutines * messages

	var wg sync.WaitGroup
	var mu sync.Mutex
	offsets := make(map[int64]struct{}, wantTotal)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < messages; j++ {
				key := []byte(fmt.Sprintf("goroutine-%d-message-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				off, err := producer.Send(ctx, "stress-concurrent-send", 0, key, value)
				if err != nil {
					t.Errorf("producer.Send goroutine %d message %d: %v", id, j, err)
					return
				}
				mu.Lock()
				offsets[off] = struct{}{}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	if len(offsets) != wantTotal {
		t.Fatalf("offset set size = %d, want %d (missing or duplicate offsets)", len(offsets), wantTotal)
	}
	for i := int64(0); i < int64(wantTotal); i++ {
		if _, ok := offsets[i]; !ok {
			t.Fatalf("missing offset %d in [0, %d)", i, wantTotal)
		}
	}
}
