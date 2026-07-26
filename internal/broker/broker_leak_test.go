//go:build stress

package broker

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

// listenerCount returns the total number of long-poll listener channels
// currently registered in the broker. It is defined in a test file so it only
// exists in stress builds.
func (b *Broker) listenerCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
	for _, pm := range b.listeners {
		for _, chs := range pm {
			count += len(chs)
		}
	}
	return count
}

// TestListenerNoLeak sends 200 Fetch requests to a non-existent topic with a
// short MaxWaitMs timeout. Each fetch registers a long-poll listener that is only
// closed when a producer appends data. Since the topic is never produced to, the
// listeners should be cleaned up after the request completes; the current code
// leaks them, so listenerCount remains non-zero. This exposes the T-11 listener
// leak.
func TestListenerNoLeak(t *testing.T) {
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

	b, err := New(cfg)
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

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Errorf("net.DialTimeout: %v", err)
				return
			}
			defer func() { _ = conn.Close() }()

			fetchReq := &protocol.FetchRequest{
				MaxWaitMs: 50,
				MinBytes:  1,
				MaxBytes:  1024 * 1024,
				Topics: []protocol.FetchRequestTopic{
					{
						Name: "leak-test-topic",
						Partitions: []protocol.FetchRequestPartition{
							{
								PartitionID: 0,
								FetchOffset: 0,
								MaxBytes:    1024 * 1024,
							},
						},
					},
				},
			}

			var payload bytes.Buffer
			if err := fetchReq.Encode(&payload); err != nil {
				t.Errorf("encode FetchRequest: %v", err)
				return
			}

			frame := &protocol.RequestFrame{
				ApiKey:        1, // Fetch
				ApiVersion:    1,
				CorrelationID: 1,
				ClientID:      "leak-test",
				Payload:       payload.Bytes(),
			}

			if _, err := frame.Write(conn); err != nil {
				t.Errorf("write request frame: %v", err)
				return
			}

			// Read the (empty) response so the server handler can finish.
			_, _ = protocol.ReadResponseFrame(conn)
		}()
	}

	wg.Wait()

	// Allow a brief grace period for any in-progress cleanup.
	time.Sleep(100 * time.Millisecond)

	if count := b.listenerCount(); count != 0 {
		t.Fatalf("listenerCount() = %d, want 0 (T-11 listener leak)", count)
	}
}
