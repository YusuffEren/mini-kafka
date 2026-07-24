//go:build chaos

// Package chaos contains lightweight failure-injection tests for mini-kafka.
//
// These tests are gated behind the "chaos" build tag and are skipped during
// the normal `go test ./...` run. Execute them explicitly with:
//
//	go test ./test/chaos/... -tags=chaos -count=1
package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/yusuf/mini-kafka/internal/broker"
	"github.com/yusuf/mini-kafka/internal/config"
	"github.com/yusuf/mini-kafka/pkg/client"
)

func newBrokerConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
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
}

func startBroker(t *testing.T) (*broker.Broker, string) {
	t.Helper()

	cfg := newBrokerConfig(t)
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

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})

	return b, addr
}

// TestConsumerKillRecovery simulates a consumer crash: produce messages, open a
// consumer and close it (kill), then open a fresh consumer and verify it can
// still read the previously published messages from offset 0.
func TestConsumerKillRecovery(t *testing.T) {
	_, addr := startBroker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer, err := client.NewProducer([]string{addr}, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	topic := "chaos-consumer-kill"
	wantKey := []byte("chaos-key")
	wantValue := []byte("chaos-value")

	offset, err := producer.Send(ctx, topic, 0, wantKey, wantValue)
	if err != nil {
		t.Fatalf("producer.Send: %v", err)
	}

	// First consumer: fetch once, then kill (Close).
	consumer1, err := client.NewConsumer([]string{addr}, client.DefaultConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer (1): %v", err)
	}

	msgs1, err := consumer1.Fetch(ctx, topic, 0, 0)
	if err != nil {
		_ = consumer1.Close()
		t.Fatalf("consumer1.Fetch: %v", err)
	}
	if len(msgs1) != 1 {
		_ = consumer1.Close()
		t.Fatalf("consumer1: expected 1 message, got %d", len(msgs1))
	}

	// Simulate consumer process kill.
	if err := consumer1.Close(); err != nil {
		t.Fatalf("consumer1.Close: %v", err)
	}

	// Second consumer: new client must still see durable messages.
	consumer2, err := client.NewConsumer([]string{addr}, client.DefaultConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer (2): %v", err)
	}
	defer consumer2.Close()

	msgs2, err := consumer2.Fetch(ctx, topic, 0, 0)
	if err != nil {
		t.Fatalf("consumer2.Fetch: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("consumer2: expected 1 message, got %d", len(msgs2))
	}

	got := msgs2[0]
	if string(got.Key) != string(wantKey) {
		t.Errorf("key mismatch: got %q, want %q", string(got.Key), string(wantKey))
	}
	if string(got.Value) != string(wantValue) {
		t.Errorf("value mismatch: got %q, want %q", string(got.Value), string(wantValue))
	}
	if got.Offset != offset {
		t.Errorf("offset mismatch: got %d, want %d", got.Offset, offset)
	}
}
