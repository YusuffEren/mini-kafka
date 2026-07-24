//go:build integration

// Package integration contains end-to-end round-trip tests that exercise a
// running mini-kafka broker through the high-level client package.
//
// These tests are gated behind the "integration" build tag and are skipped
// during the normal `go test ./...` run. Execute them explicitly with:
//
//	go test ./test/integration/... -tags=integration -count=1
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/pkg/client"
)

// newBrokerConfig builds a minimal broker configuration bound to an ephemeral
// port and a per-test data directory. Topic auto-creation is enabled so the
// producer can publish to a fresh topic without an explicit CreateTopics call.
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

// startBroker constructs and starts a broker, returning its actual listen
// address once the TCP listener is ready. The broker is shut down
// automatically when the test finishes.
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

// TestProduceFetchRoundTrip verifies the core produce → fetch path: a producer
// publishes a single message to an auto-created topic and a consumer reads it
// back, asserting that the key, value and assigned offset survive the round
// trip.
func TestProduceFetchRoundTrip(t *testing.T) {
	_, addr := startBroker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer, err := client.NewProducer([]string{addr}, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	topic := "round-trip-topic"
	wantKey := []byte("round-trip-key")
	wantValue := []byte("round-trip-value")

	offset, err := producer.Send(ctx, topic, 0, wantKey, wantValue)
	if err != nil {
		t.Fatalf("producer.Send: %v", err)
	}
	if offset < 0 {
		t.Fatalf("producer.Send returned negative offset: %d", offset)
	}

	consumer, err := client.NewConsumer([]string{addr}, client.DefaultConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(ctx, topic, 0, 0)
	if err != nil {
		t.Fatalf("consumer.Fetch: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	got := messages[0]
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

// TestProduceBatchFetchMultiple verifies that multiple records published in a
// single batch are all retrievable in fetch order with monotonically
// increasing offsets.
func TestProduceBatchFetchMultiple(t *testing.T) {
	_, addr := startBroker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer, err := client.NewProducer([]string{addr}, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	topic := "batch-topic"
	msgs := []client.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}

	offsets, err := producer.SendBatch(ctx, topic, 0, msgs)
	if err != nil {
		t.Fatalf("producer.SendBatch: %v", err)
	}
	if len(offsets) != len(msgs) {
		t.Fatalf("expected %d offsets, got %d", len(msgs), len(offsets))
	}

	consumer, err := client.NewConsumer([]string{addr}, client.DefaultConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	got, err := consumer.Fetch(ctx, topic, 0, 0)
	if err != nil {
		t.Fatalf("consumer.Fetch: %v", err)
	}
	if len(got) != len(msgs) {
		t.Fatalf("expected %d messages, got %d", len(msgs), len(got))
	}

	for i, m := range msgs {
		if string(got[i].Key) != string(m.Key) {
			t.Errorf("msg[%d].key = %q, want %q", i, string(got[i].Key), string(m.Key))
		}
		if string(got[i].Value) != string(m.Value) {
			t.Errorf("msg[%d].value = %q, want %q", i, string(got[i].Value), string(m.Value))
		}
		if got[i].Offset != offsets[i] {
			t.Errorf("msg[%d].offset = %d, want %d", i, got[i].Offset, offsets[i])
		}
	}
}
