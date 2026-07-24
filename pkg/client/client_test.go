package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
)

func startTestBroker(t *testing.T) (*broker.Broker, string) {
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

	go func() {
		_ = b.Start()
	}()

	for i := 0; i < 50; i++ {
		if addr := b.Addr(); addr != nil {
			return b, addr.String()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("broker listener did not start in time")
	return nil, ""
}

func TestClient_Producer_and_Consumer(t *testing.T) {
	b, addr := startTestBroker(t)
	defer func() {
		_ = b.Shutdown(context.Background())
	}()

	producer, err := NewProducer([]string{addr}, DefaultProducerConfig())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	consumer, err := NewConsumer([]string{addr}, DefaultConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	ctx := context.Background()

	// 1. Send single message
	offset, err := producer.Send(ctx, "test-topic", 0, []byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatalf("producer.Send: %v", err)
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}

	// 2. Send batch
	offsets, err := producer.SendBatch(ctx, "test-topic", 0, []Message{
		{Key: []byte("key2"), Value: []byte("val2")},
		{Key: []byte("key3"), Value: []byte("val3")},
	})
	if err != nil {
		t.Fatalf("producer.SendBatch: %v", err)
	}
	if len(offsets) != 2 || offsets[0] != 1 || offsets[1] != 2 {
		t.Fatalf("unexpected batch offsets: %v", offsets)
	}

	// 3. Fetch messages from offset 0
	msgs, err := consumer.Fetch(ctx, "test-topic", 0, 0)
	if err != nil {
		t.Fatalf("consumer.Fetch: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("len(msgs) = %d, want 3", len(msgs))
	}

	expected := []struct {
		k, v string
		off  int64
	}{
		{"key1", "val1", 0},
		{"key2", "val2", 1},
		{"key3", "val3", 2},
	}

	for i, exp := range expected {
		if string(msgs[i].Key) != exp.k || string(msgs[i].Value) != exp.v || msgs[i].Offset != exp.off {
			t.Errorf("msg %d mismatch: got key=%s val=%s off=%d, want key=%s val=%s off=%d",
				i, string(msgs[i].Key), string(msgs[i].Value), msgs[i].Offset, exp.k, exp.v, exp.off)
		}
	}
}

func TestClient_InvalidBrokerAddress(t *testing.T) {
	producer, err := NewProducer([]string{"127.0.0.1:1"}, DefaultProducerConfig())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = producer.Send(ctx, "t", 0, []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected error on invalid broker address, got nil")
	}

	fmt.Println("Invalid broker address error:", err)
}
