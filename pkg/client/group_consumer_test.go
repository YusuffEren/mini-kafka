package client_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/pkg/client"
)

func setupTestBroker(t *testing.T) (*broker.Broker, string, func()) {
	t.Helper()
	dir := t.TempDir()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			ID:               1,
			Host:             "127.0.0.1",
			Port:             port,
			DataDir:          dir,
			MaxConnections:   1024,
			RequestTimeoutMs: 30000,
		},
		Topic: config.TopicConfig{
			AutoCreate:        true,
			DefaultPartitions: 4,
		},
	}
	cfg.WithDefaults()

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	go func() {
		_ = b.Start()
	}()

	addrStr := fmt.Sprintf("127.0.0.1:%d", port)
	time.Sleep(50 * time.Millisecond)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}

	return b, addrStr, cleanup
}

func TestGroupConsumer_SingleConsumer(t *testing.T) {
	_, addr, cleanup := setupTestBroker(t)
	defer cleanup()

	// 1. Produce some messages
	prod, err := client.NewProducer([]string{addr}, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close()

	ctx := context.Background()
	topic := "group-test-topic"

	for i := 0; i < 10; i++ {
		_, err := prod.Send(ctx, topic, int32(i%4), []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("val-%d", i)))
		if err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
	}

	// 2. Start GroupConsumer
	cfg := client.DefaultGroupConsumerConfig()
	cfg.AutoOffsetReset = "earliest"
	gc, err := client.NewGroupConsumer([]string{addr}, "test-group-1", []string{topic}, cfg)
	if err != nil {
		t.Fatalf("new group consumer: %v", err)
	}
	defer gc.Close()

	// 3. Poll messages
	msgs, err := gc.Poll(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(msgs) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(msgs))
	}

	// 4. Commit offsets
	if err := gc.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestGroupConsumer_Rebalance(t *testing.T) {
	_, addr, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "rebalance-topic"
	groupID := "rebalance-group"

	// Create 2 consumers in same group
	cfg1 := client.DefaultGroupConsumerConfig()
	cfg1.ClientID = "consumer-1"

	gc1, err := client.NewGroupConsumer([]string{addr}, groupID, []string{topic}, cfg1)
	if err != nil {
		t.Fatalf("gc1 init: %v", err)
	}
	defer gc1.Close()

	cfg2 := client.DefaultGroupConsumerConfig()
	cfg2.ClientID = "consumer-2"

	gc2, err := client.NewGroupConsumer([]string{addr}, groupID, []string{topic}, cfg2)
	if err != nil {
		t.Fatalf("gc2 init: %v", err)
	}
	defer gc2.Close()

	// Both consumers should be running without error
	ctx := context.Background()
	_, _ = gc1.Poll(ctx, 100*time.Millisecond)
	_, _ = gc2.Poll(ctx, 100*time.Millisecond)
}

func TestGroupConsumer_Concurrent_Race(t *testing.T) {
	_, addr, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "race-topic"
	groupID := "race-group"

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cfg := client.DefaultGroupConsumerConfig()
			cfg.ClientID = fmt.Sprintf("race-consumer-%d", id)

			gc, err := client.NewGroupConsumer([]string{addr}, groupID, []string{topic}, cfg)
			if err != nil {
				return
			}
			defer gc.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					_, _ = gc.Poll(ctx, 50*time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()
}
