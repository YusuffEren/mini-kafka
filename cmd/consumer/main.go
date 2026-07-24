package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YusuffEren/mini-kafka/pkg/client"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "broker host:port address")
	topic := flag.String("topic", "test", "target topic name")
	fromBeginning := flag.Bool("from-beginning", false, "start consuming from offset 0")
	flag.Parse()

	cfg := client.DefaultConsumerConfig()
	cfg.ClientID = "mini-kafka-cli-consumer"
	cfg.MaxWaitMs = 1000

	consumer, err := client.NewConsumer([]string{*brokerAddr}, cfg)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down consumer...")
		cancel()
	}()

	currentOffset := int64(0)
	if !*fromBeginning {
		// If not from beginning, attempt to read starting at offset 0 or current LEO
		currentOffset = 0
	}

	fmt.Printf("Consumer listening on %s (topic: %s, starting offset: %d)...\n", *brokerAddr, *topic, currentOffset)

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer loop stopped.")
			return
		default:
		}

		msgs, err := consumer.Fetch(ctx, *topic, 0, currentOffset)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, msg := range msgs {
			if len(msg.Key) > 0 {
				fmt.Printf("[%s] offset=%d key=%s value=%s\n", *topic, msg.Offset, string(msg.Key), string(msg.Value))
			} else {
				fmt.Printf("[%s] offset=%d value=%s\n", *topic, msg.Offset, string(msg.Value))
			}
			currentOffset = msg.Offset + 1
		}
	}
}
