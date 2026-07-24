package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/YusuffEren/mini-kafka/pkg/client"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "broker host:port address")
	topic := flag.String("topic", "test", "target topic name")
	key := flag.String("key", "", "message key (optional)")
	value := flag.String("value", "", "message value (optional)")
	flag.Parse()

	cfg := client.DefaultProducerConfig()
	cfg.ClientID = "mini-kafka-cli-producer"

	producer, err := client.NewProducer([]string{*brokerAddr}, cfg)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()

	// If explicit value is provided via flag, send single message
	if *value != "" {
		offset, err := producer.Send(ctx, *topic, 0, []byte(*key), []byte(*value))
		if err != nil {
			log.Fatalf("failed to send message: %v", err)
		}
		fmt.Printf("Message sent to topic %s [partition 0] at offset %d\n", *topic, offset)
		return
	}

	// Otherwise, read interactively from stdin
	fmt.Printf("Interactive producer started targeting %s (topic: %s). Enter text and press Enter to send (Ctrl+C to exit):\n", *brokerAddr, *topic)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := strings.TrimRight(scanner.Text(), "\r\n")
		if text == "" {
			continue
		}

		offset, err := producer.Send(ctx, *topic, 0, []byte(*key), []byte(text))
		if err != nil {
			log.Printf("send error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Printf("Sent offset %d: %s\n", offset, text)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("error reading stdin: %v", err)
	}
}
