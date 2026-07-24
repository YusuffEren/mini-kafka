package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
)

func main() {
	configPath := flag.String("config", "config/broker.yaml", "path to broker configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config from %s: %v", *configPath, err)
	}

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("Starting mini-kafka broker on %s:%d (data dir: %s)...", cfg.Broker.Host, cfg.Broker.Port, cfg.Broker.DataDir)
		errCh <- b.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("broker start error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("Received signal %s, initiating graceful shutdown...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := b.Shutdown(ctx); err != nil {
			log.Printf("error during shutdown: %v", err)
		} else {
			log.Println("Broker shutdown cleanly.")
		}
	}
}
