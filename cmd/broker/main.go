package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
)

// parseLogLevel maps the configured LogLevel string onto the corresponding
// slog.Level. Unknown or empty values fall back to slog.LevelInfo so the
// broker always starts with a usable logger.
func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "warn", "WARN":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	case "info", "INFO":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}

func main() {
	configPath := flag.String("config", "config/broker.yaml", "path to broker configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		// The logger is not configured yet; use the standard logger at the
		// highest severity so a config load failure is never silenced.
		slog.Error("failed to load config", "path", *configPath, "err", err)
		os.Exit(1)
	}

	// Configure the structured logger from the loaded config so every
	// downstream component (broker, server mux, handlers) inherits the same
	// level and output destination.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.Broker.LogLevel),
	}))
	slog.SetDefault(logger)

	b, err := broker.New(cfg)
	if err != nil {
		slog.Error("failed to create broker", "err", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting mini-kafka broker",
			"host", cfg.Broker.Host,
			"port", cfg.Broker.Port,
			"data_dir", cfg.Broker.DataDir,
		)
		errCh <- b.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			slog.Error("broker start error", "err", err)
			os.Exit(1)
		}
	case sig := <-sigCh:
		slog.Info("received signal, initiating graceful shutdown", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := b.Shutdown(ctx); err != nil {
			slog.Error("error during shutdown", "err", err)
		} else {
			slog.Info("broker shutdown cleanly")
		}
	}
}
