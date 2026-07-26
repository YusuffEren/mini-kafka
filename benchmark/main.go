package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/broker"
	"github.com/YusuffEren/mini-kafka/internal/config"
	"github.com/YusuffEren/mini-kafka/pkg/client"
)

// Environment captures the runtime context in which the benchmark executed.
// Persisting it alongside the results makes runs comparable across machines
// and over time.
type Environment struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	NumCPU    int    `json:"num_cpu"`
	Timestamp string `json:"timestamp"`
	CommitSHA string `json:"commit_sha,omitempty"`
	Notes     string `json:"notes,omitempty"`
}

// BenchmarkResult holds the measurements for a single scenario. Latencies are
// stored both in microseconds (the native resolution used during measurement)
// and milliseconds (3-decimal friendly) so consumers of the JSON can pick the
// unit they need without re-deriving it.
type BenchmarkResult struct {
	Scenario         string  `json:"scenario"`
	Producers        int     `json:"producers"`
	DurationMs       int64   `json:"duration_ms"`
	TotalMessages    int     `json:"total_messages"`
	WarmupMessages   int     `json:"warmup_messages"`
	MeasuredMessages int     `json:"measured_messages"`
	MsgPerSec        float64 `json:"msg_per_sec"`
	MBPerSec         float64 `json:"mb_per_sec"`
	P50LatencyUs     float64 `json:"p50_latency_us"`
	P95LatencyUs     float64 `json:"p95_latency_us"`
	P99LatencyUs     float64 `json:"p99_latency_us"`
	MaxLatencyUs     float64 `json:"max_latency_us"`
	P50LatencyMs     float64 `json:"p50_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms"`
	P99LatencyMs     float64 `json:"p99_latency_ms"`
	MaxLatencyMs     float64 `json:"max_latency_ms"`
}

// Report is the top-level JSON document: the environment block followed by
// the per-scenario results.
type Report struct {
	Environment Environment       `json:"environment"`
	Results     []BenchmarkResult `json:"results"`
}

func main() {
	outDir := flag.String("out", "benchmark_results.json", "output JSON file path")
	producers := flag.Int("producers", 1, "number of concurrent producer goroutines")
	commit := flag.String("commit", "", "git commit SHA recorded with the results")
	notes := flag.String("notes", "", "free-form notes recorded with the results")
	flag.Parse()

	if *producers < 1 {
		*producers = 1
	}

	fmt.Println("🚀 Starting mini-kafka Benchmark Suite...")
	fmt.Printf("   producers=%d  out=%s\n", *producers, *outDir)

	dir, err := os.MkdirTemp("", "mini-kafka-bench-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Printf("Failed to listen: %v\n", err)
		os.Exit(1)
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
		fmt.Printf("Failed to create broker: %v\n", err)
		os.Exit(1)
	}

	go func() {
		_ = b.Start()
	}()

	addrStr := fmt.Sprintf("127.0.0.1:%d", port)
	time.Sleep(100 * time.Millisecond)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	env := Environment{
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		CommitSHA: *commit,
		Notes:     *notes,
	}

	var results []BenchmarkResult

	// Scenario 1: Single Producer 1KB messages (acks=1)
	res1 := runProducerBenchmark(addrStr, "Single Producer 1KB (acks=1)", 10000, 1024, client.DefaultProducerConfig(), *producers)
	results = append(results, res1)

	// Scenario 2: Message Size Impact (100B, 1KB, 10KB)
	res2a := runProducerBenchmark(addrStr, "Producer 100B (acks=1)", 10000, 100, client.DefaultProducerConfig(), *producers)
	res2b := runProducerBenchmark(addrStr, "Producer 10KB (acks=1)", 5000, 10240, client.DefaultProducerConfig(), *producers)
	results = append(results, res2a, res2b)

	// Scenario 3: Consumer Throughput
	res3 := runConsumerBenchmark(addrStr, "Group Consumer Poll", 10000)
	results = append(results, res3)

	// Scenario 4: Acks Impact (acks=0 vs acks=all)
	cfgAcks0 := client.DefaultProducerConfig()
	cfgAcks0.Acks = 0
	res4a := runProducerBenchmark(addrStr, "Producer 1KB (acks=0)", 10000, 1024, cfgAcks0, *producers)
	cfgAcksAll := client.DefaultProducerConfig()
	cfgAcksAll.Acks = -1
	res4b := runProducerBenchmark(addrStr, "Producer 1KB (acks=all)", 10000, 1024, cfgAcksAll, *producers)
	results = append(results, res4a, res4b)

	// Scenario 5: Batching Impact (LingerMs=5 vs LingerMs=0)
	cfgBatch := client.DefaultProducerConfig()
	cfgBatch.LingerMs = 5
	cfgBatch.BatchSize = 65536
	res5a := runProducerBenchmark(addrStr, "Producer 1KB (linger=5ms, batch=64KB)", 10000, 1024, cfgBatch, *producers)
	res5b := runProducerBenchmark(addrStr, "Producer 1KB (linger=0ms, batch=16KB)", 10000, 1024, client.DefaultProducerConfig(), *producers)
	results = append(results, res5a, res5b)

	report := Report{
		Environment: env,
		Results:     results,
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Printf("Failed to marshal results: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(*outDir, data, 0644); err != nil {
		fmt.Printf("Failed to write results file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n✅ Benchmark completed! Results written to %s\n", *outDir)
}

// runProducerBenchmark drives `producers` goroutines that publish concurrently
// to a single shared Producer (which is safe for concurrent use). The first 10%
// of each goroutine's sends are treated as warmup and excluded from latency
// statistics so that JIT warm-up, connection establishment and initial batch
// sizing do not skew the percentiles.
func runProducerBenchmark(addr, scenario string, count, payloadSize int, cfg client.ProducerConfig, producers int) BenchmarkResult {
	fmt.Printf("Running: %s ... ", scenario)
	if producers < 1 {
		producers = 1
	}

	prod, err := client.NewProducer([]string{addr}, cfg)
	if err != nil {
		fmt.Printf("producer init err: %v\n", err)
		return BenchmarkResult{Scenario: scenario, Producers: producers}
	}
	defer func() { _ = prod.Close() }()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = 'a'
	}

	ctx := context.Background()
	topic := "bench-" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Distribute the requested total across the producer goroutines. Any
	// remainder is assigned to the first goroutines so the total is exact.
	perProducer := count / producers
	remainder := count % producers
	if perProducer < 1 {
		perProducer = 1
		remainder = 0
	}

	// 10% warmup per goroutine (at least 1 if there is anything to send).
	warmupPerProducer := perProducer / 10
	if warmupPerProducer < 1 && perProducer > 0 {
		warmupPerProducer = 1
	}

	var (
		latMu      sync.Mutex
		allLatency []time.Duration
		sendErrors int64
		wg         sync.WaitGroup
	)

	start := time.Now()

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()

			localCount := perProducer
			if pid < remainder {
				localCount++
			}
			// Warmup scales with this goroutine's share so the global 10%
			// invariant holds even when remainder goroutines send one extra.
			localWarmup := warmupPerProducer
			if pid < remainder {
				localWarmup = (localCount) / 10
				if localWarmup < 1 {
					localWarmup = 1
				}
			}

			localLats := make([]time.Duration, 0, localCount-localWarmup)
			for i := 0; i < localCount; i++ {
				key := []byte(fmt.Sprintf("k-%d-%d", pid, i))
				t0 := time.Now()
				_, err := prod.Send(ctx, topic, int32(i%4), key, payload)
				d := time.Since(t0)
				if err != nil {
					atomic.AddInt64(&sendErrors, 1)
					continue
				}
				if i >= localWarmup {
					localLats = append(localLats, d)
				}
			}

			latMu.Lock()
			allLatency = append(allLatency, localLats...)
			latMu.Unlock()
		}(p)
	}
	wg.Wait()

	duration := time.Since(start)

	measured := len(allLatency)
	warmupTotal := count - measured
	if warmupTotal < 0 {
		warmupTotal = 0
	}

	sort.Slice(allLatency, func(i, j int) bool { return allLatency[i] < allLatency[j] })

	durMs := duration.Milliseconds()
	if durMs == 0 {
		durMs = 1
	}

	msgPerSec := float64(measured) / (float64(durMs) / 1000.0)
	mbPerSec := (float64(measured*payloadSize) / (1024.0 * 1024.0)) / (float64(durMs) / 1000.0)

	p50us, p95us, p99us, maxUs := latencyStatsUs(allLatency)

	res := BenchmarkResult{
		Scenario:         scenario,
		Producers:        producers,
		DurationMs:       durMs,
		TotalMessages:    count,
		WarmupMessages:   warmupTotal,
		MeasuredMessages: measured,
		MsgPerSec:        msgPerSec,
		MBPerSec:         mbPerSec,
		P50LatencyUs:     p50us,
		P95LatencyUs:     p95us,
		P99LatencyUs:     p99us,
		MaxLatencyUs:     maxUs,
		P50LatencyMs:     p50us / 1000.0,
		P95LatencyMs:     p95us / 1000.0,
		P99LatencyMs:     p99us / 1000.0,
		MaxLatencyMs:     maxUs / 1000.0,
	}

	fmt.Printf("Done in %d ms (%.2f msg/sec, %.2f MB/sec, p50=%.3fms, p99=%.3fms, max=%.3fms, errors=%d)\n",
		durMs, msgPerSec, mbPerSec, res.P50LatencyMs, res.P99LatencyMs, res.MaxLatencyMs, sendErrors)
	return res
}

// runConsumerBenchmark measures group-consumer poll latency. The first 10% of
// consumed records are treated as warmup and excluded from latency stats.
func runConsumerBenchmark(addr, scenario string, count int) BenchmarkResult {
	fmt.Printf("Running: %s ... ", scenario)
	topic := "bench-consumer-" + fmt.Sprintf("%d", time.Now().UnixNano())
	ctx := context.Background()

	// Seed data
	prod, _ := client.NewProducer([]string{addr}, client.DefaultProducerConfig())
	payload := make([]byte, 1024)
	for i := 0; i < count; i++ {
		_, _ = prod.Send(ctx, topic, int32(i%4), []byte(fmt.Sprintf("k-%d", i)), payload)
	}
	_ = prod.Close()

	cfg := client.DefaultGroupConsumerConfig()
	cfg.AutoOffsetReset = "earliest"
	gc, err := client.NewGroupConsumer([]string{addr}, "bench-group", []string{topic}, cfg)
	if err != nil {
		fmt.Printf("consumer init err: %v\n", err)
		return BenchmarkResult{Scenario: scenario}
	}
	defer func() { _ = gc.Close() }()

	warmupTarget := count / 10
	if warmupTarget < 1 {
		warmupTarget = 1
	}

	start := time.Now()
	consumed := 0
	var latencies []time.Duration

	for consumed < count {
		t0 := time.Now()
		msgs, err := gc.Poll(ctx, 1*time.Second)
		d := time.Since(t0)
		if err != nil || len(msgs) == 0 {
			break
		}
		// Only record latency for the measured (post-warmup) portion.
		if consumed >= warmupTarget {
			latencies = append(latencies, d)
		}
		consumed += len(msgs)
	}

	duration := time.Since(start)
	durMs := duration.Milliseconds()
	if durMs == 0 {
		durMs = 1
	}

	measured := consumed - warmupTarget
	if measured < 0 {
		measured = 0
	}

	msgPerSec := float64(measured) / (float64(durMs) / 1000.0)
	mbPerSec := (float64(measured*1024) / (1024.0 * 1024.0)) / (float64(durMs) / 1000.0)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50us, p95us, p99us, maxUs := latencyStatsUs(latencies)

	res := BenchmarkResult{
		Scenario:         scenario,
		Producers:        1,
		DurationMs:       durMs,
		TotalMessages:    count,
		WarmupMessages:   warmupTarget,
		MeasuredMessages: measured,
		MsgPerSec:        msgPerSec,
		MBPerSec:         mbPerSec,
		P50LatencyUs:     p50us,
		P95LatencyUs:     p95us,
		P99LatencyUs:     p99us,
		MaxLatencyUs:     maxUs,
		P50LatencyMs:     p50us / 1000.0,
		P95LatencyMs:     p95us / 1000.0,
		P99LatencyMs:     p99us / 1000.0,
		MaxLatencyMs:     maxUs / 1000.0,
	}

	fmt.Printf("Done in %d ms (%.2f msg/sec, %.2f MB/sec, p50=%.3fms, p99=%.3fms, max=%.3fms)\n",
		durMs, msgPerSec, mbPerSec, res.P50LatencyMs, res.P99LatencyMs, res.MaxLatencyMs)
	return res
}

// latencyStatsUs computes p50/p95/p99/max from a sorted-ascending slice of
// durations and returns them in microseconds (float64). An empty input yields
// all zeros so callers can build a result without special-casing.
func latencyStatsUs(sorted []time.Duration) (p50, p95, p99, max float64) {
	if len(sorted) == 0 {
		return 0, 0, 0, 0
	}
	// Use nanoseconds / 1000.0 instead of Microseconds() so that
	// sub-microsecond latencies (common on localhost) are not truncated
	// to zero — that would make p50 read 0.000ms and hide real signal.
	toUs := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000.0 }
	p50 = toUs(sorted[percentileIndex(len(sorted), 50)])
	p95 = toUs(sorted[percentileIndex(len(sorted), 95)])
	p99 = toUs(sorted[percentileIndex(len(sorted), 99)])
	max = toUs(sorted[len(sorted)-1])
	return p50, p95, p99, max
}

// percentileIndex maps a percentile (0-100) to an index into a sorted slice of
// length n using the nearest-rank method. The returned index is always in range.
func percentileIndex(n int, pct float64) int {
	if n <= 0 {
		return 0
	}
	idx := int(float64(n) * (pct / 100.0))
	if idx >= n {
		idx = n - 1
	}
	if idx < 0 {
		idx = 0
	}
	return idx
}
