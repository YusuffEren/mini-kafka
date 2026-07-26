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
	"strconv"
	"strings"
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
	// BatchFillRatio is set only for LingerMs > 0 scenarios (average of
	// batch_bytes/BatchSize across flushes). Omitted from JSON otherwise.
	BatchFillRatio *float64 `json:"batch_fill_ratio,omitempty"`
	// BatchingSenders is set only for the batching scenario: the number of
	// concurrent Send goroutines (M) driving the shared Producer. It is
	// distinct from Producers (the -producers flag value) so the two are
	// reported independently. Omitted from JSON otherwise.
	BatchingSenders *int `json:"batching_senders,omitempty"`
}

// Report is the top-level JSON document: the environment block followed by
// the per-scenario results.
type Report struct {
	Environment Environment       `json:"environment"`
	Results     []BenchmarkResult `json:"results"`
}

func main() {
	outDir := flag.String("out", "benchmark_results.json", "output JSON file path")
	producersFlag := flag.String("producers", "1", "comma-separated concurrent producer counts, e.g. 1,4,16")
	commit := flag.String("commit", "", "git commit SHA recorded with the results")
	notes := flag.String("notes", "", "free-form notes recorded with the results")
	mdOut := flag.String("md-out", "", "write results as a Markdown table to the given file path")
	flag.Parse()

	producerCounts := parseProducerCounts(*producersFlag)

	fmt.Fprintf(os.Stderr, "🚀 Starting mini-kafka Benchmark Suite...\n")
	fmt.Fprintf(os.Stderr, "   producers=%v  out=%s  md-out=%q\n", producerCounts, *outDir, *mdOut)

	dir, err := os.MkdirTemp("", "mini-kafka-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
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
		fmt.Fprintf(os.Stderr, "Failed to create broker: %v\n", err)
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

	// Producer scenarios are repeated for each -producers value so the
	// scaling curve (1 → 4 → 16) is visible in a single run.
	for _, nProd := range producerCounts {
		label := func(base string) string {
			if len(producerCounts) == 1 {
				return base
			}
			return fmt.Sprintf("%s [producers=%d]", base, nProd)
		}

		// Scenario 1: Single Producer 1KB messages (acks=1)
		res1 := runProducerBenchmark(addrStr, label("Single Producer 1KB (acks=1)"), 10000, 1024, client.DefaultProducerConfig(), nProd)
		results = append(results, res1)

		// Scenario 2: Message Size Impact (100B, 1KB, 10KB)
		res2a := runProducerBenchmark(addrStr, label("Producer 100B (acks=1)"), 10000, 100, client.DefaultProducerConfig(), nProd)
		res2b := runProducerBenchmark(addrStr, label("Producer 10KB (acks=1)"), 5000, 10240, client.DefaultProducerConfig(), nProd)
		results = append(results, res2a, res2b)

		// Scenario 4: Acks Impact (acks=0 vs acks=all)
		cfgAcks0 := client.DefaultProducerConfig()
		cfgAcks0.Acks = 0
		res4a := runProducerBenchmark(addrStr, label("Producer 1KB (acks=0)"), 10000, 1024, cfgAcks0, nProd)
		cfgAcksAll := client.DefaultProducerConfig()
		cfgAcksAll.Acks = -1
		res4b := runProducerBenchmark(addrStr, label("Producer 1KB (acks=all)"), 10000, 1024, cfgAcksAll, nProd)
		results = append(results, res4a, res4b)

		// Scenario 5: Batching Impact (LingerMs=5 vs LingerMs=0).
		// Linger>0 uses a shared Producer + M concurrent Send goroutines so
		// the batcher can actually fill (single-goroutine Send blocks until
		// flush → one record per batch).
		cfgBatch := client.DefaultProducerConfig()
		cfgBatch.LingerMs = 5
		cfgBatch.BatchSize = 65536
		res5a := runBatchingBenchmark(addrStr, label("Producer 1KB (linger=5ms, batch=64KB)"), 10000, 1024, cfgBatch, nProd)
		res5b := runProducerBenchmark(addrStr, label("Producer 1KB (linger=0ms, batch=16KB)"), 10000, 1024, client.DefaultProducerConfig(), nProd)
		results = append(results, res5a, res5b)
	}

	// Scenario 3: Consumer Throughput (independent of producer count)
	res3 := runConsumerBenchmark(addrStr, "Group Consumer Poll", 10000)
	results = append(results, res3)

	report := Report{
		Environment: env,
		Results:     results,
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal results: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(*outDir, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write results file: %v\n", err)
		os.Exit(1)
	}

	if *mdOut != "" {
		md := renderMarkdown(report)
		if err := os.WriteFile(*mdOut, []byte(md), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write markdown file: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Markdown written to %s\n", *mdOut)
	}

	fmt.Fprintf(os.Stderr, "\n✅ Benchmark completed! Results written to %s\n", *outDir)
}

// parseProducerCounts parses a comma-separated list of positive integers.
// Invalid or empty tokens are skipped; an empty result defaults to []int{1}.
func parseProducerCounts(s string) []int {
	parts := strings.Split(s, ",")
	var out []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 {
			continue
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return []int{1}
	}
	return out
}

// renderMarkdown renders the report as Markdown tables and returns the text.
// Latency values are printed as measured (no "<1" substitution). When p50
// would display as 0.000 a [*] marker is appended and a footnote is added.
func renderMarkdown(report Report) string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString("## Ortam\n")
	b.WriteString("| Alan | Değer |\n")
	b.WriteString("|---|---|\n")
	fmt.Fprintf(&b, "| GoVersion | %s |\n", report.Environment.GoVersion)
	fmt.Fprintf(&b, "| GOOS | %s |\n", report.Environment.GOOS)
	fmt.Fprintf(&b, "| GOARCH | %s |\n", report.Environment.GOARCH)
	fmt.Fprintf(&b, "| CPU | %d |\n", report.Environment.NumCPU)
	fmt.Fprintf(&b, "| Timestamp | %s |\n", report.Environment.Timestamp)
	if report.Environment.CommitSHA != "" {
		fmt.Fprintf(&b, "| CommitSHA | %s |\n", report.Environment.CommitSHA)
	}
	if report.Environment.Notes != "" {
		fmt.Fprintf(&b, "| Notes | %s |\n", report.Environment.Notes)
	}

	hasFill := false
	for _, r := range report.Results {
		if r.BatchFillRatio != nil {
			hasFill = true
			break
		}
	}

	b.WriteString("\n")
	b.WriteString("## Sonuçlar\n")
	if hasFill {
		b.WriteString("| Senaryo | Producers | Throughput (msg/s) | p50 | p95 | p99 | max | batch_fill_ratio |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|\n")
	} else {
		b.WriteString("| Senaryo | Producers | Throughput (msg/s) | p50 | p95 | p99 | max |\n")
		b.WriteString("|---|---|---|---|---|---|---|\n")
	}

	p50Footnote := false
	for _, r := range report.Results {
		p50Str, marked := formatP50Latency(r.P50LatencyUs)
		if marked {
			p50Footnote = true
		}
		// Print raw measured latencies (µs). Values < 1 µs stay as 0 or 0.xx —
		// never rewrite them to "<1".
		if hasFill {
			fill := ""
			if r.BatchFillRatio != nil {
				fill = fmt.Sprintf("%.4f", *r.BatchFillRatio)
			}
			fmt.Fprintf(&b, "| %s | %d | %.2f | %s | %s | %s | %s | %s |\n",
				r.Scenario,
				r.Producers,
				r.MsgPerSec,
				p50Str,
				formatLatency(r.P95LatencyUs),
				formatLatency(r.P99LatencyUs),
				formatLatency(r.MaxLatencyUs),
				fill,
			)
		} else {
			fmt.Fprintf(&b, "| %s | %d | %.2f | %s | %s | %s | %s |\n",
				r.Scenario,
				r.Producers,
				r.MsgPerSec,
				p50Str,
				formatLatency(r.P95LatencyUs),
				formatLatency(r.P99LatencyUs),
				formatLatency(r.MaxLatencyUs),
			)
		}
	}

	if p50Footnote {
		b.WriteString("\n")
		b.WriteString("[*] p50 degeri sistem saat cozunurlugunun altinda; Windows'ta ~500us kuantum. Daha hassas olcum icin Linux'ta kosun.\n")
	}

	return b.String()
}

// formatLatency prints a latency in microseconds without the "<1" rewrite.
func formatLatency(us float64) string {
	if us == float64(int64(us)) && us >= 1 {
		return fmt.Sprintf("%.0f", us)
	}
	return fmt.Sprintf("%.3f", us)
}

// formatP50Latency formats p50 like formatLatency, but appends " [*]" when the
// displayed value would be 0.000 (below system clock resolution on many OSes).
// The bool is true when the marker was added.
func formatP50Latency(us float64) (string, bool) {
	s := formatLatency(us)
	if s == "0.000" {
		return s + " [*]", true
	}
	return s, false
}

// runProducerBenchmark drives `producers` goroutines that each own a dedicated
// Producer (separate TCP connection). The first 10% of each goroutine's sends
// are treated as warmup and excluded from latency statistics so that connection
// establishment and initial batch sizing do not skew the percentiles.
func runProducerBenchmark(addr, scenario string, count, payloadSize int, cfg client.ProducerConfig, producers int) BenchmarkResult {
	fmt.Fprintf(os.Stderr, "Running: %s ... ", scenario)
	if producers < 1 {
		producers = 1
	}

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
		// One Producer per goroutine — closed after wg.Wait.
		prods = make([]*client.Producer, producers)
	)

	// Create all producers up front so a dial failure aborts cleanly before
	// any Send work starts. Each Producer opens its own TCP connection.
	for p := 0; p < producers; p++ {
		prod, err := client.NewProducer([]string{addr}, cfg)
		if err != nil {
			// Close any already-created producers.
			for i := 0; i < p; i++ {
				if prods[i] != nil {
					_ = prods[i].Close()
				}
			}
			fmt.Fprintf(os.Stderr, "producer init err: %v\n", err)
			return BenchmarkResult{Scenario: scenario, Producers: producers}
		}
		prods[p] = prod
	}
	defer func() {
		for _, prod := range prods {
			if prod != nil {
				_ = prod.Close()
			}
		}
	}()

	start := time.Now()

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			prod := prods[pid]

			localCount := perProducer
			if pid < remainder {
				localCount++
			}
			// Warmup scales with this goroutine's share so the global 10%
			// invariant holds even when remainder goroutines send one extra.
			localWarmup := warmupPerProducer
			if pid < remainder {
				localWarmup = localCount / 10
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

	// Average batch fill ratio across all per-goroutine producers (LingerMs > 0 only).
	var batchFill *float64
	if cfg.LingerMs > 0 {
		var sum float64
		var n int
		for _, prod := range prods {
			if prod == nil {
				continue
			}
			sum += prod.AvgBatchFillRatio()
			n++
		}
		if n > 0 {
			avg := sum / float64(n)
			batchFill = &avg
		}
	}

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
		BatchFillRatio:   batchFill,
	}

	fillNote := ""
	if batchFill != nil {
		fillNote = fmt.Sprintf(", batch_fill=%.4f", *batchFill)
	}
	fmt.Fprintf(os.Stderr, "Done in %d ms (%.2f msg/sec, %.2f MB/sec, p50=%.3fms, p99=%.3fms, max=%.3fms, errors=%d%s)\n",
		durMs, msgPerSec, mbPerSec, res.P50LatencyMs, res.P99LatencyMs, res.MaxLatencyMs, sendErrors, fillNote)
	return res
}

// runBatchingBenchmark measures producer batching with a single shared Producer
// and M concurrent Send goroutines. Send blocks until its batch flushes, so a
// single goroutine can only enqueue one record per batch; concurrent callers
// fill the same pending batch (Producer is concurrent-safe via reqMu/batch.mu)
// and yield a realistic batch_fill_ratio.
//
// M (the number of concurrent Send goroutines) is derived from the -producers
// value as M = producers * 2 so that batch_fill_ratio scales honestly with the
// requested producer count: producers=1 → M=2 (sparse batches), producers=16 →
// M=32 (genuinely filled batches). The reported Producers column reflects the
// -producers flag value; the internal M is exposed separately as
// BatchingSenders so the two are not conflated.
func runBatchingBenchmark(addr, scenario string, count, payloadSize int, cfg client.ProducerConfig, producers int) BenchmarkResult {
	fmt.Fprintf(os.Stderr, "Running: %s ... ", scenario)
	if producers < 1 {
		producers = 1
	}
	// M scales with the requested producer count so batch_fill_ratio varies
	// meaningfully across the scaling curve instead of being pinned near 0.5.
	m := producers * 2
	if m < 2 {
		m = 2
	}

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = 'a'
	}

	ctx := context.Background()
	topic := "bench-batch-" + fmt.Sprintf("%d", time.Now().UnixNano())

	prod, err := client.NewProducer([]string{addr}, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer init err: %v\n", err)
		return BenchmarkResult{Scenario: scenario, Producers: producers}
	}
	defer func() { _ = prod.Close() }()

	perWorker := count / m
	remainder := count % m
	if perWorker < 1 {
		perWorker = 1
		remainder = 0
	}

	warmupPerWorker := perWorker / 10
	if warmupPerWorker < 1 && perWorker > 0 {
		warmupPerWorker = 1
	}

	var (
		latMu      sync.Mutex
		allLatency []time.Duration
		sendErrors int64
		wg         sync.WaitGroup
	)

	start := time.Now()

	for w := 0; w < m; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()

			localCount := perWorker
			if wid < remainder {
				localCount++
			}
			localWarmup := warmupPerWorker
			if wid < remainder {
				localWarmup = localCount / 10
				if localWarmup < 1 {
					localWarmup = 1
				}
			}

			localLats := make([]time.Duration, 0, localCount-localWarmup)
			for i := 0; i < localCount; i++ {
				key := []byte(fmt.Sprintf("k-%d-%d", wid, i))
				t0 := time.Now()
				// Shared producer: concurrent Send fills the same batch.
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
		}(w)
	}
	wg.Wait()

	duration := time.Since(start)

	var batchFill *float64
	if cfg.LingerMs > 0 {
		avg := prod.AvgBatchFillRatio()
		batchFill = &avg
	}

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

	// Capture M into a stable address so the pointer can escape safely.
	senders := m
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
		BatchFillRatio:   batchFill,
		BatchingSenders:  &senders,
	}

	fillNote := ""
	if batchFill != nil {
		fillNote = fmt.Sprintf(", batch_fill=%.4f", *batchFill)
	}
	fmt.Fprintf(os.Stderr, "Done in %d ms (%.2f msg/sec, %.2f MB/sec, p50=%.3fms, p99=%.3fms, max=%.3fms, errors=%d%s)\n",
		durMs, msgPerSec, mbPerSec, res.P50LatencyMs, res.P99LatencyMs, res.MaxLatencyMs, sendErrors, fillNote)
	return res
}

// consumerPollSamples is the number of non-empty Poll calls used for latency
// percentiles in the consumer benchmark. Multiple samples give a real
// p50/p95/p99 curve instead of a single-point degenerate distribution.
const consumerPollSamples = 100

// runConsumerBenchmark measures group-consumer poll latency over many Poll
// samples. Messages are seeded first; then up to consumerPollSamples non-empty
// Poll calls are issued. Empty polls (0 messages) are excluded from latency
// stats and from the reported poll count. The first 10% of successful
// non-empty Poll latencies are warmup.
func runConsumerBenchmark(addr, scenario string, count int) BenchmarkResult {
	fmt.Fprintf(os.Stderr, "Running: %s ... ", scenario)
	topic := "bench-consumer-" + fmt.Sprintf("%d", time.Now().UnixNano())
	ctx := context.Background()

	// Seed data — enough for many Poll rounds.
	prod, err := client.NewProducer([]string{addr}, client.DefaultProducerConfig())
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer init err: %v\n", err)
		return BenchmarkResult{Scenario: scenario}
	}
	payload := make([]byte, 1024)
	for i := 0; i < count; i++ {
		_, _ = prod.Send(ctx, topic, int32(i%4), []byte(fmt.Sprintf("k-%d", i)), payload)
	}
	_ = prod.Close()

	cfg := client.DefaultGroupConsumerConfig()
	cfg.AutoOffsetReset = "earliest"
	// Cap MaxBytes so individual Polls return smaller batches and we get
	// enough distinct samples for percentile stats.
	cfg.MaxBytes = 32 * 1024
	gc, err := client.NewGroupConsumer([]string{addr}, "bench-group-"+fmt.Sprintf("%d", time.Now().UnixNano()), []string{topic}, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consumer init err: %v\n", err)
		return BenchmarkResult{Scenario: scenario}
	}
	defer func() { _ = gc.Close() }()

	start := time.Now()
	consumed := 0
	var latencies []time.Duration
	// nonEmptyPolls = polls that returned at least one message (reported count).
	nonEmptyPolls := 0
	// emptyAfterDrain limits spinning once the log is exhausted.
	emptyAfterDrain := 0
	const maxEmptyAfterDrain = 5
	// Safety cap so a stuck broker cannot loop forever.
	maxAttempts := consumerPollSamples * 20
	attempts := 0

	for nonEmptyPolls < consumerPollSamples && attempts < maxAttempts {
		attempts++
		t0 := time.Now()
		msgs, err := gc.Poll(ctx, 1*time.Second)
		d := time.Since(t0)
		if err != nil {
			break
		}
		if len(msgs) == 0 {
			// Empty poll: exclude from latency and poll count.
			if consumed >= count {
				emptyAfterDrain++
				if emptyAfterDrain >= maxEmptyAfterDrain {
					break
				}
			}
			continue
		}
		emptyAfterDrain = 0
		latencies = append(latencies, d)
		nonEmptyPolls++
		consumed += len(msgs)
	}

	duration := time.Since(start)
	durMs := duration.Milliseconds()
	if durMs == 0 {
		durMs = 1
	}

	// 10% of non-empty poll samples are warmup.
	warmupPolls := len(latencies) / 10
	if warmupPolls < 1 && len(latencies) > 1 {
		warmupPolls = 1
	}
	measuredLats := latencies
	if warmupPolls > 0 && warmupPolls < len(latencies) {
		measuredLats = latencies[warmupPolls:]
	}

	// Approximate measured messages excluding a proportional warmup share.
	warmupMsgs := 0
	if len(latencies) > 0 {
		warmupMsgs = consumed * warmupPolls / len(latencies)
	}
	measured := consumed - warmupMsgs
	if measured < 0 {
		measured = 0
	}

	msgPerSec := float64(measured) / (float64(durMs) / 1000.0)
	mbPerSec := (float64(measured*1024) / (1024.0 * 1024.0)) / (float64(durMs) / 1000.0)

	sort.Slice(measuredLats, func(i, j int) bool { return measuredLats[i] < measuredLats[j] })

	p50us, p95us, p99us, maxUs := latencyStatsUs(measuredLats)

	res := BenchmarkResult{
		Scenario:         scenario,
		Producers:        1,
		DurationMs:       durMs,
		TotalMessages:    count,
		WarmupMessages:   warmupMsgs,
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

	fmt.Fprintf(os.Stderr, "Done in %d ms (%.2f msg/sec, %.2f MB/sec, p50=%.3fms, p99=%.3fms, max=%.3fms, polls=%d)\n",
		durMs, msgPerSec, mbPerSec, res.P50LatencyMs, res.P99LatencyMs, res.MaxLatencyMs, nonEmptyPolls)
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
