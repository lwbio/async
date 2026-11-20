package async

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lwbio/async/internal/topology"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Logarithmic buckets bound memory independently of b.N. Reported percentiles
// are bucket upper bounds (at most 10% above the observed value).
type benchmarkLatency struct {
	buckets [256]atomic.Int64
}

func (h *benchmarkLatency) record(d time.Duration) {
	us := math.Max(1, float64(d)/float64(time.Microsecond))
	i := int(math.Ceil(math.Log(us) / math.Log(1.1)))
	if i >= len(h.buckets) {
		i = len(h.buckets) - 1
	}
	h.buckets[i].Add(1)
}

func (h *benchmarkLatency) report(b *testing.B) {
	var total int64
	for i := range h.buckets {
		total += h.buckets[i].Load()
	}
	for _, p := range []int{50, 95, 99} {
		target := int64(math.Ceil(float64(total) * float64(p) / 100))
		var seen int64
		for i := range h.buckets {
			seen += h.buckets[i].Load()
			if seen >= target {
				b.ReportMetric(math.Pow(1.1, float64(i)), fmt.Sprintf("p%d-us", p))
				break
			}
		}
	}
}

func benchmarkSetting(b *testing.B, name string, fallback int) int {
	b.Helper()
	s := os.Getenv(name)
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		b.Fatalf("%s must be a positive integer", name)
	}
	return n
}

func benchmarkRuntime(b *testing.B, result bool, pending *sync.Map, processed *atomic.Int64) (*Runtime, string) {
	b.Helper()
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		b.Skip("set ASYNC_TEST_AMQP_URL to a disposable RabbitMQ 4.2+ broker")
	}
	var provider metric.MeterProvider = noop.NewMeterProvider()
	switch mode := os.Getenv("ASYNC_BENCH_METRICS"); mode {
	case "", "noop":
	case "sdk":
		sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
		b.Cleanup(func() { _ = sdk.Shutdown(context.Background()) })
		provider = sdk
	case "json":
		path := os.Getenv("ASYNC_BENCH_METRICS_FILE")
		if path == "" {
			b.Fatal("json metrics require ASYNC_BENCH_METRICS_FILE")
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = file.Close() })
		exporter, err := stdoutmetric.New(stdoutmetric.WithWriter(file))
		if err != nil {
			b.Fatal(err)
		}
		sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Second))))
		b.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := sdk.Shutdown(ctx); err != nil {
				b.Errorf("export metrics: %v", err)
			}
		})
		provider = sdk
	default:
		b.Fatalf("ASYNC_BENCH_METRICS must be noop, sdk or json, got %q", mode)
	}
	r, err := New(Config{URL: url, Namespace: "bench_" + uuid.NewString(), PublishTimeout: 10 * time.Second,
		MeterProvider: provider, PublishConcurrency: benchmarkSetting(b, "ASYNC_BENCH_PUBLISHERS", 1)})
	if err != nil {
		b.Fatal(err)
	}
	concurrency := benchmarkSetting(b, "ASYNC_BENCH_CONCURRENCY", 8)
	prefetch := benchmarkSetting(b, "ASYNC_BENCH_PREFETCH", 16)
	handlerDelay := time.Duration(0)
	if raw := os.Getenv("ASYNC_BENCH_HANDLER_DELAY"); raw != "" {
		var err error
		handlerDelay, err = time.ParseDuration(raw)
		if err != nil || handlerDelay < 0 {
			b.Fatal("ASYNC_BENCH_HANDLER_DELAY must be a non-negative duration")
		}
	}
	_, err = r.Register(Subscription{Identity: "worker", Kind: Request, Operations: []string{"bench.echo"},
		Concurrency: concurrency, Prefetch: prefetch, Retries: []time.Duration{},
		Handler: func(ctx context.Context, m Message) (*Message, error) {
			if handlerDelay > 0 {
				timer := time.NewTimer(handlerDelay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			processed.Add(1)
			return &Message{Body: m.Body}, nil
		}})
	if err != nil {
		b.Fatal(err)
	}
	replyTo := ""
	if result {
		replyTo, err = r.Register(Subscription{Identity: "results", Kind: Result, Concurrency: concurrency,
			Prefetch: prefetch, Retries: []time.Duration{}, Handler: func(_ context.Context, m Message) (*Message, error) {
				if done, ok := pending.LoadAndDelete(m.CorrelationID); ok {
					close(done.(chan struct{}))
				}
				return nil, nil
			}})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := r.Stop(ctx); err != nil {
			b.Errorf("stop benchmark runtime: %v", err)
		}
		conn, err := amqp.Dial(url)
		if err != nil {
			b.Errorf("cleanup dial: %v", err)
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			b.Error(err)
			return
		}
		defer ch.Close()
		for _, reg := range r.regs {
			for _, queue := range []string{reg.queue, topology.DeadQueue(reg.queue)} {
				if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
					b.Errorf("cleanup queue %s: %v", queue, err)
				}
			}
		}
		if err := ch.ExchangeDelete(r.cfg.Namespace+".v1.events", false, false); err != nil {
			b.Errorf("cleanup exchange: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		b.Fatal(err)
	}
	if nodes := os.Getenv("ASYNC_TEST_QUORUM_NODES"); nodes != "" {
		names := strings.Split(nodes, ",")
		if len(names) != 3 {
			b.Fatal("expected three explicitly named disposable quorum nodes")
		}
		for _, reg := range r.regs {
			expr := fmt.Sprintf(`{ok,Q}=rabbit_amqqueue:lookup({resource,<<"/">>,queue,<<"%s">>}), length(rabbit_quorum_queue:get_replicas(Q)).`, reg.queue)
			checkCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			out, err := exec.CommandContext(checkCtx, "docker", "exec", "--user", "rabbitmq", names[0], "rabbitmqctl", "eval", expr).CombinedOutput()
			cancel()
			if err != nil || strings.TrimSpace(string(out)) != "3" {
				b.Fatalf("expected three actual queue replicas: %s (%v)", out, err)
			}
		}
	}
	return r, replyTo
}

func BenchmarkRabbitMQPublish(b *testing.B) { benchmarkRabbitMQ(b, false) }

func BenchmarkRabbitMQRequestResult(b *testing.B) { benchmarkRabbitMQ(b, true) }

func benchmarkRabbitMQ(b *testing.B, result bool) {
	for _, size := range []int{256, 4096} {
		b.Run(fmt.Sprintf("bytes_%d", size), func(b *testing.B) {
			b.StopTimer()
			var pending sync.Map
			var processed atomic.Int64
			r, replyTo := benchmarkRuntime(b, result, &pending, &processed)
			body := make([]byte, size)
			for i := range body {
				body[i] = 'x'
			}
			var latency benchmarkLatency
			b.ReportAllocs()
			b.SetBytes(int64(size))
			before := r.Stats()
			beforeGoroutines := runtime.NumGoroutine()
			b.ResetTimer()
			b.StartTimer()
			start := time.Now()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					m := Message{ID: uuid.NewString(), Kind: Request, Operation: "bench.echo",
						ContentType: "application/octet-stream", Body: body, ReplyTo: replyTo}
					var done chan struct{}
					if result {
						done = make(chan struct{})
						pending.Store(m.ID, done)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					opStart := time.Now()
					_, err := r.Publish(ctx, m, 0)
					if err == nil && result {
						select {
						case <-done:
						case <-ctx.Done():
							err = ctx.Err()
						}
					}
					cancel()
					if err != nil {
						pending.Delete(m.ID)
						b.Errorf("publish/result failed: %v", err)
						return
					}
					latency.record(time.Since(opStart))
				}
			})
			elapsed := time.Since(start)
			b.StopTimer()
			// Confirm-only runs still drain business processing before cleanup.
			deadline := time.Now().Add(30 * time.Second)
			for processed.Load() < int64(b.N) && !b.Failed() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if processed.Load() < int64(b.N) {
				b.Errorf("only %d/%d requests processed", processed.Load(), b.N)
			}
			latency.report(b)
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "requests/s")
			after := r.Stats()
			b.ReportMetric(float64(after.Unknown-before.Unknown), "unknown")
			b.ReportMetric(float64(after.Unroutable-before.Unroutable), "unroutable")
			b.ReportMetric(float64(runtime.NumGoroutine()-beforeGoroutines), "goroutines-delta")
		})
	}
}

func BenchmarkHandlerInstrumentation(b *testing.B) {
	for _, mode := range []string{"noop", "sdk"} {
		b.Run(mode, func(b *testing.B) {
			var provider metric.MeterProvider = noop.NewMeterProvider()
			if mode == "sdk" {
				sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
				b.Cleanup(func() { _ = sdk.Shutdown(context.Background()) })
				provider = sdk
			}
			r, err := New(Config{URL: "amqp://localhost", Namespace: "bench", MeterProvider: provider})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = r.Stop(context.Background()) })
			reg := registration{Subscription: Subscription{Kind: Request, Identity: "noop",
				Handler: func(context.Context, Message) (*Message, error) { return nil, nil }}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.invokeHandler(context.Background(), reg, Message{})
			}
		})
	}
}
