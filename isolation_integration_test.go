package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	amqp "github.com/rabbitmq/amqp091-go"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestIntegrationMissingResultDoesNotInterruptOtherSubscriptions(t *testing.T) {
	r := integrationRuntime(t)
	var calls, received atomic.Int32
	_, err := r.Register(Subscription{Identity: "blocked", Kind: Request, Operations: []string{"blocked"},
		Concurrency: 1, Prefetch: 1, Handler: func(context.Context, Message) (*Message, error) {
			calls.Add(1)
			return &Message{Body: []byte(`"done"`)}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Register(Subscription{Identity: "healthy", Kind: Event, Operations: []string{"healthy"},
		Handler: func(context.Context, Message) (*Message, error) { received.Add(1); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	r.mu.Lock()
	original := r.gen
	r.mu.Unlock()
	m := testMessage(Request, "blocked")
	m.ReplyTo = r.ResultTarget("temporarily-missing")
	id, err := r.Publish(context.Background(), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.Stats().Unroutable > 0 })
	// Span several local recovery attempts, not just the first failure.
	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := r.Publish(ctx, testMessage(Event, "healthy"), 0)
		cancel()
		if err != nil {
			t.Fatalf("healthy subscription interrupted: %v", err)
		}
		r.mu.Lock()
		sameGeneration := r.gen == original && r.state == Ready
		r.mu.Unlock()
		if !sameGeneration {
			t.Fatal("local routing failure replaced the shared connection")
		}
		time.Sleep(100 * time.Millisecond)
	}
	waitFor(t, func() bool { return received.Load() == 25 })
	if r.Stats().Reconnects != 0 || calls.Load() > 5 {
		t.Fatalf("reconnect or retry hot loop: stats=%+v calls=%d", r.Stats(), calls.Load())
	}
	conn, err := amqp.Dial(r.cfg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := topology.Declare(ch, topology.Subscription{Queue: m.ReplyTo}); err != nil {
		t.Fatal(err)
	}
	var result amqp.Delivery
	waitFor(t, func() bool {
		var ok bool
		result, ok, err = ch.Get(m.ReplyTo, true)
		return err == nil && ok
	})
	if result.CorrelationId != id || result.MessageId != protocol.ResultID(id) || calls.Load() < 2 {
		t.Fatalf("result identity or recovery lost: calls=%d result=%+v", calls.Load(), result)
	}
}

func TestIntegrationConsumeChannelRecoversWithoutReplacingConnection(t *testing.T) {
	for _, fault := range []string{"channel-close", "queue-delete"} {
		t.Run(fault, func(t *testing.T) {
			r := integrationRuntime(t)
			var calls atomic.Int32
			queue, err := r.Register(Subscription{Identity: "local", Kind: Event, Operations: []string{"local"},
				Handler: func(context.Context, Message) (*Message, error) { calls.Add(1); return nil, nil }})
			if err != nil {
				t.Fatal(err)
			}
			startTest(t, r)
			r.mu.Lock()
			g := r.gen
			r.mu.Unlock()
			if fault == "channel-close" {
				if err := g.consumers[0].first.ch.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				conn, err := amqp.Dial(r.cfg.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				ch, err := conn.Channel()
				if err != nil {
					t.Fatal(err)
				}
				defer ch.Close()
				if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err := r.Publish(ctx, testMessage(Event, "local"), 0)
				if err != nil && !errors.Is(err, ErrUnroutable) {
					t.Fatalf("unexpected publication error: %v", err)
				}
				return calls.Load() > 0
			})
			r.mu.Lock()
			unchanged := r.gen == g && r.state == Ready
			r.mu.Unlock()
			if !unchanged || r.Stats().Reconnects != 0 {
				t.Fatal("consume channel failure replaced the shared connection")
			}
		})
	}
}

func TestIntegrationStopDuringSubscriptionBackoffRetainsRequest(t *testing.T) {
	r := integrationRuntime(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var err error
	r.metrics, err = newRuntimeMetrics(provider, r.cfg.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := r.Register(Subscription{Identity: "paused", Kind: Request, Operations: []string{"work"},
		Concurrency: 1, Prefetch: 1, Handler: func(context.Context, Message) (*Message, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	m := testMessage(Request, "work")
	m.ReplyTo = r.ResultTarget("absent")
	id, err := r.Publish(context.Background(), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.Stats().Unroutable > 0 })
	waitFor(t, func() bool {
		m, ok := collectMetrics(t, reader)["async.subscription.recovering"]
		return ok && m.Data.(metricdata.Sum[int64]).DataPoints[0].Value == 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("backoff blocked shutdown: %v", err)
	}
	if collectMetrics(t, reader)["async.subscription.recovering"].Data.(metricdata.Sum[int64]).DataPoints[0].Value != 0 {
		t.Fatal("subscription recovering gauge leaked after stop")
	}
	conn, err := amqp.Dial(r.cfg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	var d amqp.Delivery
	waitFor(t, func() bool {
		var ok bool
		d, ok, err = ch.Get(queue, true)
		return err == nil && ok
	})
	decoded, err := protocol.Decode(d)
	if err != nil || decoded.ID != id || decoded.Attempt != 0 {
		t.Fatalf("request identity or retry budget changed: %+v %v", decoded, err)
	}
}

func TestIntegrationConcurrencyBoundAcrossSubscriptionRecovery(t *testing.T) {
	r := integrationRuntime(t)
	var calls, active, peak atomic.Int32
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	_, err := r.Register(Subscription{Identity: "slow-local", Kind: Event, Operations: []string{"work"},
		Concurrency: 1, Prefetch: 1, Handler: func(context.Context, Message) (*Message, error) {
			calls.Add(1)
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			<-release // Intentionally ignores cancellation across consume epochs.
			return nil, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	if _, err := r.Publish(context.Background(), testMessage(Event, "work"), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	r.mu.Lock()
	g := r.gen
	r.mu.Unlock()
	if err := g.consumers[0].first.ch.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // The first local restart has a 1s backoff.
	if calls.Load() != 1 {
		t.Fatal("new consume epoch bypassed the shared concurrency limit")
	}
	close(release)
	waitFor(t, func() bool { return calls.Load() >= 2 })
	if peak.Load() != 1 || r.Stats().Reconnects != 0 {
		t.Fatalf("local concurrency/recovery changed: peak=%d stats=%+v", peak.Load(), r.Stats())
	}
}
