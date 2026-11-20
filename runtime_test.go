package async

import (
	"context"
	"testing"
	"time"

	"github.com/lwbio/async/internal/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	r, err := New(Config{URL: "amqp://guest:guest@localhost:5672/", Namespace: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestLifecycleBeforeStart(t *testing.T) {
	r := newTestRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if r.State() != Closed {
		t.Fatal(r.State())
	}
	if err := r.Start(ctx); err == nil {
		t.Fatal("restarted closed runtime")
	}
}

func TestSubscriptionValidationAndOptionOwnership(t *testing.T) {
	r := newTestRuntime(t)
	h := func(context.Context, Message) (*Message, error) { return nil, nil }
	delays := []time.Duration{time.Second}
	_, err := r.Register(Subscription{Kind: Event, Operations: []string{"a.b"}, Handler: h, Retries: delays})
	if err != nil {
		t.Fatal(err)
	}
	delays[0] = 0
	if r.regs[0].Retries[0] != time.Second {
		t.Fatal("caller can mutate retry configuration")
	}
	if _, err = r.Register(Subscription{Kind: Event, Operations: []string{"a.b"}, Handler: h}); err == nil {
		t.Fatal("accepted duplicate identity")
	}
	if _, err = r.Register(Subscription{Kind: Event, Identity: "other", Operations: []string{"a"}, Handler: h, Concurrency: 10, Prefetch: 2}); err == nil {
		t.Fatal("invalid prefetch")
	}
}

func TestPanicIsRetryable(t *testing.T) {
	_, err := invoke(context.Background(), func(context.Context, Message) (*Message, error) { panic("private details") }, Message{})
	if err == nil || err.Error() != "handler panic" {
		t.Fatalf("panic not safely recovered: %v", err)
	}
}

func TestPublishConcurrencyConfiguration(t *testing.T) {
	for _, n := range []int{-1, 33} {
		if _, err := New(Config{URL: "amqp://localhost", Namespace: "test", PublishConcurrency: n}); err == nil {
			t.Fatalf("accepted invalid publisher count %d", n)
		}
	}
	r := newTestRuntime(t)
	defer r.Stop(context.Background())
	if r.cfg.PublishConcurrency != 1 {
		t.Fatal("default publisher concurrency changed")
	}
}

func TestDeadLetterMetadataDoesNotMutateOriginalHeaders(t *testing.T) {
	headers := amqp.Table{"business": "preserve", protocol.DeadCodeHeader: "stale"}
	p := deadLetterPublishing(amqp.Publishing{Headers: headers, Body: []byte("private")},
		registration{Subscription: Subscription{Identity: "worker"}}, "invalid_protocol", "")
	if p.Headers[protocol.DeadReasonHeader] != "invalid_protocol" || p.Headers[protocol.DeadSubscriptionHeader] != "worker" {
		t.Fatalf("missing failure context: %+v", p.Headers)
	}
	if _, err := time.Parse(time.RFC3339Nano, p.Headers[protocol.DeadAtHeader].(string)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Headers[protocol.DeadCodeHeader]; ok || headers[protocol.DeadCodeHeader] != "stale" {
		t.Fatal("retained stale code or mutated source headers")
	}
	if _, ok := headers[protocol.DeadReasonHeader]; ok {
		t.Fatal("mutated source headers")
	}
}
