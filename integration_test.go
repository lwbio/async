package async

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	"github.com/lwbio/async/internal/transport"
	amqp "github.com/rabbitmq/amqp091-go"
)

func integrationRuntime(t *testing.T) *Runtime {
	t.Helper()
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("set ASYNC_TEST_AMQP_URL for real RabbitMQ integration tests")
	}
	r, err := New(Config{URL: url, Namespace: "test_" + uuid.NewString(), PublishTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
	})
	return r
}

func TestIntegrationMissingResultTargetRecovers(t *testing.T) {
	r := integrationRuntime(t)
	var calls atomic.Int32
	_, err := r.Register(Subscription{Kind: Request, Operations: []string{"echo"}, Handler: func(context.Context, Message) (*Message, error) {
		calls.Add(1)
		return &Message{Body: []byte(`"ok"`)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	m := testMessage(Request, "echo")
	m.ReplyTo = r.ResultTarget("late")
	id, err := r.Publish(context.Background(), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.Stats().Unroutable > 0 })
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
	if err = topology.Declare(ch, topology.Subscription{Queue: m.ReplyTo}); err != nil {
		t.Fatal(err)
	}
	var result amqp.Delivery
	waitFor(t, func() bool { var ok bool; result, ok, err = ch.Get(m.ReplyTo, true); return err == nil && ok })
	if result.CorrelationId != id || result.MessageId != protocol.ResultID(id) || calls.Load() < 2 {
		t.Fatalf("request not recovered: calls=%d result=%+v", calls.Load(), result)
	}
}

func TestIntegrationRetryTransferRetainedWithoutTarget(t *testing.T) {
	r := integrationRuntime(t)
	s, err := transport.Dial(context.Background(), r.cfg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ch, err := s.Conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	queue := r.cfg.Namespace + ".transfer"
	top := topology.Subscription{Queue: queue, Retries: []time.Duration{100 * time.Millisecond}}
	if err = topology.Declare(ch, top); err != nil {
		t.Fatal(err)
	}
	if _, err = ch.QueueDelete(queue, false, false, false); err != nil {
		t.Fatal(err)
	}
	m := testMessage(Event, "transfer")
	m.ID = uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = s.Publish(ctx, "", topology.RetryQueue(queue, 1), true, m.Publishing()); err != nil {
		t.Fatal(err)
	}
	// Let the retry TTL expire while there is no destination queue.
	time.Sleep(time.Second)
	if err = topology.Declare(ch, top); err != nil {
		t.Fatal(err)
	}
	// RabbitMQ 4.2 defaults to a 180s DLX confirmation retry interval.
	// Dedicated CI brokers shorten that interval; production defaults also pass.
	deadline := time.Now().Add(200 * time.Second)
	for time.Now().Before(deadline) {
		d, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if d.MessageId != m.ID {
				t.Fatal("message identity changed")
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("retained retry was not delivered within the broker recovery deadline")
}

func TestIntegrationSharedAndIndependentSubscriptions(t *testing.T) {
	a := integrationRuntime(t)
	b := integrationRuntime(t)
	b.cfg.Namespace = a.cfg.Namespace
	var shared, independent atomic.Int32
	h := func(context.Context, Message) (*Message, error) { shared.Add(1); return nil, nil }
	for _, r := range []*Runtime{a, b} {
		if _, err := r.Register(Subscription{Kind: Event, Identity: "shared", Operations: []string{"orders.#"}, Handler: h}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Register(Subscription{Kind: Event, Identity: "independent", Operations: []string{"orders.created"}, Handler: func(context.Context, Message) (*Message, error) { independent.Add(1); return nil, nil }}); err != nil {
		t.Fatal(err)
	}
	startTest(t, a)
	startTest(t, b)
	for i := 0; i < 12; i++ {
		if _, err := a.Publish(context.Background(), testMessage(Event, "orders.created"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return shared.Load() == 12 && independent.Load() == 12 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if shared.Load() != 12 || independent.Load() != 12 {
		t.Fatalf("unexpected fanout: shared=%d independent=%d", shared.Load(), independent.Load())
	}
}

func TestIntegrationLostConfirmIsUnknown(t *testing.T) {
	testIntegrationLostConfirm(t, 1)
}

func TestIntegrationLostConfirmPoolIsUnknown(t *testing.T) {
	testIntegrationLostConfirm(t, 4)
}

func testIntegrationLostConfirm(t *testing.T, publishers int) {
	r := integrationRuntime(t)
	r.cfg.PublishConcurrency = publishers
	upstream, err := url.Parse(r.cfg.URL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var drop atomic.Bool
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", upstream.Host)
			if err != nil {
				client.Close()
				return
			}
			go func() { defer client.Close(); defer server.Close(); _, _ = io.Copy(server, client) }()
			go func() {
				defer client.Close()
				defer server.Close()
				buffer := make([]byte, 65536)
				for {
					n, err := server.Read(buffer)
					if n > 0 && !drop.Load() {
						if _, writeErr := client.Write(buffer[:n]); writeErr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	proxy := *upstream
	proxy.Host = listener.Addr().String()
	r.cfg.URL = proxy.String()
	_, err = r.Register(Subscription{Kind: Event, Identity: "sink", Operations: []string{"work"}, Handler: func(context.Context, Message) (*Message, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	drop.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = r.Publish(ctx, testMessage(Event, "work"), 0)
	if !errors.Is(err, ErrUnknown) {
		t.Fatalf("lost confirmation must be unknown, got %v", err)
	}
	drop.Store(false)
}

func TestIntegrationConcurrencyBoundAcrossRecovery(t *testing.T) {
	r := integrationRuntime(t)
	var calls, active, max atomic.Int32
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	_, err := r.Register(Subscription{Kind: Event, Identity: "bounded", Operations: []string{"work"}, Concurrency: 1, Prefetch: 1, Handler: func(context.Context, Message) (*Message, error) {
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := max.Load(); n > old; old = max.Load() {
			if max.CompareAndSwap(old, n) {
				break
			}
		}
		<-release // Deliberately simulate a handler that is slow to observe cancellation.
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	if _, err = r.Publish(context.Background(), testMessage(Event, "work"), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	r.mu.Lock()
	old := r.gen
	r.mu.Unlock()
	old.session.Close()
	waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.state == Ready && r.gen != old })
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("new generation exceeded existing handler limit")
	}
	close(release)
	waitFor(t, func() bool { return calls.Load() >= 2 })
	if max.Load() != 1 {
		t.Fatalf("maximum concurrency = %d", max.Load())
	}
}

func startTest(t *testing.T, r *Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
}
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
func testMessage(kind Kind, operation string) Message {
	return Message{Kind: kind, Operation: operation, ContentType: "application/json", Body: []byte(`{"ok":true}`)}
}

func TestIntegrationRetryResultAndDeadLetter(t *testing.T) {
	r := integrationRuntime(t)
	results := make(chan Message, 4)
	target, err := r.Register(Subscription{Kind: Result, Identity: "results", Handler: func(_ context.Context, m Message) (*Message, error) { results <- m; return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	_, err = r.Register(Subscription{Kind: Request, Operations: []string{"orders.Create"}, Retries: []time.Duration{100 * time.Millisecond, 100 * time.Millisecond}, Handler: func(ctx context.Context, m Message) (*Message, error) {
		metadata, ok := MessageFromContext(ctx)
		if !ok || metadata.ID != m.ID {
			return nil, Permanent("METADATA", "missing identity")
		}
		calls.Add(1)
		if m.Attempt < 2 {
			return nil, errors.New("temporary")
		}
		return &Message{Body: []byte(`{"created":true}`)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	dead, err := r.Register(Subscription{Kind: Request, Operations: []string{"orders.Reject"}, Handler: func(context.Context, Message) (*Message, error) { return nil, Permanent("REJECTED", "not allowed") }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	m := testMessage(Request, "orders.Create")
	m.ReplyTo = target
	id, err := r.Publish(context.Background(), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-results:
		if result.CorrelationID != id || result.ID != protocol.ResultID(id) || result.ErrorCode != "" {
			t.Fatalf("bad result %+v", result)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no retry result")
	}
	if calls.Load() != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
	m = testMessage(Request, "orders.Reject")
	m.ReplyTo = target
	_, err = r.Publish(context.Background(), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-results:
		if result.ErrorCode != "REJECTED" {
			t.Fatalf("wrong failure %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no failure result")
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
	waitFor(t, func() bool {
		d, ok, e := ch.Get(topology.DeadQueue(dead), true)
		return e == nil && ok && d.MessageId != ""
	})
}

func TestIntegrationUnroutableAndRepeatedRecovery(t *testing.T) {
	r := integrationRuntime(t)
	var received sync.Map
	_, err := r.Register(Subscription{Kind: Event, Identity: "consumer", Operations: []string{"orders.created"}, Handler: func(_ context.Context, m Message) (*Message, error) { received.Store(m.ID, true); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	_, err = r.Publish(context.Background(), testMessage(Request, "missing"), 0)
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("expected unroutable, got %v", err)
	}
	for i := 1; i <= 3; i++ {
		r.mu.Lock()
		old := r.gen
		r.mu.Unlock()
		old.session.Close()
		waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.state == Ready && r.gen != old })
		id, err := r.Publish(context.Background(), testMessage(Event, "orders.created"), 0)
		if err != nil {
			t.Fatal(err)
		}
		// Disconnecting between handling and ACK can legitimately redeliver older IDs.
		waitFor(t, func() bool { _, ok := received.Load(id); return ok })
	}
}

func TestIntegrationGracefulDrainAndDeadline(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprint(expire), func(t *testing.T) {
			r := integrationRuntime(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			exited := make(chan struct{})
			_, err := r.Register(Subscription{Kind: Event, Identity: "slow", Operations: []string{"work"}, Concurrency: 1, Prefetch: 1, Handler: func(ctx context.Context, _ Message) (*Message, error) {
				close(entered)
				defer close(exited)
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			startTest(t, r)
			if _, err = r.Publish(context.Background(), testMessage(Event, "work"), 0); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("not entered")
			}
			duration := 3 * time.Second
			if expire {
				duration = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			stopped := make(chan error, 1)
			go func() { stopped <- r.Stop(ctx) }()
			if !expire {
				select {
				case err := <-stopped:
					t.Fatalf("returned before drain: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				close(release)
			}
			err = <-stopped
			if expire && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want deadline, got %v", err)
			}
			if !expire && err != nil {
				t.Fatal(err)
			}
			select {
			case <-exited:
			case <-time.After(3 * time.Second):
				t.Fatal("handler not cancelled")
			}
		})
	}
}
