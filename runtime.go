package async

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	"github.com/lwbio/async/internal/transport"
	"github.com/lwbio/async/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type State string

const (
	Created    State = "created"
	Starting   State = "starting"
	Ready      State = "ready"
	Recovering State = "recovering"
	Draining   State = "draining"
	Closed     State = "closed"
)

type Config struct {
	URL, Namespace string
	EnableDelay    bool
	PublishTimeout time.Duration
	// PublishConcurrency bounds confirm channels; zero selects one.
	PublishConcurrency int
	Tracing            []tracing.Option
	Logger             *slog.Logger
	// MeterProvider defaults to the global OpenTelemetry provider.
	MeterProvider metric.MeterProvider
}

type registration struct {
	Subscription
	queue string
	slots chan struct{}
}
type generation struct {
	session   *transport.Session
	consumers []*subscriptionConsumer
	cancel    context.CancelFunc
}

type Runtime struct {
	cfg                Config
	mu                 sync.Mutex
	state              State
	regs               []registration
	gen                *generation
	ctx                context.Context
	cancel             context.CancelFunc
	stop, done         chan struct{}
	stopOnce           sync.Once
	workers            sync.WaitGroup
	producer, consumer *tracing.Tracer
	lastErr            error
	stats              runtimeStats
	metrics            *runtimeMetrics
}

func New(cfg Config) (*Runtime, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("RabbitMQ URL is required")
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`).MatchString(cfg.Namespace) {
		return nil, fmt.Errorf("namespace must contain 1-64 letters, digits, underscores or hyphens")
	}
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = 10 * time.Second
	}
	if cfg.PublishTimeout < 0 {
		return nil, fmt.Errorf("publish timeout must be positive")
	}
	if cfg.PublishConcurrency == 0 {
		cfg.PublishConcurrency = 1
	}
	if cfg.PublishConcurrency < 1 || cfg.PublishConcurrency > 32 {
		return nil, fmt.Errorf("publish concurrency must be between 1 and 32")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	metrics, err := newRuntimeMetrics(cfg.MeterProvider, cfg.Namespace)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{cfg: cfg, metrics: metrics, state: Created, ctx: ctx, cancel: cancel, stop: make(chan struct{}), done: make(chan struct{}), producer: tracing.NewTracer(trace.SpanKindProducer, cfg.Tracing...), consumer: tracing.NewTracer(trace.SpanKindConsumer, cfg.Tracing...)}, nil
}

func (r *Runtime) State() State     { r.mu.Lock(); defer r.mu.Unlock(); return r.state }
func (r *Runtime) LastError() error { r.mu.Lock(); defer r.mu.Unlock(); return r.lastErr }
func (r *Runtime) ResultTarget(identity string) string {
	return protocol.Queue(r.cfg.Namespace, "result", identity)
}

func (r *Runtime) Register(s Subscription) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Created {
		return "", fmt.Errorf("register subscriptions before Start")
	}
	if s.Handler == nil {
		return "", fmt.Errorf("nil handler")
	}
	if s.Identity == "" {
		var err error
		s.Identity, err = HandlerIdentity(s.Handler)
		if err != nil {
			return "", err
		}
	}
	if s.Kind != Request && s.Kind != Result && s.Kind != Event {
		return "", fmt.Errorf("invalid subscription kind")
	}
	if s.Kind != Result && len(s.Operations) == 0 {
		return "", fmt.Errorf("operations required")
	}
	if s.Kind == Request && len(s.Operations) != 1 {
		return "", fmt.Errorf("request subscription requires one operation")
	}
	for _, op := range s.Operations {
		if op == "" {
			return "", fmt.Errorf("empty operation")
		}
		if s.Kind != Event && strings.ContainsAny(op, "*#") {
			return "", fmt.Errorf("request and result operations must be exact names")
		}
	}
	if s.Concurrency == 0 {
		s.Concurrency = 8
	}
	if s.Prefetch == 0 {
		s.Prefetch = 16
	}
	if s.Concurrency < 1 || s.Prefetch < s.Concurrency {
		return "", fmt.Errorf("prefetch must be >= concurrency > 0")
	}
	if s.Retries == nil {
		s.Retries = []time.Duration{time.Second, 10 * time.Second, 60 * time.Second}
	}
	for _, d := range s.Retries {
		if d < time.Millisecond {
			return "", fmt.Errorf("retry delays must be >= 1ms")
		}
	}
	s.Retries = append([]time.Duration{}, s.Retries...)
	s.Operations = append([]string{}, s.Operations...)
	id := s.Identity
	if s.Kind == Request {
		id = s.Operations[0]
	}
	queue := protocol.Queue(r.cfg.Namespace, string(s.Kind), id)
	for _, existing := range r.regs {
		if existing.queue == queue {
			return "", fmt.Errorf("duplicate subscription %q", s.Identity)
		}
	}
	r.regs = append(r.regs, registration{Subscription: s, queue: queue, slots: make(chan struct{}, s.Concurrency)})
	return queue, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.state != Created {
		r.mu.Unlock()
		return fmt.Errorf("runtime can only start once")
	}
	r.state = Starting
	r.mu.Unlock()
	stop := context.AfterFunc(ctx, r.cancel)
	g, err := r.connect()
	stop()
	r.mu.Lock()
	if err != nil || r.ctx.Err() != nil {
		if err == nil {
			err = ErrClosed
		}
		r.state = Closed
		r.lastErr = err
		r.cancel()
		r.mu.Unlock()
		if g != nil {
			g.cancel()
			g.session.Close()
		}
		close(r.done)
		return err
	}
	r.gen = g
	r.state = Ready
	r.mu.Unlock()
	go r.supervise(g)
	return nil
}

func (r *Runtime) connect() (*generation, error) {
	s, err := transport.DialWithPublishers(r.ctx, r.cfg.URL, r.cfg.PublishConcurrency)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(r.ctx)
	g := &generation{session: s, cancel: cancel}
	stop := context.AfterFunc(ctx, s.Close)
	defer stop()
	ok := false
	defer func() {
		if !ok {
			cancel()
			s.Close()
		}
	}()
	version, _ := s.Conn.Properties["version"].(string)
	var major, minor int
	if _, err := fmt.Sscanf(version, "%d.%d", &major, &minor); err != nil || major < 4 || (major == 4 && minor < 2) {
		return nil, fmt.Errorf("RabbitMQ 4.2 or newer is required (server %q)", version)
	}
	ch, err := s.Conn.Channel()
	if err != nil {
		return nil, err
	}
	if err = ch.ExchangeDeclare(r.cfg.Namespace+".v1.events", "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}
	if r.cfg.EnableDelay {
		if err = ch.ExchangeDeclare(r.cfg.Namespace+".v1.delay", "x-delayed-message", true, false, false, false, amqp.Table{"x-delayed-type": "direct"}); err != nil {
			return nil, err
		}
	}
	for _, reg := range r.regs {
		if err = r.declareSubscription(ch, reg); err != nil {
			return nil, err
		}
	}
	if err = ch.Close(); err != nil {
		return nil, err
	}
	// All declarations must succeed before any consumer starts.
	for _, reg := range r.regs {
		epoch, err := r.openConsumer(ctx, g, reg, false)
		if err != nil {
			return nil, err
		}
		g.consumers = append(g.consumers, &subscriptionConsumer{
			r: r, g: g, reg: reg, ctx: ctx, first: epoch, drain: make(chan struct{}),
		})
	}
	for _, consumer := range g.consumers {
		r.workers.Add(1)
		go consumer.run()
	}
	ok = true
	return g, nil
}

func (r *Runtime) declareSubscription(ch *amqp.Channel, reg registration) error {
	t := topology.Subscription{Queue: reg.queue, Retries: reg.Retries}
	if reg.Kind == Event {
		t.Exchange = r.cfg.Namespace + ".v1.events"
		t.Key = reg.Operations[0]
	}
	if err := topology.Declare(ch, t); err != nil {
		return err
	}
	if reg.Kind == Event {
		for _, op := range reg.Operations[1:] {
			if err := ch.QueueBind(reg.queue, op, t.Exchange, false, nil); err != nil {
				return err
			}
		}
	}
	if reg.Kind == Request && r.cfg.EnableDelay {
		return ch.QueueBind(reg.queue, reg.queue, r.cfg.Namespace+".v1.delay", false, nil)
	}
	return nil
}

func (r *Runtime) supervise(g *generation) {
	defer func() {
		r.cancel()
		r.mu.Lock()
		r.gen = nil
		r.state = Closed
		r.mu.Unlock()
		close(r.done)
	}()
	backoff := time.Second
	for {
		select {
		case <-r.stop:
			r.drain(g)
			return
		case <-r.ctx.Done():
			g.cancel()
			g.session.Close()
			return
		case <-g.session.Done():
		}
		g.cancel()
		g.session.Close()
		r.mu.Lock()
		r.gen = nil
		r.state = Recovering
		r.mu.Unlock()
		r.cfg.Logger.Warn("async connection recovering", "namespace", r.cfg.Namespace)
		recoveryStart := time.Now()
		for {
			timer := time.NewTimer(backoff)
			select {
			case <-r.stop:
				timer.Stop()
				r.recordRecovery(recoveryStart, "stopped")
				r.drain(g)
				return
			case <-r.ctx.Done():
				timer.Stop()
				r.recordRecovery(recoveryStart, "canceled")
				return
			case <-timer.C:
			}
			next, err := r.connect()
			outcome := "success"
			if err != nil {
				outcome = "error"
			}
			r.metrics.recoveryAttempts.Add(r.ctx, 1, metric.WithAttributes(append(r.metrics.attrs("", ""), attribute.String("async.outcome", outcome))...))
			if err != nil {
				r.mu.Lock()
				r.lastErr = err
				r.mu.Unlock()
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				continue
			}
			g = next
			backoff = time.Second
			r.mu.Lock()
			if r.state == Draining || r.ctx.Err() != nil {
				r.mu.Unlock()
				r.recordRecovery(recoveryStart, "stopped")
				r.drain(g)
				return
			}
			r.gen = g
			r.state = Ready
			r.lastErr = nil
			r.stats.reconnects.Add(1)
			r.mu.Unlock()
			r.recordRecovery(recoveryStart, "success")
			break
		}
	}
}

func (r *Runtime) drain(g *generation) {
	r.mu.Lock()
	r.state = Draining
	r.gen = nil
	r.mu.Unlock()
	done := make(chan struct{})
	go func() {
		for _, consumer := range g.consumers {
			consumer.stopAccepting()
		}
		r.workers.Wait()
		g.session.Drain(r.ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-r.ctx.Done():
	}
	g.cancel()
	g.session.Close()
}

func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	if r.state == Created {
		r.state = Closed
		r.cancel()
		close(r.done)
	}
	if r.state == Starting {
		r.cancel()
	}
	r.stopOnce.Do(func() { close(r.stop) })
	if r.state == Ready || r.state == Recovering {
		r.state = Draining
	}
	r.mu.Unlock()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		r.cancel()
		return ctx.Err()
	}
}

func (r *Runtime) Publish(ctx context.Context, m Message, delay time.Duration) (id string, publishErr error) {
	start := time.Now()
	submitted := false
	defer func() {
		if !submitted {
			r.metrics.recordPublish(ctx, string(m.Kind), start, publishErr)
		}
	}()
	if m.ID == "" {
		m.ID = uuid.NewString()
	}
	if err := m.Validate(); err != nil {
		return m.ID, err
	}
	if delay < 0 {
		return m.ID, fmt.Errorf("negative delay")
	}
	if delay > 0 && delay < time.Millisecond {
		return m.ID, fmt.Errorf("delay must be at least 1ms")
	}
	if delay > 0 && (!r.cfg.EnableDelay || m.Kind != Request) {
		return m.ID, fmt.Errorf("delay requires enabled request delay capability")
	}
	r.mu.Lock()
	g := r.gen
	state := r.state
	r.mu.Unlock()
	if state != Ready || g == nil {
		return m.ID, ErrUnavailable
	}
	ex, key := "", protocol.Queue(r.cfg.Namespace, "request", m.Operation)
	if m.Kind == Event {
		ex = r.cfg.Namespace + ".v1.events"
		key = m.Operation
	}
	if m.Kind == Result {
		return m.ID, fmt.Errorf("results are published by request handlers")
	}
	p := m.Publishing()
	if delay > 0 {
		ex = r.cfg.Namespace + ".v1.delay"
		p.Headers["x-delay"] = delay.Milliseconds()
	}
	submitted = true
	return m.ID, r.publish(ctx, g, ex, key, delay == 0, p)
}

func (r *Runtime) publish(ctx context.Context, g *generation, ex, key string, mandatory bool, p amqp.Publishing) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	ctx, span := r.producer.Start(ctx, p.Type+" publish", tracing.NewPublishingCarrier(&p))
	defer span.End()
	err := g.session.PublishObserved(ctx, ex, key, mandatory, p, func(wait time.Duration) {
		r.metrics.publishWait.Record(ctx, wait.Seconds(), metric.WithAttributes(r.metrics.attrs(p.Type, "")...))
	})
	r.metrics.recordPublish(ctx, p.Type, start, err)
	span.SetAttributes(attribute.String("messaging.message.id", p.MessageId), attribute.String("messaging.destination.name", key))
	if err == nil {
		r.stats.published.Add(1)
	}
	if errors.Is(err, ErrUnroutable) {
		r.stats.unroutable.Add(1)
	}
	if errors.Is(err, ErrUnknown) {
		r.stats.unknown.Add(1)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
	}
	return err
}

func (r *Runtime) recordRecovery(start time.Time, outcome string) {
	r.metrics.recoveryDuration.Record(context.Background(), time.Since(start).Seconds(),
		metric.WithAttributes(append(r.metrics.attrs("", ""), attribute.String("async.outcome", outcome))...))
	r.cfg.Logger.Info("async recovery ended", "namespace", r.cfg.Namespace, "outcome", outcome, "duration", time.Since(start))
}
