package async

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric"
)

var errConsumerStopped = errors.New("async: consumer stopped")

// Each subscription owns a recoverable consume channel. Publishing ambiguity
// still invalidates the transport session; definite local failures do not.
type subscriptionConsumer struct {
	r     *Runtime
	g     *generation
	reg   registration
	ctx   context.Context
	first *consumerEpoch
	drain chan struct{}
	once  sync.Once
}

type consumerEpoch struct {
	ch         *amqp.Channel
	ctx        context.Context
	cancel     context.CancelFunc
	deliveries <-chan amqp.Delivery
	closed     <-chan *amqp.Error
	canceled   <-chan string
	failed     chan error
	progress   chan struct{}
	done       chan struct{}
}

func (r *Runtime) openConsumer(ctx context.Context, g *generation, reg registration, restore bool) (*consumerEpoch, error) {
	ch, err := g.session.Conn.Channel()
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = ch.Close()
		}
	}()
	if restore {
		if err := r.declareSubscription(ch, reg); err != nil {
			return nil, err
		}
	}
	if err := ch.Qos(reg.Prefetch, 0, false); err != nil {
		return nil, err
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	canceled := ch.NotifyCancel(make(chan string, 1))
	deliveries, err := ch.Consume(reg.queue, "async", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	ok = true
	return &consumerEpoch{ch: ch, ctx: ctx, cancel: cancel, deliveries: deliveries, closed: closed,
		canceled: canceled, failed: make(chan error, 1), progress: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func (c *subscriptionConsumer) stopAccepting() { c.once.Do(func() { close(c.drain) }) }

func (e *consumerEpoch) close() {
	e.cancel()
	// Closing only this consume channel requeues its unacked deliveries.
	_ = e.ch.Close()
}

func (c *subscriptionConsumer) start(e *consumerEpoch) {
	var workers sync.WaitGroup
	for i := 0; i < c.reg.Concurrency; i++ {
		workers.Add(1)
		c.r.workers.Add(1)
		go func() {
			defer workers.Done()
			c.r.work(e.ctx, c.g, c.reg, e)
		}()
	}
	go func() { workers.Wait(); close(e.done) }()
}

func (c *subscriptionConsumer) run() {
	defer c.r.workers.Done()
	e := c.first
	backoff := time.Second
	for {
		c.start(e)
		err := c.wait(e, &backoff)
		e.close()
		if c.stopping() {
			return
		}
		c.r.mu.Lock()
		c.r.lastErr = err
		c.r.mu.Unlock()
		c.r.cfg.Logger.Warn("async subscription recovering", "namespace", c.r.cfg.Namespace,
			"subscription", c.reg.Identity, "kind", c.reg.Kind, "outcome", publishOutcome(err))
		attrs := metric.WithAttributes(c.r.metrics.attrs(string(c.reg.Kind), c.reg.Identity)...)
		start := time.Now()
		c.r.metrics.subscriptionRecovering.Add(c.ctx, 1, attrs)
		var next *consumerEpoch
		for !c.stopping() {
			timer := time.NewTimer(backoff)
			select {
			case <-c.ctx.Done():
				timer.Stop()
			case <-c.r.stop:
				timer.Stop()
			case <-c.drain:
				timer.Stop()
			case <-timer.C:
				var openErr error
				next, openErr = c.r.openConsumer(c.ctx, c.g, c.reg, true)
				c.r.metrics.subscriptionRecoveryAttempts.Add(c.ctx, 1, attrs)
				if openErr != nil {
					c.r.mu.Lock()
					c.r.lastErr = openErr
					c.r.mu.Unlock()
				}
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			if next != nil {
				break
			}
		}
		c.r.metrics.subscriptionRecovering.Add(c.ctx, -1, attrs)
		c.r.metrics.subscriptionRecoveryDuration.Record(c.ctx, time.Since(start).Seconds(), attrs)
		if next == nil {
			return
		}
		if c.stopping() {
			next.close()
			return
		}
		e = next
	}
}

func (c *subscriptionConsumer) stopping() bool {
	select {
	case <-c.ctx.Done():
		return true
	case <-c.drain:
		return true
	case <-c.r.stop:
		return true
	default:
		return false
	}
}

func (c *subscriptionConsumer) wait(e *consumerEpoch, backoff *time.Duration) error {
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-c.drain:
			if err := e.ch.Cancel("async", false); err != nil {
				return err
			}
			// Do not cancel successful handlers while draining; a failed
			// transfer still exits and leaves its original message unacked.
			select {
			case <-e.done:
				return nil
			case err := <-e.failed:
				return err
			case reason := <-e.closed:
				if reason != nil {
					return reason
				}
				return errConsumerStopped
			case <-c.ctx.Done():
				return c.ctx.Err()
			}
		case err := <-e.failed:
			return err
		case reason := <-e.closed:
			if reason != nil {
				return reason
			}
			return errConsumerStopped
		case <-e.canceled:
			return errConsumerStopped
		case <-e.done:
			return errConsumerStopped
		case <-e.progress:
			*backoff = time.Second
		}
	}
}
