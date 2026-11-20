package async

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/lwbio/async"

type runtimeMetrics struct {
	publishes, deliveries, handlers, retries, deadLetters, recoveryAttempts          metric.Int64Counter
	publishDuration, publishWait, processDuration, handlerDuration, recoveryDuration metric.Float64Histogram
	inFlight                                                                         metric.Int64UpDownCounter
	namespace                                                                        string
	subscriptionRecoveryAttempts                                                     metric.Int64Counter
	subscriptionRecoveryDuration                                                     metric.Float64Histogram
	subscriptionRecovering                                                           metric.Int64UpDownCounter
}

func newRuntimeMetrics(provider metric.MeterProvider, namespace string) (*runtimeMetrics, error) {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}
	meter := provider.Meter(instrumentationName)
	m := &runtimeMetrics{namespace: namespace}
	var errs []error
	counter := func(name, description string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(description), metric.WithUnit("{message}"))
		errs = append(errs, err)
		return c
	}
	duration := func(name, description string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithDescription(description), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5, 10, 30))
		errs = append(errs, err)
		return h
	}
	m.publishes = counter("async.publish.count", "Publish attempts, including API rejections, results and transfers")
	m.deliveries = counter("async.process.count", "Delivery processing attempts, including redeliveries")
	m.handlers = counter("async.handler.count", "Business handler invocations")
	m.retries = counter("async.retry.count", "Confirmed transfers to retry queues")
	m.deadLetters = counter("async.dead_letter.count", "Confirmed transfers to dead-letter queues")
	var err error
	m.recoveryAttempts, err = meter.Int64Counter("async.recovery.attempts", metric.WithUnit("{attempt}"))
	errs = append(errs, err)
	m.publishDuration = duration("async.publish.duration", "Publish latency including lane wait and broker confirmation")
	m.publishWait = duration("async.publish.wait.duration", "Time waiting for the serial publish lane, including canceled waits")
	m.processDuration = duration("async.process.duration", "Delivery latency including handler, transfers and acknowledgment")
	m.handlerDuration = duration("async.handler.duration", "Business handler latency")
	m.recoveryDuration = duration("async.recovery.duration", "Time in recovery, including backoff and topology restoration")
	m.inFlight, err = meter.Int64UpDownCounter("async.process.in_flight", metric.WithUnit("{message}"))
	errs = append(errs, err)
	m.subscriptionRecoveryAttempts, err = meter.Int64Counter("async.subscription.recovery.attempts", metric.WithUnit("{attempt}"))
	errs = append(errs, err)
	m.subscriptionRecoveryDuration = duration("async.subscription.recovery.duration", "Consume channel recovery duration including backoff")
	m.subscriptionRecovering, err = meter.Int64UpDownCounter("async.subscription.recovering", metric.WithUnit("{subscription}"))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("create async metrics: %w", err)
	}
	return m, nil
}

func (m *runtimeMetrics) attrs(kind, subscription string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("async.namespace", m.namespace)}
	if kind != "" {
		switch kind {
		case string(Request), string(Result), string(Event):
		default:
			kind = "unknown"
		}
		attrs = append(attrs, attribute.String("async.kind", kind))
	}
	if subscription != "" {
		attrs = append(attrs, attribute.String("async.subscription", subscription))
	}
	return attrs
}

func publishOutcome(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrUnknown):
		return "unknown"
	case errors.Is(err, ErrUnroutable):
		return "unroutable"
	case errors.Is(err, ErrNack):
		return "nack"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "error"
	}
}

func (m *runtimeMetrics) recordPublish(ctx context.Context, kind string, start time.Time, err error) {
	attrs := append(m.attrs(kind, ""), attribute.String("async.outcome", publishOutcome(err)))
	opt := metric.WithAttributes(attrs...)
	m.publishes.Add(ctx, 1, opt)
	m.publishDuration.Record(ctx, time.Since(start).Seconds(), opt)
}
