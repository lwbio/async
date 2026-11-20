package async

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func metricRuntime(t *testing.T) (*Runtime, *sdkmetric.ManualReader, *bytes.Buffer) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var logs bytes.Buffer
	r, err := New(Config{URL: "amqp://localhost", Namespace: "metrics", MeterProvider: provider,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	return r, reader, &logs
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	result := make(map[string]metricdata.Metrics)
	for _, scope := range data.ScopeMetrics {
		if scope.Scope.Name != instrumentationName {
			t.Fatalf("unexpected scope: %s", scope.Scope.Name)
		}
		for _, m := range scope.Metrics {
			result[m.Name] = m
		}
	}
	return result
}

func TestPublishMetricsOutcomes(t *testing.T) {
	r, reader, _ := metricRuntime(t)
	errs := []error{nil, ErrUnknown, ErrUnroutable, ErrNack, ErrUnavailable, context.Canceled, context.DeadlineExceeded, errors.New("secret")}
	for _, err := range errs {
		r.metrics.recordPublish(context.Background(), string(Request), time.Now().Add(-time.Millisecond), err)
	}
	data := collectMetrics(t, reader)
	counts := data["async.publish.count"].Data.(metricdata.Sum[int64])
	if len(counts.DataPoints) != len(errs) {
		t.Fatalf("outcomes: %+v", counts.DataPoints)
	}
	for _, dp := range counts.DataPoints {
		if dp.Value != 1 || dp.Attributes.Len() != 3 {
			t.Fatalf("unexpected publish point: %+v", dp)
		}
	}
	hist := data["async.publish.duration"].Data.(metricdata.Histogram[float64])
	for _, dp := range hist.DataPoints {
		if dp.Count != 1 || dp.Sum <= 0 {
			t.Fatalf("unexpected duration: %+v", dp)
		}
	}
	attrs := r.metrics.attrs("untrusted-type", "")
	if attrs[1].Value.AsString() != "unknown" {
		t.Fatal("unbounded message kind label")
	}
}

func TestHandlerMetricsAndSafePanicLog(t *testing.T) {
	r, reader, logs := metricRuntime(t)
	handlers := []Handler{
		func(context.Context, Message) (*Message, error) { return nil, nil },
		func(context.Context, Message) (*Message, error) { return nil, errors.New("secret-error") },
		func(context.Context, Message) (*Message, error) { return nil, Permanent("BAD", "secret-failure") },
		func(context.Context, Message) (*Message, error) { panic("secret-panic") },
	}
	for _, h := range handlers {
		r.invokeHandler(context.Background(), registration{Subscription: Subscription{Identity: "worker", Kind: Request, Handler: h}}, Message{ID: "id"})
	}
	data := collectMetrics(t, reader)
	points := data["async.handler.count"].Data.(metricdata.Sum[int64]).DataPoints
	if len(points) != 4 {
		t.Fatalf("missing handler outcomes: %+v", points)
	}
	for _, secret := range []string{"secret-error", "secret-failure", "secret-panic"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log leaked %s", secret)
		}
	}
	if !strings.Contains(logs.String(), "stack") || !strings.Contains(logs.String(), "invoke") {
		t.Fatal("panic log missing stack")
	}
}

type metricAck struct{ err error }

func (a metricAck) Ack(uint64, bool) error        { return a.err }
func (a metricAck) Nack(uint64, bool, bool) error { return a.err }
func (a metricAck) Reject(uint64, bool) error     { return a.err }

func TestProcessMetricsIncludeAckFailureAndBalanceInFlight(t *testing.T) {
	for _, ackErr := range []error{nil, errors.New("ack failed")} {
		t.Run(publishOutcome(ackErr), func(t *testing.T) {
			r, reader, _ := metricRuntime(t)
			spans := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			r.consumer = tracing.NewTracer(trace.SpanKindConsumer, tracing.WithTracerProvider(provider))
			m := Message{ID: "id", Kind: Request, Operation: "work", ContentType: "application/json", Body: []byte("{}")}
			p := m.Publishing()
			d := amqp.Delivery{Acknowledger: metricAck{ackErr}, Headers: p.Headers, MessageId: p.MessageId,
				Type: p.Type, ContentType: p.ContentType, Body: p.Body}
			reg := registration{Subscription: Subscription{Identity: "worker", Kind: Request, Operations: []string{"work"},
				Handler: func(context.Context, Message) (*Message, error) { return nil, nil }}}
			if _, err := protocol.Decode(d); err != nil {
				t.Fatal(err)
			}
			if err := r.process(context.Background(), nil, reg, d); !errors.Is(err, ackErr) {
				t.Fatalf("process error: %v", err)
			}
			data := collectMetrics(t, reader)
			dp := data["async.process.count"].Data.(metricdata.Sum[int64]).DataPoints[0]
			got, _ := dp.Attributes.Value(attribute.Key("async.outcome"))
			if got.AsString() != publishOutcome(ackErr) {
				t.Fatalf("outcome: %v", got)
			}
			inFlight := data["async.process.in_flight"].Data.(metricdata.Sum[int64]).DataPoints[0]
			if inFlight.Value != 0 || r.Stats().InFlight != 0 {
				t.Fatal("in-flight leaked")
			}
			ended := spans.Ended()
			if len(ended) != 1 {
				t.Fatalf("consumer spans: %d", len(ended))
			}
			if (ended[0].Status().Code == codes.Error) != (ackErr != nil) {
				t.Fatalf("ACK failure span status: %+v", ended[0].Status())
			}
		})
	}
}

func TestRecoveryMetrics(t *testing.T) {
	r, reader, _ := metricRuntime(t)
	r.metrics.recoveryAttempts.Add(context.Background(), 1, metric.WithAttributes(r.metrics.attrs("", "")...))
	r.recordRecovery(time.Now().Add(-time.Second), "success")
	data := collectMetrics(t, reader)
	if data["async.recovery.attempts"].Data.(metricdata.Sum[int64]).DataPoints[0].Value != 1 {
		t.Fatal("missing recovery attempt")
	}
	if data["async.recovery.duration"].Data.(metricdata.Histogram[float64]).DataPoints[0].Sum < 1 {
		t.Fatal("missing recovery duration")
	}
}

func TestPublishRejectedBeforeTransportIsMeasured(t *testing.T) {
	r, reader, _ := metricRuntime(t)
	_, err := r.Publish(context.Background(), Message{Kind: Request, Operation: "work", ContentType: "application/json"}, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	dp := collectMetrics(t, reader)["async.publish.count"].Data.(metricdata.Sum[int64]).DataPoints[0]
	outcome, _ := dp.Attributes.Value(attribute.Key("async.outcome"))
	if dp.Value != 1 || outcome.AsString() != "unavailable" {
		t.Fatalf("API rejection not measured: %+v", dp)
	}
}

func TestIntegrationMetricsRetryAndDeadLetter(t *testing.T) {
	r := integrationRuntime(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var err error
	r.metrics, err = newRuntimeMetrics(provider, r.cfg.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Register(Subscription{Identity: "worker", Kind: Request, Operations: []string{"work"},
		Retries: []time.Duration{time.Millisecond}, Handler: func(_ context.Context, m Message) (*Message, error) {
			switch string(m.Body) {
			case "retry":
				if m.Attempt == 0 {
					return nil, errors.New("retry")
				}
			case "dead":
				return nil, Permanent("BAD", "failed")
			}
			return nil, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	for _, body := range []string{"ok", "retry", "dead"} {
		m := testMessage(Request, "work")
		m.Body = []byte(body)
		if _, err := r.Publish(context.Background(), m, 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		stats := r.Stats()
		if stats.Retries != 1 || stats.DeadLetters != 1 || stats.Published != 5 {
			return false
		}
		// An empty in-flight snapshot can occur while the retry is still in
		// its TTL queue; wait for completion rather than stopping in that gap.
		data := collectMetrics(t, reader)
		m, ok := data["async.process.count"]
		if !ok {
			return false
		}
		var count int64
		for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
			count += dp.Value
		}
		return count == 4
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	data := collectMetrics(t, reader)
	for _, name := range []string{"async.retry.count", "async.dead_letter.count"} {
		if data[name].Data.(metricdata.Sum[int64]).DataPoints[0].Value != 1 {
			t.Fatalf("unexpected %s", name)
		}
	}
	var processed int64
	for _, dp := range data["async.process.count"].Data.(metricdata.Sum[int64]).DataPoints {
		processed += dp.Value
	}
	if processed != 4 {
		t.Fatalf("process count: %d", processed)
	}
	if data["async.publish.wait.duration"].Data.(metricdata.Histogram[float64]).DataPoints[0].Count != 5 {
		t.Fatal("missing publish waits")
	}
}
