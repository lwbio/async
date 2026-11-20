package maintenance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	"github.com/lwbio/async/internal/transport"
	amqp "github.com/rabbitmq/amqp091-go"
)

func options() Options {
	return Options{Namespace: "test", Identity: "worker", Kind: protocol.Event, Limit: 20, Rate: 1000}
}

func TestUnsafeOptionsRejected(t *testing.T) {
	for _, mutate := range []func(*Options){
		func(o *Options) { o.Namespace = "../other" },
		func(o *Options) { o.Identity = "" },
		func(o *Options) { o.Kind = "unknown" },
		func(o *Options) { o.Limit = 0 },
		func(o *Options) { o.Limit = 1001 },
		func(o *Options) { o.Rate = math.NaN() },
		func(o *Options) { o.Rate = math.Inf(1) },
		func(o *Options) { o.Rate = math.SmallestNonzeroFloat64 },
		func(o *Options) { o.Rate = 0 },
		func(o *Options) { o.Apply = true },
	} {
		o := options()
		mutate(&o)
		if o.validate(nil) == nil {
			t.Fatalf("accepted unsafe options: %+v", o)
		}
	}
}

func delivery(m protocol.Message) amqp.Delivery {
	p := m.Publishing()
	return amqp.Delivery{MessageId: p.MessageId, CorrelationId: p.CorrelationId, ReplyTo: p.ReplyTo,
		Type: p.Type, ContentType: p.ContentType, Headers: p.Headers, Body: p.Body}
}

func TestReplayPreservesIdentityAndRequiresExplicitRetryReset(t *testing.T) {
	m := protocol.Message{ID: "stable", CorrelationID: "", Kind: protocol.Event, Operation: "orders.created",
		Attempt: 3, ContentType: "application/json", Body: []byte(`{"private":true}`),
		Headers: amqp.Table{protocol.DeadReasonHeader: "retries_exhausted", "x-death": "broker-history", "business": "preserved"}}
	for _, reset := range []bool{false, true} {
		o := options()
		o.ResetRetries = reset
		p, err := prepare(delivery(m), o, "replay-id", "queue.dead")
		if err != nil {
			t.Fatal(err)
		}
		wantAttempt := int32(3)
		if reset {
			wantAttempt = 0
		}
		if p.MessageId != m.ID || !bytes.Equal(p.Body, m.Body) || p.Headers["async-attempt"] != wantAttempt ||
			p.Headers[protocol.ReplayIDHeader] != "replay-id" || p.Headers["business"] != "preserved" {
			t.Fatalf("replay changed protocol: %+v", p)
		}
		if _, ok := p.Headers[protocol.DeadReasonHeader]; ok {
			t.Fatal("retained obsolete dead-letter reason")
		}
		if m.Headers[protocol.DeadReasonHeader] != "retries_exhausted" {
			t.Fatal("mutated source headers")
		}
	}
}

type failedAudit struct{}

func (failedAudit) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type phaseAudit struct {
	bytes.Buffer
	failAt int
	writes int
}

func (a *phaseAudit) Write(p []byte) (int, error) {
	a.writes++
	if a.writes == a.failAt {
		return 0, io.ErrClosedPipe
	}
	return a.Buffer.Write(p)
}

func waitQueueMessages(t *testing.T, ch *amqp.Channel, queue string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, err := ch.QueueInspect(queue)
		if err != nil {
			t.Fatal(err)
		}
		if state.Messages == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %s: wanted %d ready messages, got %+v", queue, want, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIntegrationDryRunAndConfirmedQueueScopedReplay(t *testing.T) {
	for _, auditFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "audit_failure"}[auditFails], func(t *testing.T) {
			url := os.Getenv("ASYNC_TEST_AMQP_URL")
			if url == "" {
				t.Skip("requires disposable RabbitMQ")
			}
			o := options()
			o.Namespace = "maint_" + uuid.NewString()
			target := protocol.Queue(o.Namespace, string(o.Kind), o.Identity)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s, err := transport.Dial(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ch, err := s.Conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			defer ch.Close()
			if err := topology.Declare(ch, topology.Subscription{Queue: target}); err != nil {
				t.Fatal(err)
			}
			defer ch.QueueDelete(target, false, false, false)
			defer ch.QueueDelete(topology.DeadQueue(target), false, false, false)
			m := protocol.Message{ID: "stable-id", Kind: o.Kind, Operation: "orders.created", Attempt: 3,
				ContentType: "application/json", Body: []byte(`{"secret":"must-not-appear-in-audit"}`)}
			if err := s.Publish(ctx, "", topology.DeadQueue(target), true, m.Publishing()); err != nil {
				t.Fatal(err)
			}
			report, err := Run(ctx, url, o, nil)
			if err != nil || report.Moved != 0 || len(report.Samples) != 1 || report.Samples[0].ID != m.ID {
				t.Fatalf("dry-run: %+v %v", report, err)
			}
			// Quorum requeue on channel close is asynchronous relative to
			// a passive declare on a different connection.
			waitQueueMessages(t, ch, topology.DeadQueue(target), 1)
			o.Apply, o.ResetRetries, o.Actor = true, true, "test-operator"
			var audit bytes.Buffer
			var writer io.Writer = &audit
			if auditFails {
				writer = failedAudit{}
			}
			report, err = Run(ctx, url, o, writer)
			if auditFails {
				if err == nil {
					t.Fatal("ignored audit failure")
				}
				q, _ := ch.QueueInspect(target)
				if q.Messages != 0 {
					t.Fatal("mutated queues after failed audit")
				}
				waitQueueMessages(t, ch, topology.DeadQueue(target), 1)
				return
			}
			if err != nil || report.Moved != 1 {
				t.Fatalf("replay: %+v %v", report, err)
			}
			d, ok, err := ch.Get(target, true)
			if err != nil || !ok || d.MessageId != m.ID {
				t.Fatalf("replayed delivery: %+v %v", d, err)
			}
			got, err := protocol.Decode(d)
			if err != nil || got.Attempt != 0 || stringHeader(d.Headers, protocol.ReplayIDHeader) != report.ReplayID {
				t.Fatalf("replay metadata: %+v %v", got, err)
			}
			if strings.Contains(audit.String(), "must-not-appear") {
				t.Fatal("audit leaked payload")
			}
			decoder := json.NewDecoder(&audit)
			for _, outcome := range []string{"intent", "confirmed", "moved"} {
				var event Audit
				if err := decoder.Decode(&event); err != nil || event.Outcome != outcome {
					t.Fatalf("audit phase: %+v %v", event, err)
				}
			}
		})
	}
}

func TestMalformedMessagesCannotBeReplayed(t *testing.T) {
	_, err := prepare(amqp.Delivery{}, options(), "id", "source")
	if err == nil || errors.Is(err, transport.ErrUnknown) {
		t.Fatal("malformed replay was accepted")
	}
}

type failedSyncAudit struct{ bytes.Buffer }

func (*failedSyncAudit) Sync() error { return io.ErrClosedPipe }

func TestAuditSyncFailureIsNotIgnored(t *testing.T) {
	if err := record(&failedSyncAudit{}, Audit{Actor: "operator"}, "intent"); err == nil {
		t.Fatal("ignored durable audit sync failure")
	}
}

type shortAudit struct{}

func (shortAudit) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestAuditShortWriteIsNotIgnored(t *testing.T) {
	if err := record(shortAudit{}, Audit{Actor: "operator"}, "intent"); err == nil {
		t.Fatal("ignored short audit write")
	}
}

func TestIntegrationAuditFailureAfterPublishRetainsSourceForRecovery(t *testing.T) {
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires disposable RabbitMQ")
	}
	o := options()
	o.Namespace = "maint_" + uuid.NewString()
	o.Apply, o.Actor = true, "test-operator"
	target := protocol.Queue(o.Namespace, string(o.Kind), o.Identity)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := transport.Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ch, err := s.Conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := topology.Declare(ch, topology.Subscription{Queue: target}); err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(target, false, false, false)
	defer ch.QueueDelete(topology.DeadQueue(target), false, false, false)
	m := protocol.Message{ID: "same-id", Kind: o.Kind, Operation: "orders.created", ContentType: "application/json", Body: []byte("{}")}
	if err := s.Publish(ctx, "", topology.DeadQueue(target), true, m.Publishing()); err != nil {
		t.Fatal(err)
	}
	audit := &phaseAudit{failAt: 2} // Publication confirmed, durable audit not yet recorded.
	report, err := Run(ctx, url, o, audit)
	if err == nil || report.Moved != 0 {
		t.Fatalf("ignored pre-ACK audit failure: %+v %v", report, err)
	}
	waitQueueMessages(t, ch, target, 1)
	waitQueueMessages(t, ch, topology.DeadQueue(target), 1)
	// The possible duplicate is deliberate: never discard the source after
	// losing audit/confirmation evidence, and always retain stable identity.
	for _, queue := range []string{target, topology.DeadQueue(target)} {
		d, ok, err := ch.Get(queue, true)
		if err != nil || !ok || d.MessageId != m.ID {
			t.Fatalf("identity/recovery lost on %s: %+v %v", queue, d, err)
		}
	}
}
