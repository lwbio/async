// Package maintenance implements bounded, explicit dead-letter inspection and replay.
// It is separate from Runtime: administration never creates queues or starts handlers.
package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	"github.com/lwbio/async/internal/transport"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Options struct {
	Namespace, Identity string
	Kind                protocol.Kind
	Limit               int
	Rate                float64
	Apply, ResetRetries bool
	Reason, Actor       string
}

type Sample struct {
	ID, CorrelationID, Operation string
	Attempt                      int32
	Reason, FailedAt, ErrorCode  string
	Bytes                        int
	Valid                        bool
}

type Report struct {
	Source, Target, ReplayID string
	Ready, Scanned, Moved    int
	Samples                  []Sample
}

// Audit contains metadata only. intent and confirmed are durable before the
// next mutating step when the writer supports Sync (as CLI audit files do).
type Audit struct {
	Time, ReplayID, Actor, Source, Target, MessageID, Outcome string
	AttemptBefore, AttemptAfter                               int32
}

func (o Options) validate(audit io.Writer) error {
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`).MatchString(o.Namespace) || o.Identity == "" {
		return errors.New("valid namespace and subscription identity are required")
	}
	if o.Kind != protocol.Request && o.Kind != protocol.Result && o.Kind != protocol.Event {
		return errors.New("kind must be request, result or event")
	}
	if o.Limit < 1 || o.Limit > 1000 {
		return errors.New("limit must be between 1 and 1000")
	}
	if math.IsNaN(o.Rate) || math.IsInf(o.Rate, 0) || o.Rate <= 0 || o.Rate > 1000 {
		return errors.New("rate must be greater than zero and at most 1000 messages/s")
	}
	if float64(time.Second)/o.Rate >= float64(math.MaxInt64) {
		return errors.New("rate is too small to represent a safe replay interval")
	}
	if o.Apply && (o.Actor == "" || audit == nil) {
		return errors.New("apply requires an actor and an audit writer")
	}
	return nil
}

func stringHeader(h amqp.Table, key string) string {
	s, _ := h[key].(string)
	return s
}

func sample(d amqp.Delivery) Sample {
	m, err := protocol.Decode(d)
	return Sample{ID: d.MessageId, CorrelationID: d.CorrelationId, Operation: m.Operation,
		Attempt: m.Attempt, Reason: stringHeader(d.Headers, protocol.DeadReasonHeader),
		FailedAt: stringHeader(d.Headers, protocol.DeadAtHeader), ErrorCode: stringHeader(d.Headers, protocol.DeadCodeHeader),
		Bytes: len(d.Body), Valid: err == nil}
}

func prepare(d amqp.Delivery, o Options, replayID, source string) (amqp.Publishing, error) {
	m, err := protocol.Decode(d)
	if err != nil || m.Kind != o.Kind {
		return amqp.Publishing{}, errors.New("invalid protocol or message kind; leave source unacknowledged")
	}
	if o.ResetRetries {
		m.Attempt = 0
	}
	p := m.Publishing()
	for _, key := range []string{protocol.DeadReasonHeader, protocol.DeadAtHeader,
		protocol.DeadSubscriptionHeader, protocol.DeadCodeHeader, "x-death", "x-first-death-queue",
		"x-first-death-reason", "x-first-death-exchange", "x-last-death-queue",
		"x-last-death-reason", "x-last-death-exchange"} {
		delete(p.Headers, key)
	}
	p.Headers[protocol.ReplayIDHeader] = replayID
	p.Headers[protocol.ReplayAtHeader] = time.Now().UTC().Format(time.RFC3339Nano)
	p.Headers[protocol.ReplaySourceHeader] = source
	return p, nil
}

func record(writer io.Writer, event Audit, outcome string) error {
	event.Time = time.Now().UTC().Format(time.RFC3339Nano)
	event.Outcome = outcome
	data, err := json.Marshal(event)
	if err != nil {
		return errors.New("encode replay audit failed")
	}
	data = append(data, '\n')
	n, err := writer.Write(data)
	if err != nil || n != len(data) {
		return errors.New("write replay audit failed")
	}
	if durable, ok := writer.(interface{ Sync() error }); ok {
		if err := durable.Sync(); err != nil {
			return errors.New("sync replay audit failed")
		}
	}
	return nil
}

// Run peeks at most Limit deliveries. Unless Apply is explicit, all fetched
// messages remain unacked and are requeued on close (order/redelivery may change).
// Replay uses the original subscription queue, never an event exchange.
func Run(ctx context.Context, url string, o Options, audit io.Writer) (Report, error) {
	var report Report
	if err := o.validate(audit); err != nil {
		return report, err
	}
	report.Target = protocol.Queue(o.Namespace, string(o.Kind), o.Identity)
	report.Source = topology.DeadQueue(report.Target)
	report.ReplayID = uuid.NewString()
	s, err := transport.Dial(ctx, url)
	if err != nil {
		// Never format URL parse errors: they can echo credentials.
		return report, errors.New("could not connect to broker")
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, s.Close)
	defer stop()
	ch, err := s.Conn.Channel()
	if err != nil {
		return report, errors.New("could not open maintenance channel")
	}
	defer ch.Close()
	queue, err := ch.QueueInspect(report.Source)
	if err != nil {
		return report, errors.New("source dead-letter queue is missing or inaccessible")
	}
	report.Ready = queue.Messages
	if o.Apply {
		if _, err := ch.QueueInspect(report.Target); err != nil {
			return report, errors.New("original subscription queue is missing or inaccessible")
		}
	}
	var next time.Time
	for report.Scanned < o.Limit {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if o.Apply && !next.IsZero() {
			timer := time.NewTimer(time.Until(next))
			select {
			case <-ctx.Done():
				timer.Stop()
				return report, ctx.Err()
			case <-timer.C:
			}
		}
		d, ok, err := ch.Get(report.Source, false)
		if err != nil {
			return report, errors.New("fetch dead-letter failed")
		}
		if !ok {
			break
		}
		report.Scanned++
		view := sample(d)
		if o.Reason != "" && view.Reason != o.Reason {
			continue
		}
		report.Samples = append(report.Samples, view)
		if !o.Apply {
			continue
		}
		p, err := prepare(d, o, report.ReplayID, report.Source)
		event := Audit{ReplayID: report.ReplayID, Actor: o.Actor, Source: report.Source, Target: report.Target,
			MessageID: d.MessageId, AttemptBefore: view.Attempt}
		if err != nil {
			if auditErr := record(audit, event, "skipped_invalid"); auditErr != nil {
				return report, auditErr
			}
			continue
		}
		event.AttemptAfter = p.Headers["async-attempt"].(int32)
		if err := record(audit, event, "intent"); err != nil {
			return report, err
		}
		publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = s.Publish(publishCtx, "", report.Target, true, p)
		cancel()
		next = time.Now().Add(time.Duration(float64(time.Second) / o.Rate))
		if err != nil {
			outcome := "publish_failed"
			if errors.Is(err, transport.ErrUnknown) {
				outcome = "publish_unknown"
			}
			if auditErr := record(audit, event, outcome); auditErr != nil {
				return report, errors.Join(err, auditErr)
			}
			return report, fmt.Errorf("replay publication: %w", err)
		}
		if err := record(audit, event, "confirmed"); err != nil {
			return report, err
		}
		if err := d.Ack(false); err != nil {
			if auditErr := record(audit, event, "ack_failed"); auditErr != nil {
				return report, errors.Join(errors.New("source acknowledgment failed"), auditErr)
			}
			return report, errors.New("source acknowledgment failed")
		}
		report.Moved++
		if err := record(audit, event, "moved"); err != nil {
			return report, err
		}
	}
	return report, nil
}
