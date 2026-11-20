package async

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	"github.com/lwbio/async/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

func (r *Runtime) work(ctx context.Context, g *generation, reg registration, epoch *consumerEpoch) {
	defer r.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-epoch.deliveries:
			if !ok {
				if r.State() != Draining && ctx.Err() == nil {
					select {
					case epoch.failed <- errConsumerStopped:
					default:
					}
				}
				return
			}
			if ctx.Err() != nil {
				return
			}
			// Share the limit across generations, including slow-to-cancel handlers.
			select {
			case reg.slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			if ctx.Err() != nil {
				<-reg.slots
				return
			}
			err := r.process(ctx, g, reg, d)
			<-reg.slots
			if err != nil {
				select {
				case epoch.failed <- err:
				default:
				}
				return
			}
			select {
			case epoch.progress <- struct{}{}:
			default:
			}
		}
	}
}

type handlerPanic struct{ stack []byte }

func (*handlerPanic) Error() string { return "handler panic" }

func invoke(ctx context.Context, h Handler, m Message) (reply *Message, err error) {
	defer func() {
		if recover() != nil {
			// Do not retain the panic value: it may contain payloads or credentials.
			err = &handlerPanic{stack: debug.Stack()}
		}
	}()
	return h(ctx, m)
}

func (r *Runtime) process(ctx context.Context, g *generation, reg registration, d amqp.Delivery) (processErr error) {
	start := time.Now()
	attrs := r.metrics.attrs(string(reg.Kind), reg.Identity)
	opt := metric.WithAttributes(attrs...)
	outcome := "success"
	r.metrics.inFlight.Add(ctx, 1, opt)
	defer func() {
		r.metrics.inFlight.Add(ctx, -1, opt)
		if processErr != nil {
			outcome = "error"
			if ctx.Err() != nil {
				outcome = "canceled"
			}
		}
		completed := metric.WithAttributes(append(attrs, attribute.String("async.outcome", outcome))...)
		r.metrics.deliveries.Add(ctx, 1, completed)
		r.metrics.processDuration.Record(ctx, time.Since(start).Seconds(), completed)
	}()
	r.stats.inFlight.Add(1)
	defer r.stats.inFlight.Add(-1)
	ctx, span := r.consumer.Start(ctx, string(reg.Kind)+" process", tracing.NewDeliveryCarrier(&d))
	span.SetAttributes(attribute.String("messaging.message.id", d.MessageId), attribute.String("messaging.destination.name", reg.queue),
		attribute.String("async.subscription", reg.Identity), attribute.Bool("messaging.rabbitmq.message.redelivered", d.Redelivered))
	defer func() {
		if processErr != nil {
			span.RecordError(processErr)
			span.SetStatus(codes.Error, "message processing failed")
		}
		span.End()
	}()
	m, err := protocol.Decode(d)
	if err == nil && m.Kind != reg.Kind {
		err = fmt.Errorf("wrong message kind")
	}
	if err == nil && len(reg.Operations) > 0 {
		found := false
		for _, op := range reg.Operations {
			if op == m.Operation || (reg.Kind == Event && protocol.MatchTopic(op, m.Operation)) {
				found = true
			}
		}
		if !found {
			err = fmt.Errorf("unexpected operation")
		}
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid protocol")
		// Preserve malformed data for diagnosis without trusting its reply target.
		p := amqp.Publishing{Headers: d.Headers, Body: d.Body, ContentType: d.ContentType, MessageId: d.MessageId, CorrelationId: d.CorrelationId, Type: d.Type, DeliveryMode: amqp.Persistent}
		if err = r.publish(ctx, g, "", topology.DeadQueue(reg.queue), true, deadLetterPublishing(p, reg, "invalid_protocol", "")); err != nil {
			return err
		}
		r.stats.deadLetters.Add(1)
		r.metrics.deadLetters.Add(ctx, 1, opt)
		outcome = "invalid_protocol"
		return d.Ack(false)
	}
	ctx = context.WithValue(ctx, deliveryKey{}, m)
	span.SetAttributes(attribute.Int64("async.attempt", int64(m.Attempt)))
	reply, handlerErr := r.invokeHandler(ctx, reg, m)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if handlerErr != nil {
		span.RecordError(handlerErr)
		span.SetStatus(codes.Error, "handler failed")
		var failure *Failure
		permanent := errors.As(handlerErr, &failure)
		if !permanent && int(m.Attempt) < len(reg.Retries) {
			m.Attempt++
			if err = r.publish(ctx, g, "", topology.RetryQueue(reg.queue, int(m.Attempt)), true, m.Publishing()); err != nil {
				return err
			}
			r.stats.retries.Add(1)
			r.metrics.retries.Add(ctx, 1, opt)
			outcome = "retry"
			return d.Ack(false)
		}
		if failure == nil {
			failure = &Failure{Code: "RETRIES_EXHAUSTED", Message: "request processing failed"}
		}
		if m.Kind == Request && m.ReplyTo != "" {
			if err = r.result(ctx, g, m, nil, failure); err != nil {
				return err
			}
		}
		reason := "retries_exhausted"
		if permanent {
			reason = "permanent_error"
		}
		if err = r.publish(ctx, g, "", topology.DeadQueue(reg.queue), true, deadLetterPublishing(m.Publishing(), reg, reason, failure.Code)); err != nil {
			return err
		}
		r.stats.deadLetters.Add(1)
		r.metrics.deadLetters.Add(ctx, 1, opt)
		outcome = "dead_letter"
		return d.Ack(false)
	}
	if m.Kind == Request && m.ReplyTo != "" {
		if err = r.result(ctx, g, m, reply, nil); err != nil {
			return err
		}
	}
	return d.Ack(false)
}

func deadLetterPublishing(p amqp.Publishing, reg registration, reason, code string) amqp.Publishing {
	headers := make(amqp.Table, len(p.Headers)+4)
	for key, value := range p.Headers {
		headers[key] = value
	}
	headers[protocol.DeadReasonHeader] = reason
	headers[protocol.DeadAtHeader] = time.Now().UTC().Format(time.RFC3339Nano)
	headers[protocol.DeadSubscriptionHeader] = reg.Identity
	delete(headers, protocol.DeadCodeHeader)
	if code != "" {
		headers[protocol.DeadCodeHeader] = code
	}
	p.Headers = headers
	return p
}

func (r *Runtime) invokeHandler(ctx context.Context, reg registration, m Message) (*Message, error) {
	start := time.Now()
	reply, err := invoke(ctx, reg.Handler, m)
	elapsed := time.Since(start)
	outcome := "success"
	if err != nil {
		outcome = "error"
		var failure *Failure
		if errors.As(err, &failure) {
			outcome = "permanent_error"
		}
		fields := []any{"namespace", r.cfg.Namespace, "message_id", m.ID, "subscription", reg.Identity,
			"attempt", m.Attempt, "error_type", fmt.Sprintf("%T", err)}
		var panicErr *handlerPanic
		if errors.As(err, &panicErr) {
			outcome = "panic"
			fields = append(fields, "stack", string(panicErr.stack))
		}
		// Error text is deliberately excluded: business errors can contain secrets.
		r.cfg.Logger.Warn("async handler failed", fields...)
	}
	if ctx.Err() != nil {
		outcome = "canceled"
	}
	opt := metric.WithAttributes(append(r.metrics.attrs(string(reg.Kind), reg.Identity),
		attribute.String("async.outcome", outcome))...)
	r.metrics.handlers.Add(ctx, 1, opt)
	r.metrics.handlerDuration.Record(ctx, elapsed.Seconds(), opt)
	return reply, err
}

func (r *Runtime) result(ctx context.Context, g *generation, request Message, reply *Message, failure *Failure) error {
	m := Message{ID: protocol.ResultID(request.ID), CorrelationID: request.ID, Kind: Result, Operation: request.Operation, ContentType: request.ContentType}
	if reply != nil {
		m.Body = reply.Body
		if reply.ContentType != "" {
			m.ContentType = reply.ContentType
		}
	}
	if failure != nil {
		m.ErrorCode = failure.Code
		if m.ErrorCode == "" {
			m.ErrorCode = "PERMANENT_FAILURE"
		}
		m.ErrorMessage = failure.Message
	}
	return r.publish(ctx, g, "", request.ReplyTo, true, m.Publishing())
}
