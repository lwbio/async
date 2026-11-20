// Package request implements asynchronous requests and separately delivered results.
package request

import (
	"context"
	"fmt"
	"time"

	async "github.com/lwbio/async"
	"github.com/lwbio/async/encoding"
)

type Client struct {
	runtime *async.Runtime
	codec   string
}

func NewClient(r *async.Runtime) *Client { return &Client{runtime: r, codec: "json"} }

type CallOptions struct {
	MessageID, ReplyTo, Codec string
	Delay                     time.Duration
}
type CallOption func(*CallOptions)

func WithMessageID(id string) CallOption       { return func(o *CallOptions) { o.MessageID = id } }
func WithReplyTo(target string) CallOption     { return func(o *CallOptions) { o.ReplyTo = target } }
func WithCodec(name string) CallOption         { return func(o *CallOptions) { o.Codec = name } }
func WithDelay(delay time.Duration) CallOption { return func(o *CallOptions) { o.Delay = delay } }

// Submit returns the request ID and publication outcome, never a business result.
func (c *Client) Submit(ctx context.Context, operation string, value any, opts ...CallOption) (string, error) {
	o := CallOptions{Codec: c.codec}
	for _, apply := range opts {
		apply(&o)
	}
	codec := encoding.GetCodec(o.Codec)
	if codec == nil {
		return o.MessageID, fmt.Errorf("unknown codec %q", o.Codec)
	}
	body, err := codec.Marshal(value)
	if err != nil {
		return o.MessageID, err
	}
	return c.runtime.Publish(ctx, async.Message{ID: o.MessageID, Kind: async.Request, Operation: operation, ReplyTo: o.ReplyTo, ContentType: "application/" + codec.Name(), Body: body}, o.Delay)
}

// Register preserves the identity of the business method before wrapping it.
func Register[In, Out any](r *async.Runtime, operation string, handler func(context.Context, *In) (*Out, error), opts ...async.SubscriptionOption) error {
	id, err := async.HandlerIdentity(handler)
	if err != nil {
		return err
	}
	s := async.Subscription{Identity: id, Kind: async.Request, Operations: []string{operation}}
	for _, apply := range opts {
		apply(&s)
	}
	s.Handler = func(ctx context.Context, m async.Message) (*async.Message, error) {
		var in In
		if err := encoding.Decode(m.ContentType, m.Body, &in); err != nil {
			return nil, async.Permanent("INVALID_REQUEST", "invalid request payload or codec")
		}
		out, err := handler(ctx, &in)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		codec := encoding.ForContentType(m.ContentType)
		body, err := codec.Marshal(out)
		if err != nil {
			return nil, err
		}
		return &async.Message{ContentType: m.ContentType, Body: body}, nil
	}
	_, err = r.Register(s)
	return err
}

type Result[T any] struct {
	MessageID, RequestID, Operation string
	Value                           *T
	Failure                         *async.Failure
}

// Listen creates a durable result subscription; pass its target to WithReplyTo.
func Listen[T any](r *async.Runtime, handler func(context.Context, Result[T]) error, opts ...async.SubscriptionOption) (string, error) {
	id, err := async.HandlerIdentity(handler)
	if err != nil {
		return "", err
	}
	s := async.Subscription{Identity: id, Kind: async.Result}
	for _, apply := range opts {
		apply(&s)
	}
	s.Handler = func(ctx context.Context, m async.Message) (*async.Message, error) {
		result := Result[T]{MessageID: m.ID, RequestID: m.CorrelationID, Operation: m.Operation}
		if m.ErrorCode != "" {
			result.Failure = &async.Failure{Code: m.ErrorCode, Message: m.ErrorMessage}
		} else if len(m.Body) > 0 {
			result.Value = new(T)
			if err := encoding.Decode(m.ContentType, m.Body, result.Value); err != nil {
				return nil, async.Permanent("INVALID_RESULT", "invalid result payload or codec")
			}
		}
		return nil, handler(ctx, result)
	}
	return r.Register(s)
}

// DecodeResult is used by generated clients when sharing a raw result subscription.
func DecodeResult[T any](m async.Message) (Result[T], error) {
	result := Result[T]{MessageID: m.ID, RequestID: m.CorrelationID, Operation: m.Operation}
	if m.Kind != async.Result {
		return result, fmt.Errorf("not a result message")
	}
	if m.ErrorCode != "" {
		result.Failure = &async.Failure{Code: m.ErrorCode, Message: m.ErrorMessage}
		return result, nil
	}
	if len(m.Body) == 0 {
		return result, nil
	}
	result.Value = new(T)
	return result, encoding.Decode(m.ContentType, m.Body, result.Value)
}

func DecodeResultFor[T any](m async.Message, operation string) (Result[T], error) {
	if m.Operation != operation {
		return Result[T]{}, fmt.Errorf("unexpected result operation %q", m.Operation)
	}
	return DecodeResult[T](m)
}
