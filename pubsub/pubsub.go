// Package pubsub provides independent durable event subscriptions.
package pubsub

import (
	"context"
	"fmt"

	async "github.com/lwbio/async"
	"github.com/lwbio/async/encoding"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Publisher struct{ runtime *async.Runtime }

func NewPublisher(r *async.Runtime) *Publisher { return &Publisher{runtime: r} }

type PublishOptions struct{ MessageID, Codec string }
type PublishOption func(*PublishOptions)

func WithMessageID(id string) PublishOption { return func(o *PublishOptions) { o.MessageID = id } }
func WithCodec(name string) PublishOption   { return func(o *PublishOptions) { o.Codec = name } }

func (p *Publisher) Publish(ctx context.Context, event string, value any, opts ...PublishOption) (string, error) {
	o := PublishOptions{Codec: "json"}
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
	return p.runtime.Publish(ctx, async.Message{ID: o.MessageID, Kind: async.Event, Operation: event, ContentType: "application/" + codec.Name(), Body: body}, 0)
}

func Subscribe[T any](r *async.Runtime, events []string, handler func(context.Context, *T) error, opts ...async.SubscriptionOption) (string, error) {
	id, err := async.HandlerIdentity(handler)
	if err != nil {
		return "", err
	}
	s := async.Subscription{Identity: id, Kind: async.Event, Operations: events}
	for _, apply := range opts {
		apply(&s)
	}
	s.Handler = func(ctx context.Context, m async.Message) (*async.Message, error) {
		value := new(T)
		if err := encoding.Decode(m.ContentType, m.Body, value); err != nil {
			return nil, async.Permanent("INVALID_EVENT", "invalid event payload or codec")
		}
		return nil, handler(ctx, value)
	}
	return r.Register(s)
}

// EventName includes the enum type, so equal numbers from different enums cannot collide.
func EventName(event protoreflect.Enum) string {
	d := event.Descriptor()
	v := d.Values().ByNumber(event.Number())
	if v != nil {
		return string(d.FullName()) + "." + string(v.Name())
	}
	return fmt.Sprintf("%s.%d", d.FullName(), event.Number())
}
