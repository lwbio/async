package pubsub

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/lwbio/async"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type PublisherOptionFunc func(*Publisher)

func WithPbEvents(ets ...PbEvent) PublisherOptionFunc {
	return func(p *Publisher) {
		for _, et := range ets {
			p.exs[int32(et.Number())] = Ex(et)
		}
	}
}

type PublishOption struct {
	rk        string
	mandatory bool
}

type PublishOptionFunc func(*PublishOption)

func WithRoutingKey(rk string) PublishOptionFunc {
	return func(o *PublishOption) {
		o.rk = rk
	}
}

func WithMandatory() PublishOptionFunc {
	return func(o *PublishOption) {
		o.mandatory = true
	}
}

type Publisher struct {
	ch  *amqp.Channel
	exs map[int32]string
	log *log.Helper
}

func NewPublisher(conn async.Conn, logger log.Logger, opts ...PublisherOptionFunc) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	p := Publisher{
		ch:  ch,
		exs: make(map[int32]string),
		log: log.NewHelper(log.With(logger, "module", "data/pub")),
	}

	for _, opt := range opts {
		opt(&p)
	}

	// 创建交换机
	for _, ex := range p.exs {
		if err := ch.ExchangeDeclare(
			ex,
			amqp.ExchangeTopic,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func (p *Publisher) publish(ctx context.Context, et PbEvent, m proto.Message, opts ...PublishOptionFunc) error {
	o := &PublishOption{}
	for _, opt := range opts {
		opt(o)
	}

	payload, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        payload,
	}

	ex, ok := p.exs[int32(et.Number())]
	if !ok {
		return ErrExchangeNotInit
	}

	return p.ch.PublishWithContext(ctx, ex, o.rk, o.mandatory, false, msg)
}

func (p *Publisher) Publish(ctx context.Context, et PbEvent, m proto.Message) error {
	return p.publish(ctx, et, m)
}

func (p *Publisher) Close() error {
	return p.ch.Close()
}
