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

func WithLogger(logger log.Logger) PublisherOptionFunc {
	return func(p *Publisher) {
		p.log = log.NewHelper(logger)
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
	ch      *amqp.Channel
	choseCh chan struct{}
	exs     map[int32]string

	log *log.Helper
}

func NewPublisher(conn async.Conn, opts ...PublisherOptionFunc) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	p := Publisher{
		ch:      ch,
		choseCh: make(chan struct{}),
		exs:     make(map[int32]string),
		log:     log.NewHelper(log.DefaultLogger),
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
		p.log.Infof("exchange [%s] declared", ex)
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

func (p *Publisher) Publish(ctx context.Context, et PbEvent, m proto.Message, opts ...PublishOptionFunc) error {
	return p.publish(ctx, et, m, opts...)
}

func (p *Publisher) NotifyReturn(ch chan amqp.Return) {
	p.ch.NotifyReturn(ch)
}

func (p *Publisher) Close() error {
	defer close(p.choseCh)
	return p.ch.Close()
}
