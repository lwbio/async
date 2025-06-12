package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/lwbio/async"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type PublisherOptionFunc func(*Publisher)

func WithPbEvents(ets ...PbEvent) PublisherOptionFunc {
	return func(p *Publisher) {
		for _, et := range ets {
			if _, ok := p.exs[int32(et.Number())]; ok {
				p.log.Warnf("exchange [%s] already exists, skipping", Ex(et))
				continue
			}
			p.exs[int32(et.Number())] = &exchange{
				name: Ex(et),
				kind: amqp.ExchangeTopic,
			}
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

type exchange struct {
	name string
	kind string
}

type Publisher struct {
	conn    async.Conn
	ch      *amqp.Channel
	choseCh chan struct{}
	exs     map[int32]*exchange
	mu      sync.Mutex // Protects access to ch

	log *log.Helper
}

func NewPublisher(conn async.Conn, opts ...PublisherOptionFunc) (*Publisher, error) {
	p := Publisher{
		conn:    conn,
		choseCh: make(chan struct{}),
		exs:     make(map[int32]*exchange),
		log:     log.NewHelper(log.DefaultLogger),
		mu:      sync.Mutex{},
	}

	for _, opt := range opts {
		opt(&p)
	}

	// 确保连接已建立
	if err := p.establishChannel(); err != nil {
		return nil, err
	}

	// 创建交换机
	for _, ex := range p.exs {
		if err := p.ch.ExchangeDeclare(
			ex.name,
			ex.kind,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return nil, err
		}
		tips := fmt.Sprintf("exchange [%s] declared", ex.name)
		if ex.kind != amqp.ExchangeTopic {
			tips += fmt.Sprintf(" with kind [%s]", ex.kind)
		}
		p.log.Info(tips)
	}

	return &p, nil
}

func (p *Publisher) establishChannel() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch != nil { // We already have a channel.
		return nil
	}

	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	p.ch = ch
	return nil
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

	if err := p.establishChannel(); err != nil {
		return err
	}

	return p.ch.PublishWithContext(ctx, ex.name, o.rk, o.mandatory, false, msg)
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
