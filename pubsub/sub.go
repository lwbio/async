package pubsub

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/lwbio/async"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RegisterOptionFunc func(*consumer)

func WithRegisterName(name string) RegisterOptionFunc {
	return func(o *consumer) {
		o.name = name
	}
}

func WithRegisterTopic(topic string) RegisterOptionFunc {
	return func(o *consumer) {
		o.topic = topic
	}
}

func WithRegisterAllTopics() RegisterOptionFunc {
	return func(o *consumer) {
		o.topic = "*"
	}
}

func WithRegisterMoreEvents(ets ...PbEvent) RegisterOptionFunc {
	return func(o *consumer) {
		for _, et := range ets {
			o.exs = append(o.exs, Ex(et))
		}
	}
}

type SubscriberOptionFunc func(*Subscriber)

func WithSubscriberId(id string) SubscriberOptionFunc {
	return func(s *Subscriber) {
		s.id = id
	}
}

func WithSubscriberLogger(log log.Logger) SubscriberOptionFunc {
	return func(s *Subscriber) {
		s.log = log
	}
}

type consumer struct {
	name  string
	queue string
	h     Handler

	topic string
	exs   []string
}

type Subscriber struct {
	id        string
	conn      async.Conn
	channel   *amqp.Channel
	scs       []reflect.SelectCase
	consumers []consumer

	log log.Logger
}

func NewSubscriber(conn async.Conn, opts ...SubscriberOptionFunc) (*Subscriber, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	s := Subscriber{
		conn:      conn,
		channel:   channel,
		id:        "async", // TODO: package name
		consumers: make([]consumer, 0),
		log:       log.DefaultLogger,
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s, nil
}

func (s *Subscriber) register(h Handler, MustEx string, opts ...RegisterOptionFunc) error {
	c := consumer{
		h:     h,
		queue: fmt.Sprintf("%s.%s", s.id, GetFunctionName(h, '/', '.')),
		exs:   []string{MustEx},
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.name == "" {
		c.name = c.queue
	}

	// 声明queue
	q, err := s.channel.QueueDeclare(c.queue, false, false, false, false, nil) // TODO: autoDelete
	if err != nil {
		return err
	}

	// 绑定queue到exchange
	for _, ex := range c.exs {
		err = s.channel.QueueBind(q.Name, c.topic, ex, false, nil)
		if err != nil {
			return err
		}
	}

	// 监听queue
	ch, err := s.channel.Consume(q.Name, q.Name, false, false, false, false, nil)
	if err != nil {
		return err
	}

	s.scs = append(s.scs, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ch),
	})
	s.consumers = append(s.consumers, c)

	s.log.Log(log.LevelInfo, "register subscribe handler: %s", c.name)
	return nil
}

func (s *Subscriber) Register(h Handler, et PbEvent, opts ...RegisterOptionFunc) error {
	return s.register(h, Ex(et), opts...)
}

func (s *Subscriber) MustRegister(h Handler, et PbEvent, opts ...RegisterOptionFunc) {
	if err := s.register(h, Ex(et), opts...); err != nil {
		panic(err)
	}
}

func (s *Subscriber) RegisterEx(h Handler, ex string, opts ...RegisterOptionFunc) error {
	return s.register(h, ex, opts...)
}

func (s *Subscriber) MustRegisterEx(h Handler, ex string, opts ...RegisterOptionFunc) {
	if err := s.register(h, ex, opts...); err != nil {
		panic(err)
	}
}

func (s *Subscriber) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			chosen, recv, recvOk := reflect.Select(s.scs)
			if !recvOk {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			c := s.consumers[chosen]
			msg := recv.Interface().(amqp.Delivery)

			go s.handle(ctx, msg, c)
		}
	}
}

func (s *Subscriber) handle(ctx context.Context, msg amqp.Delivery, c consumer) error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Log(log.LevelError, "panic: %v", r)
		}
	}()
	defer msg.Ack(false)

	ctx = context.WithValue(ctx, KeyCorrelationID, msg.CorrelationId)
	ctx = context.WithValue(ctx, KeyReplyTo, msg.ReplyTo)
	ctx = context.WithValue(ctx, KeyExchange, msg.Exchange)

	if err := c.h(ctx, msg.Body); err != nil {
		return err
	}

	return nil
}

func (s *Subscriber) Stop(ctx context.Context) error {
	return s.channel.Close()
}
