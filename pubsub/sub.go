package pubsub

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/lwbio/async"
	amqp "github.com/rabbitmq/amqp091-go"
)

type registerOptionFunc func(*consumer)

func WithRegisterTopic(topic string) registerOptionFunc {
	return func(o *consumer) {
		o.topic = topic
	}
}

func WithRegisterMoreEvents(ets ...PbEvent) registerOptionFunc {
	return func(o *consumer) {
		for _, et := range ets {
			o.exs = append(o.exs, Ex(et))
		}
	}
}

func WithRegisterHook(hook func(h Handler) Handler) registerOptionFunc {
	return func(o *consumer) {
		o.h = hook(o.h)
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

func (s *Subscriber) register(h Handler, MustEx string, opts ...registerOptionFunc) error {
	c := consumer{
		h:     h,
		queue: fmt.Sprintf("%s.%s", s.id, GetFunctionName(h, '/', '.')),
		exs:   []string{MustEx},
	}
	for _, opt := range opts {
		opt(&c)
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

	s.log.Log(log.LevelInfo, "register handler: %s", q.Name)
	return nil
}

func (s *Subscriber) Register(h Handler, et PbEvent, opts ...registerOptionFunc) error {
	return s.register(h, Ex(et), opts...)
}

func (s *Subscriber) MustRegister(h Handler, et PbEvent, opts ...registerOptionFunc) {
	if err := s.register(h, Ex(et), opts...); err != nil {
		panic(err)
	}
}

func (s *Subscriber) RegisterEx(h Handler, ex string, opts ...registerOptionFunc) error {
	return s.register(h, ex, opts...)
}

func (s *Subscriber) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			chosen, recv, recvOk := reflect.Select(s.scs)
			if !recvOk {
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

	if err := c.h(ctx, msg.Body); err != nil {
		return err
	}

	return nil
}

func (s *Subscriber) Stop(ctx context.Context) error {
	return s.channel.Close()
}
