package async

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-kratos/kratos/v2/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type registerOption struct {
	topic string
	ets   []PbEvent
	hook  func(h Handler) Handler
}

type registerOptionFunc func(*registerOption)

func WithTopic(topic string) registerOptionFunc {
	return func(o *registerOption) {
		o.topic = topic
	}
}

func WithMoreEvents(ets ...PbEvent) registerOptionFunc {
	return func(o *registerOption) {
		o.ets = ets
	}
}

func WithHook(hook func(h Handler) Handler) registerOptionFunc {
	return func(o *registerOption) {
		o.hook = hook
	}
}

type SubscriberOptionFunc func(*Subscriber)

func WithId(id string) SubscriberOptionFunc {
	return func(s *Subscriber) {
		s.id = id
	}
}

func WithLogger(log *log.Helper) SubscriberOptionFunc {
	return func(s *Subscriber) {
		s.log = log
	}
}

type Subscriber struct {
	channel  *amqp.Channel
	handlers map[string]Handler
	scs      []reflect.SelectCase
	queues   []string
	id       string

	log *log.Helper
}

func NewSubscriber(mq MQ, opts ...SubscriberOptionFunc) (*Subscriber, error) {
	channel, err := mq.Channel()
	if err != nil {
		return nil, err
	}

	s := Subscriber{
		channel:  channel,
		id:       "async/subscriber", // TODO: package name
		handlers: make(map[string]Handler),
		log:      log.NewHelper(log.DefaultLogger),
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s, nil
}

func (s *Subscriber) register(h Handler, et PbEvent, opts ...registerOptionFunc) error {
	o := &registerOption{}
	for _, opt := range opts {
		opt(o)
	}

	// handler实例函数名
	qn := QnFn(s.id, ToSnake(GetFunctionName(h, '/', '.')))
	if _, ok := s.handlers[qn]; ok {
		panic(fmt.Errorf("handler %s already registered", qn))
	}

	// 声明queue
	_, err := s.channel.QueueDeclare(qn, false, true, false, false, nil) // TODO: autoDelete
	if err != nil {
		panic(err)
	}

	// 绑定queue到exchange
	for _, e := range append(o.ets, et) {
		err = s.channel.QueueBind(qn, o.topic, Ex(e), false, nil)
		if err != nil {
			panic(err)
		}
	}

	// 监听queue
	ch, err := s.channel.Consume(qn, qn, false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	// 注册处理函数
	if o.hook != nil {
		h = o.hook(h)
	}
	s.handlers[qn] = h
	s.queues = append(s.queues, qn)
	s.scs = append(s.scs, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ch),
	})

	s.log.Infof("register handler: %s", qn)
	return nil
}

func (s *Subscriber) Register(h Handler, et PbEvent, opts ...registerOptionFunc) error {
	return s.register(h, et, opts...)
}

func (s *Subscriber) MustRegister(h Handler, et PbEvent, opts ...registerOptionFunc) {
	if err := s.register(h, et, opts...); err != nil {
		panic(err)
	}
}

func (s *Subscriber) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// 1. 从队列中获取任务
			// 2. 根据任务类型，调用对应的处理函数
			// 3. 处理函数返回错误，重新入队列
			chosen, recv, recvOk := reflect.Select(s.scs)
			if recvOk {
				queue := s.queues[chosen]
				handler := s.handlers[queue]
				msg := recv.Interface().(amqp.Delivery)
				if err := handler(ctx, msg.Body); err != nil {
					s.log.Errorf("handler error: %v", err)
					msg.Ack(false) // TODO: MaxRetries
				} else {
					msg.Ack(false)
				}
			}
		}
	}
}

func (s *Subscriber) Stop(ctx context.Context) error {
	return s.channel.Close()
}
