package pubsub

import (
	"context"
	"fmt"
	"reflect"
	"sync"
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

func WithRegisterAllTopic() RegisterOptionFunc {
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

// 临时一次性队列，通常用于不同 pod 临时绑定
func WithRegisterOnce() RegisterOptionFunc {
	return func(c *consumer) {
		c.queue.Name = ""         // 表示请求服务器生成一个随机唯一的队列名
		c.queue.Durable = false   // 队列不持久化
		c.queue.AutoDelete = true // 队列使用完后自动删除
		c.queue.Exclusive = true  // 队列独占
	}
}

type SubscriberOptionFunc func(*Subscriber)

func WithSubscriberId(id string) SubscriberOptionFunc {
	return func(s *Subscriber) {
		s.id = id
	}
}

func WithSubscriberLogger(logger log.Logger) SubscriberOptionFunc {
	return func(s *Subscriber) {
		s.log = log.NewHelper(logger)
	}
}

type queue struct {
	Name       string // 队列名称
	Durable    bool   // 是否持久化
	AutoDelete bool   // 是否自动删除
	Exclusive  bool   // 是否独占
}

func newQueue(name string) *queue {
	return &queue{
		Name:       name,
		Durable:    false, // 默认不持久化
		AutoDelete: false, // 默认不自动删除
		Exclusive:  false, // 默认不独占
	}
}

type consumer struct {
	name  string
	queue *queue // 队列
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
	mu        sync.Mutex // Protects access to channel

	log *log.Helper
}

func NewSubscriber(conn async.Conn, opts ...SubscriberOptionFunc) (*Subscriber, error) {
	s := Subscriber{
		conn:      conn,
		id:        "async", // TODO: package name
		consumers: make([]consumer, 0),
		log:       log.NewHelper(log.DefaultLogger),
		mu:        sync.Mutex{},
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s, nil
}

func (s *Subscriber) establishChannel() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.channel != nil { // We already have a channel.
		return nil
	}

	ch, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	s.channel = ch
	return nil
}

func (s *Subscriber) register(h Handler, MustEx string, opts ...RegisterOptionFunc) error {
	queueName := fmt.Sprintf("%s.%s", s.id, GetFunctionName(h, '/', '.'))

	c := consumer{
		name:  queueName,
		h:     h,
		queue: newQueue(queueName),
		exs:   []string{MustEx},
	}

	for _, opt := range opts {
		opt(&c)
	}

	if err := s.establishChannel(); err != nil {
		return err
	}

	// 声明queue
	cq := c.queue
	q, err := s.channel.QueueDeclare(
		cq.Name,
		cq.Durable,
		cq.AutoDelete,
		cq.Exclusive,
		false, // TODO: autoDelete
		nil,
	)
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

	s.log.Infof("consumer [%s] registered", c.name)
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
			s.log.Errorf("panic: %v", r)
		}
	}()
	defer msg.Ack(false)

	ctx = context.WithValue(ctx, KeyCorrelationID, msg.CorrelationId)
	ctx = context.WithValue(ctx, KeyReplyTo, msg.ReplyTo)
	ctx = context.WithValue(ctx, KeyExchange, msg.Exchange)
	ctx = context.WithValue(ctx, KeyRoutingKey, msg.RoutingKey)

	if err := c.h(ctx, msg.Body); err != nil {
		return err
	}

	return nil
}

func (s *Subscriber) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel == nil {
		return nil
	}
	// Close the channel gracefully
	if err := s.channel.Close(); err != nil {
		s.log.Errorf("failed to close channel: %v", err)
	}
	s.channel = nil
	return nil
}
