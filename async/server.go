package async

import (
	"context"
	"reflect"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/lwbio/async"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ServerOptionFunc func(*Server)

func WithServerLogger(l log.Logger) ServerOptionFunc {
	return func(s *Server) {
		s.log = log.NewHelper(l)
	}
}

func WithServerDelayExchange(ex string) ServerOptionFunc {
	return func(s *Server) {
		s.delayEx = ex
	}
}

type RegisterOptionFunc func(*consumer)

type consumer struct {
	queue    string
	resultEx string
	handle   Handler
}

type Server struct {
	conn      async.Conn
	ch        *amqp.Channel
	scs       []reflect.SelectCase
	consumers []consumer
	dec       DecodeRequestFunc
	enc       EncodeResponseFunc
	delayEx   string

	log *log.Helper
}

func NewServer(conn async.Conn, opts ...ServerOptionFunc) (*Server, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	s := Server{
		conn:      conn,
		ch:        channel,
		scs:       make([]reflect.SelectCase, 0),
		consumers: make([]consumer, 0),
		dec:       DefaultRequestDecoder,
		enc:       DefaultResponseEncoder,
		delayEx:   DefaultDelayExchange,
		log:       log.NewHelper(log.DefaultLogger),
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s, nil
}

func (s *Server) register(h Handler, queue string, resultEx string, opts ...RegisterOptionFunc) error {
	c := consumer{
		handle:   h,
		queue:    queue,
		resultEx: resultEx,
	}
	for _, opt := range opts {
		opt(&c)
	}

	// 声明queue
	q, err := s.ch.QueueDeclare(queue, false, false, false, false, nil) // TODO: autoDelete
	if err != nil {
		return err
	}

	// 绑定queue到delay exchange
	if err := s.ch.QueueBind(q.Name, q.Name, s.delayEx, false, nil); err != nil {
		return err
	}

	// 声明result exchange
	if resultEx != "" {
		if err := s.ch.ExchangeDeclare(
			resultEx,
			amqp.ExchangeTopic,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return err
		}
	}

	// 监听queue
	channel, err := s.ch.Consume(q.Name, q.Name, false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	s.scs = append(s.scs, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(channel),
	})
	s.consumers = append(s.consumers, c)

	s.log.Infof("register handler: %s", q.Name)
	return nil
}

func (s *Server) Register(h Handler, queue string, resultEx string, opts ...RegisterOptionFunc) error {
	return s.register(h, queue, resultEx, opts...)
}

func (s *Server) MustRegister(h Handler, queue string, resultEx string, opts ...RegisterOptionFunc) {
	if err := s.Register(h, queue, resultEx, opts...); err != nil {
		panic(err)
	}
}

func (s *Server) Start(ctx context.Context) error {
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

func (s *Server) handle(ctx context.Context, msg amqp.Delivery, c consumer) error {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("handle error: %v", r)
		}
	}()
	defer msg.Ack(false)

	w := &wrapper{srv: s, ctx: ctx, req: &msg}
	if err := c.handle(w); err != nil {
		return err
	}

	if err := s.result(ctx, c.resultEx, w.resp); err != nil {
		return err
	}

	return nil
}

func (s *Server) result(ctx context.Context, ex string, msg *amqp.Publishing,
) error {
	if msg == nil {
		return nil
	}

	return s.ch.PublishWithContext(ctx, ex, msg.ReplyTo, false, false, *msg)
}

func (s *Server) Stop(ctx context.Context) error {
	return s.ch.Close()
}
