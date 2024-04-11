package async

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-kratos/kratos/v2/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ServerOptionFunc func(*Server)

type registerOption struct {
	topic    string
	returnEt PbEvent // 返参event
}

type registerOptionFunc func(*registerOption)

func WithTopic(topic string) registerOptionFunc {
	return func(o *registerOption) {
		o.topic = topic
	}
}

func WithReturnEt(et PbEvent) registerOptionFunc {
	return func(o *registerOption) {
		o.returnEt = et
	}
}

type Consumer struct {
	inQ    string
	outQ   string
	handle Handler1
}

type Server struct {
	ch        *amqp.Channel
	scs       []reflect.SelectCase
	consumers []*Consumer
	dec       DecodeRequestFunc
	enc       EncodeResponseFunc
	errCh     chan *amqp.Error

	log *log.Helper
}

func NewServer(mq MQ, opts ...ServerOptionFunc) (*Server, error) {
	channel, err := mq.Channel()
	if err != nil {
		return nil, err
	}

	s := Server{
		ch:        channel,
		scs:       make([]reflect.SelectCase, 0),
		consumers: make([]*Consumer, 0),
		dec:       DefaultRequestDecoder,
		enc:       DefaultResponseEncoder,
		log:       log.NewHelper(log.DefaultLogger),
		errCh:     make(chan *amqp.Error),
	}

	channel.NotifyClose(s.errCh)

	for _, opt := range opts {
		opt(&s)
	}

	return &s, nil
}

func (s *Server) Register(queue string, h Handler1) {
	// 生成queue name
	qn := queue

	// 声明queue
	_, err := s.ch.QueueDeclare(qn, false, false, false, false, nil) // TODO: autoDelete
	if err != nil {
		panic(err)
	}

	// 监听queue
	channel, err := s.ch.Consume(qn, qn, false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	s.scs = append(s.scs, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(channel),
	})

	c := &Consumer{
		inQ:    qn,
		handle: h,
	}
	s.consumers = append(s.consumers, c)

	s.log.Infof("register handler: %s", qn)
}

func (s *Server) Start(ctx context.Context) error {
	s.log.Infof("server start...")
	defer s.log.Infof("server stop!")
	go func() {
		for err := range s.errCh {
			s.log.Errorf("channel error: %v", err)
		}
	}()

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
				c := s.consumers[chosen]
				message := recv.Interface().(amqp.Delivery)
				go func(msg amqp.Delivery) {
					defer msg.Ack(false)

					w := &wrapper{srv: s, ctx: ctx, req: &msg, resp: nil}
					if err := c.handle(w); err != nil {
						s.log.Errorf("handle %s error: %v", c.inQ, err)
						return
					}

					fmt.Printf("result [%s]: %s\n", w.resp.CorrelationId, string(w.resp.Body))

					if c.outQ == "" || w.resp == nil {
						return
					}

					if err := s.returnResponse(ctx, c.outQ, w.resp); err != nil {
						s.log.Errorf("return response error: %v", err)
					}
				}(message)
			}
		}
	}
}

func (s *Server) returnResponse(
	ctx context.Context,
	outQ string,
	msg *amqp.Publishing,
) error {
	if msg == nil {
		return nil
	}

	return s.ch.PublishWithContext(ctx, "", outQ, false, false, *msg)
}

func (s *Server) Stop(ctx context.Context) error {
	return s.ch.Close()
}
