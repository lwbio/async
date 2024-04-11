package async

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type ClientOptionFunc func(*Client)

func WithPbEvents(ets ...PbEvent) ClientOptionFunc {
	return func(p *Client) {
		for _, et := range ets {
			p.queues[int32(et.Number())] = Qn(et)
		}
	}
}

type CallOption struct {
	rk        string
	mandatory bool
}

type PublishOptionFunc func(*CallOption)

func WithRoutingKey(rk string) PublishOptionFunc {
	return func(o *CallOption) {
		o.rk = rk
	}
}

func WithMandatory() PublishOptionFunc {
	return func(o *CallOption) {
		o.mandatory = true
	}
}

type Client struct {
	ch     *amqp.Channel
	queues map[int32]string
	log    *log.Helper
}

func NewClient(mq MQ, logger log.Logger, opts ...ClientOptionFunc) (*Client, error) {
	ch, err := mq.Channel()
	if err != nil {
		return nil, err
	}

	p := Client{
		ch:     ch,
		queues: make(map[int32]string),
		log:    log.NewHelper(log.With(logger, "aysnc", "celery/client")),
	}

	for _, opt := range opts {
		opt(&p)
	}

	// 创建队列
	for _, queue := range p.queues {
		if _, err := ch.QueueDeclare(
			queue,
			false,
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

func (p *Client) call(ctx context.Context, et PbEvent, m proto.Message, opts ...PublishOptionFunc) (string, error) {
	o := &CallOption{}
	for _, opt := range opts {
		opt(o)
	}

	payload, err := proto.Marshal(m)
	if err != nil {
		return "", err
	}

	id := uuid.New().String()

	msg := amqp.Publishing{
		CorrelationId: id,
		ContentType:   "application/octet-stream",
		Body:          payload,
	}

	return id, p.ch.PublishWithContext(ctx, "", Qn(et), o.mandatory, false, msg)
}

func (p *Client) Call(ctx context.Context, et PbEvent, m proto.Message) (string, error) {
	return p.call(ctx, et, m)
}

func (cli *Client) DirectCall(ctx context.Context, et PbEvent, m proto.Message) error {
	_, err := cli.call(ctx, et, m)
	return err
}

func (cli *Client) DirectCall1(ctx context.Context, queue string, m proto.Message) (string, error) {
	payload, err := proto.Marshal(m)
	if err != nil {
		return "", err
	}

	id := uuid.New().String()

	msg := amqp.Publishing{
		CorrelationId: id,
		ContentType:   "application/proto",
		Body:          payload,
	}

	return id, cli.ch.PublishWithContext(ctx, "", queue, false, false, msg)
}

func (p *Client) Close() error {
	return p.ch.Close()
}
