package async

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/lwbio/async"
	"github.com/lwbio/async/encoding"
	json_enc "github.com/lwbio/async/encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ClientOptionFunc func(*Client)

func WithCodec(c encoding.Codec) ClientOptionFunc {
	return func(p *Client) {
		p.codec = c
	}
}

func WithLogger(logger log.Logger) ClientOptionFunc {
	return func(p *Client) {
		p.log = logger
	}
}

func WithReplyTo(replyTo string) ClientOptionFunc {
	return func(p *Client) {
		p.replyTo = replyTo
	}
}

type CallOption struct {
	id string
}

type CallOptionFunc func(*CallOption)

func WithCallID(id string) CallOptionFunc {
	return func(o *CallOption) {
		o.id = id
	}
}

type Client struct {
	replyTo string
	ch      *amqp.Channel
	codec   encoding.Codec

	log log.Logger
}

func NewClient(conn async.Conn, opts ...ClientOptionFunc) (*Client, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	p := Client{
		ch:    ch,
		codec: json_enc.Codec{},
		log:   log.DefaultLogger,
	}

	for _, opt := range opts {
		opt(&p)
	}

	return &p, nil
}

func (cli *Client) call(ctx context.Context, queue string, m interface{}, opts ...CallOptionFunc) (string, error) {
	o := &CallOption{}
	for _, opt := range opts {
		opt(o)
	}

	payload, err := cli.codec.Marshal(m)
	if err != nil {
		return "", err
	}

	if o.id == "" {
		o.id = uuid.NewString()
	}

	msg := amqp.Publishing{
		CorrelationId: o.id,
		ReplyTo:       cli.replyTo,
		ContentType:   ContentType(cli.codec.Name()),
		Body:          payload,
	}

	return o.id, cli.ch.PublishWithContext(
		ctx,
		"", // 默认 exchange
		queue,
		true, // mandatory：当消息无法路由到队列时，会触发Return
		false,
		msg,
	)
}

func (p *Client) Call(ctx context.Context, queue string, m interface{}, opts ...CallOptionFunc) (string, error) {
	return p.call(ctx, queue, m)
}

func (cli *Client) Do(ctx context.Context, queue string, m interface{}, opts ...CallOptionFunc) error {
	_, err := cli.call(ctx, queue, m)
	return err
}

func (p *Client) Close() error {
	return p.ch.Close()
}
