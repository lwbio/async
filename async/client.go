package async

import (
	"context"
	"time"

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

func WithDelayExchange(ex string) ClientOptionFunc {
	return func(p *Client) {
		p.delayEx = ex
	}
}

type CallOption struct {
	id    string
	delay time.Duration
}

type CallOptionFunc func(*CallOption)

func WithCallID(id string) CallOptionFunc {
	return func(o *CallOption) {
		o.id = id
	}
}

func WithCallDelay(delay time.Duration) CallOptionFunc {
	return func(o *CallOption) {
		o.delay = delay
	}
}

type Client struct {
	replyTo string
	delayEx string
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
		ch:      ch,
		delayEx: DefaultDelayExchange,
		codec:   json_enc.Codec{},
		log:     log.DefaultLogger,
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

	var ex string
	msg := amqp.Publishing{
		CorrelationId: o.id,
		ReplyTo:       cli.replyTo,
		ContentType:   ContentType(cli.codec.Name()),
		Body:          payload,
	}

	if o.delay > 0 {
		ex = cli.delayEx
		msg.Headers = amqp.Table{
			"x-delay": o.delay.Milliseconds(),
		}
	}

	return o.id, cli.ch.PublishWithContext(
		ctx,
		ex, // 默认 exchange OR delay exchange
		queue,
		true, // mandatory：当消息无法路由到队列时，会触发Return
		false,
		msg,
	)
}

func (cli *Client) Call(ctx context.Context, queue string, m interface{}, opts ...CallOptionFunc) (string, error) {
	return cli.call(ctx, queue, m, opts...)
}

func (cli *Client) DirectCall(ctx context.Context, queue string, m interface{}, opts ...CallOptionFunc) error {
	_, err := cli.call(ctx, queue, m, opts...)
	return err
}

func (p *Client) Close() error {
	return p.ch.Close()
}
