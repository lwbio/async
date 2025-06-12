package async

import (
	"context"
	"sync"
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
	conn    async.Conn
	codec   encoding.Codec
	mu      sync.Mutex // Protects access to ch

	log log.Logger
}

func NewClient(conn async.Conn, opts ...ClientOptionFunc) (*Client, error) {
	p := Client{
		conn:    conn,
		delayEx: DefaultDelayExchange,
		codec:   json_enc.Codec{},
		log:     log.DefaultLogger,
		mu:      sync.Mutex{},
	}

	for _, opt := range opts {
		opt(&p)
	}

	return &p, nil
}

func (c *Client) establishChannel() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch != nil { // We already have a channel.
		return nil
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	c.ch = ch
	return nil
}

func (cli *Client) publish(ctx context.Context, exchange, key string, msg *amqp.Publishing) error {
	if err := cli.establishChannel(); err != nil {
		return err
	}

	return cli.ch.PublishWithContext(
		ctx,
		exchange, // 默认 exchange
		key,      // routing key
		true,     // mandatory：当消息无法路由到队列时，会触发Return
		false,    // immediate：如果设置为true，消息会被立即投递到消费者
		*msg,     // 消息内容
	)
}

func (cli *Client) call(ctx context.Context, queue string, m any, opts ...CallOptionFunc) (string, error) {
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

	return o.id, cli.publish(ctx, ex, queue, &msg)
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
