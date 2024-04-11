package async

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var _ Context = (*wrapper)(nil)

// Context is an HTTP Context.
type Context interface {
	context.Context
	Encode(interface{}) error
	Decode(interface{}) error
}

type wrapper struct {
	srv  *Server
	ctx  context.Context
	req  *amqp091.Delivery
	resp *amqp091.Publishing
}

func (c *wrapper) Deadline() (time.Time, bool) {
	if c.ctx == nil {
		return time.Time{}, false
	}
	return c.ctx.Deadline()
}

func (c *wrapper) Done() <-chan struct{} {
	if c.ctx == nil {
		return nil
	}
	return c.ctx.Done()
}

func (c *wrapper) Err() error {
	if c.ctx == nil {
		return context.Canceled
	}
	return c.ctx.Err()
}

func (c *wrapper) Value(key interface{}) interface{} {
	if c.ctx == nil {
		return nil
	}
	return c.ctx.Value(key)
}

func (c *wrapper) Encode(v interface{}) (err error) {
	c.resp, err = c.srv.enc(c.req, v)
	return
}

func (c *wrapper) Decode(v interface{}) error {
	return c.srv.dec(c.req, v)
}
