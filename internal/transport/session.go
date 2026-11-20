// Package transport owns a connection generation and its confirmed publisher.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrUnavailable = errors.New("async: unavailable")
	ErrUnknown     = errors.New("async: publish outcome unknown")
	ErrUnroutable  = errors.New("async: message unroutable")
	ErrNack        = errors.New("async: broker rejected publish")
)

type Session struct {
	Conn *amqp.Connection
	// pub aliases the first lane for channel-failure diagnostics.
	pub    *amqp.Channel
	raw    net.Conn
	closed chan *amqp.Error
	lanes  chan *publishLane
	once   sync.Once
}

type publishLane struct {
	pub      *amqp.Channel
	confirm  <-chan amqp.Confirmation
	returned <-chan amqp.Return
}

func Dial(ctx context.Context, url string) (*Session, error) {
	return DialWithPublishers(ctx, url, 1)
}

// DialWithPublishers bounds parallel confirms. Each channel has exactly one
// in-flight publish, so mandatory Return and confirm remain unambiguous.
func DialWithPublishers(ctx context.Context, url string, publishers int) (*Session, error) {
	if publishers < 1 || publishers > 32 {
		return nil, fmt.Errorf("publishers must be between 1 and 32")
	}
	s := &Session{lanes: make(chan *publishLane, publishers)}
	var stop func() bool
	c, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		s.raw = conn
		deadline := time.Now().Add(10 * time.Second)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		_ = conn.SetDeadline(deadline)
		stop = context.AfterFunc(ctx, func() { _ = conn.Close() })
		return conn, nil
	}})
	if stop != nil {
		defer stop()
	}
	if err != nil {
		if s.raw != nil {
			_ = s.raw.Close()
		}
		return nil, err
	}
	s.Conn = c
	_ = s.raw.SetDeadline(time.Time{})
	s.closed = c.NotifyClose(make(chan *amqp.Error, 1))
	for i := 0; i < publishers; i++ {
		pub, err := c.Channel()
		if err != nil {
			s.Close()
			return nil, err
		}
		if err := pub.Confirm(false); err != nil {
			s.Close()
			return nil, err
		}
		if i == 0 {
			s.pub = pub
		}
		s.lanes <- &publishLane{pub: pub,
			confirm:  pub.NotifyPublish(make(chan amqp.Confirmation, 1)),
			returned: pub.NotifyReturn(make(chan amqp.Return, 1))}
		pubClosed := pub.NotifyClose(make(chan *amqp.Error, 1))
		go func() { <-pubClosed; s.Close() }()
	}
	return s, nil
}

func (s *Session) Done() <-chan *amqp.Error { return s.closed }
func (s *Session) Close()                   { s.once.Do(func() { _ = s.raw.Close() }) }

func (s *Session) Drain(ctx context.Context) {
	held := make([]*publishLane, 0, cap(s.lanes))
	defer func() {
		for _, lane := range held {
			s.lanes <- lane
		}
	}()
	for len(held) < cap(s.lanes) {
		select {
		case lane := <-s.lanes:
			held = append(held, lane)
		case <-ctx.Done():
			return
		}
	}
}

// Publish serializes one in-flight message per channel so returns cannot be misattributed.
// Cancellation after submission invalidates the generation before releasing the lane.
func (s *Session) Publish(ctx context.Context, exchange, key string, mandatory bool, p amqp.Publishing) error {
	return s.PublishObserved(ctx, exchange, key, mandatory, p, nil)
}

// PublishObserved reports lane wait even when the caller cancels before admission.
// observeWait must not block or call back into the session.
func (s *Session) PublishObserved(ctx context.Context, exchange, key string, mandatory bool, p amqp.Publishing, observeWait func(time.Duration)) error {
	start := time.Now()
	var lane *publishLane
	select {
	case lane = <-s.lanes:
	case <-ctx.Done():
		if observeWait != nil {
			observeWait(time.Since(start))
		}
		return ctx.Err()
	}
	defer func() { s.lanes <- lane }()
	if observeWait != nil {
		observeWait(time.Since(start))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Conn.IsClosed() || lane.pub.IsClosed() {
		s.Close()
		return ErrUnavailable
	}
	sent := make(chan error, 1)
	go func() { sent <- lane.pub.PublishWithContext(ctx, exchange, key, mandatory, false, p) }()
	select {
	case err := <-sent:
		if err != nil {
			s.Close()
			return fmt.Errorf("%w: %v", ErrUnknown, err)
		}
	case <-ctx.Done():
		s.Close()
		return fmt.Errorf("%w: %v", ErrUnknown, ctx.Err())
	}
	var ret *amqp.Return
	for {
		select {
		case r, ok := <-lane.returned:
			if !ok {
				s.Close()
				return ErrUnknown
			}
			ret = &r
		case c, ok := <-lane.confirm:
			if !ok {
				s.Close()
				return ErrUnknown
			}
			// RabbitMQ sends Return before confirm, but select may choose confirm first.
			select {
			case r, ok := <-lane.returned:
				if ok {
					ret = &r
				}
			default:
			}
			if ret != nil {
				return fmt.Errorf("%w: %d %s", ErrUnroutable, ret.ReplyCode, ret.ReplyText)
			}
			if !c.Ack {
				return ErrNack
			}
			return nil
		case <-ctx.Done():
			s.Close()
			return fmt.Errorf("%w: %v", ErrUnknown, ctx.Err())
		}
	}
}
