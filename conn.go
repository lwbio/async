package async

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Conn interface {
	Channel() (*amqp.Channel, error)
	Close() error
}

type Connection struct {
	addr       string
	shutdown   atomic.Bool
	conn       *amqp.Connection
	closeErrCh chan *amqp.Error
	log        *log.Helper
	mu         sync.Mutex
}

func NewConnection(addr string, logger log.Logger) (Conn, error) {
	c := &Connection{
		addr:       addr,
		closeErrCh: make(chan *amqp.Error, 1),
		log:        log.NewHelper(logger),
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	go c.connector()
	return c, nil
}

func (c *Connection) connect() error {
	if c.shutdown.Load() {
		return nil
	}

	conn, err := amqp.Dial(c.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.conn.NotifyClose(c.closeErrCh)
	c.log.Info("Connected to AMQP broker.")
	return nil
}

func (c *Connection) connector() {
	backoff := time.Second

	for err := range c.closeErrCh {
		if err == nil || c.shutdown.Load() {
			c.log.Info("AMQP connection closed or shutdown requested.")
			return
		}

		c.log.Errorf("Connection lost: %v", err)

		for {
			if c.shutdown.Load() {
				c.log.Info("Connector exiting due to shutdown.")
				return
			}

			time.Sleep(backoff)

			if err := c.connect(); err != nil {
				c.log.Errorf("Reconnection failed: %v", err)
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				continue
			}

			c.log.Info("Reconnected to AMQP broker successfully.")
			backoff = time.Second
			break
		}
	}
}

func (c *Connection) Channel() (*amqp.Channel, error) {
	if c.shutdown.Load() {
		return nil, fmt.Errorf("connection is shutdown")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}
	return ch, nil
}

func (c *Connection) Close() error {
	if c.shutdown.CompareAndSwap(false, true) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.conn != nil {
			return c.conn.Close()
		}
	}
	return nil
}
