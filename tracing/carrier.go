package tracing

import (
	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
)

type amqpHeaderCarrier struct {
	headers amqp091.Table
}

func (c *amqpHeaderCarrier) Get(key string) string {
	if value, ok := c.headers[key]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

func (c *amqpHeaderCarrier) Set(key, value string) {
	if c.headers == nil {
		c.headers = make(amqp091.Table)
	}
	c.headers[key] = value
}

func (c *amqpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.headers))
	for k := range c.headers {
		keys = append(keys, k)
	}
	return keys
}

func NewPublishingCarrier(msg *amqp091.Publishing) propagation.TextMapCarrier {
	if msg.Headers == nil {
		msg.Headers = make(amqp091.Table)
	}
	return &amqpHeaderCarrier{headers: msg.Headers}
}

func NewDeliveryCarrier(msg *amqp091.Delivery) propagation.TextMapCarrier {
	if msg.Headers == nil {
		msg.Headers = make(amqp091.Table)
	}
	return &amqpHeaderCarrier{headers: msg.Headers}
}
