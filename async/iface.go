package async

import (
	"errors"

	"github.com/rabbitmq/amqp091-go"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
	ErrExchangeNotInit = errors.New("exchange not init")
)

type Handler func(ctx Context) error

type MQ interface {
	Channel() (*amqp091.Channel, error)
}
