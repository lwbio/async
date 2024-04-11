package async

import (
	"context"
	"errors"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
	ErrExchangeNotInit = errors.New("exchange not init")
)

type Handler func(ctx context.Context, payload []byte) error

type MQ interface {
	Channel() (*amqp091.Channel, error)
}

type PbEvent interface {
	String() string
	Descriptor() protoreflect.EnumDescriptor
	Number() protoreflect.EnumNumber
}
