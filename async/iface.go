package async

import (
	"context"
	"errors"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
	ErrExchangeNotInit = errors.New("exchange not init")
)

type Handler1 func(ctx Context) error
type ResponseHandler func(ctx context.Context, resp proto.Message) error

type MQ interface {
	Channel() (*amqp091.Channel, error)
}

type PbEvent interface {
	String() string
	Descriptor() protoreflect.EnumDescriptor
	Number() protoreflect.EnumNumber
}
