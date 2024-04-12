package pubsub

import (
	"context"
	"errors"

	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
	ErrExchangeNotInit = errors.New("exchange not init")
)

type Handler func(ctx context.Context, payload []byte) error

type PbEvent interface {
	String() string
	Descriptor() protoreflect.EnumDescriptor
	Number() protoreflect.EnumNumber
}
