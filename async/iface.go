package async

import (
	"errors"
)

const (
	DefaultDelayExchange = "delay"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
	ErrExchangeNotInit = errors.New("exchange not init")
)

type Handler func(ctx Context) error
