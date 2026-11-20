// Package async provides a RabbitMQ-specific, confirmed messaging runtime.
package async

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/transport"
)

type Message = protocol.Message
type Kind = protocol.Kind

const (
	Request = protocol.Request
	Result  = protocol.Result
	Event   = protocol.Event
)

var (
	ErrUnavailable = transport.ErrUnavailable
	ErrUnknown     = transport.ErrUnknown
	ErrUnroutable  = transport.ErrUnroutable
	ErrNack        = transport.ErrNack
	ErrClosed      = errors.New("async: runtime closed")
)

// Failure contains a safe public error, never an internal error or stack trace.
type Failure struct{ Code, Message string }

func (e *Failure) Error() string { return e.Code + ": " + e.Message }
func Permanent(code, message string) error {
	if code == "" {
		code = "PERMANENT_FAILURE"
	}
	return &Failure{Code: code, Message: message}
}

type Handler func(context.Context, Message) (*Message, error)
type deliveryKey struct{}

// MessageFromContext exposes stable IDs and attempt metadata to typed handlers.
// Treat the returned message and its headers as read-only.
func MessageFromContext(ctx context.Context) (Message, bool) {
	m, ok := ctx.Value(deliveryKey{}).(Message)
	return m, ok
}

type Subscription struct {
	Identity string
	Kind     Kind
	// Operations are exact names; requests register exactly one operation.
	Operations            []string
	Handler               Handler
	Concurrency, Prefetch int
	// Nil selects 1s,10s,60s; an empty non-nil slice disables business retries.
	Retries []time.Duration
}

// HandlerIdentity uses the full package/function name; closures can collide.
func HandlerIdentity(handler any) (string, error) {
	v := reflect.ValueOf(handler)
	if !v.IsValid() || v.Kind() != reflect.Func || v.IsNil() {
		return "", fmt.Errorf("handler must be a non-nil function")
	}
	f := runtime.FuncForPC(v.Pointer())
	if f == nil {
		return "", fmt.Errorf("handler identity unavailable")
	}
	return strings.TrimSuffix(f.Name(), "-fm"), nil
}
