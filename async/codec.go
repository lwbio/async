package async

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/rabbitmq/amqp091-go"
)

type DecodeRequestFunc func(r *amqp091.Delivery, v interface{}) error
type EncodeResponseFunc func(r *amqp091.Delivery, v interface{}) (rr *amqp091.Publishing, err error)

// DefaultRequestDecoder decodes the request body to object.
func DefaultRequestDecoder(r *amqp091.Delivery, v interface{}) error {
	codec, ok := CodecForRequest(r)
	if !ok {
		return errors.BadRequest("CODEC", fmt.Sprintf("unregister Content-Type: %s", r.ContentType))
	}

	if err := codec.Unmarshal(r.Body, v); err != nil {
		return errors.BadRequest("CODEC", fmt.Sprintf("body unmarshal %s", err.Error()))
	}
	return nil
}

// DefaultResponseEncoder encodes the object to the HTTP response.
func DefaultResponseEncoder(r *amqp091.Delivery, v interface{}) (rr *amqp091.Publishing, err error) {
	if v == nil {
		return nil, nil
	}

	codec, _ := CodecForRequest(r)
	data, err := codec.Marshal(v)
	if err != nil {
		return nil, errors.InternalServer("CODEC", fmt.Sprintf("body marshal %s", err.Error()))
	}

	return &amqp091.Publishing{
		CorrelationId: r.CorrelationId,
		ContentType:   ContentType(codec.Name()),
		Body:          data,
	}, nil

}
