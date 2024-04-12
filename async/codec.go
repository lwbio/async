package async

import (
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/lwbio/async/encoding"
	"github.com/rabbitmq/amqp091-go"
)

const (
	baseContentType = "application"
)

type DecodeRequestFunc func(r *amqp091.Delivery, v interface{}) error
type EncodeResponseFunc func(r *amqp091.Delivery, v interface{}) (rr *amqp091.Publishing, err error)

func ContentSubtype(contentType string) string {
	left := strings.Index(contentType, "/")
	if left == -1 {
		return ""
	}
	right := strings.Index(contentType, ";")
	if right == -1 {
		right = len(contentType)
	}
	if right < left {
		return ""
	}
	return contentType[left+1 : right]
}

// CodecForRequest get encoding.Codec via amqp091.Delivery
func CodecForRequest(r *amqp091.Delivery) (encoding.Codec, bool) {
	codec := encoding.GetCodec(ContentSubtype(r.ContentType))
	if codec != nil {
		return codec, true
	}

	return encoding.GetCodec("json"), false
}

// ContentType returns the content-type with base prefix.
func ContentType(subtype string) string {
	return strings.Join([]string{baseContentType, subtype}, "/")
}

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
		ReplyTo:       r.ReplyTo,
		Type:          r.Type,
		ContentType:   ContentType(codec.Name()),
		Body:          data,
	}, nil

}
