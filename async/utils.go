package async

import (
	"strings"

	"github.com/lwbio/async/encoding"
	"github.com/rabbitmq/amqp091-go"
)

const (
	baseContentType = "application"
)

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
