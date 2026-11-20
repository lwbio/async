// Package protocol defines the versioned RabbitMQ wire format.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

const Version = int32(1)
const prefix = "async-"

type Kind string

const (
	Request Kind = "request"
	Result  Kind = "result"
	Event   Kind = "event"
)

type Message struct {
	ID, CorrelationID, Operation, ReplyTo, ContentType string
	Kind                                               Kind
	Body                                               []byte
	Attempt                                            int32
	Redelivered                                        bool
	ErrorCode, ErrorMessage                            string
	Headers                                            amqp.Table
}

func (m Message) Validate() error {
	if m.ID == "" || m.Operation == "" {
		return fmt.Errorf("message ID and operation are required")
	}
	if strings.ContainsAny(m.Operation, "*#") {
		return fmt.Errorf("message operation must be an exact name")
	}
	if m.Kind != Request && m.Kind != Result && m.Kind != Event {
		return fmt.Errorf("invalid message kind %q", m.Kind)
	}
	if m.Attempt < 0 {
		return fmt.Errorf("negative attempt")
	}
	if m.Kind == Result && m.CorrelationID == "" {
		return fmt.Errorf("result requires correlation ID")
	}
	if m.Kind != Request && m.ReplyTo != "" {
		return fmt.Errorf("only requests may specify a result target")
	}
	if m.Kind != Result && (m.ErrorCode != "" || m.ErrorMessage != "") {
		return fmt.Errorf("only results may carry an error")
	}
	if !strings.HasPrefix(m.ContentType, "application/") {
		return fmt.Errorf("invalid content type")
	}
	return nil
}

func (m Message) Publishing() amqp.Publishing {
	h := make(amqp.Table, len(m.Headers)+6)
	for k, v := range m.Headers {
		h[k] = v
	}
	h[prefix+"version"] = Version
	h[prefix+"kind"] = string(m.Kind)
	h[prefix+"operation"] = m.Operation
	h[prefix+"attempt"] = m.Attempt
	h[prefix+"error-code"] = m.ErrorCode
	h[prefix+"error-message"] = m.ErrorMessage
	return amqp.Publishing{MessageId: m.ID, CorrelationId: m.CorrelationID, ReplyTo: m.ReplyTo, ContentType: m.ContentType, Type: string(m.Kind), Body: m.Body, Headers: h, DeliveryMode: amqp.Persistent}
}

func Decode(d amqp.Delivery) (Message, error) {
	version, ok := d.Headers[prefix+"version"].(int32)
	if !ok || version != Version {
		return Message{}, fmt.Errorf("unsupported protocol version")
	}
	kind, _ := d.Headers[prefix+"kind"].(string)
	if d.Type != kind {
		return Message{}, fmt.Errorf("AMQP type differs from protocol kind")
	}
	op, _ := d.Headers[prefix+"operation"].(string)
	attempt, ok := d.Headers[prefix+"attempt"].(int32)
	if !ok {
		return Message{}, fmt.Errorf("invalid attempt")
	}
	code, _ := d.Headers[prefix+"error-code"].(string)
	message, _ := d.Headers[prefix+"error-message"].(string)
	m := Message{ID: d.MessageId, CorrelationID: d.CorrelationId, Operation: op, ReplyTo: d.ReplyTo, ContentType: d.ContentType, Kind: Kind(kind), Body: d.Body, Attempt: attempt, ErrorCode: code, ErrorMessage: message, Headers: d.Headers}
	m.Redelivered = d.Redelivered
	return m, m.Validate()
}

func ResultID(requestID string) string {
	sum := sha256.Sum256([]byte("async.v1.result\x00" + requestID))
	return hex.EncodeToString(sum[:])
}

func Queue(namespace, kind, identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return namespace + ".v1." + kind + "." + hex.EncodeToString(sum[:16])
}

// MatchTopic uses RabbitMQ's dot-separated * (one word) and # (zero or more).
func MatchTopic(pattern, key string) bool {
	p, k := strings.Split(pattern, "."), strings.Split(key, ".")
	previous := make([]bool, len(k)+1)
	previous[0] = true
	for _, part := range p {
		next := make([]bool, len(k)+1)
		if part == "#" {
			next[0] = previous[0]
		}
		for j := 1; j <= len(k); j++ {
			if part == "#" {
				next[j] = previous[j] || next[j-1]
			} else {
				next[j] = previous[j-1] && (part == "*" || part == k[j-1])
			}
		}
		previous = next
	}
	return previous[len(k)]
}
