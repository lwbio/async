package topology

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Subscription struct {
	Queue, Exchange, Key string
	Retries              []time.Duration
}

func RetryQueue(queue string, attempt int) string { return fmt.Sprintf("%s.retry.%d", queue, attempt) }
func DeadQueue(queue string) string               { return queue + ".dead" }

func Declare(ch *amqp.Channel, s Subscription) error {
	if s.Exchange != "" {
		if err := ch.ExchangeDeclare(s.Exchange, "topic", true, false, false, false, nil); err != nil {
			return err
		}
	}
	args := amqp.Table{"x-queue-type": "quorum", "x-overflow": "reject-publish", "x-delivery-limit": int32(-1)}
	for _, q := range []string{s.Queue, DeadQueue(s.Queue)} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, args); err != nil {
			return err
		}
	}
	if s.Exchange != "" {
		if err := ch.QueueBind(s.Queue, s.Key, s.Exchange, false, nil); err != nil {
			return err
		}
	}
	for i, delay := range s.Retries {
		a := amqp.Table{"x-queue-type": "quorum", "x-overflow": "reject-publish", "x-delivery-limit": int32(-1), "x-message-ttl": delay.Milliseconds(), "x-dead-letter-exchange": "", "x-dead-letter-routing-key": s.Queue, "x-dead-letter-strategy": "at-least-once"}
		if _, err := ch.QueueDeclare(RetryQueue(s.Queue, i+1), true, false, false, false, a); err != nil {
			return err
		}
	}
	return nil
}
