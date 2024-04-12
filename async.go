package async

import "github.com/rabbitmq/amqp091-go"

type Conn interface {
	Channel() (*amqp091.Channel, error)
}
