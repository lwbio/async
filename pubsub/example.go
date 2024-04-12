package async

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Async struct{}

func (a *Async) Channel() (*amqp.Channel, error) {
	return nil, nil
}

type Service struct {
}

func (s *Service) Test(ctx context.Context, body []byte) error {
	return nil
}

func NewSubscriberServer(mq MQ, svc *Service, logger log.Logger) *Subscriber {
	var opts = []SubscriberOptionFunc{
		WithSubscriberLogger(log.With(logger, "module", "server/sub")),
	}
	srv, err := NewSubscriber(mq, opts...)
	if err != nil {
		panic(err)
	}

	// 注册处理函数
	srv.MustRegister(svc.Test, nil)

	return srv
}
