package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	async "github.com/lwbio/async"
	"github.com/lwbio/async/examples/echo"
	"github.com/lwbio/async/request"
)

type service struct{}

func (*service) Say(_ context.Context, in *echo.EchoRequest) (*echo.EchoResponse, error) {
	return &echo.EchoResponse{Message: in.Message}, nil
}

type sink struct{ done chan struct{} }

func (s *sink) receive(_ context.Context, result request.Result[echo.EchoResponse]) error {
	if result.Failure != nil {
		fmt.Println(result.Failure)
	} else if result.Value != nil {
		fmt.Println(result.RequestID, result.Value.Message)
	}
	select {
	case s.done <- struct{}{}:
	default:
	}
	return nil
}

func run() error {
	r, err := async.New(async.Config{URL: os.Getenv("AMQP_URL"), Namespace: "echo_demo"})
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
	}()
	if err = echo.RegisterEchoAsyncServer(r, &service{}); err != nil {
		return err
	}
	s := &sink{done: make(chan struct{}, 1)}
	target, err := request.Listen(r, s.receive)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = r.Start(ctx); err != nil {
		return err
	}
	_, err = echo.NewEchoAsyncClient(r).Say(ctx, &echo.EchoRequest{Message: "hello async"}, request.WithReplyTo(target))
	if err != nil {
		return err
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
