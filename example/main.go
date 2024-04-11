package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/lwbio/async/async"
	"github.com/rabbitmq/amqp091-go"
)

type Service struct{}

// Sub implements ServiceAsyncServer.
func (s *Service) Sub(context.Context, *SubRequest) (*SubResponse, error) {
	panic("unimplemented")
}

func (s *Service) Add(ctx context.Context, req *AddRequest) (*AddResponse, error) {
	time.Sleep(2 * time.Second)
	return &AddResponse{
		Sum: req.A + req.B,
	}, nil
}

func client(ctx context.Context, conn *amqp091.Connection) error {
	println("client start...")
	defer println("client stop!")
	client, err := async.NewClient(conn, log.DefaultLogger)
	if err != nil {
		return err
	}
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			req := AddRequest{
				A: int64(rand.Intn(100)),
				B: int64(rand.Intn(100)),
			}
			if id, err := client.DirectCall1(ctx, AsyncOperationServiceAdd, &req); err != nil {
				return err
			} else {
				fmt.Printf("call [%s]: %d + %d\n", id, req.A, req.B)
			}
		}
	}
}

func server(ctx context.Context, conn *amqp091.Connection) error {
	println("server start...")
	defer println("server stop!")
	server, err := async.NewServer(conn)
	if err != nil {
		return err
	}
	svc := &Service{}
	RegisterServiceAsyncServer(server, svc)
	return server.Start(ctx)
}

func main() {
	println("Hello, World!")
	conn, err := amqp091.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := client(ctx, conn); err != nil {
			fmt.Printf("%s\n", err.Error())
		}
	}()

	go func() {
		defer wg.Done()
		server(ctx, conn)
	}()

	wg.Wait()

	println("done!")
}
