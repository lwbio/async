package echo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	async "github.com/lwbio/async"
	"github.com/lwbio/async/request"
)

type echoService struct{}

func (echoService) Say(_ context.Context, in *EchoRequest) (*EchoResponse, error) {
	return &EchoResponse{Message: in.Message}, nil
}

func TestGeneratedRoundTrip(t *testing.T) {
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires test RabbitMQ")
	}
	for _, codec := range []string{"json", "proto"} {
		t.Run(codec, func(t *testing.T) {
			r, err := async.New(async.Config{URL: url, Namespace: "echo_" + uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = r.Stop(ctx)
			}()
			if err = RegisterEchoAsyncServer(r, echoService{}); err != nil {
				t.Fatal(err)
			}
			results := make(chan request.Result[EchoResponse], 1)
			target, err := r.Register(async.Subscription{Kind: async.Result, Identity: "results", Handler: func(_ context.Context, m async.Message) (*async.Message, error) {
				result, err := DecodeEchoSayResult(m)
				if err == nil {
					results <- result
				}
				return nil, err
			}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err = r.Start(ctx); err != nil {
				t.Fatal(err)
			}
			id, err := NewEchoAsyncClient(r).Say(ctx, &EchoRequest{Message: "typed"}, request.WithReplyTo(target), request.WithCodec(codec))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-results:
				if result.RequestID != id || result.Value == nil || result.Value.Message != "typed" {
					t.Fatalf("bad result %+v", result)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
