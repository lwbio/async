package async_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/google/uuid"
	async "github.com/lwbio/async"
)

func TestKratosStartupFailure(t *testing.T) {
	r, err := async.New(async.Config{URL: "://invalid", Namespace: "kratos_test"})
	if err != nil {
		t.Fatal(err)
	}
	app := kratos.New(kratos.Server(r), kratos.StopTimeout(time.Second))
	t.Cleanup(func() { _ = app.Stop() })
	done := make(chan error, 1)
	go func() { done <- app.Run() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Kratos did not propagate startup failure")
		}
		if r.State() != async.Closed {
			t.Fatalf("runtime state: %s", r.State())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Kratos did not exit after startup failure")
	}
}

func TestIntegrationKratosLifecycle(t *testing.T) {
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires test RabbitMQ")
	}
	r, err := async.New(async.Config{URL: url, Namespace: "kratos_" + uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan struct{}, 1)
	_, err = r.Register(async.Subscription{Kind: async.Event, Operations: []string{"kratos.test"}, Handler: func(context.Context, async.Message) (*async.Message, error) {
		received <- struct{}{}
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	app := kratos.New(kratos.Server(r), kratos.StopTimeout(3*time.Second))
	t.Cleanup(func() { _ = app.Stop() })
	done := make(chan error, 1)
	go func() { done <- app.Run() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for r.State() != async.Ready {
		select {
		case err := <-done:
			t.Fatalf("app exited before ready: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	_, err = r.Publish(ctx, async.Message{Kind: async.Event, Operation: "kratos.test", ContentType: "application/json", Body: []byte(`{}`)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("non-blocking Start terminated the app: %v", err)
	default:
	}
	if err = app.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
		if r.State() != async.Closed {
			t.Fatalf("runtime state: %s", r.State())
		}
	case <-ctx.Done():
		t.Fatal("Kratos did not stop the runtime")
	}
}
