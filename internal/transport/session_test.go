package transport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublishObservedCanceledLaneWait(t *testing.T) {
	s := &Session{lanes: make(chan *publishLane, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := s.PublishObserved(ctx, "", "", true, amqp.Publishing{}, func(wait time.Duration) {
		calls++
		if wait < 0 {
			t.Fatal("negative wait")
		}
	})
	if err != context.Canceled || calls != 1 || len(s.lanes) != 0 {
		t.Fatalf("canceled wait: err=%v calls=%d", err, calls)
	}
}

func TestDrainCanceledPoolRestoresAcquiredLanes(t *testing.T) {
	s := &Session{lanes: make(chan *publishLane, 2)}
	lane := &publishLane{}
	s.lanes <- lane // The other lane represents a still-running publish.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	s.Drain(ctx)
	if len(s.lanes) != 1 || <-s.lanes != lane {
		t.Fatal("drain lost a previously acquired lane")
	}
}

func TestIntegrationConcurrentReturnsStayOnTheirOwnPublishLane(t *testing.T) {
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires test RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := DialWithPublishers(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ch, err := s.Conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			key := q.Name
			if i%2 != 0 {
				key += ".missing"
			}
			err := s.Publish(ctx, "", key, true, amqp.Publishing{MessageId: fmt.Sprint(i), Body: []byte("test")})
			if (i%2 == 0 && err != nil) || (i%2 != 0 && !errors.Is(err, ErrUnroutable)) {
				t.Errorf("publication %d misclassified: %v", i, err)
			}
		}()
	}
	workers.Wait()
	q, err = ch.QueueInspect(q.Name)
	if err != nil || q.Messages != 50 {
		t.Fatalf("routed messages: %+v %v", q, err)
	}
	s.Drain(ctx)
	if len(s.lanes) != 4 {
		t.Fatal("pool did not fully drain")
	}
}

func TestIntegrationPublisherChannelCloseInvalidatesSession(t *testing.T) {
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires test RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.pub.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Done():
	case <-ctx.Done():
		t.Fatal("publisher channel closed without invalidating connection generation")
	}
}
