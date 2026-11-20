package async

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegrationBrokerRestart(t *testing.T) {
	for _, publishers := range []int{1, 4} {
		t.Run(fmt.Sprintf("publishers_%d", publishers), func(t *testing.T) {
			testIntegrationBrokerRestart(t, publishers)
		})
	}
}

func testIntegrationBrokerRestart(t *testing.T, publishers int) {
	node := os.Getenv("ASYNC_TEST_RESTART_NODE")
	if node == "" {
		t.Skip("set ASYNC_TEST_RESTART_NODE to a disposable Docker broker")
	}
	r := integrationRuntime(t)
	r.cfg.PublishConcurrency = publishers
	var received atomic.Int32
	_, err := r.Register(Subscription{Kind: Event, Identity: "restart", Operations: []string{"work"}, Handler: func(context.Context, Message) (*Message, error) { received.Add(1); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	if _, err = r.Publish(context.Background(), testMessage(Event, "work"), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return received.Load() == 1 })
	r.mu.Lock()
	old := r.gen
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", "restart", "--time", "2", node).CombinedOutput(); err != nil {
		t.Fatalf("broker restart: %v %s", err, output)
	}
	waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.state == Ready && r.gen != old })
	if _, err = r.Publish(context.Background(), testMessage(Event, "work"), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return received.Load() == 2 })
}
