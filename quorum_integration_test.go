package async

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This test pauses only the explicitly named disposable Docker test nodes.
func TestIntegrationQuorumAvailability(t *testing.T) {
	for _, publishers := range []int{1, 4} {
		t.Run(fmt.Sprintf("publishers_%d", publishers), func(t *testing.T) {
			testIntegrationQuorumAvailability(t, publishers)
		})
	}
}

func testIntegrationQuorumAvailability(t *testing.T, publishers int) {
	names := strings.Split(os.Getenv("ASYNC_TEST_QUORUM_NODES"), ",")
	if len(names) != 3 {
		t.Skip("set ASYNC_TEST_QUORUM_NODES to three disposable cluster container names")
	}
	r := integrationRuntime(t)
	r.cfg.PublishConcurrency = publishers
	var received atomic.Int32
	queue, err := r.Register(Subscription{Kind: Event, Identity: "cluster", Operations: []string{"work"}, Retries: []time.Duration{}, Handler: func(context.Context, Message) (*Message, error) { received.Add(1); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	docker := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v: %s", args, err, out)
		}
		return string(out)
	}
	expr := fmt.Sprintf(`{ok,Q}=rabbit_amqqueue:lookup({resource,<<"/">>,queue,<<"%s">>}), length(rabbit_quorum_queue:get_replicas(Q)).`, queue)
	if out := docker("exec", "--user", "rabbitmq", names[0], "rabbitmqctl", "eval", expr); strings.TrimSpace(out) != "3" {
		t.Fatalf("expected three actual queue replicas, got %s", out)
	}
	defer func() {
		for _, name := range names[1:] {
			_ = exec.Command("docker", "unpause", name).Run()
		}
	}()
	docker("pause", names[1])
	// Election may invalidate one attempt; a surviving majority must eventually confirm.
	waitFor(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := r.Publish(ctx, testMessage(Event, "work"), 0)
		return err == nil
	})
	waitFor(t, func() bool { return received.Load() > 0 })
	docker("pause", names[2])
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = r.Publish(ctx, testMessage(Event, "work"), 0)
	cancel()
	if err == nil {
		t.Fatal("published successfully without a quorum")
	}
	if !errors.Is(err, ErrUnknown) && !errors.Is(err, ErrUnavailable) && !errors.Is(err, ErrNack) {
		t.Fatalf("unexpected majority failure: %v", err)
	}
	docker("unpause", names[1])
	docker("unpause", names[2])
	waitFor(t, func() bool { return r.State() == Ready })
	waitFor(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := r.Publish(ctx, testMessage(Event, "work"), 0)
		return err == nil
	})
}
