package async

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestIntegrationOptionalDelay(t *testing.T) {
	if os.Getenv("ASYNC_TEST_DELAY_PLUGIN") != "1" {
		t.Skip("set ASYNC_TEST_DELAY_PLUGIN=1 on a broker with the matching delay plugin")
	}
	r := integrationRuntime(t)
	r.cfg.EnableDelay = true
	received := make(chan time.Time, 1)
	_, err := r.Register(Subscription{Kind: Request, Operations: []string{"later"}, Handler: func(context.Context, Message) (*Message, error) { received <- time.Now(); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	startTest(t, r)
	start := time.Now()
	if _, err = r.Publish(context.Background(), testMessage(Request, "later"), 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-received:
		if at.Sub(start) < 250*time.Millisecond {
			t.Fatal("request did not respect delay")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delayed request never arrived")
	}
}
