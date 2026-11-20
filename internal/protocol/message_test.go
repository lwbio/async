package protocol

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"
)

func TestWireRoundTripAndValidation(t *testing.T) {
	m := Message{ID: "request-1", Kind: Request, Operation: "orders.Create", ContentType: "application/json", ReplyTo: "results", Body: []byte(`{"id":1}`), Attempt: 2}
	p := m.Publishing()
	d := amqp.Delivery{MessageId: p.MessageId, CorrelationId: p.CorrelationId, ReplyTo: p.ReplyTo, ContentType: p.ContentType, Body: p.Body, Headers: p.Headers, Type: p.Type}
	got, err := Decode(d)
	if err != nil || got.ID != m.ID || got.Attempt != 2 || string(got.Body) != string(m.Body) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	d.Headers["async-version"] = int32(2)
	if _, err = Decode(d); err == nil {
		t.Fatal("accepted future protocol")
	}
	d.Headers["async-version"] = Version
	d.Headers["async-attempt"] = int32(-1)
	if _, err = Decode(d); err == nil {
		t.Fatal("accepted negative retry")
	}
}

func TestIdentityIsolation(t *testing.T) {
	if ResultID("one") != ResultID("one") || ResultID("one") == ResultID("two") {
		t.Fatal("unstable result identity")
	}
	if Queue("app", "event", "a.Handler") == Queue("app", "event", "b.Handler") {
		t.Fatal("package names lost")
	}
	if Queue("app", "result", "handler") == Queue("other", "result", "handler") {
		t.Fatal("namespace not isolated")
	}
}

func TestTopicPatterns(t *testing.T) {
	for _, tc := range []struct {
		pattern, key string
		want         bool
	}{{"orders.*", "orders.created", true}, {"orders.*", "orders.item.created", false}, {"orders.#", "orders", true}, {"orders.#", "orders.item.created", true}, {"#.created", "orders.item.created", true}, {"billing.#", "orders.created", false}} {
		if got := MatchTopic(tc.pattern, tc.key); got != tc.want {
			t.Errorf("%s matches %s = %v", tc.pattern, tc.key, got)
		}
	}
}
