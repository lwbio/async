package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lwbio/async/internal/maintenance"
	"github.com/lwbio/async/internal/protocol"
	"github.com/lwbio/async/internal/topology"
	"github.com/lwbio/async/internal/transport"
)

func TestUnsafeCommandsRejectedBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"unknown"},
		{"inspect", "--apply"},
		{"replay", "--apply"},
		{"replay", "--apply", "--actor", "operator"},
		{"inspect", "--timeout", "-1s"},
		{"inspect", "extra"},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(context.Background(), args, "invalid://private-secret", &stdout, &stderr); err == nil {
			t.Fatalf("accepted unsafe command: %v", args)
		}
		if bytes.Contains(stdout.Bytes(), []byte("private-secret")) || bytes.Contains(stderr.Bytes(), []byte("private-secret")) {
			t.Fatal("leaked broker URL")
		}
	}
}

func TestIntegrationCLIReplayUsesPrivateDurableAudit(t *testing.T) {
	url := os.Getenv("ASYNC_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires disposable RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := transport.Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ch, err := s.Conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	namespace := "ctl_" + uuid.NewString()
	target := protocol.Queue(namespace, "request", "work")
	if err := topology.Declare(ch, topology.Subscription{Queue: target}); err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(target, false, false, false)
	defer ch.QueueDelete(topology.DeadQueue(target), false, false, false)
	m := protocol.Message{ID: "id", Kind: protocol.Request, Operation: "work", Attempt: 3,
		ContentType: "application/json", Body: []byte(`{"secret":"not-in-output"}`)}
	if err := s.Publish(ctx, "", topology.DeadQueue(target), true, m.Publishing()); err != nil {
		t.Fatal(err)
	}
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	var stdout, stderr bytes.Buffer
	err = run(ctx, []string{"replay", "--namespace", namespace, "--kind", "request", "--identity", "work",
		"--apply", "--actor", "operator", "--audit", auditPath, "--reset-retries"}, url, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var report maintenance.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || report.Moved != 1 {
		t.Fatalf("CLI progress: %+v %v", report, err)
	}
	info, err := os.Stat(auditPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("audit permissions: %v %v", info, err)
	}
	audit, err := os.ReadFile(auditPath)
	if err != nil || bytes.Count(audit, []byte("\n")) != 3 {
		t.Fatalf("audit phases: %v", err)
	}
	if bytes.Contains(stdout.Bytes(), []byte("not-in-output")) || bytes.Contains(audit, []byte("not-in-output")) ||
		bytes.Contains(stdout.Bytes(), []byte(url)) || bytes.Contains(audit, []byte(url)) {
		t.Fatal("CLI leaked payload or credentials")
	}
	d, ok, err := ch.Get(target, true)
	if err != nil || !ok || d.MessageId != m.ID || d.Headers["async-attempt"] != int32(0) {
		t.Fatalf("CLI replay lost identity: %+v %v", d, err)
	}
}
