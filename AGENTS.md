# Repository Guidelines

## Project Structure & Module Organization

This is a RabbitMQ-specific Go messaging library. Root files (`runtime.go`, `consumer.go`, `message.go`) implement the shared runtime. `request/` provides asynchronous requests and result subscriptions; `pubsub/` provides events. `internal/transport/`, `internal/topology/`, and `internal/protocol/` own confirmed publishing, queue declarations, and wire encoding. `encoding/` and `tracing/` provide codecs and OpenTelemetry integration.

`cmd/protoc-gen-go-async/` is a separate Go module. The canonical annotation schema is `annotations/async/async.proto`; the generator also carries generated annotation bindings. `examples/echo/` contains generated bindings, and `examples/echoapp/` is runnable. Consult `docs/architecture.md` for module responsibilities, `docs/protocol.md` when changing messages, and `docs/operations.md` for broker requirements. Record agreed terminology in `GLOSSARY.md` and consequential design decisions in `docs/adr/`.

## Build, Test, and Development Commands

Use the toolchain declared in root `go.mod`.

- `make build`: build the library and generator modules.
- `make test`: run both modules' tests with race detection.
- `make vet`: run Go static checks in both modules.
- `make check`: run all three checks.
- `make generate`: build the local generator and regenerate annotations and the example; requires `protoc` and `protoc-gen-go`.
- `AMQP_URL=... go run ./examples/echoapp`: exercise a complete request/result flow.

## Coding Style & Naming Conventions

Run `gofmt` on changed Go files. Follow Go naming conventions and existing functional options such as `WithSubscription`. Keep raw AMQP channels inside runtime internals. Regenerate derived protobuf files from schemas or generator templates instead of editing them manually. Preserve explicit message identity, error classification, and publication uncertainty across interfaces.

## Testing Guidelines

Use standard Go `testing`, colocated `*_test.go` files, and `TestXxx` names. No numeric coverage threshold is enforced. Add behavioral regression tests for delivery, recovery, cancellation, and protocol changes.

Set `ASYNC_TEST_AMQP_URL` to a disposable RabbitMQ 4.2+ broker for integration tests; otherwise they skip. These tests create queues and may delete their own destinations. `scripts/test-cluster.sh` creates and cleans up an isolated three-node cluster for quorum fault tests. Root `go test ./...` excludes the generator module.

## Commit & Pull Request Guidelines

History uses short Chinese and English subjects without mandatory prefixes. Keep commits focused. PRs should explain behavior changes, link relevant issues, and report actual validation results, skipped integration checks, protocol changes, and deployment requirements.
