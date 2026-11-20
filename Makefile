.PHONY: all build test vet check generate plugin bench

all: build

build:
	go build ./...
	cd cmd/protoc-gen-go-async && go build ./...

test:
	go test -race ./...
	cd cmd/protoc-gen-go-async && go test -race ./...

vet:
	go vet ./...
	cd cmd/protoc-gen-go-async && go vet ./...

check: build test vet

# Set ASYNC_TEST_AMQP_URL for broker benchmarks; omit -race for capacity measurements.
bench:
	go test -run '^$$' -bench 'Benchmark(HandlerInstrumentation|RabbitMQ)' -benchmem -benchtime=5s ./...

plugin:
	mkdir -p build
	cd cmd/protoc-gen-go-async && go build -o ../../build/protoc-gen-go-async .

generate: plugin
	protoc -I annotations --go_out=paths=source_relative:annotations async/async.proto
	protoc -I annotations --go_out=paths=source_relative:cmd/protoc-gen-go-async async/async.proto
	protoc -I examples/echo -I annotations --go_out=paths=source_relative:examples/echo --plugin=protoc-gen-go-async=$(CURDIR)/build/protoc-gen-go-async --go-async_out=paths=source_relative:examples/echo echo.proto
