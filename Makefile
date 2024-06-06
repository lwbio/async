
WORKSPACE=$(shell pwd)


all:
	cd cmd/protoc-gen-go-async && \
	protoc --go_out=paths=source_relative:. async/async.proto && \
	cp -r async $(WORKSPACE)/annotations && \
	go build -o build/ && \
	cp build/protoc-gen-go-async ~/.go/bin/protoc-gen-go-async && \
	rm -rf build/ && \
	cd $(WORKSPACE)


api:
	protoc --proto_path=./async/example \
		   --proto_path=./annotations \
 	       --go_out=paths=source_relative:./async/example \
		   --go-async_out=paths=source_relative:./async/example \
	       api.proto