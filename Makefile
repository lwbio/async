API_PROTO_FILES=$(shell find example -name *.proto)
GOBIN	:=	$(shell echo ${GOBIN} | cut -d':' -f1)

all:
	cd cmd/protoc-gen-go-async && go build -o build/ && cd - > /dev/null

.PHONY: api
api:all
	cp ./cmd/protoc-gen-go-async/build/protoc-gen-go-async ~/.go/bin/protoc-gen-go-async

	protoc --proto_path=./example \
 	       --go_out=paths=source_relative:./example \
		   --go-async_out=paths=source_relative:./example \
	       $(API_PROTO_FILES)