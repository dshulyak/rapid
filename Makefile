.PHONY: generate
generate:
	go generate ./...


.PHONY: protoc
protoc:
	protoc --gogofaster_out=$(GOPATH)/src types/types.proto
	protoc --gogofaster_out=plugins=grpc:$(GOPATH)/src network/grpc/service/service.proto


.PHONY: build
build:
	mkdir -p ./build
	go build -o ./build/rapid ./example
