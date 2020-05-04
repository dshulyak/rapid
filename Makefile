.PHONY: generate
generate:
	go generate ./...


.PHONY: protoc
protoc:
	protoc --gogofaster_out=$(GOPATH)/src types/types.proto
	protoc --gogofaster_out=$(GOPATH)/src consensus/types/types.proto
	protoc --gogofaster_out=plugins=grpc:$(GOPATH)/src consensus/swarms/grpc/service/service.proto
	protoc --gogofaster_out=$(GOPATH)/src monitor/types/types.proto
