.PHONY: generate
generate:
	go generate ./...


.PHONY: protoc
protoc:
	protoc --gogofaster_out=$(GOPATH)/src types/types.proto
	protoc --gogofaster_out=$(GOPATH)/src consensus/types/types.proto
