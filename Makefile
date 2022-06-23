PHONY: protos
protos:
	@./build/protos.sh

PHONY: install
install:
	@go build -o protoc-gen-prokaf *.go
	@mv protoc-gen-prokaf ${GOBIN}/protoc-gen-prokaf
