#!/bin/sh

repo=$(git rev-parse --show-toplevel)

protoPath="$repo"
goImports="-I=$GOPATH/src"

GO111MODULE=off go get github.com/googleapis/googleapis/...

go install \
	google.golang.org/protobuf/cmd/protoc-gen-go \
	google.golang.org/grpc/cmd/protoc-gen-go-grpc

protoc $goImports -I$protoPath -I$GOPATH/src/github.com/googleapis/googleapis\
  --go_out=$GOPATH/src \
  --prokaf_out=$GOPATH/src \
  --go-grpc_out=$GOPATH/src $protoPath/prokaf.proto

protoc $goImports -I$protoPath -I$GOPATH/src/github.com/googleapis/googleapis\
  --go_out=$GOPATH/src \
  --prokaf_out=$GOPATH/src \
  --descriptor_set_out=$protoPath/test/descriptor/example.desc \
  --include_imports \
  --go-grpc_out=$GOPATH/src $protoPath/test/proto/example.proto
