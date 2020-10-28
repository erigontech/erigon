package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/commands"
)

// generate the messages
//go:generate protoc --proto_path=../../interfaces/p2psentry --go_out=./core --go-grpc_out=./core "control.proto" -I=. -I=./../../build/include/google
//go:generate protoc --proto_path=../../interfaces/p2psentry --go_out=./sentry --go-grpc_out=./sentry "sentry.proto" -I=. -I=./../../build/include/google

func main() {
	commands.Execute()
}
