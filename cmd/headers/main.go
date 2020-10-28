package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/commands"
)

// generate the messages
//go:generate protoc --proto_path=../../interfaces --go_out=. "p2psentry/control.proto" -I=. -I=./../../build/include/google
//go:generate protoc --proto_path=../../interfaces --go_out=. "../../interfaces/p2psentry/sentry.proto" -I=. -I=./../../build/include/google

// generate the services
//go:generate protoc --proto_path=../../interfaces --go-grpc_out=. "p2psentry/control.proto" -I=. -I=./../../build/include/google
//go:generate protoc --proto_path=../../interfaces --go-grpc_out=. "p2psentry/sentry.proto" -I=. -I=./../../build/include/google

func main() {
	commands.Execute()
}
