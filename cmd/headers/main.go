package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/commands"
)

// generate the messages
//go:generate protoc --proto_path=../../interfaces/p2psentry --go_out=. --go-grpc_out=. "sentry.proto" -I=. -I=./../../build/include/google

func main() {
	commands.Execute()
}
