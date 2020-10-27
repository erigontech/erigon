package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/commands"
)

// generate the messages
//go:generate protoc --go_out=. "./proto/control.proto" -I=. -I=./../../build/include/google
//go:generate protoc --go_out=. "./proto/sentry.proto" -I=. -I=./../../build/include/google

// generate the services
//go:generate protoc --go-grpc_out=. "./proto/control.proto" -I=. -I=./../../build/include/google
//go:generate protoc --go-grpc_out=. "./proto/sentry.proto" -I=. -I=./../../build/include/google

func main() {
	commands.Execute()
}
