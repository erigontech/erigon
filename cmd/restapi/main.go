package main

import "github.com/ledgerwatch/turbo-geth/cmd/restapi/commands"

func main() {
	err := commands.ServeREST("localhost:8080", "localhost:9999")
	if err != nil {
		panic(err)
	}
}
