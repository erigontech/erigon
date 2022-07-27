package main

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnettest/commands"
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
)

func main() {
	defer services.ClearDevDB()

	erigon.StartProcess()

	time.Sleep(10 * time.Second)

	fmt.Printf("SUCCESS => Started!\n\n")
	err := commands.Execute()
	if err != nil {
		panic(err)
	}
}
