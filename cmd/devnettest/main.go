package main

import (
	"github.com/ledgerwatch/erigon/cmd/devnettest/commands"
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
	"time"
)

func main() {
	erigon.StartProcess()

	/*
		Execute all eth_methods here

		Start with running get-balance: this is the balance of the account
		Run send-tx: Send Ether to account provided in code, with tx mining
		Run get-balance: Check that amount returned from get-balance is updated
	*/
	time.Sleep(3 * time.Second)
	err := commands.Execute()
	if err != nil {
		panic(err)
	}

	//Uncomment below line to run shell commands instead
	//Note: Have to comment commands.Execute()
	//shell.Execute()
}
