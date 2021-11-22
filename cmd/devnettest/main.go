package main

import (
	"flag"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
)

func main() {
	var (
		to            string
		value         uint64
		blockNum      string
		getBalance    bool
		sendTx        bool
		txPoolContent bool
		clearDev      bool
	)

	flag.StringVar(&to, "to", "", "String Address to send to")
	flag.Uint64Var(&value, "value", uint64(0), "Uint64 Value to send")
	flag.StringVar(&blockNum, "block-num", "latest", "String denoting block number")
	flag.BoolVar(&getBalance, "get-balance", false, "Boolean Flag to determine if API should get balance")
	flag.BoolVar(&sendTx, "send-tx", false, "Boolean Flag to determine if API should send transaction")
	flag.BoolVar(&txPoolContent, "txpool-content", false, "Boolean Flag to determine if API should get content of txpool")
	flag.BoolVar(&clearDev, "clear-dev", false, "Boolean Flag to determine if service should clear /dev after this call")
	flag.Parse()

	//fmt.Printf("to: %v\n", to)
	//fmt.Printf("value: %v\n", value)
	//fmt.Printf("blockNum: %v\n", blockNum)
	//fmt.Printf("getBalance: %v\n", getBalance)
	//fmt.Printf("sendTx: %v\n", sendTx)
	//fmt.Printf("txPoolContent: %v\n", txPoolContent)
	//fmt.Printf("clearDev: %v\n", clearDev)

	services.ValidateInputs(&getBalance, &sendTx, &txPoolContent, &blockNum, &value, &to)

	services.ParseRequests(&getBalance, &sendTx, &txPoolContent, &clearDev, &blockNum, &value, &to)

	fmt.Print("\n")
	fmt.Print("Finished processing\n")
}
