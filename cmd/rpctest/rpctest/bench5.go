package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

func Bench5(erigonURL string) {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	file, err := os.Open("txs.txt")
	if err != nil {
		panic(err)
	}
	req_id := 0
	template := `{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0x%s"],"id":%d}`
	var receipt EthReceipt
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		req_id++
		if err = post(client, erigonURL, fmt.Sprintf(template, scanner.Text(), req_id), &receipt); err != nil {
			fmt.Printf("Count not get receipt: %s: %v\n", scanner.Text(), err)
			return
		}
		if receipt.Error != nil {
			fmt.Printf("Error getting receipt: %d %s\n", receipt.Error.Code, receipt.Error.Message)
			return
		}
	}
	err = scanner.Err()
	if err != nil {
		panic(err)
	}
}
