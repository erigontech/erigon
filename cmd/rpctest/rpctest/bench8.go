package rpctest

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"net/http"
	"time"
)

func Bench8() {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	turbogethURL := "http://localhost:8545"
	reqID := 1
	to := common.HexToAddress("0x9653c9859b18f8777fe4eec9a67c9f64f3d6f62a")

	reqID++
	template := `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x"}],"id":%d}`
	var logs EthLogs
	if err := post(client, turbogethURL, fmt.Sprintf(template, 49000, 49100, to, reqID), &logs); err != nil {
		fmt.Printf("Could not get eth_getLogs: %v\n", err)
		return
	}
	if logs.Error != nil {
		fmt.Printf("Error getting eth_getLogs: %d %s\n", logs.Error.Code, logs.Error.Message)
	}
}

