package rpctest

import (
	"fmt"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func Bench6(erigon_url string) {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	req_id := 0

	req_id++
	template := `
{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}
`
	var blockNumber EthBlockNumber
	if err := post(client, erigon_url, fmt.Sprintf(template, req_id), &blockNumber); err != nil {
		fmt.Printf("Could not get block number: %v\n", err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)
	accounts := make(map[libcommon.Address]struct{})
	firstBn := 100000
	for bn := firstBn; bn <= int(lastBlock); bn++ {
		req_id++
		template := `
{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}
`
		var b EthBlockByNumber
		if err := post(client, erigon_url, fmt.Sprintf(template, bn, req_id), &b); err != nil {
			fmt.Printf("Could not retrieve block %d: %v\n", bn, err)
			return
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}
		accounts[b.Result.Miner] = struct{}{}
		for _, tx := range b.Result.Transactions {
			accounts[tx.From] = struct{}{}
			if tx.To != nil {
				accounts[*tx.To] = struct{}{}
			}
			req_id++
			template = `
{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":%d}
`
			var receipt EthReceipt
			if err := post(client, erigon_url, fmt.Sprintf(template, tx.Hash, req_id), &receipt); err != nil {
				fmt.Printf("Count not get receipt: %s: %v\n", tx.Hash, err)
				print(client, erigon_url, fmt.Sprintf(template, tx.Hash, req_id))
				return
			}
			if receipt.Error != nil {
				fmt.Printf("Error getting receipt: %d %s\n", receipt.Error.Code, receipt.Error.Message)
				return
			}
		}
	}
}
