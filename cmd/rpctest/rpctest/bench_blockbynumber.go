package rpctest

import (
	"fmt"
	"net/http"
	"time"
)

// BenchEthGetBlockByNumber generates lots of requests for eth_getBlockByNumber to attempt to reproduce issue where empty results are being returned
func BenchEthGetBlockByNumber(erigonURL string) {
	setRoutes(erigonURL, erigonURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}
	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	for bn := uint64(0); bn <= uint64(blockNumber.Number); bn++ {
		reqGen.reqID++
		res = reqGen.Erigon2("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, false /* withTxs */))
		if res.Err != nil {
			fmt.Printf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
			return
		}
		if errVal := res.Result.Get("error"); errVal != nil {
			fmt.Printf("error: %d %s", errVal.GetInt("code"), errVal.GetStringBytes("message"))
			return
		}
		if res.Result.Get("hash") == nil {
			fmt.Printf("empty result\n")
			return
		}
	}
}
