package rpctest

import (
	"fmt"
	"net/http"
	"time"
)

// bench12 compares response of TurboGeth with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call TurboGeth and doesn't compare responses
// 		use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Turbogeth
func Bench12(tgURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(tgURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.TurboGeth("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	for bn := blockFrom; bn <= blockTo; bn++ {
		reqGen.reqID++
		var b EthBlockByNumber
		res = reqGen.TurboGeth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn), &b)
		if res.Err != nil {
			fmt.Printf("Could not retrieve block (turbo-geth) %d: %v\n", bn, res.Err)
			return
		}

		if b.Error != nil {
			fmt.Printf("Error retrieving block (turbo-geth): %d %s\n", b.Error.Code, b.Error.Message)
			return
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn), &bg)
			if res.Err != nil {
				fmt.Printf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
				return
			}
			if bg.Error != nil {
				fmt.Printf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
				return
			}
			if !compareBlocks(&b, &bg) {
				fmt.Printf("Block difference for %d\n", bn)
				return
			}
		}

		for _, tx := range b.Result.Transactions {
			reqGen.reqID++

			var trace EthTxTrace
			res = reqGen.TurboGeth("debug_traceCall", reqGen.debugTraceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1), &trace)
			if res.Err != nil {
				fmt.Printf("Could not debug traceCall (turbo-geth) %s: %v\n", tx.Hash, res.Err)
				return
			}

			if trace.Error != nil {
				fmt.Printf("Error tracing call (turbo-geth): %d %s\n", trace.Error.Code, trace.Error.Message)
				return
			}

			if needCompare {
				var traceg EthTxTrace
				res = reqGen.Geth("debug_traceCall", reqGen.debugTraceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1), &traceg)
				if res.Err != nil {
					fmt.Printf("Could not debug traceCall (geth) %s: %v\n", tx.Hash, res.Err)
				} else {
					if traceg.Error != nil {
						fmt.Printf("Error tracing call (geth): %d %s\n", traceg.Error.Code, traceg.Error.Message)
						return
					}
					if res.Err == nil && trace.Error == nil {
						if !compareTraces(&trace, &traceg) {
							fmt.Printf("Different traces block %d, tx %s\n", bn, tx.Hash)
							return
						}
					}
				}
			}
		}
	}
}
