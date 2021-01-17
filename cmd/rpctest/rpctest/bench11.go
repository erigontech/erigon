package rpctest

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
)

// Transactions on which OpenEthereum reports incorrect traces
var wrongTxs = []string{
	"0xfbd66bcbc4cb374946f350ca6835571b09f68c5f635ff9fc533c3fa2ac0d19cb", // Block 9000004
	"0x928b01dd36bcf142bf0d4b1e75239bec8ee68a68aa3739e4f9a1b4a17785651b", // Block 9000010
	"0x45b60cfbcad50b24b313a40644061f36e04b4baf516a9db1a8a386863eed6070", // Block 9000023
	"0x9d2cb4ad7851bd745a952d9e0d42e1c3d6ee1d37ce37eb05863bdce82016078b", // Block 9000027
}

// bench1 compares response of TurboGeth with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call TurboGeth and doesn't compare responses
// 		use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Turbogeth
func Bench11(tgURL, oeURL string, needCompare bool, blockNum uint64) {
	setRoutes(tgURL, oeURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	skipTxs := make(map[common.Hash]struct{})
	for _, txHash := range wrongTxs {
		skipTxs[common.HexToHash(txHash)] = struct{}{}
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
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)
	firstBn := int(blockNum)
	for bn := firstBn; bn <= int(lastBlock); bn++ {
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
			if _, skip := skipTxs[common.HexToHash(tx.Hash)]; skip {
				continue
			}
			reqGen.reqID++

			res = reqGen.TurboGeth2("trace_call", reqGen.traceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1))
			if res.Err != nil {
				fmt.Printf("Could not trace call (turbo-geth) %s: %v\n", tx.Hash, res.Err)
				return
			}
			if errVal := res.Result.Get("error"); errVal != nil {
				fmt.Printf("Error tracing call (turbo-geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
				return
			}
			if needCompare {
				resg := reqGen.Geth2("trace_call", reqGen.traceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1))
				if resg.Err != nil {
					fmt.Printf("Could not trace call (oe) %s: %v\n", tx.Hash, resg.Err)
					return
				}
				if errVal := resg.Result.Get("error"); errVal != nil {
					fmt.Printf("Error tracing call (oe): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
					return
				}
				if resg.Err == nil && resg.Result.Get("error") == nil {
					if err := compareTraceCalls(res.Result, resg.Result); err != nil {
						fmt.Printf("Different traces block %d, tx %s: %v\n", bn, tx.Hash, err)
						fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
						fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
						return
					}
				}
			}
		}
	}
}
