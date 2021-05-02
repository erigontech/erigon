package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
)

// Transactions on which OpenEthereum reports incorrect traces
var wrongTxs = []string{
	//"0xe47180a05a7cc25c187b426fed5390365874add72a5681242ac4b288d4a6833a", // Block 7000000
	//"0x76e720b0530aa72926319853f62c97c5907b26e2fc5c8ad5f51173f531d98d11", // Block 7000063
	//"0xc6f3cadc90aece146a7a90191d6668c7f152a2daf759ae97bde7df7f5d78ab3a", // Block 7000068
	"0xd5a9b32b262202cda422dd5a2ccf8d7d56e9b3425ba7d350548e62a5bd26b481", // Block 8000011
	"0x1953ad3591fa0f6f3f00dfa0f93a57e1dc7fa003e2192a18c64c71847cf64e0c", // Block 8000035
	"0xfbd66bcbc4cb374946f350ca6835571b09f68c5f635ff9fc533c3fa2ac0d19cb", // Block 9000004
	"0x928b01dd36bcf142bf0d4b1e75239bec8ee68a68aa3739e4f9a1b4a17785651b", // Block 9000010
	"0x45b60cfbcad50b24b313a40644061f36e04b4baf516a9db1a8a386863eed6070", // Block 9000023
	"0x9d2cb4ad7851bd745a952d9e0d42e1c3d6ee1d37ce37eb05863bdce82016078b", // Block 9000027
	"0xcee0adc637910d9baa3c60e186ac0c270af89a836a5277846926b6913d5cee65", // Block 9500001
}

// bench1 compares response of TurboGeth with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call TurboGeth and doesn't compare responses
// 		use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Turbogeth
func Bench11(tgURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(tgURL, oeURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	var rec *bufio.Writer
	if recordFile != "" {
		f, err := os.Create(recordFile)
		if err != nil {
			fmt.Printf("Cannot create file %s for recording: %v\n", recordFile, err)
			return
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
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
			if _, skip := skipTxs[common.HexToHash(tx.Hash)]; skip {
				continue
			}
			recording := rec != nil // This flag will be set to false if recording is not to be performed
			reqGen.reqID++
			request := reqGen.traceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1)
			res = reqGen.TurboGeth2("trace_call", request)
			if res.Err != nil {
				fmt.Printf("Could not trace call (turbo-geth) %s: %v\n", tx.Hash, res.Err)
				return
			}
			if errVal := res.Result.Get("error"); errVal != nil {
				fmt.Printf("Error tracing call (turbo-geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
				return
			}
			if needCompare {
				resg := reqGen.Geth2("trace_call", request)
				if resg.Err != nil {
					fmt.Printf("Could not trace call (oe) %s: %v\n", tx.Hash, resg.Err)
					return
				}
				if errVal := resg.Result.Get("error"); errVal != nil {
					fmt.Printf("Error tracing call (oe): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
					return
				}
				if resg.Err == nil && resg.Result.Get("error") == nil {
					if err := compareResults(res.Result, resg.Result); err != nil {
						fmt.Printf("Different traces block %d, tx %s: %v\n", bn, tx.Hash, err)
						fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
						fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
						return
					}
				}
			}
			if recording {
				fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
			}
		}
	}
}
