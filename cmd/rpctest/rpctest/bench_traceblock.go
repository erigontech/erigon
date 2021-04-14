package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
)

// Compares response of TurboGeth with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call TurboGeth and doesn't compare responses
// 		use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Turbogeth
func BenchTraceBlock(tgURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(tgURL, gethURL)
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

		recording := rec != nil // This flag will be set to false if recording is not to be performed
		reqGen.reqID++
		request := reqGen.traceBlockByNumber(bn)
		res = reqGen.TurboGeth2("trace_BlockByNumber", request)
		if res.Err != nil {
			fmt.Printf("Could not trace block (turbo-geth) %d: %v\n", bn, res.Err)
			return
		}
		if errVal := res.Result.Get("error"); errVal != nil {
			fmt.Printf("Error tracing block (turbo-geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
			return
		}
		if needCompare {
			resg := reqGen.Geth2("debug_traceBlockByNumber", reqGen.debugTraceBlockByNumber(bn))
			if resg.Err != nil {
				fmt.Printf("Could not trace block (geth) %d: %v\n", bn, resg.Err)
				return
			}
			if errVal := resg.Result.Get("error"); errVal != nil {
				fmt.Printf("Error tracing call (geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
				return
			}
			if resg.Err == nil && resg.Result.Get("error") == nil {
				if err := compareResults(res.Result, resg.Result); err != nil {
					fmt.Printf("Different traces block %d, block %d: %v\n", bn, bn, err)
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
