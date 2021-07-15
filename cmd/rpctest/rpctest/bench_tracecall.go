package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// bench1 compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
// 		use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
func BenchTraceCall(erigonURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) {
	setRoutes(erigonURL, oeURL)
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
	var errs *bufio.Writer
	if errorFile != "" {
		ferr, err := os.Create(errorFile)
		if ferr != nil {
			fmt.Printf("Cannot create file %s for error output: %v\n", errorFile, err)
			return
		}
		defer ferr.Close()
		errs = bufio.NewWriter(ferr)
		defer errs.Flush()
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
	for bn := blockFrom; bn <= blockTo; bn++ {
		reqGen.reqID++
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn), &b)
		if res.Err != nil {
			fmt.Printf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
			return
		}

		if b.Error != nil {
			fmt.Printf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
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
			recording := rec != nil // This flag will be set to false if recording is not to be performed
			reqGen.reqID++
			request := reqGen.traceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1)
			res = reqGen.Erigon2("trace_call", request)
			if res.Err != nil {
				fmt.Printf("Could not trace call (Erigon) %s: %v\n", tx.Hash, res.Err)
				return
			}
			if errVal := res.Result.Get("error"); errVal != nil {
				fmt.Printf("Error tracing call (Erigon): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
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
					recording = false
					if err := compareResults(res.Result, resg.Result); err != nil {
						fmt.Printf("Different traces block %d, tx %s: %v\n", bn, tx.Hash, err)
						if errs != nil {
							fmt.Fprintf(errs, "Different traces block %d, tx %s: %v\n", bn, tx.Hash, err)
							fmt.Fprintf(errs, "\n\nTG response=================================\n%s\n", res.Response)
							fmt.Fprintf(errs, "\n\nG response=================================\n%s\n", resg.Response)
							errs.Flush() // nolint:errcheck
							// Keep going
						} else {
							fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
							fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
							return
						}
					}
				}
			}
			if recording {
				fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
			}
		}
	}
}
