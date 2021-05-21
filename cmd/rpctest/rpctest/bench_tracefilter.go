package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/common"
)

// Compares response of TurboGeth with OpenEthereum
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call TurboGeth and doesn't compare responses
// 		use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Turbogeth
func BenchTraceFilter(tgURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
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
	prevBn := blockFrom
	for bn := blockFrom + 100; bn < blockTo; bn += 100 {
		// Checking modified accounts
		reqGen.reqID++
		var mag DebugModifiedAccounts
		res = reqGen.TurboGeth("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
		if res.Err != nil {
			fmt.Printf("Could not get modified accounts (turbo-geth): %v\n", res.Err)
			return
		}
		if mag.Error != nil {
			fmt.Printf("Error getting modified accounts (turbo-geth): %d %s\n", mag.Error.Code, mag.Error.Message)
			return
		}
		if res.Err == nil && mag.Error == nil {
			accountSet := extractAccountMap(&mag)
			for account := range accountSet {
				recording := rec != nil // This flag will be set to false if recording is not to be performed
				reqGen.reqID++
				request := reqGen.traceFilterFrom(prevBn, bn, account)
				res = reqGen.TurboGeth2("trace_filter", request)
				if res.Err != nil {
					fmt.Printf("Could not trace filter from (turbo-geth) %d: %v\n", bn, res.Err)
					return
				}
				if errVal := res.Result.Get("error"); errVal != nil {
					fmt.Printf("Error tracing filter from (turbo-geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
					return
				}
				if needCompare {
					resg := reqGen.Geth2("trace_filter", request)
					if resg.Err != nil {
						fmt.Printf("Could not trace filter from (OE) %d: %v\n", bn, resg.Err)
						return
					}
					if errVal := resg.Result.Get("error"); errVal != nil {
						fmt.Printf("Error tracing filter from (OE): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
						return
					}
					if resg.Err == nil && resg.Result.Get("error") == nil {
						if err := compareResults(res.Result, resg.Result); err != nil {
							fmt.Printf("Different traces fromBlock %d, toBlock %d, fromAddress %x: %v\n", prevBn, bn, account, err)
							fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
							fmt.Printf("\n\nOE response=================================\n%s\n", resg.Response)
							return
						}
					}
				}
				if recording {
					fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
				}
				reqGen.reqID++
				request = reqGen.traceFilterTo(prevBn, bn, account)
				res = reqGen.TurboGeth2("trace_filter", request)
				if res.Err != nil {
					fmt.Printf("Could not trace filter to (turbo-geth) %d: %v\n", bn, res.Err)
					return
				}
				if errVal := res.Result.Get("error"); errVal != nil {
					fmt.Printf("Error tracing filter to (turbo-geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
					return
				}
				if needCompare {
					resg := reqGen.Geth2("trace_filter", request)
					if resg.Err != nil {
						fmt.Printf("Could not trace filter from (OE) %d: %v\n", bn, resg.Err)
						return
					}
					if errVal := resg.Result.Get("error"); errVal != nil {
						fmt.Printf("Error tracing filter from (OE): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
						return
					}
					if resg.Err == nil && resg.Result.Get("error") == nil {
						if err := compareResults(res.Result, resg.Result); err != nil {
							fmt.Printf("Different traces fromBlock %d, toBlock %d, toAddress %x: %v\n", prevBn, bn, account, err)
							fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
							fmt.Printf("\n\nOE response=================================\n%s\n", resg.Response)
							return
						}
					}
				}
				if recording {
					fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
				}
			}
		}
		fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
		prevBn = bn
	}
}
