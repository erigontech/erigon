package rpctest

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// Compares response of Erigon with OpenEthereum
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
func BenchTraceFilter(erigonURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) {
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
		if err != nil {
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
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
	prevBn := blockFrom
	for bn := blockFrom + 100; bn < blockTo; bn += 100 {
		// Checking modified accounts
		reqGen.reqID++
		var mag DebugModifiedAccounts
		res = reqGen.Erigon("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
		if res.Err != nil {
			fmt.Printf("Could not get modified accounts (Erigon): %v\n", res.Err)
			return
		}
		if mag.Error != nil {
			fmt.Printf("Error getting modified accounts (Erigon): %d %s\n", mag.Error.Code, mag.Error.Message)
			return
		}
		if res.Err == nil && mag.Error == nil {
			accountSet := extractAccountMap(&mag)
			accounts := make([]libcommon.Address, 0, len(accountSet))
			for account := range accountSet {
				accounts = append(accounts, account)
			}
			// Randomly select 100 accounts
			selects := 100
			if len(accounts) < 100 {
				selects = len(accounts)
			}
			for i := 0; i < selects; i++ {
				idx := i
				if len(accounts) > 100 {
					idx = int(rnd.Int31n(int32(len(accounts))))
				}
				account := accounts[idx]
				reqGen.reqID++
				request := reqGen.traceFilterFrom(prevBn, bn, account)
				errCtx := fmt.Sprintf("traceFilterFrom fromBlock %d, toBlock %d, fromAddress %x", prevBn, bn, account)
				if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
					fmt.Println(err)
					return
				}
				reqGen.reqID++
				request = reqGen.traceFilterTo(prevBn, bn, account)
				errCtx = fmt.Sprintf("traceFilterTo fromBlock %d, toBlock %d, fromAddress %x", prevBn, bn, account)
				if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
					fmt.Println(err)
					return
				}
			}
			/*
				if len(accounts) > 1 {
					from := accounts[0]
					to := accounts[1]
					reqGen.reqID++
					request := reqGen.traceFilterUnion(prevBn, bn, from, to)
					errCtx := fmt.Sprintf("traceFilterUnion fromBlock %d, toBlock %d, fromAddress %x, toAddress %x", prevBn, bn, from, to)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
						fmt.Println(err)
						return
					}
					reqGen.reqID++
					request = reqGen.traceFilterAfter(prevBn, bn, 1)
					errCtx = fmt.Sprintf("traceFilterAfter fromBlock %d, toBlock %d, after %x", prevBn, bn, 1)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
						fmt.Println(err)
						return
					}
					reqGen.reqID++
					request = reqGen.traceFilterCount(prevBn, bn, 1)
					errCtx = fmt.Sprintf("traceFilterCount fromBlock %d, toBlock %d, count %x", prevBn, bn, 1)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
						fmt.Println(err)
						return
					}
					reqGen.reqID++
					request = reqGen.traceFilterCountAfter(prevBn, bn, 1, 1)
					errCtx = fmt.Sprintf("traceFilterCountAfter fromBlock %d, toBlock %d, count %x, after %x", prevBn, bn, 1, 1)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
						fmt.Println(err)
						return
					}
				}
			*/
		}
		fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
		prevBn = bn
	}
}
