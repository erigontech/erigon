package rpctest

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// BenchEthGetLogs compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth or infura
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//	recordFile stores all eth_getlogs returned with success
//	errorFile stores information when erigon and geth doesn't return same data
func BenchEthGetLogs(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) {
	setRoutes(erigonURL, gethURL)
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

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_getModifiedAccountsByNumber", "eth_getLogs"}, resultsCh)
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

	prevBn := blockFrom
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
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
			for account := range accountSet {
				reqGen.reqID++
				request := reqGen.getLogs(prevBn, bn, account)
				errCtx := fmt.Sprintf("account %x blocks %d-%d", account, prevBn, bn)
				if err := requestAndCompare(request, "eth_getLogs", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
					fmt.Println(err)
					return
				}
				topics := getTopics(res.Result)
				// All combination of account and one topic
				for _, topic := range topics {
					reqGen.reqID++
					request = reqGen.getLogs1(prevBn, bn+10000, account, topic)
					errCtx := fmt.Sprintf("account %x topic %x blocks %d-%d", account, topic, prevBn, bn)
					if err := requestAndCompare(request, "eth_getLogs", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
						fmt.Println(err)
						return
					}
				}
				// Random combinations of two topics
				if len(topics) >= 2 {
					idx1 := rnd.Int31n(int32(len(topics)))
					idx2 := rnd.Int31n(int32(len(topics) - 1))
					if idx2 >= idx1 {
						idx2++
					}
					reqGen.reqID++
					request = reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2])
					errCtx := fmt.Sprintf("account %x topic1 %x topic2 %x blocks %d-%d", account, topics[idx1], topics[idx2], prevBn, bn)
					if err := requestAndCompare(request, "eth_getLogs", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
						fmt.Println(err)
						return
					}
				}
			}
		}
		fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
		prevBn = bn
	}
}
