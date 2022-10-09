package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// BenchEthCall compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth or infura
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//			    false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//	                 recordFile stores all eth_call returned with success
//	                 errorFile stores information when erigon and geth doesn't return same data
func BenchEthCall(erigonURL, gethURL string, needCompare, latest bool, blockFrom, blockTo uint64, recordFile string, errorFile string) {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
	var errs *bufio.Writer
	var resultsCh chan CallResult = nil
	var nTransactions = 0

	if errorFile != "" {
		f, err := os.Create(errorFile)
		if err != nil {
			fmt.Printf("Cannot create file %s for errorFile: %v\n", errorFile, err)
			return
		}
		defer f.Close()
		errs = bufio.NewWriter(f)
		defer errs.Flush()
	}

	if recordFile != "" {
		frec, errRec := os.Create(recordFile)
		if errRec != nil {
			fmt.Printf("Cannot create file %s for errorFile: %v\n", recordFile, errRec)
			return
		}
		defer frec.Close()
		rec = bufio.NewWriter(frec)
		defer rec.Flush()
	}

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_call"}, resultsCh)
	}
	var res CallResult

	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++

	for bn := blockFrom; bn <= blockTo; bn++ {
		reqGen.reqID++
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
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
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
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
				if rec != nil {
					fmt.Fprintf(rec, "Block difference for block=%d\n", bn)
					rec.Flush()
					continue
				} else {
					return
				}
			}
		}

		for _, tx := range b.Result.Transactions {

			reqGen.reqID++
			nTransactions = nTransactions + 1

			var request string
			if latest {
				request = reqGen.ethCallLatest(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input)
			} else {
				request = reqGen.ethCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1)
			}
			errCtx := fmt.Sprintf(" bn=%d hash=%s", bn, tx.Hash)

			if err := requestAndCompare(request, "eth_call", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
				fmt.Println(err)
				return
			}
		}

		fmt.Println("\nProcessed Transactions: ", nTransactions)
	}
}
