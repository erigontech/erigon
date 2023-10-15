package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// BenchEthGetTransactionByHash compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth or infura
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//
// recordFile stores all eth_GetTransactionByHash returned with success
//
//	errorFile stores information when erigon and geth doesn't return same data
func BenchEthGetTransactionByHash(erigonURL, gethURL string, needCompare bool, blockFrom, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
	var errs *bufio.Writer
	var resultsCh chan CallResult = nil
	var nTransactions = 0

	if errorFileName != "" {
		f, err := os.Create(errorFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for errorFile: %v\n", errorFileName, err)
		}
		defer f.Close()
		errs = bufio.NewWriter(f)
		defer errs.Flush()
	}

	if recordFileName != "" {
		frec, errRec := os.Create(recordFileName)
		if errRec != nil {
			return fmt.Errorf("Cannot create file %s for errorFile: %v\n", recordFileName, errRec)
		}
		defer frec.Close()
		rec = bufio.NewWriter(frec)
		defer rec.Flush()
	}

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getTransactionByHash"}, resultsCh)
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
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}

		if b.Error != nil {
			return fmt.Errorf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
			if res.Err != nil {
				return fmt.Errorf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
			}
			if bg.Error != nil {
				return fmt.Errorf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
			}
			if !compareBlocks(&b, &bg) {
				if rec != nil {
					fmt.Fprintf(rec, "Block difference for block=%d\n", bn)
					rec.Flush()
					continue
				} else {
					return fmt.Errorf("Block one or more fields areis different for block %d\n", bn)
				}
			}
		}

		for _, tx := range b.Result.Transactions {

			reqGen.reqID++
			nTransactions = nTransactions + 1

			var request string
			request = reqGen.getTransactionByHash(tx.Hash)
			errCtx := fmt.Sprintf(" bn=%d hash=%s", bn, tx.Hash)

			if err := requestAndCompare(request, "eth_getTransactionByHash", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
				return err
			}
		}

		fmt.Println("\nProcessed Transactions: ", nTransactions)
	}
	return nil
}
