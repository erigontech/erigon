package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// benchTxReceipt compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
func BenchTxReceipt(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
	var resultsCh chan CallResult = nil

	if recordFileName != "" {
		f, err := os.Create(recordFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for recording: %v\n", recordFileName, err)
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
	}
	var errs *bufio.Writer
	if errorFileName != "" {
		ferr, err := os.Create(errorFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for error output: %v\n", errorFileName, err)
		}
		defer ferr.Close()
		errs = bufio.NewWriter(ferr)
		defer errs.Flush()
	}

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getTransactionReceipt"}, resultsCh)
	}

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
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

		for _, tx := range b.Result.Transactions {
			reqGen.reqID++
			request := reqGen.getTransactionReceipt(tx.Hash)
			errCtx := fmt.Sprintf("block %d, tx %s", bn, tx.Hash)
			if err := requestAndCompare(request, "eth_getTransactionReceipt", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
				return err
			}
		}
	}
	return nil
}
