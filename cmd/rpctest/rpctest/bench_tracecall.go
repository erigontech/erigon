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
func BenchTraceCall(erigonURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonURL, oeURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	var rec *bufio.Writer
	if recordFile != "" {
		f, err := os.Create(recordFile)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for recording: %v\n", recordFile, err)
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
	}
	var errs *bufio.Writer
	if errorFile != "" {
		ferr, err := os.Create(errorFile)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for error output: %v\n", errorFile, err)
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
			request := reqGen.traceCall(tx.From, tx.To, &tx.Gas, &tx.GasPrice, &tx.Value, tx.Input, bn-1)
			errCtx := fmt.Sprintf("block %d, tx %s", bn, tx.Hash)
			if err := requestAndCompare(request, "trace_call", errCtx, reqGen, needCompare, rec, errs, nil /* insertOnlyIfSuccess */, false); err != nil {
				return err
			}
		}
	}
	return nil
}
