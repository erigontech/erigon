package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// Compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with OpenEthereum
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
func BenchTraceBlock(erigonURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) {
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

		reqGen.reqID++
		request := reqGen.traceBlock(bn)
		errCtx := fmt.Sprintf("block %d", bn)
		if err := requestAndCompare(request, "trace_block", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
			fmt.Println(err)
			return
		}
	}
}
