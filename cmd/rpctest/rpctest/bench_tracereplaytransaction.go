package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

func BenchTraceReplayTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonUrl, gethUrl)
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

	for bn := blockFrom; bn < blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
		}
		if b.Error != nil {
			return fmt.Errorf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
		}
		for _, tx := range b.Result.Transactions {
			reqGen.reqID++
			request := reqGen.traceReplayTransaction(tx.Hash)
			errCtx := fmt.Sprintf("block %d, tx %s", bn, tx.Hash)
			if err := requestAndCompare(request, "trace_replayTransaction", errCtx, reqGen, needCompare, rec, errs, nil,
				/* insertOnlyIfSuccess */ false); err != nil {
				fmt.Println(err)
				return err
			}
		}
	}
	return nil
}
