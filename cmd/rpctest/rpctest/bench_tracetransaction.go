package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

func BenchTraceBlockByHash(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) {
	setRoutes(erigonUrl, gethUrl)
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

	for bn := blockFrom; bn < blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			fmt.Printf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
			return
		}
		if b.Error != nil {
			fmt.Printf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
			return
		}
		reqGen.reqID++
		request := reqGen.traceBlockByHash(b.Result.Hash.Hex())
		errCtx := fmt.Sprintf("block %d, tx %s", bn, b.Result.Hash.Hex())
		if err := requestAndCompare(request, "debug_traceBlockByHash", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
			fmt.Println(err)
			return
		}
	}
}

func BenchTraceTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) {
	setRoutes(erigonUrl, gethUrl)
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

	for bn := blockFrom; bn < blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			fmt.Printf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
			return
		}
		if b.Error != nil {
			fmt.Printf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
			return
		}
		for _, tx := range b.Result.Transactions {
			reqGen.reqID++
			request := reqGen.traceTransaction(tx.Hash)
			errCtx := fmt.Sprintf("block %d, tx %s", bn, tx.Hash)
			if err := requestAndCompare(request, "debug_traceTransaction", errCtx, reqGen, needCompare, rec, errs, nil); err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}
