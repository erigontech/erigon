package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

func BenchDebugTraceBlockByNumber(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonUrl, gethUrl)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
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

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_traceBlockByNumber"}, resultsCh)
	}

	reqGen := &RequestGenerator{
		client: client,
	}

	for bn := blockFrom; bn < blockTo; bn++ {
		reqGen.reqID++
		request := reqGen.debugTraceBlockByNumber(bn)
		errCtx := fmt.Sprintf("block %d", bn)
		if err := requestAndCompare(request, "debug_traceBlockByNumber", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
			return err
		}
	}
	return nil
}
