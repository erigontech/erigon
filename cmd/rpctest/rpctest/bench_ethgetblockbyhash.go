package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// BenchEthGetBlockByHash compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth or infura
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//			    false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//	                 recordFileName stores all eth_call returned wite success
//	                 errorFileName stores information when erigon and geth doesn't return same data
func BenchEthGetBlockByHash(erigonURL, gethURL string, needCompare, latest bool, blockFrom, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
	var errs *bufio.Writer
	var resultsCh chan CallResult = nil
	var nBlocks = 0

	if errorFileName != "" {
		f, err := os.Create(errorFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for errorFileName: %v\n", errorFileName, err)
		}
		defer f.Sync()
		defer f.Close()
		errs = bufio.NewWriter(f)
		defer errs.Flush()
	}

	if recordFileName != "" {
		frec, err := os.Create(recordFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for errorFile: %v\n", recordFileName, err)
		}
		defer frec.Close()
		rec = bufio.NewWriter(frec)
		defer rec.Flush()
	}

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getBlockByHash"}, resultsCh)
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
					return fmt.Errorf("block %d has different fields\n", bn)
				}
			}
		}

		reqGen.reqID++
		nBlocks++
		var request string
		request = reqGen.getBlockByHash(b.Result.Hash, true)
		errCtx := fmt.Sprintf(" bn=%d hash=%s", bn, b.Result.Hash)

		if err := requestAndCompare(request, "eth_getBlockByHash", errCtx, reqGen, needCompare, rec, errs, resultsCh); err != nil {
			return err
		}

		fmt.Println("\nProcessed Blocks: ", nBlocks)
	}
	return nil
}
