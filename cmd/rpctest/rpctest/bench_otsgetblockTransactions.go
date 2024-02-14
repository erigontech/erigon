package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

// BenchOtsGetBlockTransactions compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
func BenchOtsGetBlockTransactions(erigonURL, gethURL string, needCompare, visitAllPages bool, latest bool, blockFrom, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
	var errs *bufio.Writer
	var resultsCh chan CallResult = nil

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
		go vegetaWrite(true, []string{"ots_getBlockTransactions"}, resultsCh)
	}

	var res CallResult

	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++

	for bn := blockFrom; bn <= blockTo; bn++ {

		var pageCount uint64 = 0
		pageEnded := false

		for !pageEnded {
			reqGen.reqID++
			var b OtsBlockTransactions
			res = reqGen.Erigon("ots_getBlockTransactions", reqGen.otsGetBlockTransactions(bn, pageCount, 10), &b)

			if len(b.Result.FullBlock.Transactions) < 1 || !visitAllPages {
				pageEnded = true
			}

			if !needCompare {
				resultsCh <- res
			}

			if res.Err != nil {
				return fmt.Errorf("Could not retrieve transactions of block (Erigon) %d: %v\n", bn, res.Err)
			}

			if b.Error != nil {
				return fmt.Errorf("Error retrieving transactions of block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
			}

			if needCompare {
				var bg OtsBlockTransactions
				res = reqGen.Geth("ots_getBlockTransactions", reqGen.otsGetBlockTransactions(bn, pageCount, 10), &bg)
				if res.Err != nil {
					return fmt.Errorf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
				}
				if bg.Error != nil {
					return fmt.Errorf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
				}
				if !compareBlockTransactions(&b, &bg) {
					if rec != nil {
						fmt.Fprintf(rec, "Block difference for block=%d\n", bn)
						rec.Flush()
						continue
					} else {
						return fmt.Errorf("block %d has different fields\n", bn)
					}
				}
			}
			pageCount++
		}

	}

	return nil
}
